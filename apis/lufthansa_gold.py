import duckdb
from sqlalchemy import create_engine, text

# Az indexeléshez szükségünk lesz a Postgres engine-re
DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
sql_engine = create_engine(DB_CONFIG)

def run_gold_transformation():
    conn = duckdb.connect()
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute("ATTACH 'dbname=n8n user=sql_admin password=sql_pass host=postgresql port=5432' AS pg (TYPE POSTGRES);")
    conn.execute("CREATE SCHEMA IF NOT EXISTS pg.gold;")
    
    # ============================================================
    # 1. gold.fact_flights (FIZIKAI TÁBLA)
    # ============================================================
    # Megjegyzés: A korábbi kódodban a silver.fact_lh_flights-ot hoztuk létre, azt használjuk alapként!
    conn.execute("""
        CREATE OR REPLACE TABLE pg.gold.fact_lh_flights AS
        SELECT flight_key, route_key, dep_airport_code, arr_airport_code, equipment__aircraft_code, dep_sch_ts::DATE AS flight_date_id,  -- dim_date-hez
            
            -- DuckDB-ben a date_diff('minute', start, end) sokkal tisztább, mint az EPOCH-os osztás!
            COALESCE(date_diff('minute', dep_sch_ts, dep_act_ts), 0) AS dep_delay_min,
            COALESCE(date_diff('minute', arr_sch_ts, arr_act_ts), 0) AS arr_delay_min,
            
            CASE WHEN status__code = 'LD' THEN 1 ELSE 0 END AS is_landed,
            CASE WHEN status__code = 'ON' THEN 1 ELSE 0 END AS is_ontime,
            CASE WHEN status__code = 'DL' THEN 1 ELSE 0 END AS is_delayed,
            
            dep_sch_ts, dep_act_ts, arr_sch_ts, arr_act_ts, _ingested_at
        FROM pg.silver.fact_lh_flights
        WHERE dep_sch_ts IS NOT NULL AND arr_sch_ts IS NOT NULL;
    """)

    # ============================================================
    # 2. gold.dim_airports (FIZIKAI TÁBLA)
    # ============================================================
    conn.execute("""
        CREATE OR REPLACE TABLE pg.gold.dim_lh_airports AS
        WITH all_airports AS (
            SELECT dep_airport_code AS airport_code FROM pg.silver.fact_lh_flights WHERE dep_airport_code IS NOT NULL
            UNION
            SELECT arr_airport_code AS airport_code FROM pg.silver.fact_lh_flights WHERE arr_airport_code IS NOT NULL
        )
        SELECT DISTINCT 
            airport_code,
            CAST(NULL AS VARCHAR) AS airport_name,
            CAST(NULL AS VARCHAR) AS city,
            CAST(NULL AS VARCHAR) AS country
        FROM all_airports;
    """)

    # ============================================================
    # 3. gold.dim_aircraft (FIZIKAI TÁBLA)
    # ============================================================
    conn.execute("""
        CREATE OR REPLACE TABLE pg.gold.dim_lh_aircraft AS
        SELECT DISTINCT
            equipment__aircraft_code AS aircraft_code,
            CASE 
                WHEN equipment__aircraft_code LIKE '32%' THEN 'Airbus A320 Family'
                WHEN equipment__aircraft_code LIKE '35%' THEN 'Airbus A350'
                WHEN equipment__aircraft_code LIKE '38%' THEN 'Airbus A380'
                WHEN equipment__aircraft_code LIKE '74%' THEN 'Boeing 747'
                WHEN equipment__aircraft_code LIKE '78%' THEN 'Boeing 787'
                ELSE 'Other'
            END AS aircraft_family,
            operating_carrier__airline_id AS carrier_code
        FROM pg.silver.fact_lh_flights
        WHERE equipment__aircraft_code IS NOT NULL;
    """)

    # ============================================================
    # 4. gold.dim_date (Dátum dimenzió generálás DuckDB függvényekkel)
    # ============================================================
    # A DuckDB a range() függvénnyel generál sorozatokat, a dayname() és monthname() beépített!
    conn.execute("""
        CREATE OR REPLACE TABLE pg.gold.dim_lh_date AS
        WITH bounds AS (
            SELECT MIN(dep_sch_ts::DATE) AS min_date, MAX(dep_sch_ts::DATE) + INTERVAL '1 year' AS max_date
            FROM pg.silver.fact_lh_flights
        ),
        date_range AS (
            SELECT CAST(generate_series(min_date, max_date, INTERVAL '1 day') AS DATE) AS date_val
            FROM bounds
        )
        SELECT date_val AS date_id, EXTRACT(YEAR FROM date_val) AS year, EXTRACT(MONTH FROM date_val) AS month, EXTRACT(DAY FROM date_val) AS day,
            EXTRACT(isodow FROM date_val) AS day_of_week, dayname(date_val) AS day_name, monthname(date_val) AS month_name,
            CASE 
                WHEN EXTRACT(isodow FROM date_val) IN (6, 7) THEN TRUE 
                ELSE FALSE 
            END AS is_weekend
        FROM date_range;
    """)
    print("Gold réteg sikeresen frissítve a Postgres-ben!")
  
    conn.close() # A DuckDB-t lezárjuk, hogy elengedje a Postgres táblákat

    # 2. POSTGRES INDEXELÉS (Közvetlenül a Gold frissítése után!)
    with sql_engine.connect() as pg_conn:
        # Elsődleges kulcsok (A korábbi indexeket le kell dobni, vagy cserélni, mert a CREATE OR REPLACE TABLE törli a constraint-eket)
        pg_conn.execute(text("ALTER TABLE gold.dim_lh_date ADD PRIMARY KEY (date_id);"))
        pg_conn.execute(text("ALTER TABLE gold.dim_lh_airports ADD PRIMARY KEY (airport_code);"))
        pg_conn.execute(text("ALTER TABLE gold.dim_lh_aircraft ADD PRIMARY KEY (aircraft_code);"))
        
        # Idegen kulcs indexek a DirectQuery-hez
        pg_conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_date ON gold.fact_lh_flights (flight_date_id);"))
        pg_conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_dep_air ON gold.fact_lh_flights (dep_airport_code);"))
        pg_conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_arr_air ON gold.fact_lh_flights (arr_airport_code);"))
        pg_conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_aircraft ON gold.fact_lh_flights (equipment__aircraft_code);"))
        pg_conn.commit() # Psycopg-nál fontos a commit!
