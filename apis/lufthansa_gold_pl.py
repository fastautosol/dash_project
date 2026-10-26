import duckdb
import polars as pl
from datetime import timedelta
from sqlalchemy import create_engine, text
from apis.lufthansa_airports import AIRPORTS

DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
sql_engine = create_engine(DB_CONFIG)

def run_gold_transformation():
    # DuckDB in-memory kapcsolat a PostgreSQL I/O hídhoz
    conn = duckdb.connect()
    
    try:
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute("ATTACH 'dbname=n8n user=sql_admin password=sql_pass host=postgresql port=5432' AS pg (TYPE POSTGRES);")
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg.gold;")

        # =====================================================================
        # 0. ADATOK BEOLVASÁSA SILVERBŐL POLARS-BA (ZERO-COPY ARROW)
        # =====================================================================
        df_sil_flights = pl.from_arrow(conn.execute("SELECT * FROM pg.silver.fact_lh_flights").arrow())
        df_sil_schedule = pl.from_arrow(conn.execute("SELECT * FROM pg.silver.lh_schedule").arrow())

        # =====================================================================
        # 1. GOLD.FACT_LH_FLIGHTS TRANSZFORMÁCIÓ
        # =====================================================================
        # Csak azokat tartjuk meg, ahol a menetrendi idők nem üresek
        df_fact_flights_prep = df_sil_flights.filter(
            pl.col("dep_sch_ts").is_not_null() & pl.col("arr_sch_ts").is_not_null()
        )

        df_gold_flights = (
            df_fact_flights_prep
            .with_columns([
                pl.col("dep_sch_ts").dt.date().alias("flight_date_id"),
                
                # Késések kiszámítása percben (COALESCE megfelelője fill_null)
                ((pl.col("dep_act_ts") - pl.col("dep_sch_ts")).dt.total_minutes()).fill_null(0).alias("dep_delay_min"),
                ((pl.col("arr_act_ts") - pl.col("arr_sch_ts")).dt.total_minutes()).fill_null(0).alias("arr_delay_min"),
                
                # Landolt státusz jelző
                pl.when(pl.col("status__code") == "LD").then(1).otherwise(0).alias("is_landed")
            ])
            .with_columns([
                # Összesített késés
                (pl.col("dep_delay_min") + pl.col("arr_delay_min")).alias("total_delay_min"),
                
                # Pontossági és Késési KPI-ok (Nemzetközi turnus szabvány)
                pl.when((pl.col("dep_delay_min") <= 15) & (pl.col("arr_delay_min") <= 15)).then(1).otherwise(0).alias("is_ontime"),
                pl.when((pl.col("dep_delay_min") > 15) | (pl.col("arr_delay_min") > 15)).then(1).otherwise(0).alias("is_delayed")
            ])
        )

        # =====================================================================
        # 2. GOLD.DIM_LH_SCHEDULE TRANSZFORMÁCIÓ
        # =====================================================================
        df_gold_schedule = (
            df_sil_schedule
            .with_columns([
                pl.col("flight_key").alias("schedule_id"),
                pl.col("origin").alias("dep_airport_code"),
                pl.col("destination").alias("arr_airport_code"),
                pl.col("schedule_start_date").alias("flight_date_id"),
                
                # Kompozit üzleti kulcs összefűzése
                (
                    pl.col("airline") + "_" + 
                    pl.col("flight_number").cast(pl.Utf8) + "_" + 
                    pl.col("schedule_start_date").cast(pl.Utf8)
                ).alias("business_flight_key")
            ])
            .select([
                "schedule_id", "airline", "flight_number", "flight_suffix", "operating_carrier",
                "dep_airport_code", "arr_airport_code", "route_key", "sequence_number",
                "flight_date_id", "schedule_end_date", "days_of_operation", "aircraft_type",
                "aircraft_configuration", "registration", "service_type", "is_operating",
                "departure_time_utc", "arrival_time_utc", "departure_time_lt", "arrival_time_lt",
                "departure_terminal", "arrival_terminal", "marketing_flights", "booking_classes",
                "e_ticket_indicator", "business_flight_key", "_ingested_at"
            ])
        )

        # =====================================================================
        # 3. GOLD.DIM_LH_AIRPORTS TRANSZFORMÁCIÓ
        # =====================================================================
        # Összegyűjtjük az egyedi repülőtér kódokat az összes tény- és dimenziós oszlopból (UNION megfelelője)
        airport_codes = pl.concat([
            df_gold_flights.select(pl.col("dep_airport_code").alias("airport_code")),
            df_gold_flights.select(pl.col("arr_airport_code").alias("airport_code")),
            df_gold_schedule.select(pl.col("dep_airport_code").alias("airport_code")),
            df_gold_schedule.select(pl.col("arr_airport_code").alias("airport_code"))
        ]).filter(pl.col("airport_code").is_not_null()).unique()

        # Referencia szótár (AIRPORTS) beolvasása Polars DataFrame-be
        ref_airports_data = [
            {"airport_code": code, "airport_name": vals[0], "city": vals[1], "country": vals[2]}
            for code, vals in AIRPORTS.items()
        ]
        df_ref_airports = pl.DataFrame(ref_airports_data)

        # LEFT JOIN a referencia adatokkal
        df_gold_airports = airport_codes.join(df_ref_airports, on="airport_code", how="left")

        # =====================================================================
        # 4. GOLD.DIM_LH_AIRCRAFT TRANSZFORMÁCIÓ
        # =====================================================================
        df_gold_aircraft = (
            df_gold_flights
            .filter(pl.col("equipment__aircraft_code").is_not_null())
            .group_by("equipment__aircraft_code")
            .agg(pl.col("operating_carrier__airline_id").min().alias("carrier_code"))
            .with_columns([
                pl.col("equipment__aircraft_code").alias("aircraft_code"),
                pl.when(pl.col("equipment__aircraft_code").str.starts_with("32")).then(pl.lit("Airbus A320 Family"))
                .when(pl.col("equipment__aircraft_code").str.starts_with("35")).then(pl.lit("Airbus A350"))
                .when(pl.col("equipment__aircraft_code").str.starts_with("38")).then(pl.lit("Airbus A380"))
                .when(pl.col("equipment__aircraft_code").str.starts_with("74")).then(pl.lit("Boeing 747"))
                .when(pl.col("equipment__aircraft_code").str.starts_with("78")).then(pl.lit("Boeing 787"))
                .otherwise(pl.lit("Other"))
                .alias("aircraft_family")
            ])
            .select(["aircraft_code", "aircraft_family", "carrier_code"])
        )

        # =====================================================================
        # 5. GOLD.DIM_LH_DATE (DÁTUM DIMENZIÓ GENERÁLÁSA POLARS-BAN)
        # =====================================================================
        # Minimum és maximum dátumok lekérése a tényadatból
        min_date = df_gold_flights.select(pl.col("dep_sch_ts").dt.date().min()).item()
        max_date = df_gold_flights.select(pl.col("dep_sch_ts").dt.date().max()).item()
        
        if min_date and max_date:
            extended_max_date = max_date + timedelta(days=365) # + INTERVAL '1 year'
            
            # Polars idősor generálás (generate_series helyett datetime_range)
            df_gold_date = pl.datetime_range(
                start=min_date, 
                end=extended_max_date, 
                interval="1d", 
                eager=True
            ).to_frame().select([
                pl.col("datetime").dt.date().alias("date_id")
            ]).with_columns([
                pl.col("date_id").dt.year().alias("year"),
                pl.col("date_id").dt.month().alias("month"),
                pl.col("date_id").dt.day().alias("day"),
                pl.col("date_id").dt.weekday().alias("day_of_week"), # ISO szerint 1-7
                # Hónap és nap nevek előállítása formázással
                pl.col("date_id").dt.to_string("%A").alias("day_name"),
                pl.col("date_id").dt.to_string("%B").alias("month_name"),
                # Hétvége jelző (ISO szerint 6: Szombat, 7: Vasárnap)
                pl.col("date_id").dt.weekday().is_in([6, 7]).alias("is_weekend")
            ])
        else:
            # Fallback üres táblára, ha nincs tényadat
            df_gold_date = pl.DataFrame(schema={
                "date_id": pl.Date, "year": pl.Int32, "month": pl.Int32, "day": pl.Int32, 
                "day_of_week": pl.Int8, "day_name": pl.Utf8, "month_name": pl.Utf8, "is_weekend": pl.Boolean
            })

        # =====================================================================
        # 6. VISSZAÍRÁS POSTGRESQL GOLD SÉMÁBA (VILLÁMGYORSAN DUCKDB-VEL)
        # =====================================================================
        # A DuckDB a python scope-ból közvetlenül beolvassa a Polars DataFrame-eket
        conn.execute("CREATE OR REPLACE TABLE pg.gold.fact_lh_flights AS SELECT * FROM df_gold_flights")
        conn.execute("CREATE OR REPLACE TABLE pg.gold.dim_lh_schedule AS SELECT * FROM df_gold_schedule")
        conn.execute("CREATE OR REPLACE TABLE pg.gold.dim_lh_airports AS SELECT * FROM df_gold_airports")
        conn.execute("CREATE OR REPLACE TABLE pg.gold.dim_lh_aircraft AS SELECT * FROM df_gold_aircraft")
        conn.execute("CREATE OR REPLACE TABLE pg.gold.dim_lh_date AS SELECT * FROM df_gold_date")

        print("Gold réteg sikeresen frissítve a Postgres-ben Polars/DuckDB segítségével!")
        
    finally:
        conn.close()

    # =====================================================================
    # 7. POSTGRES INDEXELÉS ÉS MEGSZORÍTÁSOK (CONSTRAINTS) ÚJJÁÉPÍTÉSE
    # =====================================================================
    with sql_engine.begin() as pg_conn:
      pg_conn.execute(text("ALTER TABLE gold.dim_lh_date ADD PRIMARY KEY (date_id);"))
      pg_conn.execute(text("ALTER TABLE gold.dim_lh_airports ADD PRIMARY KEY (airport_code);"))
      pg_conn.execute(text("ALTER TABLE gold.dim_lh_aircraft ADD PRIMARY KEY (aircraft_code);"))
      pg_conn.execute(text("ALTER TABLE gold.dim_lh_schedule ADD PRIMARY KEY (schedule_id);"))
      # Relációs indexek a Power BI és csillagséma kapcsolatok gyorsítására
      pg_conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_date ON gold.fact_lh_flights (flight_date_id);"))
      pg_conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_dep_air ON gold.fact_lh_flights (dep_airport_code);"))
      pg_conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_arr_air ON gold.fact_lh_flights (arr_airport_code);"))
      pg_conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_aircraft ON gold.fact_lh_flights (equipment__aircraft_code);"))
      pg_conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sched_date ON gold.dim_lh_schedule (flight_date_id);"))
      pg_conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sched_route ON gold.dim_lh_schedule (route_key);"))
