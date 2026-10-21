import duckdb

def run_silver_transformation():
    conn = duckdb.connect()
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute("ATTACH 'dbname=n8n user=sql_admin password=sql_pass host=postgresql port=5432' AS pg (TYPE POSTGRES);")
    
    conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;")

    conn.execute("""
        CREATE OR REPLACE TABLE pg.silver.fact_lh_flights AS
        SELECT
            flight_key,
            route_key,
            status__code,
            status__description,
            equipment__aircraft_code,
            operating_carrier__airline_id,
            operating_carrier__flight_number,
            
            -- Departure
            departure__airport_code AS dep_airport_code,
            departure__status__code AS dep_status_code,
            departure__terminal__name AS dep_terminal,
            departure__terminal__gate AS dep_gate,
            
            -- DuckDB-ben a datetime + time egyszerűen egybeönthető, ha nem nullok
            CASE 
                WHEN departure__actual__date IS NOT NULL AND departure__actual__time IS NOT NULL 
                THEN (departure__actual__date::DATE + departure__actual__time::TIME)
                ELSE NULL 
            END AS dep_act_ts,
            
            CASE 
                WHEN departure__scheduled__date IS NOT NULL AND departure__scheduled__time IS NOT NULL 
                THEN (departure__scheduled__date::DATE + departure__scheduled__time::TIME)
                ELSE NULL 
            END AS dep_sch_ts,
            
            -- Arrival
            arrival__airport_code AS arr_airport_code,
            arrival__status__code AS arr_status_code,
            arrival__terminal__name AS arr_terminal,
            arrival__terminal__gate AS arr_gate,
            
            CASE 
                WHEN arrival__actual__date IS NOT NULL AND arrival__actual__time IS NOT NULL 
                THEN (arrival__actual__date::DATE + arrival__actual__time::TIME)
                ELSE NULL 
            END AS arr_act_ts,
            
            CASE 
                WHEN arrival__scheduled__date IS NOT NULL AND arrival__scheduled__time IS NOT NULL 
                THEN (arrival__scheduled__date::DATE + arrival__scheduled__time::TIME)
                ELSE NULL 
            END AS arr_sch_ts,
            
            _ingested_at,
            _dlt_load_id,
            _dlt_id
        FROM pg.bronze.lh_flights
        WHERE 
            route_key IS NOT NULL
            AND flight_key IS NOT NULL
            AND departure__scheduled__date IS NOT NULL;
    """)
    print("Silver transzformáció sikeresen lefutott a DuckDB segítségével!")

    conn.close()
