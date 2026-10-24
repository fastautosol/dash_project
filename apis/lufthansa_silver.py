# 2026.10.24  18.00
import os
import duckdb

def run_silver_transformation():

    conn = duckdb.connect()

    try:
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute("ATTACH 'dbname=n8n user=sql_admin password=sql_pass host=postgresql port=5432' AS pg (TYPE POSTGRES);")   
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;") # Pontosvessző kitéve

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

                -- Departure actual timestamp (DuckDB-Postgres kompatibilis TIMESTAMP-re alakítás)
                CASE
                    WHEN departure__actual__date IS NOT NULL AND departure__actual__time IS NOT NULL
                    THEN (departure__actual__date::TEXT || ' ' || departure__actual__time::TEXT)::TIMESTAMP
                    ELSE NULL
                END AS dep_act_ts,

                -- Departure scheduled timestamp
                CASE
                    WHEN departure__scheduled__date IS NOT NULL AND departure__scheduled__time IS NOT NULL
                    THEN (departure__scheduled__date::TEXT || ' ' || departure__scheduled__time::TEXT)::TIMESTAMP
                    ELSE NULL
                END AS dep_sch_ts,

                -- Arrival
                arrival__airport_code AS arr_airport_code,
                arrival__status__code AS arr_status_code,
                arrival__terminal__name AS arr_terminal,
                arrival__terminal__gate AS arr_gate,

                -- Arrival actual timestamp
                CASE
                    WHEN arrival__actual__date IS NOT NULL AND arrival__actual__time IS NOT NULL
                    THEN (arrival__actual__date::TEXT || ' ' || arrival__actual__time::TEXT)::TIMESTAMP
                    ELSE NULL
                END AS arr_act_ts,

                -- Arrival scheduled timestamp
                CASE
                    WHEN arrival__scheduled__date IS NOT NULL AND arrival__scheduled__time IS NOT NULL
                    THEN (arrival__scheduled__date::TEXT || ' ' || arrival__scheduled__time::TEXT)::TIMESTAMP
                    ELSE NULL
                END AS arr_sch_ts,

                -- DLT metadata
                _ingested_at,
                _dlt_load_id,
                _dlt_id

            FROM pg.bronze.lh_flights

            WHERE route_key IS NOT NULL
              AND flight_key IS NOT NULL
              AND departure__scheduled__date IS NOT NULL;
        """)

        print("Silver réteg sikeresen frissítve a Postgres-ben!")

    except Exception as exc:
        print(f"Silver Table error: {exc}")
        raise

    finally:
        conn.close()

