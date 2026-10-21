from sqlalchemy import create_engine, text

DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
sql_engine = create_engine(DB_CONFIG)

# A tábla és a PK a setup_tables.sql-ben van (egyszeri létrehozás).
SILVER_SQL = """
INSERT INTO silver.fact_lh_flights (
    flight_key, route_key, status__code, status__description, equipment__aircraft_code,
    operating_carrier__airline_id, operating_carrier__flight_number,
    dep_airport_code, dep_status_code, dep_terminal, dep_gate, dep_act_ts, dep_sch_ts,
    arr_airport_code, arr_status_code, arr_terminal, arr_gate, arr_act_ts, arr_sch_ts,
    _ingested_at, _dlt_load_id, _dlt_id
)
SELECT
    flight_key, route_key, status__code, status__description, equipment__aircraft_code,
    operating_carrier__airline_id, operating_carrier__flight_number,

    departure__airport_code, departure__status__code, departure__terminal__name, departure__terminal__gate,
    -- date + time = timestamp; ha bármelyik NULL, az eredmény is NULL
    (departure__actual__date::date    + departure__actual__time::time),
    (departure__scheduled__date::date + departure__scheduled__time::time),

    arrival__airport_code, arrival__status__code, arrival__terminal__name, arrival__terminal__gate,
    (arrival__actual__date::date      + arrival__actual__time::time),
    (arrival__scheduled__date::date   + arrival__scheduled__time::time),

    _ingested_at, _dlt_load_id, _dlt_id
FROM bronze.lh_flights
WHERE route_key IS NOT NULL
  AND flight_key IS NOT NULL
  AND departure__scheduled__date IS NOT NULL
"""


def run_silver_transformation():
    # Egy tranzakció: a Power BI vagy a régi, vagy az új állapotot látja, soha a félkészet
    with sql_engine.begin() as c:
        c.execute(text("TRUNCATE silver.fact_lh_flights"))
        c.execute(text(SILVER_SQL))
    print("Silver transzformáció sikeresen lefutott (Postgres SQL)!")
