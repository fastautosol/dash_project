from sqlalchemy import create_engine, text
from apis.lufthansa_airports import AIRPORTS

# Táblák, PK-k, indexek: setup_tables.sql (egyszeri létrehozás)

FACT_SQL = """
INSERT INTO gold.fact_lh_flights (
    flight_key, route_key, dep_airport_code, arr_airport_code, equipment__aircraft_code,
    flight_date_id, dep_delay_min, arr_delay_min, is_landed, is_ontime, is_delayed,
    dep_sch_ts, dep_act_ts, arr_sch_ts, arr_act_ts, _ingested_at
)
SELECT
    flight_key, route_key, dep_airport_code, arr_airport_code, equipment__aircraft_code,
    dep_sch_ts::date,
    COALESCE((EXTRACT(EPOCH FROM (dep_act_ts - dep_sch_ts)) / 60)::int, 0),
    COALESCE((EXTRACT(EPOCH FROM (arr_act_ts - arr_sch_ts)) / 60)::int, 0),
    CASE WHEN status__code = 'LD' THEN 1 ELSE 0 END,
    CASE WHEN status__code = 'ON' THEN 1 ELSE 0 END,
    CASE WHEN status__code = 'DL' THEN 1 ELSE 0 END,
    dep_sch_ts, dep_act_ts, arr_sch_ts, arr_act_ts, _ingested_at
FROM silver.fact_lh_flights
WHERE dep_sch_ts IS NOT NULL AND arr_sch_ts IS NOT NULL
"""

AIRPORT_UPSERT_SQL = """
INSERT INTO gold.dim_lh_airports (airport_code, airport_name, city, country)
VALUES (:code, :name, :city, :country)
ON CONFLICT (airport_code) DO UPDATE
SET airport_name = EXCLUDED.airport_name, city = EXCLUDED.city, country = EXCLUDED.country
"""

# Ami a forgalmi adatban szerepel, de nincs a statikus listában: üres névvel felvesszük
AIRPORT_MISSING_SQL = """
INSERT INTO gold.dim_lh_airports (airport_code)
SELECT airport_code FROM (
    SELECT dep_airport_code AS airport_code FROM silver.fact_lh_flights
    UNION
    SELECT arr_airport_code FROM silver.fact_lh_flights
) a
WHERE airport_code IS NOT NULL
ON CONFLICT (airport_code) DO NOTHING
"""

AIRCRAFT_SQL = """
INSERT INTO gold.dim_lh_aircraft (aircraft_code, aircraft_family, carrier_code)
SELECT
    equipment__aircraft_code,
    CASE
        WHEN equipment__aircraft_code LIKE '32%' THEN 'Airbus A320 Family'
        WHEN equipment__aircraft_code LIKE '35%' THEN 'Airbus A350'
        WHEN equipment__aircraft_code LIKE '38%' THEN 'Airbus A380'
        WHEN equipment__aircraft_code LIKE '74%' THEN 'Boeing 747'
        WHEN equipment__aircraft_code LIKE '78%' THEN 'Boeing 787'
        ELSE 'Other'
    END,
    MIN(operating_carrier__airline_id)
FROM silver.fact_lh_flights
WHERE equipment__aircraft_code IS NOT NULL
GROUP BY equipment__aircraft_code
ON CONFLICT (aircraft_code) DO UPDATE
SET aircraft_family = EXCLUDED.aircraft_family, carrier_code = EXCLUDED.carrier_code
"""

DATE_SQL = """
INSERT INTO gold.dim_lh_date (date_id, year, month, day, day_of_week, day_name, month_name, is_weekend)
SELECT
    d::date,
    EXTRACT(YEAR FROM d)::int,
    EXTRACT(MONTH FROM d)::int,
    EXTRACT(DAY FROM d)::int,
    EXTRACT(ISODOW FROM d)::int,
    trim(to_char(d, 'Day')),
    trim(to_char(d, 'Month')),
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM (
    SELECT generate_series(MIN(dep_sch_ts::date),
                           MAX(dep_sch_ts::date) + INTERVAL '1 year',
                           INTERVAL '1 day') AS d
    FROM silver.fact_lh_flights
) s
ON CONFLICT (date_id) DO NOTHING
"""


def run_gold_transformation():
    # Egy tranzakció: DirectQuery közben mindig konzisztens állapot látszik
    with sql_engine.begin() as c:
        # Fact: teljes újraépítés (az indexek és a PK megmaradnak)
        c.execute(text("TRUNCATE gold.fact_lh_flights"))
        c.execute(text(FACT_SQL))

        # Dimenziók: upsert, nem kell törölni őket
        c.execute(
            text(AIRPORT_UPSERT_SQL),
            [{"code": k, "name": v[0], "city": v[1], "country": v[2]} for k, v in AIRPORTS.items()],
        )
        c.execute(text(AIRPORT_MISSING_SQL))
        c.execute(text(AIRCRAFT_SQL))
        c.execute(text(DATE_SQL))
    print("Gold réteg sikeresen frissítve a Postgres-ben!")
