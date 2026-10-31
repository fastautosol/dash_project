# 2026.10.31 EIA silver: dim_respondent, dim_fueltype, demand_lmp_hourly (DuckDB -> Postgres, mint a lufthansa_silver)
import os
import logging
import duckdb

logger = logging.getLogger(__name__)

PG_CONN = os.getenv("PG_CONN", "dbname=n8n user=sql_admin password=sql_pass host=postgresql port=5432")

# --------------------------------------------------------------------------------------------------------------------
# Statikus torzsadatok. A respondent hivatalos nevet a bronze tablabol vesszuk (respondent_name), igy nem a memoriambol.
# tz: a respondent fo terhelesi kozpontjanak idozonaja (elemzesi celra, pl. helyi ora szerinti hotermap), nem EIA-hivatalos adat.
# (code, region, operator_type, iso_code, is_core, tz, datacenter_market)
# --------------------------------------------------------------------------------------------------------------------
RESPONDENTS = [
    ("CISO", "West",       "ISO/RTO", "CAISO",  True,  "America/Los_Angeles", "Bay Area, Los Angeles"),
    ("ERCO", "Texas",      "ISO/RTO", "ERCOT",  True,  "America/Chicago",     "Dallas, Austin, San Antonio"),
    ("PJM",  "Mid-Atlantic", "ISO/RTO", "PJM",  True,  "America/New_York",    "Northern Virginia, Ohio"),
    ("MISO", "Midwest",    "ISO/RTO", "MISO",   True,  "America/Chicago",     "Iowa, Minnesota"),
    ("SWPP", "Central",    "ISO/RTO", "SPP",    False, "America/Chicago",     "Oklahoma, Nebraska"),
    ("SOCO", "Southeast",  "Utility/BA", None,  False, "America/New_York",    "Atlanta"),
    ("DUK",  "Southeast",  "Utility/BA", None,  False, "America/New_York",    None),
    ("TVA",  "Southeast",  "Utility/BA", None,  False, "America/Chicago",     None),
    ("FPL",  "Southeast",  "Utility/BA", None,  False, "America/New_York",    None),
    ("FPC",  "Southeast",  "Utility/BA", None,  False, "America/New_York",    None),
    ("TEC",  "Southeast",  "Utility/BA", None,  False, "America/New_York",    None),
    ("NYIS", "Northeast",  "ISO/RTO", "NYISO",  False, "America/New_York",    None),
    ("ISNE", "Northeast",  "ISO/RTO", "ISONE",  False, "America/New_York",    None),
    ("AZPS", "Southwest",  "Utility/BA", None,  False, "America/Phoenix",     "Phoenix"),
    ("SRP",  "Southwest",  "Utility/BA", None,  False, "America/Phoenix",     "Phoenix"),
    ("TEPC", "Southwest",  "Utility/BA", None,  False, "America/Phoenix",     None),
    ("NEVP", "Southwest",  "Utility/BA", None,  False, "America/Los_Angeles", None),
    ("PGE",  "Northwest",  "Utility/BA", None,  False, "America/Los_Angeles", "Portland (Hillsboro)"),
    ("PACW", "Northwest",  "Utility/BA", None,  False, "America/Los_Angeles", None),
    ("PACE", "Northwest",  "Utility/BA", None,  False, "America/Denver",      None),
    ("BPAT", "Northwest",  "Utility/BA", None,  False, "America/Los_Angeles", None),
]

# EIA-930 fuel type kodok. A nevek sajat cimkek; ismeretlen kodot az ellenorzes jelez a logban.
# is_carbon_free: uzemi (nem eletciklus) szemlelet; tarolok es "Other" -> NULL (nem egyertelmu)
# (code, name, fuel_group, is_renewable, is_carbon_free, sort_order)
FUELTYPES = [
    ("NUC", "Nuclear",                "Nuclear",   False, True,  1),
    ("COL", "Coal",                   "Fossil",    False, False, 2),
    ("NG",  "Natural gas",            "Fossil",    False, False, 3),
    ("OIL", "Petroleum",              "Fossil",    False, False, 4),
    ("OTH", "Other",                  "Other",     None,  None,  5),
    ("WAT", "Hydro",                  "Renewable", True,  True,  6),
    ("GEO", "Geothermal",             "Renewable", True,  True,  7),
    ("WND", "Wind",                   "Renewable", True,  True,  8),
    ("SUN", "Solar",                  "Renewable", True,  True,  9),
    ("BAT", "Battery storage",        "Storage",   None,  None,  10),
    ("PS",  "Pumped storage",         "Storage",   None,  None,  11),
    ("UNK", "Unknown energy storage", "Storage",   None,  None,  12),
]

# referencia helyszin ISO-nkent: ennek az arat kapja a ref_lmp oszlop (az atlag mellett). Szabadon cserelheto.
REF_LOCATIONS = [ ("CAISO", "TH_NP15_GEN-APND"), ("NYISO", "N.Y.C.")]

def _has_column(conn, schema: str, table: str, column: str) -> bool:
    n = conn.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_catalog = 'pg' AND table_schema = ? AND table_name = ? AND column_name = ?",
        [schema, table, column],
    ).fetchone()[0]
    return n > 0


def _column_type(conn, schema: str, table: str, column: str) -> str:
    row = conn.execute(
        "SELECT data_type FROM information_schema.columns WHERE table_catalog = 'pg' AND table_schema = ? AND table_name = ? AND column_name = ?",
        [schema, table, column],).fetchone()
    return row[0].upper() if row else ""


def build_dim_respondent(conn):
    conn.execute("CREATE OR REPLACE TEMP TABLE _resp_static (respondent_code VARCHAR, region VARCHAR, operator_type VARCHAR, "
                 "iso_code VARCHAR, is_core BOOLEAN, tz VARCHAR, datacenter_market VARCHAR)")
    conn.executemany("INSERT INTO _resp_static VALUES (?, ?, ?, ?, ?, ?, ?)", RESPONDENTS)

    # hivatalos nev a bronze-bol (ha van respondent_name oszlop), kulonben a kod marad nev
    if _has_column(conn, "bronze", "eia_elec_hourly_demand", "respondent_name"):
        names_sql = ("SELECT respondent, any_value(respondent_name) AS respondent_name "
                     "FROM pg.bronze.eia_elec_hourly_demand GROUP BY respondent")
    else:
        logger.warning("bronze.eia_elec_hourly_demand: nincs respondent_name oszlop, a nev a kod lesz")
        names_sql = "SELECT CAST(NULL AS VARCHAR) AS respondent, CAST(NULL AS VARCHAR) AS respondent_name WHERE FALSE"

    conn.execute(f"""
        CREATE OR REPLACE TABLE pg.silver.dim_respondent AS
        SELECT
            s.respondent_code,
            COALESCE(n.respondent_name, s.respondent_code) AS respondent_name,
            s.region,
            s.operator_type,
            s.iso_code,
            s.is_core,
            s.tz,
            s.datacenter_market
        FROM _resp_static s
        LEFT JOIN ({names_sql}) n ON n.respondent = s.respondent_code
        ORDER BY s.respondent_code;
    """)


def build_dim_fueltype(conn):
    conn.execute("CREATE OR REPLACE TEMP TABLE _fuel_static (fueltype_code VARCHAR, fueltype_name VARCHAR, fuel_group VARCHAR, "
                 "is_renewable BOOLEAN, is_carbon_free BOOLEAN, sort_order INTEGER)")
    conn.executemany("INSERT INTO _fuel_static VALUES (?, ?, ?, ?, ?, ?)", FUELTYPES)
    conn.execute("CREATE OR REPLACE TABLE pg.silver.dim_fueltype AS SELECT * FROM _fuel_static ORDER BY sort_order;")


def build_demand_lmp_hourly(conn):
    conn.execute("CREATE OR REPLACE TEMP TABLE _ref_loc (iso VARCHAR, location VARCHAR)")
    conn.executemany("INSERT INTO _ref_loc VALUES (?, ?)", REF_LOCATIONS)

    # a ghg oszlop csak a frissitett betoltes utan letezik a bronze-ban
    ghg_expr = "AVG(ghg)" if _has_column(conn, "bronze", "iso_lmp_hourly", "ghg") else "CAST(NULL AS DOUBLE)"

    # a dlt a period oszlopot idobelyegkent (timestamptz) is felismerheti; szoveg eseten 'YYYY-MM-DDTHH' (UTC)
    if "TIMESTAMP" in _column_type(conn, "bronze", "eia_elec_hourly_demand", "period"):
        ts_expr = "CAST(period AS TIMESTAMPTZ)"
    else:
        ts_expr = "strptime(CAST(period AS VARCHAR), '%Y-%m-%dT%H') AT TIME ZONE 'UTC'"

    conn.execute(f"""
        CREATE OR REPLACE TABLE pg.silver.demand_lmp_hourly AS
        WITH demand AS (
            SELECT ({ts_expr}) AS ts,
                   respondent,
                   value AS demand_mwh
            FROM pg.bronze.eia_elec_hourly_demand
            WHERE "type" = 'D'
        ),
        lmp AS (
            -- egy sor / respondent / ora: a helyszinek (hubok, zonak) osszesitve
            SELECT CAST(interval_start_utc AS TIMESTAMPTZ) AS ts,
                   eia_respondent AS respondent,
                   any_value(iso) AS iso,
                   COUNT(*) AS n_locations,
                   AVG(lmp) AS lmp_avg,
                   MIN(lmp) AS lmp_min,
                   MAX(lmp) AS lmp_max,
                   AVG(energy) AS energy_avg,
                   AVG(congestion) AS congestion_avg,
                   AVG(loss) AS loss_avg,
                   {ghg_expr} AS ghg_avg
            FROM pg.bronze.iso_lmp_hourly
            GROUP BY 1, 2
        ),
        ref AS (
            SELECT CAST(b.interval_start_utc AS TIMESTAMPTZ) AS ts,
                   b.eia_respondent AS respondent,
                   b.location AS ref_location,
                   b.lmp AS ref_lmp
            FROM pg.bronze.iso_lmp_hourly b
            JOIN _ref_loc r ON b.iso = r.iso AND b.location = r.location
        ),
        joined AS (
            -- FULL JOIN: a day-ahead arak elobb vannak meg, mint a tenyleges fogyasztas (ilyenkor demand_mwh NULL),
            -- a LMP nelkuli respondentek (ERCO, PJM, ...) pedig csak fogyasztassal szerepelnek
            SELECT COALESCE(d.ts, l.ts) AS ts,
                   COALESCE(d.respondent, l.respondent) AS respondent,
                   d.demand_mwh,
                   l.iso, l.n_locations, l.lmp_avg, l.lmp_min, l.lmp_max,
                   l.energy_avg, l.congestion_avg, l.loss_avg, l.ghg_avg
            FROM demand d
            FULL OUTER JOIN lmp l ON d.ts = l.ts AND d.respondent = l.respondent
        ),
        enriched AS (
            SELECT j.*, ref.ref_location, ref.ref_lmp,
                   timezone(dr.tz, j.ts) AS local_ts
            FROM joined j
            LEFT JOIN ref ON ref.ts = j.ts AND ref.respondent = j.respondent
            LEFT JOIN pg.silver.dim_respondent dr ON dr.respondent_code = j.respondent
        )
        SELECT
            ts AS interval_start_utc,
            respondent,
            local_ts,
            CAST(local_ts AS DATE) AS local_date,
            CAST(EXTRACT(hour FROM local_ts) AS INTEGER) AS local_hour,
            CAST(isodow(local_ts) AS INTEGER) AS local_isodow,
            demand_mwh,
            iso,
            n_locations,
            lmp_avg, lmp_min, lmp_max,
            ref_location, ref_lmp,
            energy_avg, congestion_avg, loss_avg, ghg_avg
        FROM enriched
        ORDER BY respondent, interval_start_utc;
    """)


def _check_unmapped(conn):
    """Figyelmeztet, ha a bronze-ban van olyan respondent vagy fueltype kod, ami nincs a dimenziotablaban."""
    checks = [
        ("respondent", "bronze.eia_elec_hourly_demand", "respondent", "silver.dim_respondent", "respondent_code"),
        ("respondent", "bronze.eia_elec_daily_region", "respondent", "silver.dim_respondent", "respondent_code"),
        ("fueltype", "bronze.eia_elec_daily_fuel_type", "fueltype", "silver.dim_fueltype", "fueltype_code"),
    ]
    for label, src, src_col, dim, dim_col in checks:
        try:
            rows = conn.execute(
                f"SELECT DISTINCT {src_col} FROM pg.{src} WHERE {src_col} IS NOT NULL "
                f"AND {src_col} NOT IN (SELECT {dim_col} FROM pg.{dim})"
            ).fetchall()
            if rows:
                logger.warning("Nincs a dimenziotablaban (%s) ez a %s kod a %s-ben: %s",
                               dim, label, src, sorted(r[0] for r in rows))
        except Exception as exc:  # pl. meg nem letezo bronze tabla
            logger.warning("Ellenorzes kihagyva (%s): %s", src, exc)


def run_eia_silver_transformation(conn=None):
    own_conn = conn is None
    if own_conn:
        conn = duckdb.connect()

    try:
        if own_conn:
            conn.execute("INSTALL postgres; LOAD postgres;")
            conn.execute(f"ATTACH '{PG_CONN}' AS pg (TYPE POSTGRES);")
        conn.execute("SET TimeZone = 'UTC';")
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;")

        # 1-2. dimenziok
        build_dim_respondent(conn)
        build_dim_fueltype(conn)

        # 3. kozos orás tabla: fogyasztas + LMP
        build_demand_lmp_hourly(conn)

        n = conn.execute("SELECT COUNT(*) FROM pg.silver.demand_lmp_hourly").fetchone()[0]
        logger.info("EIA silver kesz: dim_respondent=%s, dim_fueltype=%s, demand_lmp_hourly=%s sor",
                    conn.execute("SELECT COUNT(*) FROM pg.silver.dim_respondent").fetchone()[0],
                    conn.execute("SELECT COUNT(*) FROM pg.silver.dim_fueltype").fetchone()[0], n)
        _check_unmapped(conn)
        print("EIA silver réteg sikeresen frissítve a Postgres-ben!")

    except Exception as exc:
        logger.exception("EIA silver hiba: %s", exc)
        print(f"EIA silver table error: {exc}")
        raise

    finally:
        if own_conn:
            conn.close()
