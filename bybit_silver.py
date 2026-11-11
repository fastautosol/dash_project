# 2026.11.11  15.00
import os
import time
import logging
from datetime import datetime, UTC, timedelta
import duckdb
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================
REFRESH_HOURS = 3  # ennyi órát számol újra minden futásnál
SRC_TABLE = "pg.bronze.bybit_tickers"          # ezt írja a bybit-bot.py
DST_TABLE = "pg.silver.bybit_indicators"

# Sampler-only módban a close, funding, oi, turnover24h érhető el.
SILVER_SELECT = f"""
WITH base AS (
    SELECT symbol, timestamp, close, funding, oi, turnover24h,
        100.0 * (close - LAG(close, 1)  OVER w) / NULLIF(LAG(close, 1)  OVER w, 0) AS r5,
        100.0 * (close - LAG(close, 3)  OVER w) / NULLIF(LAG(close, 3)  OVER w, 0) AS r15,
        100.0 * (close - LAG(close, 6)  OVER w) / NULLIF(LAG(close, 6)  OVER w, 0) AS r30,
        100.0 * (close - LAG(close, 12) OVER w) / NULLIF(LAG(close, 12) OVER w, 0) AS r1h,
        AVG(close) OVER w50  AS ema50,
        AVG(close) OVER w100 AS ema100,
        AVG(close) OVER w20  AS bb_basis,
        STDDEV_SAMP(close) OVER w20 AS bb_std
    FROM {SRC_TABLE}
    WINDOW
        w    AS (PARTITION BY symbol ORDER BY timestamp),
        w50  AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
        w100 AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
        w20  AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
)
SELECT symbol, timestamp, close, funding, oi, turnover24h,
    ROUND(r5::numeric, 4) AS r5,
    ROUND(r15::numeric, 4) AS r15,
    ROUND(r30::numeric, 4) AS r30,
    ROUND(r1h::numeric, 4) AS r1h,
    ROUND(ema50::numeric, 8) AS ema50,
    ROUND(ema100::numeric, 8) AS ema100,
    ROUND(bb_basis::numeric, 8) AS bb_basis,
    ROUND((bb_basis + 2 * bb_std)::numeric, 8) AS bb_upper,
    ROUND((bb_basis - 2 * bb_std)::numeric, 8) AS bb_lower,
    CURRENT_TIMESTAMP AS calculated_at
FROM base
"""

# ============================================================
# POSTGRES CONNECTION
# ============================================================
def _build_attach_dsn() -> str:
    return (
        f"host='{os.getenv('DB_HOST', 'postgresql')}' "
        f"port='{os.getenv('DB_PORT', '5432')}' "
        f"dbname='{os.getenv('DB_NAME', 'n8n')}' "
        f"user='{os.getenv('DB_USER', 'sql_admin')}' "
        f"password='{os.getenv('DB_PASSWORD', 'sql_pass')}'"
    )


def _columns(conn, query: str) -> list[str]:
    """Egy lekérdezés oszlopnevei (adat nélkül)."""
    return [d[0] for d in conn.execute(f"SELECT * FROM ({query}) LIMIT 0").description]


# ============================================================
# SILVER TRANSFORMATION
# ============================================================
def run_bybit_silver_transformation() -> int:
    """Inkrementálisan frissíti a silver.bybit_indicators táblát a bronze.bybit_tickers alapján."""
    start_time = time.perf_counter()
    cutoff = datetime.now(UTC) - timedelta(hours=REFRESH_HOURS)

    attach_dsn_sql = _build_attach_dsn().replace("'", "''")
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL postgres;")
        conn.execute("LOAD postgres;")
        conn.execute(f"ATTACH '{attach_dsn_sql}' AS pg (TYPE POSTGRES);")
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;")

        # Tábla létrehozása, ha még nem létezik
        conn.execute(f"CREATE TABLE IF NOT EXISTS {DST_TABLE} AS {SILVER_SELECT} LIMIT 0")

        # Séma-eltérés (régi tábla más oszlopszámmal) -> eldobás és teljes újraszámolás
        expected_cols = _columns(conn, SILVER_SELECT)
        existing_cols = _columns(conn, f"SELECT * FROM {DST_TABLE}")
        if existing_cols != expected_cols:
            logger.warning("[SILVER] Séma-eltérés (%d vs %d oszlop), tábla újraépítése", len(existing_cols), len(expected_cols))
            conn.execute(f"DROP TABLE {DST_TABLE}")
            conn.execute(f"CREATE TABLE {DST_TABLE} AS {SILVER_SELECT} LIMIT 0")
            cutoff = datetime(1970, 1, 1, tzinfo=UTC)  # teljes bronze újraszámolása

        col_list = ", ".join(expected_cols)

        conn.execute("BEGIN")
        try:
            # 1. Régi (cutoff utáni) adatok törlése
            conn.execute(f"DELETE FROM {DST_TABLE} WHERE timestamp >= ?", [cutoff])

            # 2. Új adatok beszúrása (explicit oszloplistával)
            conn.execute(f"INSERT INTO {DST_TABLE} ({col_list}) SELECT {col_list} FROM ({SILVER_SELECT}) WHERE timestamp >= ?", [cutoff])

            # 3. Olyan sorok törlése, amik már nincsenek a bronze forrásban
            conn.execute(f"DELETE FROM {DST_TABLE} WHERE timestamp < (SELECT MIN(timestamp) FROM {SRC_TABLE})")

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        count = conn.execute(f"SELECT COUNT(*) FROM {DST_TABLE}").fetchone()[0]
        logger.info("[SILVER] Incremental Update Ready: %d rows (%.2f s)", count, time.perf_counter() - start_time)
        return count

    except Exception:
        logger.exception("[SILVER] Transzformációs hiba")
        raise
    finally:
        conn.close()
