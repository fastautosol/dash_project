# 2026.11.09  10.00
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

SILVER_SELECT = """
    WITH base AS (
        SELECT symbol, timestamp, close, open, high, low, volume, funding, oi, turnover24h,
            100.0 * (close - LAG(close, 1)  OVER w) / NULLIF(LAG(close, 1)  OVER w, 0) AS r5,
            100.0 * (close - LAG(close, 3)  OVER w) / NULLIF(LAG(close, 3)  OVER w, 0) AS r15,
            100.0 * (close - LAG(close, 6)  OVER w) / NULLIF(LAG(close, 6)  OVER w, 0) AS r30,
            100.0 * (close - LAG(close, 12) OVER w) / NULLIF(LAG(close, 12) OVER w, 0) AS r1h,
            AVG(close) OVER w50  AS ema50,
            AVG(close) OVER w100 AS ema100,
            AVG(close) OVER w20  AS bb_basis,
            STDDEV_SAMP(close) OVER w20 AS bb_std,
            volume / NULLIF(AVG(volume) OVER w20, 0) AS vol_ratio
        FROM pg.bronze.bybit_marketinfo
        WINDOW
            w    AS (PARTITION BY symbol ORDER BY timestamp),
            w50  AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
            w100 AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
            w20  AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    )
    SELECT symbol, timestamp, close, open, high, low, volume, funding, oi, turnover24h,
        ROUND(r5, 4) AS r5, ROUND(r15, 4) AS r15, ROUND(r30, 4) AS r30, ROUND(r1h, 4) AS r1h,
        ROUND(ema50, 8) AS ema50, ROUND(ema100, 8) AS ema100,
        ROUND(bb_basis, 8) AS bb_basis,
        ROUND(bb_basis + 2 * bb_std, 8) AS bb_upper,
        ROUND(bb_basis - 2 * bb_std, 8) AS bb_lower,
        ROUND(vol_ratio, 4) AS vol_ratio,
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

# ============================================================
# SILVER TRANSFORMATION
# ============================================================

def run_bybit_silver_transformation() -> int:
    """Inkrementálisan frissíti a silver.bybit_indicators táblát a bronze.bybit_marketinfo alapján."""
    start_time = time.perf_counter()
    cutoff = datetime.now(UTC) - timedelta(hours=REFRESH_HOURS)
    attach_dsn_sql = _build_attach_dsn().replace("'", "''")

    conn = duckdb.connect()
    try:
        conn.execute("INSTALL postgres;")
        conn.execute("LOAD postgres;")
        conn.execute(f"ATTACH '{attach_dsn_sql}' AS pg (TYPE POSTGRES);")
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;")
        conn.execute(f"CREATE TABLE IF NOT EXISTS pg.silver.bybit_indicators AS {SILVER_SELECT} LIMIT 0")
        conn.execute("BEGIN")
        try:
            conn.execute("DELETE FROM pg.silver.bybit_indicators WHERE timestamp >= ?", [cutoff])
            conn.execute(f"INSERT INTO pg.silver.bybit_indicators SELECT * FROM ({SILVER_SELECT}) WHERE timestamp >= ?", [cutoff])
            conn.execute("DELETE FROM pg.silver.bybit_indicators WHERE timestamp < (SELECT MIN(timestamp) FROM pg.bronze.bybit_marketinfo)")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        count = conn.execute("SELECT COUNT(*) FROM pg.silver.bybit_indicators").fetchone()[0]
        logger.info("[SILVER] Incremental Updare Ready: %d rows (%.2f mp)", count, time.perf_counter() - start_time)
        return count

    except Exception:
        logger.exception("[SILVER] Transzformációs hiba")
        raise
    finally:
        conn.close()
