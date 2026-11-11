# 2026.11.11  18.00
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

SILVER_SELECT = """
WITH src AS (
    SELECT symbol, timestamp, close, mark_price, bid, ask, vwap,
           funding, oi, turnover24h, price_24h_pcnt,
           -- smart close: 0.3%-nál nagyobb close/mark eltérés = scam wick -> mark-ot használunk
           CASE WHEN mark_price IS NOT NULL
                 AND ABS(close - mark_price) / NULLIF(mark_price, 0) > 0.003
                THEN mark_price ELSE close END AS px
    FROM pg.bronze.bybit_tickers
),
base AS (
    SELECT *,
        100.0 * (px - LAG(px, 1)  OVER w) / NULLIF(LAG(px, 1)  OVER w, 0) AS r5,
        100.0 * (px - LAG(px, 3)  OVER w) / NULLIF(LAG(px, 3)  OVER w, 0) AS r15,
        100.0 * (px - LAG(px, 6)  OVER w) / NULLIF(LAG(px, 6)  OVER w, 0) AS r30,
        100.0 * (px - LAG(px, 12) OVER w) / NULLIF(LAG(px, 12) OVER w, 0) AS r1h,
        AVG(px) OVER w50  AS ema50,
        AVG(px) OVER w100 AS ema100,
        AVG(px) OVER w20  AS bb_basis,
        STDDEV_SAMP(px) OVER w20 AS bb_std,
        100.0 * (oi / NULLIF(LAG(oi, 3)  OVER w, 0) - 1) AS oi_chg_15m,
        100.0 * (oi / NULLIF(LAG(oi, 12) OVER w, 0) - 1) AS oi_chg_1h
    FROM src
    WINDOW
        w    AS (PARTITION BY symbol ORDER BY timestamp),
        w50  AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
        w100 AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
        w20  AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
)
SELECT symbol, timestamp,
    ROUND(close::numeric, 8)      AS close,
    ROUND(px::numeric, 8)         AS smart_close,
    ROUND(mark_price::numeric, 8) AS mark_price,
    ROUND(r5::numeric, 4)  AS r5,
    ROUND(r15::numeric, 4) AS r15,
    ROUND(r30::numeric, 4) AS r30,
    ROUND(r1h::numeric, 4) AS r1h,
    ROUND((price_24h_pcnt * 100)::numeric, 4) AS r24h,
    ROUND(ema50::numeric, 8)  AS ema50,
    ROUND(ema100::numeric, 8) AS ema100,
    ROUND(bb_basis::numeric, 8) AS bb_basis,
    ROUND((bb_basis + 2 * bb_std)::numeric, 8) AS bb_upper,
    ROUND((bb_basis - 2 * bb_std)::numeric, 8) AS bb_lower,
    ROUND((CASE WHEN ask > bid AND bid > 0
                THEN 10000.0 * (ask - bid) / ((ask + bid) / 2) END)::numeric, 2) AS spread_bps,
    ROUND((100.0 * (close - vwap) / NULLIF(vwap, 0))::numeric, 4) AS vwap_dev_pct,
    ROUND((100.0 * (close - mark_price) / NULLIF(mark_price, 0))::numeric, 4) AS local_premium_pct,
    ROUND(funding::numeric, 10) AS funding,
    ROUND(oi::numeric, 4) AS oi,
    ROUND(oi_chg_15m::numeric, 4) AS oi_chg_15m,
    ROUND(oi_chg_1h::numeric, 4)  AS oi_chg_1h,
    ROUND(turnover24h::numeric, 2) AS turnover24h,
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
    start_time = time.perf_counter()
    cutoff = datetime.now(UTC) - timedelta(hours=REFRESH_HOURS)
    
    attach_dsn_sql = _build_attach_dsn().replace("'", "''")
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL postgres;")
        conn.execute("LOAD postgres;")
        conn.execute(f"ATTACH '{attach_dsn_sql}' AS pg (TYPE POSTGRES);")
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;")
        
        # Tábla létrehozása (ha még nem létezik) az első futáskor
        conn.execute(f"CREATE TABLE IF NOT EXISTS {DST_TABLE} AS {SILVER_SELECT} LIMIT 0")
        
        conn.execute("BEGIN")
        try:
            # 1. Régi (cutoff utáni) adatok törlése
            conn.execute("DELETE FROM {DST_TABLE} WHERE timestamp >= ?", [cutoff])
            
            # 2. Új adatok beszúrása
            conn.execute(f"INSERT INTO {DST_TABLE} SELECT * FROM ({SILVER_SELECT}) WHERE timestamp >= ?", [cutoff])
            
            # 3. Olyan sorok törlése, amik már nincsenek benne a bronze forrásban
            # (Itt a bybit_tickers-t nézzük, mert az a biztos forrás sampler módban)
            conn.execute("DELETE FROM {DST_TABLE} WHERE timestamp < (SELECT MIN(timestamp) FROM {SRC_TABLE})")
            
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
            
        count = conn.execute("SELECT COUNT(*) FROM {DST_TABLE}").fetchone()[0]
        logger.info("[SILVER] Incremental Update Ready: %d rows (%.2f s)", count, time.perf_counter() - start_time)
        return count
        
    except Exception:
        logger.exception("[SILVER] Transzformációs hiba")
        raise
    finally:
        conn.close()
