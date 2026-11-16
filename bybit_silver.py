# 2026.11.16  19.00
import os
import time
import logging
from datetime import datetime, UTC, timedelta
import duckdb

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================
REFRESH_HOURS = 3    # újraszámolt ablak
WARMUP_HOURS  = 12   # visszatekintés az SMA100 és RSI14 bemelegítéséhez (144 gyertya)
SRC_TABLE     = "pg.bronze.bybit_tickers"
DST_TABLE     = "pg.silver.bybit_indicators"

SILVER_SELECT = """
WITH src AS (
    SELECT symbol, timestamp, close, mark_price, high_24h, low_24h, bid, ask, bid1_size, ask1_size, vwap, funding, oi, turnover24h, price_24h_pcnt,
           CASE WHEN mark_price IS NOT NULL AND ABS(close - mark_price) / NULLIF(mark_price, 0) > 0.003 THEN mark_price ELSE close END AS px
    FROM pg.bronze.bybit_tickers
    WHERE timestamp >= ?
),
diffs AS (
    SELECT *,
        px - LAG(px, 1) OVER w AS px_diff,
        100.0 * (px - LAG(px, 1)  OVER w) / NULLIF(LAG(px, 1)  OVER w, 0) AS r5,
        100.0 * (px - LAG(px, 3)  OVER w) / NULLIF(LAG(px, 3)  OVER w, 0) AS r15,
        100.0 * (px - LAG(px, 6)  OVER w) / NULLIF(LAG(px, 6)  OVER w, 0) AS r30,
        100.0 * (px - LAG(px, 12) OVER w) / NULLIF(LAG(px, 12) OVER w, 0) AS r1h,
        100.0 * (oi / NULLIF(LAG(oi, 3)  OVER w, 0) - 1) AS oi_chg_15m,
        100.0 * (oi / NULLIF(LAG(oi, 12) OVER w, 0) - 1) AS oi_chg_1h,
        turnover24h - LAG(turnover24h, 12) OVER w AS vol_1h_delta,
        100.0 * (turnover24h / NULLIF(LAG(turnover24h, 12) OVER w, 0) - 1) AS vol_1h_pct,
        ((oi - LAG(oi, 12) OVER w) * px) / NULLIF(turnover24h, 0) AS oi_to_turnover_ratio
    FROM src
    WINDOW w AS (PARTITION BY symbol ORDER BY timestamp)
),
base AS (
    SELECT *,
        AVG(px) OVER w50  AS sma50,
        AVG(px) OVER w100 AS sma100,
        AVG(px) OVER w20  AS bb_basis,
        STDDEV_SAMP(px) OVER w20 AS bb_std,
        STDDEV_SAMP(r5) OVER w20 AS vol_20, -- realizált 5m volatilitás (%) ATR helyett
        AVG(GREATEST(px_diff, 0))  OVER w14 AS avg_gain_14,
        AVG(GREATEST(-px_diff, 0)) OVER w14 AS avg_loss_14
    FROM diffs
    WINDOW
        w14  AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
        w20  AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
        w50  AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
        w100 AS (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)
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
    ROUND(sma50::numeric, 8)  AS sma50,
    ROUND(sma100::numeric, 8) AS sma100,
    ROUND(bb_basis::numeric, 8) AS bb_basis,
    ROUND((bb_basis + 2 * bb_std)::numeric, 8) AS bb_upper,
    ROUND((bb_basis - 2 * bb_std)::numeric, 8) AS bb_lower,
    ROUND((CASE
        WHEN avg_loss_14 = 0 AND avg_gain_14 > 0 THEN 100.0
        WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN 50.0
        ELSE 100.0 - (100.0 / (1.0 + avg_gain_14 / NULLIF(avg_loss_14, 0)))
    END)::numeric, 2) AS rsi14,
    ROUND(vol_20::numeric, 4) AS vol_20,
    ROUND(((px - low_24h) / NULLIF(high_24h - low_24h, 0))::numeric, 4) AS range_24h_pos,
    ROUND((100.0 * (high_24h - low_24h) / NULLIF(low_24h, 0))::numeric, 4) AS range_24h_pct,
    ROUND((CASE WHEN ask > bid AND bid > 0 THEN 10000.0 * (ask - bid) / ((ask + bid) / 2) END)::numeric, 2) AS spread_bps,
    ROUND(((bid1_size - ask1_size) / NULLIF(bid1_size + ask1_size, 0))::numeric, 4) AS obi_l1,
    ROUND((100.0 * (close - vwap) / NULLIF(vwap, 0))::numeric, 4) AS vwap_dev_pct,
    ROUND((100.0 * (close - mark_price) / NULLIF(mark_price, 0))::numeric, 4) AS local_premium_pct,
    ROUND(funding::numeric, 10) AS funding,
    ROUND(oi::numeric, 4) AS oi,
    ROUND(oi_chg_15m::numeric, 4) AS oi_chg_15m,
    ROUND(oi_chg_1h::numeric, 4)  AS oi_chg_1h,
    ROUND(vol_1h_delta::numeric, 2) AS vol_1h_delta,
    ROUND(vol_1h_pct::numeric, 4)   AS vol_1h_pct,
    ROUND(oi_to_turnover_ratio::numeric, 6) AS oi_to_turnover_ratio,
    ROUND(turnover24h::numeric, 2) AS turnover24h,
    CURRENT_TIMESTAMP AS calculated_at
FROM base
WHERE timestamp >= ?
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
    now = datetime.now(UTC)
    cutoff = now - timedelta(hours=REFRESH_HOURS)
    warmup_cutoff = cutoff - timedelta(hours=WARMUP_HOURS)

    attach_dsn_sql = _build_attach_dsn().replace("'", "''")
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL postgres;")
        conn.execute("LOAD postgres;")
        conn.execute(f"ATTACH '{attach_dsn_sql}' AS pg (TYPE POSTGRES);")
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {DST_TABLE} AS {SILVER_SELECT} LIMIT 0", [warmup_cutoff, cutoff],)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_silver_ind_sym_ts ON pg.silver.bybit_indicators (symbol, timestamp DESC);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_silver_ind_ts ON pg.silver.bybit_indicators (timestamp);")
        conn.execute("BEGIN")
        try:
            conn.execute(f"DELETE FROM {DST_TABLE} WHERE timestamp >= ?", [cutoff])
            conn.execute(f"INSERT INTO {DST_TABLE} {SILVER_SELECT}", [warmup_cutoff, cutoff],)
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
