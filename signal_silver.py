# 2026.11.04 18.00  Signal Silver Transformation
import os
import time
import logging
import duckdb

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================
RETENTION_DAYS = 7

# Momentum LAG offsets (5, 15, 30 perc = 1, 3, 6 sor 5 perces intervallumnál)
R5_LAG = 1
R15_LAG = 3
R30_LAG = 6

# 6 elég lenne, de legyen tartalék a kimaradt sorok esetére
SILVER_LOOKBACK_ROWS = 10

FULL_REFRESH = os.getenv("SILVER_FULL_REFRESH", "0") == "1"

# ============================================================
# POSTGRES CONNECTION
# ============================================================

def _build_attach_dsn() -> str:
    return (
        f"host='{os.getenv('DB_HOST', 'postgresql')}'"
        f"port='{os.getenv('DB_PORT', '5432')}'"
        f"dbname='{os.getenv('DB_NAME', 'n8n')}'"
        f"user='{os.getenv('DB_USER', 'sql_admin')}'"
        f"password='{os.getenv('DB_PASSWORD', 'sql_pass')}'"
    )

# ============================================================
# SOURCE PREPARATION
# ============================================================
def _build_source(conn) -> int:
    """Egységes forrás előkészítése full refresh vagy incremental módban."""
    
    if FULL_REFRESH:
        # --------------------------------------------------------
        # FULL REFRESH: Összes Bronze sor
        # --------------------------------------------------------
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _silver_source AS
            SELECT symbol, snapshot_at, price, max_leverage, turnover24h, r1h, r24h, change24, funding, open_interest
            FROM (
                SELECT
                    NULLIF(TRIM(symbol::VARCHAR), '') AS symbol,
                    TRY_CAST(snapshot_at AS TIMESTAMPTZ) AS snapshot_at,
                    TRY_CAST(price AS DOUBLE) AS price,
                    TRY_CAST(max_leverage AS DOUBLE) AS max_leverage,
                    TRY_CAST(turnover24h AS DOUBLE) AS turnover24h,
                    TRY_CAST(r1h AS DOUBLE) AS r1h,
                    TRY_CAST(r24h AS DOUBLE) AS r24h,
                    TRY_CAST(change24 AS DOUBLE) AS change24,
                    TRY_CAST(funding AS DOUBLE) AS funding,
                    TRY_CAST(open_interest AS DOUBLE) AS open_interest,
                    _dlt_load_id,
                    _dlt_id,
                    ROW_NUMBER() OVER (PARTITION BY symbol, snapshot_at ORDER BY _dlt_load_id DESC NULLS LAST, _dlt_id DESC NULLS LAST) AS rn
                FROM pg.bybit_data.market_snapshots
                WHERE TRY_CAST(snapshot_at AS TIMESTAMPTZ) >= CURRENT_TIMESTAMP - INTERVAL '{RETENTION_DAYS} days'
                  AND NULLIF(TRIM(symbol::VARCHAR), '') IS NOT NULL
                  AND TRY_CAST(price AS DOUBLE) > 0
                  AND TRY_CAST(snapshot_at AS TIMESTAMPTZ) IS NOT NULL
            )
            WHERE rn = 1
        """)
    else:
        # --------------------------------------------------------
        # INCREMENTAL: Új Bronze sorok + releváns Silver sorok
        # --------------------------------------------------------        
        # 1. Új Bronze sorok (még nincsenek a Silver-ben)
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _new_bronze AS
            SELECT symbol, snapshot_at, price, max_leverage, turnover24h, r1h, r24h, change24, funding, open_interest
            FROM (
                SELECT
                    NULLIF(TRIM(b.symbol::VARCHAR), '') AS symbol,
                    TRY_CAST(b.snapshot_at AS TIMESTAMPTZ) AS snapshot_at,
                    TRY_CAST(b.price AS DOUBLE) AS price,
                    TRY_CAST(b.max_leverage AS DOUBLE) AS max_leverage,
                    TRY_CAST(b.turnover24h AS DOUBLE) AS turnover24h,
                    TRY_CAST(b.r1h AS DOUBLE) AS r1h,
                    TRY_CAST(b.r24h AS DOUBLE) AS r24h,
                    TRY_CAST(b.change24 AS DOUBLE) AS change24,
                    TRY_CAST(b.funding AS DOUBLE) AS funding,
                    TRY_CAST(b.open_interest AS DOUBLE) AS open_interest,
                    b._dlt_load_id,
                    b._dlt_id,
                    ROW_NUMBER() OVER (PARTITION BY b.symbol, b.snapshot_at ORDER BY b._dlt_load_id DESC NULLS LAST, b._dlt_id DESC NULLS LAST) AS rn
                FROM pg.bybit_data.market_snapshots b
                WHERE TRY_CAST(b.snapshot_at AS TIMESTAMPTZ) >= CURRENT_TIMESTAMP - INTERVAL '{RETENTION_DAYS} days'
                  AND NULLIF(TRIM(b.symbol::VARCHAR), '') IS NOT NULL
                  AND TRY_CAST(b.price AS DOUBLE) > 0
                  AND TRY_CAST(b.snapshot_at AS TIMESTAMPTZ) IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM pg.silver.fact_market_snapshots s WHERE s.symbol = b.symbol AND s.snapshot_at = TRY_CAST(b.snapshot_at AS TIMESTAMPTZ))
            )
            WHERE rn = 1
        """)
        
        new_count = conn.execute("SELECT COUNT(*) FROM _new_bronze").fetchone()[0]
        logger.info("[SILVER] New Bronze records: %d", new_count)
        
        if new_count == 0:
            return 0
        
        # 2. Releváns Silver sorok (utolsó N sor symbol-onként)
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _relevant_silver AS
            SELECT symbol, snapshot_at, price, max_leverage, turnover24h, r1h, r24h, change24, funding, open_interest
            FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY snapshot_at DESC) AS rn FROM pg.silver.fact_market_snapshots)
            WHERE rn <= {SILVER_LOOKBACK_ROWS}
        """)
        
        # 3. Kombinált forrás: Silver + új Bronze
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _silver_source AS
            SELECT symbol, snapshot_at, price, max_leverage, turnover24h, r1h,  r24h, change24, funding, open_interest
            FROM _relevant_silver
            UNION ALL
            SELECT symbol, snapshot_at, price, max_leverage, turnover24h, r1h, r24h, change24, funding, open_interest
            FROM _new_bronze
        """)
    
    source_count = conn.execute("SELECT COUNT(*) FROM _silver_source").fetchone()[0]
    return source_count

# ============================================================
# MOMENTUM CALCULATION
# ============================================================
def _calculate_momentum(conn) -> int:
    """Egyszerű momentum számítás toleranciák nélkül."""
    
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _silver_output AS
        SELECT symbol, snapshot_at, price, max_leverage,  turnover24h,
            CASE WHEN LAG(price, {R5_LAG}) OVER w > 0
                 THEN ((price / LAG(price, {R5_LAG}) OVER w) - 1) * 100
                 ELSE NULL
            END AS r5,
            CASE WHEN LAG(price, {R15_LAG}) OVER w > 0
                 THEN ((price / LAG(price, {R15_LAG}) OVER w) - 1) * 100
                 ELSE NULL
            END AS r15,
            CASE WHEN LAG(price, {R30_LAG}) OVER w > 0
                 THEN ((price / LAG(price, {R30_LAG}) OVER w) - 1) * 100
                 ELSE NULL
            END AS r30, r1h, r24h, change24, funding, open_interest, CURRENT_TIMESTAMP AS transformed_at
        FROM _silver_source
        WINDOW w AS (PARTITION BY symbol ORDER BY snapshot_at)
    """)
    
    return conn.execute("SELECT COUNT(*) FROM _silver_output").fetchone()[0]

# ============================================================
# WRITE SILVER
# ============================================================
def _write_silver(conn):
    """Eredmények írása a Silver táblába."""
    if FULL_REFRESH:
        logger.warning("[SILVER] FULL REFRESH: rebuilding the complete table.")
        conn.execute("CREATE OR REPLACE TABLE pg.silver.fact_market_snapshots AS SELECT * FROM _silver_output")
    else:
        # Csak az új sorokat írjuk (amik a _new_bronze-ban vannak)
        conn.execute("""
            INSERT INTO pg.silver.fact_market_snapshots (symbol, snapshot_at, price, max_leverage, turnover24h, r5, r15, r30, r1h, r24h, change24, funding, open_interest, transformed_at)
            SELECT o.symbol, o.snapshot_at, o.price, o.max_leverage, o.turnover24h, o.r5, o.r15, o.r30, o.r1h, o.r24h, o.change24, o.funding, o.open_interest, o.transformed_at
            FROM _silver_output o
            INNER JOIN _new_bronze n
            ON o.symbol = n.symbol AND o.snapshot_at = n.snapshot_at
        """)
    
    logger.info("[SILVER] Silver table updated.")

# ============================================================
# RETENTION
# ============================================================
def _apply_retention(conn):
    """Régi adatok törlése."""
    conn.execute(f"DELETE FROM pg.silver.fact_market_snapshots WHERE snapshot_at < CURRENT_TIMESTAMP - INTERVAL '{RETENTION_DAYS} days'")
    logger.info("[SILVER] Retention applied: %d days.", RETENTION_DAYS)

# ============================================================
# VALIDATION
# ============================================================
def _validate_silver(conn) -> dict:
    """Silver tábla validálása."""
    stats = conn.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT symbol) AS unique_coins,
            MIN(snapshot_at) AS first_snapshot,
            MAX(snapshot_at) AS last_snapshot,
            COUNT(*) FILTER (WHERE price IS NULL OR price <= 0) AS invalid_prices,
            COUNT(*) FILTER (WHERE r5 IS NULL) AS missing_r5,
            COUNT(*) FILTER (WHERE r15 IS NULL) AS missing_r15,
            COUNT(*) FILTER (WHERE r30 IS NULL) AS missing_r30
        FROM pg.silver.fact_market_snapshots
    """).fetchone()
    
    (total_rows, unique_coins, first_snapshot, last_snapshot,
     invalid_prices, missing_r5, missing_r15, missing_r30) = stats
    
    if invalid_prices > 0:
        raise RuntimeError(f"Invalid Silver prices: {invalid_prices}")
    
    # Duplikátumok ellenőrzése
    duplicates = conn.execute("SELECT COUNT(*) FROM (SELECT symbol, snapshot_at FROM pg.silver.fact_market_snapshots GROUP BY symbol, snapshot_at HAVING COUNT(*) > 1)").fetchone()[0]
    
    if duplicates > 0:
        raise RuntimeError(f"Duplicate Silver keys found: {duplicates}")
    
    return {
        "total_rows": total_rows,
        "unique_coins": unique_coins,
        "first_snapshot": first_snapshot,
        "last_snapshot": last_snapshot,
        "missing_r5": missing_r5,
        "missing_r15": missing_r15,
        "missing_r30": missing_r30,
    }

# ============================================================
# MAIN TRANSFORMATION
# ============================================================
def run_signal_silver_transformation() -> int:
    """Fő transzformációs függvény."""
    start_time = time.perf_counter()
    
    conn = duckdb.connect()
    try:
        # Postgres kapcsolat
        conn.execute("INSTALL postgres;")
        conn.execute("LOAD postgres;")
        attach_dsn = _build_attach_dsn()
        attach_dsn_sql = attach_dsn.replace("'", "''")
        conn.execute(f"ATTACH '{attach_dsn_sql}' AS pg (TYPE POSTGRES);")
        conn.execute("SET TimeZone = 'UTC';")
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;")
        
        # 1. Forrás előkészítése
        source_count = _build_source(conn)
        if source_count == 0:
            if FULL_REFRESH:
                raise RuntimeError("No Bronze records found.")
            logger.info("[SILVER] No new records to process.")
            _apply_retention(conn)
            return _validate_silver(conn)["total_rows"]
        
        # 2. Momentum számítás
        output_count = _calculate_momentum(conn)
        logger.info("[SILVER] Output records: %d", output_count)
        
        # 3. Silver írás
        _write_silver(conn)
        
        # 4. Retention
        _apply_retention(conn)
        
        # 5. Validáció
        stats = _validate_silver(conn)
        
        elapsed = time.perf_counter() - start_time
        logger.info("[SILVER] Execution time: %.2f sec", elapsed)
        
        return stats["total_rows"]
    
    except Exception:
        logger.exception("Signal Silver transformation failed.")
        raise
    finally:
        conn.close()
