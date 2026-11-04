# 2026.11.04  18.00
import os
import time
import logging
import duckdb

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

RETENTION_DAYS = 7

# Momentum reference windows
R5_MINUTES = 5
R5_MIN_TOLERANCE = 4
R5_MAX_TOLERANCE = 7

R15_MINUTES = 15
R15_MIN_TOLERANCE = 13
R15_MAX_TOLERANCE = 17

R30_MINUTES = 30
R30_MIN_TOLERANCE = 28
R30_MAX_TOLERANCE = 32

# LAG offsets
R5_LAG = 1
R15_LAG = 3
R30_LAG = 6

# 40 minutes of history is fetched for each new snapshot.
HISTORY_LOOKBACK_MINUTES = 40

# Set to 1 for the one-time historical recalculation.
# Set back to 0 after successful backfill.
FULL_REFRESH = os.getenv(
    "SILVER_FULL_REFRESH", "0"
) == "1"


# ============================================================
# POSTGRES CONNECTION
# ============================================================

def _conninfo_value(value: str) -> str:
    value = str(value)
    value = value.replace("\\", "\\\\")
    value = value.replace("'", "\\'")
    return f"'{value}'"


def _build_attach_dsn() -> str:
    return (
        f"host={_conninfo_value(os.getenv('DB_HOST', 'postgresql'))} "
        f"port={_conninfo_value(os.getenv('DB_PORT', '5432'))} "
        f"dbname={_conninfo_value(os.getenv('DB_NAME', 'n8n'))} "
        f"user={_conninfo_value(os.getenv('DB_USER', 'sql_admin'))} "
        f"password={_conninfo_value(os.getenv('DB_PASSWORD', 'sql_pass'))}"
    )


# ============================================================
# BRONZE CLEANING
# ============================================================

def _create_cleaned_bronze(conn):

    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _silver_cleaned AS

        SELECT
            NULLIF(TRIM(symbol::VARCHAR), '') AS symbol,

            TRY_CAST(snapshot_at AS TIMESTAMPTZ)
                AS snapshot_at,

            TRY_CAST(price AS DOUBLE)
                AS price,

            TRY_CAST(max_leverage AS DOUBLE)
                AS max_leverage,

            TRY_CAST(turnover24h AS DOUBLE)
                AS turnover24h,

            TRY_CAST(r1h AS DOUBLE)
                AS r1h,

            TRY_CAST(r24h AS DOUBLE)
                AS r24h,

            TRY_CAST(change24 AS DOUBLE)
                AS change24,

            TRY_CAST(funding AS DOUBLE)
                AS funding,

            TRY_CAST(open_interest AS DOUBLE)
                AS open_interest,

            _dlt_load_id,
            _dlt_id

        FROM pg.bybit_data.market_snapshots

        WHERE TRY_CAST(snapshot_at AS TIMESTAMPTZ)
                >= CURRENT_TIMESTAMP
                   - INTERVAL '{RETENTION_DAYS} days'

          AND NULLIF(TRIM(symbol::VARCHAR), '') IS NOT NULL

          AND TRY_CAST(price AS DOUBLE) > 0

          AND TRY_CAST(snapshot_at AS TIMESTAMPTZ)
                IS NOT NULL
    """)


# ============================================================
# DEDUPLICATION
# ============================================================

def _deduplicate(conn, source_table, target_table):

    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE {target_table} AS

        SELECT
            symbol,
            snapshot_at,
            price,
            max_leverage,
            turnover24h,
            r1h,
            r24h,
            change24,
            funding,
            open_interest

        FROM (

            SELECT
                *,

                ROW_NUMBER() OVER (
                    PARTITION BY symbol, snapshot_at
                    ORDER BY
                        _dlt_load_id DESC NULLS LAST,
                        _dlt_id DESC NULLS LAST
                ) AS rn

            FROM {source_table}

        )

        WHERE rn = 1
    """)


# ============================================================
# FULL REFRESH SOURCE
# ============================================================

def _prepare_full_refresh(conn):

    _create_cleaned_bronze(conn)

    _deduplicate(
        conn,
        "_silver_cleaned",
        "_silver_source"
    )

    source_count = conn.execute("""
        SELECT COUNT(*)
        FROM _silver_source
    """).fetchone()[0]

    logger.info(
        "[SILVER] Full refresh source: %d records",
        source_count
    )

    return source_count


# ============================================================
# INCREMENTAL SOURCE
# ============================================================

def _prepare_incremental_source(conn):

    # Only Bronze records missing from Silver.
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _new_rows AS

        SELECT
            symbol,
            snapshot_at,
            price,
            max_leverage,
            turnover24h,
            r1h,
            r24h,
            change24,
            funding,
            open_interest

        FROM (

            SELECT
                *,

                ROW_NUMBER() OVER (
                    PARTITION BY symbol, snapshot_at
                    ORDER BY
                        _dlt_load_id DESC NULLS LAST,
                        _dlt_id DESC NULLS LAST
                ) AS rn

            FROM (

                SELECT
                    NULLIF(TRIM(b.symbol::VARCHAR), '') AS symbol,

                    TRY_CAST(b.snapshot_at AS TIMESTAMPTZ)
                        AS snapshot_at,

                    TRY_CAST(b.price AS DOUBLE)
                        AS price,

                    TRY_CAST(b.max_leverage AS DOUBLE)
                        AS max_leverage,

                    TRY_CAST(b.turnover24h AS DOUBLE)
                        AS turnover24h,

                    TRY_CAST(b.r1h AS DOUBLE)
                        AS r1h,

                    TRY_CAST(b.r24h AS DOUBLE)
                        AS r24h,

                    TRY_CAST(b.change24 AS DOUBLE)
                        AS change24,

                    TRY_CAST(b.funding AS DOUBLE)
                        AS funding,

                    TRY_CAST(b.open_interest AS DOUBLE)
                        AS open_interest,

                    b._dlt_load_id,
                    b._dlt_id

                FROM pg.bybit_data.market_snapshots b

                WHERE TRY_CAST(b.snapshot_at AS TIMESTAMPTZ)
                    >= CURRENT_TIMESTAMP
                       - INTERVAL '{RETENTION_DAYS} days'

                  AND NULLIF(TRIM(b.symbol::VARCHAR), '') IS NOT NULL

                  AND TRY_CAST(b.price AS DOUBLE) > 0

                  AND TRY_CAST(b.snapshot_at AS TIMESTAMPTZ)
                        IS NOT NULL

                  AND NOT EXISTS (

                      SELECT 1

                      FROM pg.silver.fact_market_snapshots s

                      WHERE s.symbol = b.symbol
                        AND s.snapshot_at =
                            TRY_CAST(b.snapshot_at AS TIMESTAMPTZ)

                  )

            )

        )

        WHERE rn = 1
    """)

    new_count = conn.execute("""
        SELECT COUNT(*)
        FROM _new_rows
    """).fetchone()[0]

    logger.info(
        "[SILVER] New Bronze records: %d",
        new_count
    )

    if new_count == 0:
        return 0

    # --------------------------------------------------------
    # Fetch previous 40 minutes for each new snapshot.
    # This provides enough history for r5/r15/r30.
    # --------------------------------------------------------

    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _history_raw AS

        SELECT

            b.symbol,
            TRY_CAST(b.snapshot_at AS TIMESTAMPTZ)
                AS snapshot_at,

            TRY_CAST(b.price AS DOUBLE)
                AS price,

            TRY_CAST(b.max_leverage AS DOUBLE)
                AS max_leverage,

            TRY_CAST(b.turnover24h AS DOUBLE)
                AS turnover24h,

            TRY_CAST(b.r1h AS DOUBLE)
                AS r1h,

            TRY_CAST(b.r24h AS DOUBLE)
                AS r24h,

            TRY_CAST(b.change24 AS DOUBLE)
                AS change24,

            TRY_CAST(b.funding AS DOUBLE)
                AS funding,

            TRY_CAST(b.open_interest AS DOUBLE)
                AS open_interest,

            b._dlt_load_id,
            b._dlt_id

        FROM pg.bybit_data.market_snapshots b

        INNER JOIN _new_rows n

            ON b.symbol = n.symbol

           AND TRY_CAST(b.snapshot_at AS TIMESTAMPTZ)
                <= n.snapshot_at

           AND TRY_CAST(b.snapshot_at AS TIMESTAMPTZ)
                >= n.snapshot_at
                   - INTERVAL '{HISTORY_LOOKBACK_MINUTES} minutes'

        WHERE TRY_CAST(b.price AS DOUBLE) > 0

          AND TRY_CAST(b.snapshot_at AS TIMESTAMPTZ)
                IS NOT NULL
    """)

    _deduplicate(
        conn,
        "_history_raw",
        "_silver_source"
    )

    source_count = conn.execute("""
        SELECT COUNT(*)
        FROM _silver_source
    """).fetchone()[0]

    logger.info(
        "[SILVER] Incremental history source: %d records",
        source_count
    )

    return new_count


# ============================================================
# MOMENTUM CALCULATION
# ============================================================

def _calculate_momentum(conn):

    conn.execute("""
        CREATE OR REPLACE TEMP TABLE _silver_momentum AS

        SELECT

            *,

            LAG(price, 1) OVER w AS price_5m,
            LAG(snapshot_at, 1) OVER w AS time_5m,

            LAG(price, 3) OVER w AS price_15m,
            LAG(snapshot_at, 3) OVER w AS time_15m,

            LAG(price, 6) OVER w AS price_30m,
            LAG(snapshot_at, 6) OVER w AS time_30m

        FROM _silver_source

        WINDOW w AS (
            PARTITION BY symbol
            ORDER BY snapshot_at
        )
    """)

    logger.info("[SILVER] LAG calculations completed.")

    # --------------------------------------------------------
    # Build final output.
    # In incremental mode, only new rows are selected.
    # In full refresh mode, all rows are selected.
    # --------------------------------------------------------

    if FULL_REFRESH:

        output_filter = "TRUE"

    else:

        output_filter = """
            EXISTS (
                SELECT 1
                FROM _new_rows n
                WHERE n.symbol = m.symbol
                  AND n.snapshot_at = m.snapshot_at
            )
        """

    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _silver_output AS

        SELECT

            m.symbol,
            m.snapshot_at,

            m.price,
            m.max_leverage,
            m.turnover24h,

            CASE
                WHEN m.time_5m BETWEEN
                    m.snapshot_at - INTERVAL '{R5_MAX_TOLERANCE} minutes'
                    AND
                    m.snapshot_at - INTERVAL '{R5_MIN_TOLERANCE} minutes'

                 AND m.price_5m > 0

                THEN ((m.price / m.price_5m) - 1) * 100
                ELSE NULL
            END AS r5,

            CASE
                WHEN m.time_15m BETWEEN
                    m.snapshot_at - INTERVAL '{R15_MAX_TOLERANCE} minutes'
                    AND
                    m.snapshot_at - INTERVAL '{R15_MIN_TOLERANCE} minutes'

                 AND m.price_15m > 0

                THEN ((m.price / m.price_15m) - 1) * 100
                ELSE NULL
            END AS r15,

            CASE
                WHEN m.time_30m BETWEEN
                    m.snapshot_at - INTERVAL '{R30_MAX_TOLERANCE} minutes'
                    AND
                    m.snapshot_at - INTERVAL '{R30_MIN_TOLERANCE} minutes'

                 AND m.price_30m > 0

                THEN ((m.price / m.price_30m) - 1) * 100
                ELSE NULL
            END AS r30,

            m.r1h,
            m.r24h,
            m.change24,

            m.funding,
            m.open_interest,

            CURRENT_TIMESTAMP AS transformed_at

        FROM _silver_momentum m

        WHERE {output_filter}
    """)

    return conn.execute("""
        SELECT COUNT(*)
        FROM _silver_output
    """).fetchone()[0]


# ============================================================
# WRITE SILVER
# ============================================================

def _write_silver(conn):

    if FULL_REFRESH:

        logger.warning(
            "[SILVER] FULL REFRESH: rebuilding the complete table."
        )

        conn.execute("""
            CREATE OR REPLACE TABLE
                pg.silver.fact_market_snapshots AS

            SELECT *
            FROM _silver_output
        """)

    else:

        conn.execute("""
            INSERT INTO pg.silver.fact_market_snapshots (

                symbol,
                snapshot_at,

                price,
                max_leverage,
                turnover24h,

                r5,
                r15,
                r30,

                r1h,
                r24h,
                change24,

                funding,
                open_interest,

                transformed_at

            )

            SELECT

                o.symbol,
                o.snapshot_at,

                o.price,
                o.max_leverage,
                o.turnover24h,

                o.r5,
                o.r15,
                o.r30,

                o.r1h,
                o.r24h,
                o.change24,

                o.funding,
                o.open_interest,

                o.transformed_at

            FROM _silver_output o

            WHERE NOT EXISTS (

                SELECT 1

                FROM pg.silver.fact_market_snapshots s

                WHERE s.symbol = o.symbol
                  AND s.snapshot_at = o.snapshot_at

            )
        """)


# ============================================================
# SILVER RETENTION
# ============================================================

def _apply_retention(conn):

    conn.execute(f"""
        DELETE FROM pg.silver.fact_market_snapshots

        WHERE snapshot_at <
            CURRENT_TIMESTAMP - INTERVAL '{RETENTION_DAYS} days'
    """)

    logger.info(
        "[SILVER] Retention applied: %d days.",
        RETENTION_DAYS
    )


# ============================================================
# VALIDATION
# ============================================================

def _validate_silver(conn):

    stats = conn.execute("""
        SELECT

            COUNT(*) AS total_rows,

            COUNT(DISTINCT symbol) AS unique_coins,

            MIN(snapshot_at) AS first_snapshot,

            MAX(snapshot_at) AS last_snapshot,

            COUNT(*) FILTER (
                WHERE price IS NULL OR price <= 0
            ) AS invalid_prices,

            COUNT(*) FILTER (
                WHERE r5 IS NULL
            ) AS missing_r5,

            COUNT(*) FILTER (
                WHERE r15 IS NULL
            ) AS missing_r15,

            COUNT(*) FILTER (
                WHERE r30 IS NULL
            ) AS missing_r30

        FROM pg.silver.fact_market_snapshots
    """).fetchone()

    (
        total_rows,
        unique_coins,
        first_snapshot,
        last_snapshot,
        invalid_prices,
        missing_r5,
        missing_r15,
        missing_r30
    ) = stats

    if invalid_prices > 0:
        raise RuntimeError(
            f"Invalid Silver prices: {invalid_prices}"
        )

    # Check duplicate keys.
    duplicates = conn.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT symbol, snapshot_at
            FROM pg.silver.fact_market_snapshots
            GROUP BY symbol, snapshot_at
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    if duplicates > 0:
        raise RuntimeError(
            f"Duplicate Silver keys found: {duplicates}"
        )

    return {
        "total_rows": total_rows,
        "unique_coins": unique_coins,
        "first_snapshot": first_snapshot,
        "last_snapshot": last_snapshot,
        "invalid_prices": invalid_prices,
        "missing_r5": missing_r5,
        "missing_r15": missing_r15,
        "missing_r30": missing_r30,
    }


# ============================================================
# MAIN TRANSFORMATION
# ============================================================

def run_signal_silver_transformation() -> int:

    start_time = time.perf_counter()

    conn = duckdb.connect()

    try:

        conn.execute("INSTALL postgres;")
        conn.execute("LOAD postgres;")

        attach_dsn = _build_attach_dsn()
        attach_dsn_sql = attach_dsn.replace("'", "''")

        conn.execute(
            f"ATTACH '{attach_dsn_sql}' AS pg (TYPE POSTGRES);"
        )

        conn.execute("SET TimeZone = 'UTC';")

        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS pg.silver;
        """)

        logger.info(
            "[SILVER] Connected to PostgreSQL through DuckDB."
        )

        # ----------------------------------------------------
        # 1. Prepare source
        # ----------------------------------------------------

        if FULL_REFRESH:

            source_count = _prepare_full_refresh(conn)

            if source_count == 0:
                raise RuntimeError("No Bronze records found.")

        else:

            new_count = _prepare_incremental_source(conn)

            if new_count == 0:

                logger.info(
                    "[SILVER] No new records to process."
                )

                _apply_retention(conn)

                return _validate_silver(conn)["total_rows"]

        # ----------------------------------------------------
        # 2. Calculate momentum
        # ----------------------------------------------------

        output_count = _calculate_momentum(conn)

        logger.info(
            "[SILVER] Output records: %d",
            output_count
        )

        # ----------------------------------------------------
        # 3. Write Silver
        # ----------------------------------------------------

        _write_silver(conn)

        # ----------------------------------------------------
        # 4. Retention
        # ----------------------------------------------------

        _apply_retention(conn)

        # ----------------------------------------------------
        # 5. Validation
        # ----------------------------------------------------

        stats = _validate_silver(conn)

        elapsed = time.perf_counter() - start_time

        logger.info("------------------------------------------")
        logger.info("SIGNAL SILVER TRANSFORMATION SUCCESS")
        logger.info("------------------------------------------")

        logger.info(
            "Mode: %s",
            "FULL REFRESH" if FULL_REFRESH else "INCREMENTAL"
        )

        logger.info(
            "Silver rows: %d",
            stats["total_rows"]
        )

        logger.info(
            "Unique coins: %d",
            stats["unique_coins"]
        )

        logger.info(
            "First snapshot: %s",
            stats["first_snapshot"]
        )

        logger.info(
            "Last snapshot: %s",
            stats["last_snapshot"]
        )

        logger.info(
            "Missing r5/r15/r30: %d / %d / %d",
            stats["missing_r5"],
            stats["missing_r15"],
            stats["missing_r30"]
        )

        logger.info(
            "Execution time: %.2f sec",
            elapsed
        )

        logger.info("------------------------------------------")

        return stats["total_rows"]

    except Exception:

        logger.exception(
            "Signal Silver transformation failed."
        )

        raise

    finally:

        conn.close()
