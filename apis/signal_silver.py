import os
import time
import logging
import duckdb

logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

RETENTION_DAYS = 7

# r5 referencia: 5 perc +/- 1 perc
R5_MINUTES = 5
R5_TOLERANCE_MINUTES = 1

# r15 referencia: 15 perc +/- 1 perc
R15_MINUTES = 15
R15_TOLERANCE_MINUTES = 1

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
        f"password={_conninfo_value(os.getenv('DB_PASSWORD', 'sql_pass'))}")


# ============================================================
# SILVER TRANSFORMATION
# ============================================================

def run_signal_silver_transformation() -> int:

    start_time = time.perf_counter()
    conn = duckdb.connect()

    try:

        conn.execute("INSTALL postgres;")
        conn.execute("LOAD postgres;")
        attach_dsn = _build_attach_dsn()

        # Escape the complete DSN as a DuckDB SQL string.
        attach_dsn_sql = attach_dsn.replace("'", "''")

        conn.execute(f"ATTACH '{attach_dsn_sql}' AS pg (TYPE POSTGRES);")
        conn.execute("SET TimeZone = 'UTC';")
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;")

        logger.info("DuckDB csatlakozott a PostgreSQL adatbázishoz.")

        # ----------------------------------------------------
        # 2. READ + CLEAN BRONZE
        # ----------------------------------------------------

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
                    >= CURRENT_TIMESTAMP - INTERVAL '{RETENTION_DAYS} days'

              AND NULLIF(TRIM(symbol::VARCHAR), '') IS NOT NULL

              AND TRY_CAST(price AS DOUBLE) > 0

              AND TRY_CAST(snapshot_at AS TIMESTAMPTZ)
                    IS NOT NULL
        """)

        cleaned_count = conn.execute("""
            SELECT COUNT(*)
            FROM _silver_cleaned
        """).fetchone()[0]

        logger.info(
            "Bronze beolvasás és tisztítás: %d rekord",
            cleaned_count
        )

        if cleaned_count == 0:
            raise RuntimeError(
                "A Bronze táblából nem érkezett feldolgozható rekord."
            )

        # ----------------------------------------------------
        # 3. DEDUPLICATION
        # ----------------------------------------------------

        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _silver_deduplicated AS

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

                FROM _silver_cleaned

            )

            WHERE rn = 1
        """)

        deduplicated_count = conn.execute("""
            SELECT COUNT(*)
            FROM _silver_deduplicated
        """).fetchone()[0]

        logger.info(
            "Deduplikáció után: %d rekord",
            deduplicated_count
        )

        # ----------------------------------------------------
        # 4. CREATE TARGET TIMESTAMPS
        # ----------------------------------------------------

        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _silver_base AS

            SELECT
                *,

                snapshot_at - INTERVAL '5 minutes'
                    AS target_5m,

                snapshot_at - INTERVAL '15 minutes'
                    AS target_15m

            FROM _silver_deduplicated
        """)

        # ----------------------------------------------------
        # 5. CALCULATE R5
        # ----------------------------------------------------

        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _silver_r5 AS

            SELECT

                b.symbol,
                b.snapshot_at,

                CASE

                    WHEN p.snapshot_at >=
                         b.snapshot_at -
                         INTERVAL '{R5_MINUTES + R5_TOLERANCE_MINUTES} minutes'

                    THEN
                        (
                            (b.price / p.price) - 1
                        ) * 100

                    ELSE NULL

                END AS r5

            FROM _silver_base b

            ASOF LEFT JOIN _silver_deduplicated p

                ON b.symbol = p.symbol

                AND b.target_5m >= p.snapshot_at
        """)

        logger.info("r5 számítás elkészült.")

        # ----------------------------------------------------
        # 6. CALCULATE R15
        # ----------------------------------------------------

        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _silver_r15 AS

            SELECT

                b.symbol,
                b.snapshot_at,

                CASE

                    WHEN p.snapshot_at >=
                         b.snapshot_at -
                         INTERVAL '{R15_MINUTES + R15_TOLERANCE_MINUTES} minutes'

                    THEN
                        (
                            (b.price / p.price) - 1
                        ) * 100

                    ELSE NULL

                END AS r15

            FROM _silver_base b

            ASOF LEFT JOIN _silver_deduplicated p

                ON b.symbol = p.symbol

                AND b.target_15m >= p.snapshot_at
        """)

        logger.info("r15 számítás elkészült.")

        # ----------------------------------------------------
        # 7. BUILD FINAL SILVER TABLE
        # ----------------------------------------------------

        conn.execute("""
            CREATE OR REPLACE TABLE
                pg.silver.fact_market_snapshots AS

            SELECT

                d.symbol,
                d.snapshot_at,

                d.price,
                d.max_leverage,
                d.turnover24h,

                r5.r5,
                r15.r15,

                d.r1h,
                d.r24h,
                d.change24,

                d.funding,
                d.open_interest,

                CURRENT_TIMESTAMP AS transformed_at

            FROM _silver_deduplicated d

            LEFT JOIN _silver_r5 r5

                ON d.symbol = r5.symbol
                AND d.snapshot_at = r5.snapshot_at

            LEFT JOIN _silver_r15 r15

                ON d.symbol = r15.symbol
                AND d.snapshot_at = r15.snapshot_at
        """)

        # ----------------------------------------------------
        # 8. VALIDATE SILVER TABLE
        # ----------------------------------------------------

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
                ) AS missing_r15

            FROM pg.silver.fact_market_snapshots
        """).fetchone()

        (
            total_rows,
            unique_coins,
            first_snapshot,
            last_snapshot,
            invalid_prices,
            missing_r5,
            missing_r15
        ) = stats

        if total_rows == 0:
            raise RuntimeError(
                "A Silver tábla üres lett."
            )

        if invalid_prices > 0:
            raise RuntimeError(
                f"Hibás árak találhatók a Silver táblában: "
                f"{invalid_prices}"
            )

        # ----------------------------------------------------
        # 9. LOG RESULTS
        # ----------------------------------------------------

        elapsed = time.perf_counter() - start_time

        logger.info("------------------------------------------")
        logger.info("SIGNAL SILVER TRANSFORMATION SUCCESS")
        logger.info("------------------------------------------")

        logger.info("Silver table: silver.fact_market_snapshots")
        logger.info("Total rows: %d", total_rows)
        logger.info("Unique coins: %d", unique_coins)

        logger.info("First snapshot: %s", first_snapshot)
        logger.info("Last snapshot: %s", last_snapshot)

        logger.info("Missing r5: %d", missing_r5)
        logger.info("Missing r15: %d", missing_r15)

        logger.info("Invalid prices: %d", invalid_prices)

        logger.info("Execution time: %.2f sec", elapsed)

        logger.info("------------------------------------------")

        return total_rows

    except Exception:

        logger.exception(
            "Signal Silver transzformációs hiba"
        )

        raise

    finally:

        conn.close()


# ============================================================
# STANDALONE TEST
# ============================================================

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

    run_signal_silver_transformation()
