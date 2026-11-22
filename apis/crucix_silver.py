# 2026.11.22  17.00
import logging
import os
import re
import threading
import duckdb

logger = logging.getLogger("osint_silver")
SILVER_LOCK = threading.Lock()

CRYPTO_KEYWORDS = [
    "crypto", "bitcoin", "btc", "eth", "ethereum", "sol", "sui", "meme", "defi", "rwa", "tokenization", "fed", "federal reserve",
    "inflation", "interest rate", "treasury", "financial market", "stock market", "oil price"]

KEYWORD_PATTERN = r"\b(" + "|".join(re.escape(w) for w in CRYPTO_KEYWORDS) + r")\b"


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _get_attach_dsn() -> str:
    if os.getenv("PG_CONN"):
        return os.environ["PG_CONN"]
    host = os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST", "postgresql")
    port = os.getenv("POSTGRES_PORT") or os.getenv("DB_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME", "n8n")
    user = os.getenv("POSTGRES_USER") or os.getenv("DB_USER", "sql_admin")
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD", "sql_pass")
    return (
        f"host={_sql_literal(host)} "
        f"port={_sql_literal(port)} "
        f"dbname={_sql_literal(dbname)} "
        f"user={_sql_literal(user)} "
        f"password={_sql_literal(password)}"
    )


def _has_table(conn, schema: str, table: str) -> bool:
    n = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog = 'pg' AND table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()[0]
    return n > 0


def run_osint_silver_transformation() -> int:
    """Létrehozza/frissíti a silver.osint_feeds táblát a bronze.osint_feeds alapján."""
    with SILVER_LOCK:
        attach_dsn = _get_attach_dsn()
        conn = duckdb.connect()
        try:
            try:
                conn.execute("LOAD postgres;")
            except duckdb.IOException:
                conn.execute("INSTALL postgres; LOAD postgres;")
            conn.execute(f"ATTACH '{attach_dsn}' AS pg (TYPE POSTGRES);")
            conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver")

            if not _has_table(conn, "bronze", "osint_feeds"):
                logger.warning("OSINT Silver kihagyva: a bronze.osint_feeds tábla még nem létezik")
                return 0

            conn.execute(f"""
                CREATE OR REPLACE TABLE pg.silver.osint_feeds AS
                WITH cleaned AS (
                    SELECT
                        record_id::VARCHAR AS record_id,
                        NULLIF(TRIM(kind::VARCHAR), '') AS kind,
                        TRY_CAST(published_at AS TIMESTAMPTZ) AS published_at,
                        TRY_CAST(fetched_at AS TIMESTAMPTZ) AS fetched_at,
                        COALESCE(TRY_CAST(urgent AS BOOLEAN), FALSE) AS urgent,
                        NULLIF(TRIM(title::VARCHAR), '') AS title,
                        NULLIF(TRIM(content::VARCHAR), '') AS content,
                        NULLIF(TRIM(source::VARCHAR), '') AS source,
                        NULLIF(TRIM(region::VARCHAR), '') AS region,
                        NULLIF(TRIM(url::VARCHAR), '') AS url,
                        ROW_NUMBER() OVER (
                            PARTITION BY record_id ORDER BY fetched_at DESC NULLS LAST
                        ) AS rn
                    FROM pg.bronze.osint_feeds
                    WHERE record_id IS NOT NULL
                      AND (NULLIF(TRIM(title::VARCHAR), '') IS NOT NULL
                           OR NULLIF(TRIM(content::VARCHAR), '') IS NOT NULL)
                ), keyword_hits AS (
                    SELECT *,
                        regexp_matches(
                            LOWER(COALESCE(title, '') || ' ' || COALESCE(content, '') || ' ' ||
                                  COALESCE(source, '') || ' ' || COALESCE(region, '')),
                            '{KEYWORD_PATTERN}'
                        ) AS is_crypto_relevant
                    FROM cleaned
                    WHERE rn = 1
                )
                SELECT
                    record_id,
                    COALESCE(kind, 'unknown') AS kind,
                    published_at, fetched_at, urgent,
                    title, content, source, region, url,
                    is_crypto_relevant,
                    CAST((CASE WHEN urgent THEN 100 ELSE 0 END)
                        + (CASE WHEN is_crypto_relevant THEN 20 ELSE 0 END)
                        AS INTEGER) AS importance_score,
                    CURRENT_TIMESTAMP AS transformed_at
                FROM keyword_hits
                WHERE is_crypto_relevant
            """)
            count = conn.execute("SELECT COUNT(*) FROM pg.silver.osint_feeds").fetchone()[0]
            logger.info("OSINT Silver transzformáció sikeres: silver.osint_feeds, %d sor", count)
            return count
        except Exception:
            logger.exception("OSINT silver transzformációs hiba")
            raise
        finally:
            conn.close()
