import logging
import os
import duckdb

logger = logging.getLogger("cryptonews_silver")

def _sql_literal(value: str) -> str:
    """Escape SQL string literal for DuckDB ATTACH connection string."""
    return value.replace("'", "''")

def run_silver_transformation() -> int:
    """Létrehozza/frissíti a silver.cryptonews táblát a bronze.cryptonews alapján."""
    host = os.getenv("DB_HOST", "postgresql")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "n8n")
    username = os.getenv("DB_USER", "sql_admin")
    password = os.getenv("DB_PASSWORD", "sql_pass")
    attach_dsn = (
        f"host={_sql_literal(host)} port={_sql_literal(port)} "
        f"dbname={_sql_literal(database)} user={_sql_literal(username)} "
        f"password={_sql_literal(password)}"
    )

    conn = duckdb.connect()
    try:
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute(f"ATTACH '{attach_dsn}' AS pg (TYPE POSTGRES);")
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver")
        conn.execute("""
            CREATE OR REPLACE TABLE pg.silver.cryptonews AS
            WITH cleaned AS (
                SELECT
                    record_id::VARCHAR AS record_id,
                    external_id::VARCHAR AS external_id,
                    LOWER(NULLIF(TRIM(provider::VARCHAR), '')) AS provider,
                    NULLIF(TRIM(kind::VARCHAR), '') AS kind,
                    TRY_CAST(published_at AS TIMESTAMPTZ) AS published_at,
                    TRY_CAST(fetched_at AS TIMESTAMPTZ) AS fetched_at,
                    COALESCE(TRY_CAST(urgent AS BOOLEAN), FALSE) AS urgent,
                    NULLIF(TRIM(title::VARCHAR), '') AS title,
                    NULLIF(TRIM(content::VARCHAR), '') AS content,
                    NULLIF(TRIM(source::VARCHAR), '') AS source,
                    NULLIF(TRIM(url::VARCHAR), '') AS url,
                    GREATEST(COALESCE(TRY_CAST(like_count AS BIGINT), 0), 0) AS like_count,
                    GREATEST(COALESCE(TRY_CAST(retweet_count AS BIGINT), 0), 0) AS retweet_count,
                    GREATEST(COALESCE(TRY_CAST(reply_count AS BIGINT), 0), 0) AS reply_count,
                    GREATEST(COALESCE(TRY_CAST(view_count AS BIGINT), 0), 0) AS view_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY record_id ORDER BY fetched_at DESC NULLS LAST
                    ) AS rn
                FROM pg.bronze.cryptonews
                WHERE record_id IS NOT NULL
                  AND (NULLIF(TRIM(title::VARCHAR), '') IS NOT NULL
                       OR NULLIF(TRIM(content::VARCHAR), '') IS NOT NULL)
            ), scored AS (
                SELECT *, like_count + 3 * retweet_count + 2 * reply_count AS engagement_score
                FROM cleaned
                WHERE rn = 1
            )
            SELECT
                record_id, external_id, COALESCE(provider, 'unknown') AS provider, kind,
                published_at, fetched_at, urgent, title, content, source, url,
                like_count, retweet_count, reply_count, view_count, engagement_score,
                CAST((CASE WHEN urgent THEN 100 ELSE 0 END) +
                    LEAST(50, FLOOR(LN(1 + engagement_score::DOUBLE) * 5)) AS INTEGER)
                    AS importance_score,
                CURRENT_TIMESTAMP AS transformed_at
            FROM scored
        """)
        count = conn.execute("SELECT COUNT(*) FROM pg.silver.cryptonews").fetchone()[0]
        logger.info("DuckDB Silver transzformáció sikeres: silver.cryptonews, %d sor", count)
        return count
    except Exception:
        logger.exception("Silver transzformációs hiba")
        raise
    finally:
        conn.close()


