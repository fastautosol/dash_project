# 2026.11.24  15.00
import logging
import os
import threading
from pathlib import Path

import duckdb

logger = logging.getLogger("silver_transformation")

_silver_lock = threading.Lock()

PG_ATTACH_DSN = (
    f"dbname={os.getenv('POSTGRES_DB', 'n8n')} "
    f"user={os.getenv('POSTGRES_USER', 'sql_admin')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'sql_pass')} "
    f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')}"
)

INIT_FX_SQL = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.fx_rates (
    currency        TEXT PRIMARY KEY,
    rate_to_huf     NUMERIC(12, 4) NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO silver.fx_rates (currency, rate_to_huf)
VALUES ('HUF', 1.0000), ('EUR', 402.0000), ('USD', 375.0000)
ON CONFLICT (currency) DO NOTHING;
"""


def _load_bronze_catalog(con: duckdb.DuckDBPyConnection) -> dict[str, set[str]]:
    rows = con.execute(
        """
        SELECT table_name, column_name
        FROM duckdb_columns()
        WHERE database_name = 'pg' AND schema_name = 'bronze_api'
        """
    ).fetchall()

    catalog: dict[str, set[str]] = {}
    for tbl, col_name in rows:
        catalog.setdefault(tbl, set()).add(col_name)
    return catalog


def _col(catalog: dict[str, set[str]], table: str, alias: str, column: str, fallback: str = "NULL") -> str:
    if column in catalog.get(table, set()):
        return f"{alias}.{column}"
    return fallback


def _build_silver_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    union_parts: list[str],
    empty_fallback_sql: str,
    index_col: str | None = None,
) -> int:
    select_sql = " UNION ALL ".join(union_parts) if union_parts else empty_fallback_sql
    con.execute(f"CREATE OR REPLACE TABLE pg.silver.{table_name} AS {select_sql};")

    if index_col:
        con.execute(
            f"CALL postgres_execute('pg', 'CREATE INDEX IF NOT EXISTS idx_silver_{table_name}_{index_col} ON silver.{table_name} ({index_col});');"
        )

    row = con.execute(f"SELECT COUNT(*) FROM pg.silver.{table_name}").fetchone()
    return int(row[0]) if row else 0


def run_silver_transformation() -> dict[str, int]:
    """
    DuckDB CTAS alapú Bronze -> Silver transzformáció.
    A nyers bronze_api táblákból 7 egységes, típusos, HUF-ra váltott silver táblát épít.
    """
    with _silver_lock:
        Path("/tmp/duckdb_silver_tmp").mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET memory_limit = '128MB';")
            con.execute("SET threads = 2;")
            con.execute("SET temp_directory = '/tmp/duckdb_silver_tmp';")
            con.execute("LOAD postgres;")
            con.execute(f"ATTACH '{PG_ATTACH_DSN}' AS pg (TYPE POSTGRES);")

            con.execute(f"CALL postgres_execute('pg', $${INIT_FX_SQL}$$);")
            cat = _load_bronze_catalog(con)
            summary: dict[str, int] = {}

            # ----------------------------------------------------------------
            # 1. SILVER.AD_SPEND_DAILY
            # ----------------------------------------------------------------
            parts: list[str] = []
            if "meta_ads_insights" in cat:
                actions_cte = (
                    """
                    SELECT _dlt_parent_id, COALESCE(SUM(TRY_CAST(value AS DOUBLE)), 0) AS purchases
                    FROM pg.bronze_api.meta_ads_insights__actions
                    WHERE action_type = 'purchase'
                    GROUP BY _dlt_parent_id
                    """
                    if "meta_ads_insights__actions" in cat
                    else "SELECT NULL::VARCHAR AS _dlt_parent_id, 0.0 AS purchases WHERE FALSE"
                )
                values_cte = (
                    """
                    SELECT _dlt_parent_id, COALESCE(SUM(TRY_CAST(value AS DOUBLE)), 0) AS purchase_value
                    FROM pg.bronze_api.meta_ads_insights__action_values
                    WHERE action_type = 'purchase'
                    GROUP BY _dlt_parent_id
                    """
                    if "meta_ads_insights__action_values" in cat
                    else "SELECT NULL::VARCHAR AS _dlt_parent_id, 0.0 AS purchase_value WHERE FALSE"
                )

                c_pub = _col(cat, "meta_ads_insights", "m", "publisher_platform", "'meta_other'")
                c_acc = _col(cat, "meta_ads_insights", "m", "account_id", "'0'")
                c_cid = _col(cat, "meta_ads_insights", "m", "campaign_id", "'0'")
                c_cname = _col(cat, "meta_ads_insights", "m", "campaign_name")
                c_aid = _col(cat, "meta_ads_insights", "m", "adset_id", "'0'")
                c_aname = _col(cat, "meta_ads_insights", "m", "adset_name")
                c_curr = _col(cat, "meta_ads_insights", "m", "account_currency", "'HUF'")

                parts.append(
                    f"""
                    WITH meta_act AS ({actions_cte}),
                         meta_val AS ({values_cte})
                    SELECT
                        CAST(CASE
                            WHEN LOWER(COALESCE(CAST({c_pub} AS VARCHAR), '')) IN ('facebook', 'instagram')
                                THEN LOWER(CAST({c_pub} AS VARCHAR))
                            ELSE 'meta_other'
                        END AS VARCHAR) AS channel,
                        CAST('meta' AS VARCHAR) AS source_system,
                        CAST(COALESCE(CAST({c_acc} AS VARCHAR), '0') AS VARCHAR) AS account_id,
                        CAST(COALESCE(CAST({c_cid} AS VARCHAR), '0') AS VARCHAR) AS campaign_id,
                        CAST({c_cname} AS VARCHAR) AS campaign_name,
                        CAST(COALESCE(CAST({c_aid} AS VARCHAR), '0') AS VARCHAR) AS adgroup_id,
                        CAST({c_aname} AS VARCHAR) AS adgroup_name,
                        TRY_CAST(m.date_start AS DATE) AS spend_date,
                        CAST(UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF')) AS VARCHAR) AS original_currency,
                        CAST(COALESCE(TRY_CAST(m.spend AS DOUBLE), 0) AS DECIMAL(14, 2)) AS spend_original,
                        CAST(ROUND(COALESCE(TRY_CAST(m.spend AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS spend_huf,
                        COALESCE(TRY_CAST(m.impressions AS BIGINT), 0) AS impressions,
                        COALESCE(TRY_CAST(m.clicks AS BIGINT), 0) AS clicks,
                        0::BIGINT AS video_views,
                        CAST(COALESCE(act.purchases, 0) AS DECIMAL(12, 2)) AS conversions,
                        CAST(ROUND(COALESCE(val.purchase_value, 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS conversion_value_huf,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.meta_ads_insights m
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    LEFT JOIN meta_act act ON act._dlt_parent_id = m._dlt_id
                    LEFT JOIN meta_val val ON val._dlt_parent_id = m._dlt_id
                    WHERE TRY_CAST(m.date_start AS DATE) IS NOT NULL
                    """
                )

            if "google_ads_insights" in cat:
                c_type = _col(cat, "google_ads_insights", "g", "campaign__advertising_channel_type", "''")
                c_acc = _col(cat, "google_ads_insights", "g", "customer__id", "'0'")
                c_cid = _col(cat, "google_ads_insights", "g", "campaign__id", "'0'")
                c_cname = _col(cat, "google_ads_insights", "g", "campaign__name")
                c_aid = _col(cat, "google_ads_insights", "g", "adgroup__id", "'0'")
                c_aname = _col(cat, "google_ads_insights", "g", "adgroup__name")
                c_curr = _col(cat, "google_ads_insights", "g", "customer__currency_code", "'HUF'")
                c_vviews = _col(cat, "google_ads_insights", "g", "metrics__video_views", "0")
                c_conv = _col(cat, "google_ads_insights", "g", "metrics__conversions", "0")
                c_cval = _col(cat, "google_ads_insights", "g", "metrics__conversions_value", "0")

                parts.append(
                    f"""
                    SELECT
                        CAST(CASE
                            WHEN UPPER(COALESCE(CAST({c_type} AS VARCHAR), '')) IN ('VIDEO', 'DEMAND_GEN')
                                THEN 'youtube'
                            ELSE 'google_ads'
                        END AS VARCHAR) AS channel,
                        CAST('google_ads' AS VARCHAR) AS source_system,
                        CAST(COALESCE(CAST({c_acc} AS VARCHAR), '0') AS VARCHAR) AS account_id,
                        CAST(COALESCE(CAST({c_cid} AS VARCHAR), '0') AS VARCHAR) AS campaign_id,
                        CAST({c_cname} AS VARCHAR) AS campaign_name,
                        CAST(COALESCE(CAST({c_aid} AS VARCHAR), '0') AS VARCHAR) AS adgroup_id,
                        CAST({c_aname} AS VARCHAR) AS adgroup_name,
                        TRY_CAST(g.segments__date AS DATE) AS spend_date,
                        CAST(UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF')) AS VARCHAR) AS original_currency,
                        CAST(ROUND(COALESCE(TRY_CAST(g.metrics__cost_micros AS DOUBLE), 0) / 1000000.0, 2) AS DECIMAL(14, 2)) AS spend_original,
                        CAST(ROUND((COALESCE(TRY_CAST(g.metrics__cost_micros AS DOUBLE), 0) / 1000000.0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS spend_huf,
                        COALESCE(TRY_CAST(g.metrics__impressions AS BIGINT), 0) AS impressions,
                        COALESCE(TRY_CAST(g.metrics__clicks AS BIGINT), 0) AS clicks,
                        COALESCE(TRY_CAST({c_vviews} AS BIGINT), 0) AS video_views,
                        CAST(COALESCE(TRY_CAST({c_conv} AS DOUBLE), 0) AS DECIMAL(12, 2)) AS conversions,
                        CAST(ROUND(COALESCE(TRY_CAST({c_cval} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS conversion_value_huf,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.google_ads_insights g
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    WHERE TRY_CAST(g.segments__date AS DATE) IS NOT NULL
                    """
                )

            if "tiktok_ads_insights" in cat:
                c_acc = _col(cat, "tiktok_ads_insights", "t", "advertiser_id", "'0'")
                c_cid = _col(cat, "tiktok_ads_insights", "t", "dimensions__campaign_id", "'0'")
                c_cname = _col(cat, "tiktok_ads_insights", "t", "metrics__campaign_name")
                c_aid = _col(cat, "tiktok_ads_insights", "t", "dimensions__adgroup_id", "'0'")
                c_aname = _col(cat, "tiktok_ads_insights", "t", "metrics__adgroup_name")
                c_curr = _col(cat, "tiktok_ads_insights", "t", "currency", "'EUR'")
                c_cpay = _col(cat, "tiktok_ads_insights", "t", "metrics__complete_payment", "NULL")
                c_conv = _col(cat, "tiktok_ads_insights", "t", "metrics__conversion", "0")
                c_cval = _col(cat, "tiktok_ads_insights", "t", "metrics__total_complete_payment_rate", "0")

                parts.append(
                    f"""
                    SELECT
                        CAST('tiktok' AS VARCHAR) AS channel,
                        CAST('tiktok' AS VARCHAR) AS source_system,
                        CAST(COALESCE(CAST({c_acc} AS VARCHAR), '0') AS VARCHAR) AS account_id,
                        CAST(COALESCE(CAST({c_cid} AS VARCHAR), '0') AS VARCHAR) AS campaign_id,
                        CAST({c_cname} AS VARCHAR) AS campaign_name,
                        CAST(COALESCE(CAST({c_aid} AS VARCHAR), '0') AS VARCHAR) AS adgroup_id,
                        CAST({c_aname} AS VARCHAR) AS adgroup_name,
                        TRY_CAST(LEFT(CAST(t.dimensions__stat_time_day AS VARCHAR), 10) AS DATE) AS spend_date,
                        CAST(UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'EUR')) AS VARCHAR) AS original_currency,
                        CAST(COALESCE(TRY_CAST(t.metrics__spend AS DOUBLE), 0) AS DECIMAL(14, 2)) AS spend_original,
                        CAST(ROUND(COALESCE(TRY_CAST(t.metrics__spend AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 402.0), 2) AS DECIMAL(14, 2)) AS spend_huf,
                        COALESCE(TRY_CAST(t.metrics__impressions AS BIGINT), 0) AS impressions,
                        COALESCE(TRY_CAST(t.metrics__clicks AS BIGINT), 0) AS clicks,
                        0::BIGINT AS video_views,
                        CAST(COALESCE(TRY_CAST({c_cpay} AS DOUBLE), TRY_CAST({c_conv} AS DOUBLE), 0) AS DECIMAL(12, 2)) AS conversions,
                        CAST(ROUND(COALESCE(TRY_CAST({c_cval} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 402.0), 2) AS DECIMAL(14, 2)) AS conversion_value_huf,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.tiktok_ads_insights t
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'EUR'))
                    WHERE TRY_CAST(LEFT(CAST(t.dimensions__stat_time_day AS VARCHAR), 10) AS DATE) IS NOT NULL
                    """
                )

            empty_ads = """
                SELECT
                    NULL::VARCHAR AS channel, NULL::VARCHAR AS source_system, NULL::VARCHAR AS account_id,
                    NULL::VARCHAR AS campaign_id, NULL::VARCHAR AS campaign_name, NULL::VARCHAR AS adgroup_id,
                    NULL::VARCHAR AS adgroup_name, NULL::DATE AS spend_date, NULL::VARCHAR AS original_currency,
                    0::DECIMAL(14, 2) AS spend_original, 0::DECIMAL(14, 2) AS spend_huf,
                    0::BIGINT AS impressions, 0::BIGINT AS clicks, 0::BIGINT AS video_views,
                    0::DECIMAL(12, 2) AS conversions, 0::DECIMAL(14, 2) AS conversion_value_huf,
                    NOW() AS refreshed_at
                WHERE FALSE
            """
            summary["ad_spend_daily"] = _build_silver_table(con, "ad_spend_daily", parts, empty_ads, "spend_date")

            # ----------------------------------------------------------------
            # 2. SILVER.ORDERS
            # ----------------------------------------------------------------
            parts = []
            if "shopify_orders" in cat:
                c_canc = _col(cat, "shopify_orders", "o", "cancelled_at", "NULL")
                c_fin = _col(cat, "shopify_orders", "o", "financial_status", "''")
                c_ful = _col(cat, "shopify_orders", "o", "fulfillment_status", "''")
                c_src = _col(cat, "shopify_orders", "o", "source_name")
                c_curr = _col(cat, "shopify_orders", "o", "currency", "'HUF'")
                c_tax = _col(cat, "shopify_orders", "o", "total_tax", "0")
                c_sub = _col(cat, "shopify_orders", "o", "subtotal_price", "o.total_price")
                c_disc = _col(cat, "shopify_orders", "o", "total_discounts", "0")
                c_mail = _col(cat, "shopify_orders", "o", "email")
                c_comp = _col(cat, "shopify_orders", "o", "billing_address__company")
                c_bname = _col(cat, "shopify_orders", "o", "billing_address__name")
                c_city = _col(cat, "shopify_orders", "o", "billing_address__city")
                c_land = _col(cat, "shopify_orders", "o", "landing_site", "''")

                parts.append(
                    f"""
                    SELECT
                        CAST('shopify' AS VARCHAR) AS source_system,
                        CAST(o.id AS VARCHAR) AS order_id,
                        CAST(o.name AS VARCHAR) AS order_number,
                        TRY_CAST(o.created_at AS TIMESTAMPTZ) AS order_created_at,
                        CAST(TRY_CAST(o.created_at AS TIMESTAMPTZ) AT TIME ZONE 'Europe/Budapest' AS DATE) AS order_date,
                        CAST(CASE
                            WHEN {c_canc} IS NOT NULL OR CAST({c_fin} AS VARCHAR) = 'voided' THEN 'cancelled'
                            WHEN CAST({c_fin} AS VARCHAR) = 'refunded' THEN 'refunded'
                            WHEN CAST({c_ful} AS VARCHAR) = 'fulfilled' AND CAST({c_fin} AS VARCHAR) = 'paid' THEN 'completed'
                            ELSE 'pending'
                        END AS VARCHAR) AS order_status,
                        CAST(COALESCE(CAST({c_fin} AS VARCHAR), '') || '/' || COALESCE(CAST({c_ful} AS VARCHAR), 'unfulfilled') AS VARCHAR) AS raw_status,
                        CAST(CASE
                            WHEN CAST({c_fin} AS VARCHAR) IN ('paid', 'partially_paid') THEN 'paid'
                            WHEN CAST({c_fin} AS VARCHAR) = 'refunded' THEN 'refunded'
                            ELSE 'unpaid'
                        END AS VARCHAR) AS payment_status,
                        CAST({c_src} AS VARCHAR) AS payment_method,
                        CAST('Shipping' AS VARCHAR) AS shipping_method,
                        CAST(UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF')) AS VARCHAR) AS currency,
                        CAST(ROUND((COALESCE(TRY_CAST(o.total_price AS DOUBLE), 0) - COALESCE(TRY_CAST({c_tax} AS DOUBLE), 0)) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS net_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST({c_tax} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS tax_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(o.total_price AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS gross_total_huf,
                        CAST(ROUND((COALESCE(TRY_CAST(o.total_price AS DOUBLE), 0) - COALESCE(TRY_CAST({c_sub} AS DOUBLE), 0)) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS shipping_gross_huf,
                        CAST(ROUND(COALESCE(TRY_CAST({c_disc} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS discount_gross_huf,
                        CAST(LOWER(TRIM(CAST({c_mail} AS VARCHAR))) AS VARCHAR) AS customer_email,
                        CAST(COALESCE(NULLIF(TRIM(CAST({c_comp} AS VARCHAR)), ''), CAST({c_bname} AS VARCHAR)) AS VARCHAR) AS customer_name,
                        NULL::VARCHAR AS customer_tax_number,
                        CAST({c_city} AS VARCHAR) AS customer_city,
                        CAST(LOWER(COALESCE(NULLIF(regexp_extract(COALESCE(CAST({c_land} AS VARCHAR), ''), 'utm_source=([^&]+)', 1), ''), 'direct')) AS VARCHAR) AS utm_source,
                        CAST(LOWER(COALESCE(NULLIF(regexp_extract(COALESCE(CAST({c_land} AS VARCHAR), ''), 'utm_medium=([^&]+)', 1), ''), 'none')) AS VARCHAR) AS utm_medium,
                        CAST(NULLIF(regexp_extract(COALESCE(CAST({c_land} AS VARCHAR), ''), 'utm_campaign=([^&]+)', 1), '') AS VARCHAR) AS utm_campaign,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.shopify_orders o
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    """
                )

            if "unas_orders" in cat:
                c_pstat = _col(cat, "unas_orders", "u", "payment__status", "''")
                c_pname = _col(cat, "unas_orders", "u", "payment__name")
                c_sname = _col(cat, "unas_orders", "u", "shipping__name")
                c_curr = _col(cat, "unas_orders", "u", "currency", "'HUF'")
                c_scost = _col(cat, "unas_orders", "u", "shipping__cost_gross", "0")
                c_mail = _col(cat, "unas_orders", "u", "customer__email")
                c_cname = _col(cat, "unas_orders", "u", "customer__name")
                c_taxn = _col(cat, "unas_orders", "u", "customer__tax_number")
                c_city = _col(cat, "unas_orders", "u", "customer__city")
                c_usrc = _col(cat, "unas_orders", "u", "utm__utm_source", "'direct'")
                c_umed = _col(cat, "unas_orders", "u", "utm__utm_medium", "'none'")
                c_ucmp = _col(cat, "unas_orders", "u", "utm__utm_campaign")

                parts.append(
                    f"""
                    SELECT
                        CAST('unas' AS VARCHAR) AS source_system,
                        CAST(u.key AS VARCHAR) AS order_id,
                        CAST(u.key AS VARCHAR) AS order_number,
                        TRY_STRPTIME(CAST(u.date AS VARCHAR), '%Y.%m.%d %H:%M:%S') AT TIME ZONE 'Europe/Budapest' AS order_created_at,
                        CAST(TRY_STRPTIME(CAST(u.date AS VARCHAR), '%Y.%m.%d %H:%M:%S') AS DATE) AS order_date,
                        CAST(CASE
                            WHEN LOWER(COALESCE(CAST(u.status AS VARCHAR), '')) LIKE '%töröl%'
                              OR LOWER(COALESCE(CAST(u.status AS VARCHAR), '')) LIKE '%sztornó%' THEN 'cancelled'
                            WHEN LOWER(COALESCE(CAST(u.status AS VARCHAR), '')) LIKE '%visszaküld%' THEN 'refunded'
                            WHEN LOWER(COALESCE(CAST(u.status AS VARCHAR), '')) LIKE '%teljesít%' THEN 'completed'
                            ELSE 'pending'
                        END AS VARCHAR) AS order_status,
                        CAST(u.status AS VARCHAR) AS raw_status,
                        CAST(CASE WHEN LOWER(COALESCE(CAST({c_pstat} AS VARCHAR), '')) = 'paid' THEN 'paid' ELSE 'unpaid' END AS VARCHAR) AS payment_status,
                        CAST({c_pname} AS VARCHAR) AS payment_method,
                        CAST({c_sname} AS VARCHAR) AS shipping_method,
                        CAST(UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF')) AS VARCHAR) AS currency,
                        CAST(ROUND(COALESCE(TRY_CAST(u.sum_price_net AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS net_total_huf,
                        CAST(ROUND((COALESCE(TRY_CAST(u.sum_price_gross AS DOUBLE), 0) - COALESCE(TRY_CAST(u.sum_price_net AS DOUBLE), 0)) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS tax_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(u.sum_price_gross AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS gross_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST({c_scost} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS shipping_gross_huf,
                        0::DECIMAL(14, 2) AS discount_gross_huf,
                        CAST(LOWER(TRIM(CAST({c_mail} AS VARCHAR))) AS VARCHAR) AS customer_email,
                        CAST({c_cname} AS VARCHAR) AS customer_name,
                        CAST(NULLIF(TRIM(CAST({c_taxn} AS VARCHAR)), '') AS VARCHAR) AS customer_tax_number,
                        CAST({c_city} AS VARCHAR) AS customer_city,
                        CAST(LOWER(COALESCE(NULLIF(TRIM(CAST({c_usrc} AS VARCHAR)), ''), 'direct')) AS VARCHAR) AS utm_source,
                        CAST(LOWER(COALESCE(NULLIF(TRIM(CAST({c_umed} AS VARCHAR)), ''), 'none')) AS VARCHAR) AS utm_medium,
                        CAST(NULLIF(TRIM(CAST({c_ucmp} AS VARCHAR)), '') AS VARCHAR) AS utm_campaign,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.unas_orders u
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    """
                )

            if "shoprenter_orders" in cat:
                c_inv = _col(cat, "shoprenter_orders", "s", "invoiceid")
                c_inn = _col(cat, "shoprenter_orders", "s", "innerid")
                c_st = _col(cat, "shoprenter_orders", "s", "order_status__name", "''")
                c_pm = _col(cat, "shoprenter_orders", "s", "paymentmethodname")
                c_sm = _col(cat, "shoprenter_orders", "s", "shippingmethodname")
                c_curr = _col(cat, "shoprenter_orders", "s", "currency__code", "'HUF'")
                c_tnet = _col(cat, "shoprenter_orders", "s", "totalnet", "0")
                c_ttax = _col(cat, "shoprenter_orders", "s", "totaltax", "0")
                c_stot = _col(cat, "shoprenter_orders", "s", "shippingtotal", "0")
                c_mail = _col(cat, "shoprenter_orders", "s", "email")
                c_pcomp = _col(cat, "shoprenter_orders", "s", "paymentcompany")
                c_lname = _col(cat, "shoprenter_orders", "s", "lastname", "''")
                c_fname = _col(cat, "shoprenter_orders", "s", "firstname", "''")
                c_taxn = _col(cat, "shoprenter_orders", "s", "paymenttaxnumber")
                c_city = _col(cat, "shoprenter_orders", "s", "paymentcity")
                c_usrc = _col(cat, "shoprenter_orders", "s", "utmsource", "'direct'")
                c_umed = _col(cat, "shoprenter_orders", "s", "utmmedium", "'none'")
                c_ucmp = _col(cat, "shoprenter_orders", "s", "utmcampaign")

                parts.append(
                    f"""
                    SELECT
                        CAST('shoprenter' AS VARCHAR) AS source_system,
                        CAST(s.id AS VARCHAR) AS order_id,
                        CAST(COALESCE(CAST({c_inv} AS VARCHAR), CAST({c_inn} AS VARCHAR)) AS VARCHAR) AS order_number,
                        TRY_CAST(s.datecreated AS TIMESTAMP) AT TIME ZONE 'Europe/Budapest' AS order_created_at,
                        TRY_CAST(s.datecreated AS DATE) AS order_date,
                        CAST(CASE
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%sztornó%'
                              OR LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%töröl%' THEN 'cancelled'
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%vissza%' THEN 'refunded'
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%teljesít%' THEN 'completed'
                            ELSE 'pending'
                        END AS VARCHAR) AS order_status,
                        CAST({c_st} AS VARCHAR) AS raw_status,
                        CAST(CASE WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%teljesít%' THEN 'paid' ELSE 'unpaid' END AS VARCHAR) AS payment_status,
                        CAST({c_pm} AS VARCHAR) AS payment_method,
                        CAST({c_sm} AS VARCHAR) AS shipping_method,
                        CAST(UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF')) AS VARCHAR) AS currency,
                        CAST(ROUND(COALESCE(TRY_CAST({c_tnet} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS net_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST({c_ttax} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS tax_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(s.total AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS gross_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST({c_stot} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS shipping_gross_huf,
                        0::DECIMAL(14, 2) AS discount_gross_huf,
                        CAST(LOWER(TRIM(CAST({c_mail} AS VARCHAR))) AS VARCHAR) AS customer_email,
                        CAST(COALESCE(NULLIF(TRIM(CAST({c_pcomp} AS VARCHAR)), ''), TRIM(COALESCE(CAST({c_lname} AS VARCHAR), '') || ' ' || COALESCE(CAST({c_fname} AS VARCHAR), ''))) AS VARCHAR) AS customer_name,
                        CAST(NULLIF(TRIM(CAST({c_taxn} AS VARCHAR)), '') AS VARCHAR) AS customer_tax_number,
                        CAST({c_city} AS VARCHAR) AS customer_city,
                        CAST(LOWER(COALESCE(NULLIF(TRIM(CAST({c_usrc} AS VARCHAR)), ''), 'direct')) AS VARCHAR) AS utm_source,
                        CAST(LOWER(COALESCE(NULLIF(TRIM(CAST({c_umed} AS VARCHAR)), ''), 'none')) AS VARCHAR) AS utm_medium,
                        CAST(NULLIF(TRIM(CAST({c_ucmp} AS VARCHAR)), '') AS VARCHAR) AS utm_campaign,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.shoprenter_orders s
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    """
                )

            if "woocommerce_orders" in cat:
                wc_meta_cte = (
                    """
                    SELECT
                        _dlt_parent_id,
                        MAX(CASE WHEN key = '_billing_tax_number' THEN CAST(value AS VARCHAR) END) AS tax_number,
                        MAX(CASE WHEN key = '_wc_order_attribution_utm_source' THEN CAST(value AS VARCHAR) END) AS utm_source,
                        MAX(CASE WHEN key = '_wc_order_attribution_utm_medium' THEN CAST(value AS VARCHAR) END) AS utm_medium,
                        MAX(CASE WHEN key = '_wc_order_attribution_utm_campaign' THEN CAST(value AS VARCHAR) END) AS utm_campaign
                    FROM pg.bronze_api.woocommerce_orders__meta_data
                    GROUP BY _dlt_parent_id
                    """
                    if "woocommerce_orders__meta_data" in cat
                    else "SELECT NULL::VARCHAR AS _dlt_parent_id, NULL::VARCHAR AS tax_number, NULL::VARCHAR AS utm_source, NULL::VARCHAR AS utm_medium, NULL::VARCHAR AS utm_campaign WHERE FALSE"
                )

                c_num = _col(cat, "woocommerce_orders", "w", "number", "w.id")
                c_pm = _col(cat, "woocommerce_orders", "w", "payment_method_title")
                c_curr = _col(cat, "woocommerce_orders", "w", "currency", "'HUF'")
                c_ttax = _col(cat, "woocommerce_orders", "w", "total_tax", "0")
                c_stot = _col(cat, "woocommerce_orders", "w", "shipping_total", "0")
                c_stax = _col(cat, "woocommerce_orders", "w", "shipping_tax", "0")
                c_disc = _col(cat, "woocommerce_orders", "w", "discount_total", "0")
                c_mail = _col(cat, "woocommerce_orders", "w", "billing__email")
                c_comp = _col(cat, "woocommerce_orders", "w", "billing__company")
                c_lname = _col(cat, "woocommerce_orders", "w", "billing__last_name", "''")
                c_fname = _col(cat, "woocommerce_orders", "w", "billing__first_name", "''")
                c_city = _col(cat, "woocommerce_orders", "w", "billing__city")

                parts.append(
                    f"""
                    WITH wc_meta AS ({wc_meta_cte})
                    SELECT
                        CAST('woocommerce' AS VARCHAR) AS source_system,
                        CAST(w.id AS VARCHAR) AS order_id,
                        CAST({c_num} AS VARCHAR) AS order_number,
                        TRY_CAST(w.date_created AS TIMESTAMPTZ) AS order_created_at,
                        CAST(TRY_CAST(w.date_created AS TIMESTAMPTZ) AT TIME ZONE 'Europe/Budapest' AS DATE) AS order_date,
                        CAST(CASE
                            WHEN CAST(w.status AS VARCHAR) IN ('cancelled', 'failed', 'trash') THEN 'cancelled'
                            WHEN CAST(w.status AS VARCHAR) = 'refunded' THEN 'refunded'
                            WHEN CAST(w.status AS VARCHAR) = 'completed' THEN 'completed'
                            ELSE 'pending'
                        END AS VARCHAR) AS order_status,
                        CAST(w.status AS VARCHAR) AS raw_status,
                        CAST(CASE
                            WHEN CAST(w.status AS VARCHAR) = 'completed' THEN 'paid'
                            WHEN CAST(w.status AS VARCHAR) = 'refunded' THEN 'refunded'
                            ELSE 'unpaid'
                        END AS VARCHAR) AS payment_status,
                        CAST({c_pm} AS VARCHAR) AS payment_method,
                        CAST('Shipping' AS VARCHAR) AS shipping_method,
                        CAST(UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF')) AS VARCHAR) AS currency,
                        CAST(ROUND((COALESCE(TRY_CAST(w.total AS DOUBLE), 0) - COALESCE(TRY_CAST({c_ttax} AS DOUBLE), 0)) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS net_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST({c_ttax} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS tax_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(w.total AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS gross_total_huf,
                        CAST(ROUND((COALESCE(TRY_CAST({c_stot} AS DOUBLE), 0) + COALESCE(TRY_CAST({c_stax} AS DOUBLE), 0)) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS shipping_gross_huf,
                        CAST(ROUND(COALESCE(TRY_CAST({c_disc} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS discount_gross_huf,
                        CAST(LOWER(TRIM(CAST({c_mail} AS VARCHAR))) AS VARCHAR) AS customer_email,
                        CAST(COALESCE(NULLIF(TRIM(CAST({c_comp} AS VARCHAR)), ''), TRIM(COALESCE(CAST({c_lname} AS VARCHAR), '') || ' ' || COALESCE(CAST({c_fname} AS VARCHAR), ''))) AS VARCHAR) AS customer_name,
                        CAST(NULLIF(TRIM(meta.tax_number), '') AS VARCHAR) AS customer_tax_number,
                        CAST({c_city} AS VARCHAR) AS customer_city,
                        CAST(LOWER(COALESCE(NULLIF(TRIM(meta.utm_source), ''), 'direct')) AS VARCHAR) AS utm_source,
                        CAST(LOWER(COALESCE(NULLIF(TRIM(meta.utm_medium), ''), 'none')) AS VARCHAR) AS utm_medium,
                        CAST(NULLIF(TRIM(meta.utm_campaign), '') AS VARCHAR) AS utm_campaign,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.woocommerce_orders w
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    LEFT JOIN wc_meta meta ON meta._dlt_parent_id = w._dlt_id
                    """
                )

            empty_orders = """
                SELECT
                    NULL::VARCHAR AS source_system, NULL::VARCHAR AS order_id, NULL::VARCHAR AS order_number,
                    NULL::TIMESTAMPTZ AS order_created_at, NULL::DATE AS order_date, NULL::VARCHAR AS order_status,
                    NULL::VARCHAR AS raw_status, NULL::VARCHAR AS payment_status, NULL::VARCHAR AS payment_method,
                    NULL::VARCHAR AS shipping_method, NULL::VARCHAR AS currency,
                    0::DECIMAL(14, 2) AS net_total_huf, 0::DECIMAL(14, 2) AS tax_total_huf,
                    0::DECIMAL(14, 2) AS gross_total_huf, 0::DECIMAL(14, 2) AS shipping_gross_huf,
                    0::DECIMAL(14, 2) AS discount_gross_huf, NULL::VARCHAR AS customer_email,
                    NULL::VARCHAR AS customer_name, NULL::VARCHAR AS customer_tax_number, NULL::VARCHAR AS customer_city,
                    NULL::VARCHAR AS utm_source, NULL::VARCHAR AS utm_medium, NULL::VARCHAR AS utm_campaign,
                    NOW() AS refreshed_at
                WHERE FALSE
            """
            summary["orders"] = _build_silver_table(con, "orders", parts, empty_orders, "order_date")

            # ----------------------------------------------------------------
            # 3. SILVER.ORDER_ITEMS
            # ----------------------------------------------------------------
            parts = []
            if "shopify_orders__line_items" in cat and "shopify_orders" in cat:
                c_canc = _col(cat, "shopify_orders", "o", "cancelled_at", "NULL")
                c_fin = _col(cat, "shopify_orders", "o", "financial_status", "''")
                c_ful = _col(cat, "shopify_orders", "o", "fulfillment_status", "''")
                c_curr = _col(cat, "shopify_orders", "o", "currency", "'HUF'")
                c_disc = _col(cat, "shopify_orders__line_items", "li", "total_discount", "0")

                parts.append(
                    f"""
                    SELECT
                        CAST('shopify' AS VARCHAR) AS source_system,
                        CAST(o.id AS VARCHAR) AS order_id,
                        CAST(li.id AS VARCHAR) AS line_item_id,
                        CAST(TRY_CAST(o.created_at AS TIMESTAMPTZ) AT TIME ZONE 'Europe/Budapest' AS DATE) AS order_date,
                        CAST(CASE
                            WHEN {c_canc} IS NOT NULL OR CAST({c_fin} AS VARCHAR) = 'voided' THEN 'cancelled'
                            WHEN CAST({c_fin} AS VARCHAR) = 'refunded' THEN 'refunded'
                            WHEN CAST({c_ful} AS VARCHAR) = 'fulfilled' AND CAST({c_fin} AS VARCHAR) = 'paid' THEN 'completed'
                            ELSE 'pending'
                        END AS VARCHAR) AS order_status,
                        CAST(UPPER(COALESCE(NULLIF(TRIM(CAST(li.sku AS VARCHAR)), ''), 'UNKNOWN')) AS VARCHAR) AS sku,
                        CAST(li.title AS VARCHAR) AS product_name,
                        CAST(COALESCE(TRY_CAST(li.quantity AS DOUBLE), 0) AS DECIMAL(12, 3)) AS quantity,
                        CAST(ROUND((COALESCE(TRY_CAST(li.price AS DOUBLE), 0) / 1.27) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS unit_net_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(li.price AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS unit_gross_huf,
                        CAST(ROUND((((COALESCE(TRY_CAST(li.price AS DOUBLE), 0) * COALESCE(TRY_CAST(li.quantity AS DOUBLE), 0)) - COALESCE(TRY_CAST({c_disc} AS DOUBLE), 0)) / 1.27) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS line_net_huf,
                        CAST(ROUND(((COALESCE(TRY_CAST(li.price AS DOUBLE), 0) * COALESCE(TRY_CAST(li.quantity AS DOUBLE), 0)) - COALESCE(TRY_CAST({c_disc} AS DOUBLE), 0)) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS line_gross_huf,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.shopify_orders__line_items li
                    JOIN pg.bronze_api.shopify_orders o ON o._dlt_id = li._dlt_parent_id
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    """
                )

            if "unas_orders__items" in cat and "unas_orders" in cat:
                c_itid = _col(cat, "unas_orders__items", "it", "id", "it._dlt_id")
                c_curr = _col(cat, "unas_orders", "u", "currency", "'HUF'")

                parts.append(
                    f"""
                    SELECT
                        CAST('unas' AS VARCHAR) AS source_system,
                        CAST(u.key AS VARCHAR) AS order_id,
                        CAST({c_itid} AS VARCHAR) AS line_item_id,
                        CAST(TRY_STRPTIME(CAST(u.date AS VARCHAR), '%Y.%m.%d %H:%M:%S') AS DATE) AS order_date,
                        CAST(CASE
                            WHEN LOWER(COALESCE(CAST(u.status AS VARCHAR), '')) LIKE '%töröl%'
                              OR LOWER(COALESCE(CAST(u.status AS VARCHAR), '')) LIKE '%sztornó%' THEN 'cancelled'
                            WHEN LOWER(COALESCE(CAST(u.status AS VARCHAR), '')) LIKE '%visszaküld%' THEN 'refunded'
                            WHEN LOWER(COALESCE(CAST(u.status AS VARCHAR), '')) LIKE '%teljesít%' THEN 'completed'
                            ELSE 'pending'
                        END AS VARCHAR) AS order_status,
                        CAST(UPPER(COALESCE(NULLIF(TRIM(CAST(it.sku AS VARCHAR)), ''), 'UNKNOWN')) AS VARCHAR) AS sku,
                        CAST(it.name AS VARCHAR) AS product_name,
                        CAST(COALESCE(TRY_CAST(it.quantity AS DOUBLE), 0) AS DECIMAL(12, 3)) AS quantity,
                        CAST(ROUND(COALESCE(TRY_CAST(it.price_net AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS unit_net_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(it.price_gross AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS unit_gross_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(it.price_net AS DOUBLE), 0) * COALESCE(TRY_CAST(it.quantity AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS line_net_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(it.price_gross AS DOUBLE), 0) * COALESCE(TRY_CAST(it.quantity AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS line_gross_huf,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.unas_orders__items it
                    JOIN pg.bronze_api.unas_orders u ON u._dlt_id = it._dlt_parent_id
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    """
                )

            if "shoprenter_orders__orderproducts" in cat and "shoprenter_orders" in cat:
                c_st = _col(cat, "shoprenter_orders", "s", "order_status__name", "''")
                c_curr = _col(cat, "shoprenter_orders", "s", "currency__code", "'HUF'")

                parts.append(
                    f"""
                    SELECT
                        CAST('shoprenter' AS VARCHAR) AS source_system,
                        CAST(s.id AS VARCHAR) AS order_id,
                        CAST(op.id AS VARCHAR) AS line_item_id,
                        TRY_CAST(s.datecreated AS DATE) AS order_date,
                        CAST(CASE
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%sztornó%'
                              OR LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%töröl%' THEN 'cancelled'
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%vissza%' THEN 'refunded'
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%teljesít%' THEN 'completed'
                            ELSE 'pending'
                        END AS VARCHAR) AS order_status,
                        CAST(UPPER(COALESCE(NULLIF(TRIM(CAST(op.sku AS VARCHAR)), ''), 'UNKNOWN')) AS VARCHAR) AS sku,
                        CAST(op.name AS VARCHAR) AS product_name,
                        CAST(COALESCE(TRY_CAST(op.quantity AS DOUBLE), 0) AS DECIMAL(12, 3)) AS quantity,
                        CAST(ROUND(COALESCE(TRY_CAST(op.price AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS unit_net_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(op.gross_price AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS unit_gross_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(op.price AS DOUBLE), 0) * COALESCE(TRY_CAST(op.quantity AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS line_net_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(op.gross_price AS DOUBLE), 0) * COALESCE(TRY_CAST(op.quantity AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS line_gross_huf,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.shoprenter_orders__orderproducts op
                    JOIN pg.bronze_api.shoprenter_orders s ON s._dlt_id = op._dlt_parent_id
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    """
                )

            if "woocommerce_orders__line_items" in cat and "woocommerce_orders" in cat:
                c_curr = _col(cat, "woocommerce_orders", "w", "currency", "'HUF'")
                c_ltax = _col(cat, "woocommerce_orders__line_items", "li", "total_tax", "0")

                parts.append(
                    f"""
                    SELECT
                        CAST('woocommerce' AS VARCHAR) AS source_system,
                        CAST(w.id AS VARCHAR) AS order_id,
                        CAST(li.id AS VARCHAR) AS line_item_id,
                        CAST(TRY_CAST(w.date_created AS TIMESTAMPTZ) AT TIME ZONE 'Europe/Budapest' AS DATE) AS order_date,
                        CAST(CASE
                            WHEN CAST(w.status AS VARCHAR) IN ('cancelled', 'failed', 'trash') THEN 'cancelled'
                            WHEN CAST(w.status AS VARCHAR) = 'refunded' THEN 'refunded'
                            WHEN CAST(w.status AS VARCHAR) = 'completed' THEN 'completed'
                            ELSE 'pending'
                        END AS VARCHAR) AS order_status,
                        CAST(UPPER(COALESCE(NULLIF(TRIM(CAST(li.sku AS VARCHAR)), ''), 'UNKNOWN')) AS VARCHAR) AS sku,
                        CAST(li.name AS VARCHAR) AS product_name,
                        CAST(COALESCE(TRY_CAST(li.quantity AS DOUBLE), 0) AS DECIMAL(12, 3)) AS quantity,
                        CAST(ROUND(COALESCE(TRY_CAST(li.price AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS unit_net_huf,
                        CAST(ROUND((COALESCE(TRY_CAST(li.price AS DOUBLE), 0) * 1.27) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS unit_gross_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(li.total AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS line_net_huf,
                        CAST(ROUND((COALESCE(TRY_CAST(li.total AS DOUBLE), 0) + COALESCE(TRY_CAST({c_ltax} AS DOUBLE), 0)) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS line_gross_huf,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.woocommerce_orders__line_items li
                    JOIN pg.bronze_api.woocommerce_orders w ON w._dlt_id = li._dlt_parent_id
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    """
                )

            empty_order_items = """
                SELECT
                    NULL::VARCHAR AS source_system, NULL::VARCHAR AS order_id, NULL::VARCHAR AS line_item_id,
                    NULL::DATE AS order_date, NULL::VARCHAR AS order_status, NULL::VARCHAR AS sku,
                    NULL::VARCHAR AS product_name, 0::DECIMAL(12, 3) AS quantity,
                    0::DECIMAL(14, 2) AS unit_net_huf, 0::DECIMAL(14, 2) AS unit_gross_huf,
                    0::DECIMAL(14, 2) AS line_net_huf, 0::DECIMAL(14, 2) AS line_gross_huf,
                    NOW() AS refreshed_at
                WHERE FALSE
            """
            summary["order_items"] = _build_silver_table(con, "order_items", parts, empty_order_items, "order_date")

            # ----------------------------------------------------------------
            # 4. SILVER.INVOICES
            # ----------------------------------------------------------------
            parts = []
            if "billingo_documents" in cat:
                c_canc = _col(cat, "billingo_documents", "b", "cancelled", "FALSE")
                c_type = _col(cat, "billingo_documents", "b", "type", "'invoice'")
                c_ful = _col(cat, "billingo_documents", "b", "fulfillment_date", "b.invoice_date")
                c_due = _col(cat, "billingo_documents", "b", "due_date", "NULL")
                c_paid = _col(cat, "billingo_documents", "b", "paid_date", "NULL")
                c_pstat = _col(cat, "billingo_documents", "b", "payment_status", "'open'")
                c_pm = _col(cat, "billingo_documents", "b", "payment_method")
                c_curr = _col(cat, "billingo_documents", "b", "currency", "'HUF'")
                c_rate = _col(cat, "billingo_documents", "b", "conversion_rate", "NULL")
                c_pname = _col(cat, "billingo_documents", "b", "partner__name")
                c_ptax = _col(cat, "billingo_documents", "b", "partner__taxcode")

                parts.append(
                    f"""
                    SELECT
                        CAST('billingo' AS VARCHAR) AS source_system,
                        CAST(b.id AS VARCHAR) AS invoice_id,
                        CAST(b.invoice_number AS VARCHAR) AS invoice_number,
                        CAST(CASE
                            WHEN COALESCE(TRY_CAST({c_canc} AS BOOLEAN), FALSE) OR CAST({c_type} AS VARCHAR) = 'cancellation' THEN 'cancellation'
                            WHEN CAST({c_type} AS VARCHAR) = 'proforma' THEN 'proforma'
                            ELSE 'invoice'
                        END AS VARCHAR) AS invoice_type,
                        CAST((COALESCE(TRY_CAST({c_canc} AS BOOLEAN), FALSE) OR CAST({c_type} AS VARCHAR) = 'cancellation') AS BOOLEAN) AS is_cancelled,
                        TRY_CAST(b.invoice_date AS DATE) AS invoice_date,
                        COALESCE(TRY_CAST({c_ful} AS DATE), TRY_CAST(b.invoice_date AS DATE)) AS fulfillment_date,
                        TRY_CAST({c_due} AS DATE) AS due_date,
                        TRY_CAST({c_paid} AS DATE) AS paid_date,
                        CAST(CASE
                            WHEN CAST({c_pstat} AS VARCHAR) = 'paid' THEN 'paid'
                            WHEN TRY_CAST({c_due} AS DATE) < CURRENT_DATE THEN 'overdue'
                            ELSE 'open'
                        END AS VARCHAR) AS payment_status,
                        CAST({c_pm} AS VARCHAR) AS payment_method,
                        CAST(UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF')) AS VARCHAR) AS currency,
                        CAST(ROUND(COALESCE(TRY_CAST(b.net_total AS DOUBLE), 0) * COALESCE(NULLIF(TRY_CAST({c_rate} AS DOUBLE), 0), fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS net_total_huf,
                        CAST(ROUND((COALESCE(TRY_CAST(b.gross_total AS DOUBLE), 0) - COALESCE(TRY_CAST(b.net_total AS DOUBLE), 0)) * COALESCE(NULLIF(TRY_CAST({c_rate} AS DOUBLE), 0), fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS vat_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(b.gross_total AS DOUBLE), 0) * COALESCE(NULLIF(TRY_CAST({c_rate} AS DOUBLE), 0), fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS gross_total_huf,
                        CAST({c_pname} AS VARCHAR) AS partner_name,
                        CAST(NULLIF(TRIM(CAST({c_ptax} AS VARCHAR)), '') AS VARCHAR) AS partner_tax_number,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.billingo_documents b
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    """
                )

            if "szamlazzhu_invoices" in cat:
                c_tip = _col(cat, "szamlazzhu_invoices", "s", "tipus", "'SZ'")
                c_telj = _col(cat, "szamlazzhu_invoices", "s", "teljesites", "s.kelt")
                c_fhat = _col(cat, "szamlazzhu_invoices", "s", "fizhat", "NULL")
                c_kdat = _col(cat, "szamlazzhu_invoices", "s", "kifizetesdatum", "NULL")
                c_fiz = _col(cat, "szamlazzhu_invoices", "s", "fizetve", "FALSE")
                c_fmod = _col(cat, "szamlazzhu_invoices", "s", "fizmod")
                c_dev = _col(cat, "szamlazzhu_invoices", "s", "devizanem", "'HUF'")
                c_arf = _col(cat, "szamlazzhu_invoices", "s", "arfolyam", "NULL")
                c_vnev = _col(cat, "szamlazzhu_invoices", "s", "vevo__nev")
                c_vado = _col(cat, "szamlazzhu_invoices", "s", "vevo__adoszam")

                parts.append(
                    f"""
                    SELECT
                        CAST('szamlazzhu' AS VARCHAR) AS source_system,
                        CAST(s.szamlaszam AS VARCHAR) AS invoice_id,
                        CAST(s.szamlaszam AS VARCHAR) AS invoice_number,
                        CAST(CASE
                            WHEN UPPER(COALESCE(CAST({c_tip} AS VARCHAR), '')) = 'SS' THEN 'cancellation'
                            WHEN UPPER(COALESCE(CAST({c_tip} AS VARCHAR), '')) = 'D' THEN 'proforma'
                            ELSE 'invoice'
                        END AS VARCHAR) AS invoice_type,
                        CAST((UPPER(COALESCE(CAST({c_tip} AS VARCHAR), '')) = 'SS') AS BOOLEAN) AS is_cancelled,
                        TRY_CAST(s.kelt AS DATE) AS invoice_date,
                        COALESCE(TRY_CAST({c_telj} AS DATE), TRY_CAST(s.kelt AS DATE)) AS fulfillment_date,
                        TRY_CAST({c_fhat} AS DATE) AS due_date,
                        TRY_CAST({c_kdat} AS DATE) AS paid_date,
                        CAST(CASE
                            WHEN LOWER(COALESCE(CAST({c_fiz} AS VARCHAR), 'false')) = 'true' THEN 'paid'
                            WHEN TRY_CAST({c_fhat} AS DATE) < CURRENT_DATE THEN 'overdue'
                            ELSE 'open'
                        END AS VARCHAR) AS payment_status,
                        CAST({c_fmod} AS VARCHAR) AS payment_method,
                        CAST(UPPER(COALESCE(CAST({c_dev} AS VARCHAR), 'HUF')) AS VARCHAR) AS currency,
                        CAST(ROUND(COALESCE(TRY_CAST(s.netto AS DOUBLE), 0) * COALESCE(NULLIF(TRY_CAST({c_arf} AS DOUBLE), 0), fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS net_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(s.afa AS DOUBLE), 0) * COALESCE(NULLIF(TRY_CAST({c_arf} AS DOUBLE), 0), fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS vat_total_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(s.brutto AS DOUBLE), 0) * COALESCE(NULLIF(TRY_CAST({c_arf} AS DOUBLE), 0), fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS gross_total_huf,
                        CAST({c_vnev} AS VARCHAR) AS partner_name,
                        CAST(NULLIF(TRIM(CAST({c_vado} AS VARCHAR)), '') AS VARCHAR) AS partner_tax_number,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.szamlazzhu_invoices s
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_dev} AS VARCHAR), 'HUF'))
                    """
                )

            empty_invoices = """
                SELECT
                    NULL::VARCHAR AS source_system, NULL::VARCHAR AS invoice_id, NULL::VARCHAR AS invoice_number,
                    NULL::VARCHAR AS invoice_type, FALSE AS is_cancelled, NULL::DATE AS invoice_date,
                    NULL::DATE AS fulfillment_date, NULL::DATE AS due_date, NULL::DATE AS paid_date,
                    NULL::VARCHAR AS payment_status, NULL::VARCHAR AS payment_method, NULL::VARCHAR AS currency,
                    0::DECIMAL(14, 2) AS net_total_huf, 0::DECIMAL(14, 2) AS vat_total_huf,
                    0::DECIMAL(14, 2) AS gross_total_huf, NULL::VARCHAR AS partner_name,
                    NULL::VARCHAR AS partner_tax_number, NOW() AS refreshed_at
                WHERE FALSE
            """
            summary["invoices"] = _build_silver_table(con, "invoices", parts, empty_invoices, "fulfillment_date")

            # ----------------------------------------------------------------
            # 5. SILVER.INVOICE_ITEMS
            # ----------------------------------------------------------------
            parts = []
            if "billingo_documents__items" in cat and "billingo_documents" in cat:
                c_ful = _col(cat, "billingo_documents", "b", "fulfillment_date", "b.invoice_date")
                c_pid = _col(cat, "billingo_documents__items", "it", "product_id", "'UNKNOWN'")
                c_curr = _col(cat, "billingo_documents", "b", "currency", "'HUF'")
                c_rate = _col(cat, "billingo_documents", "b", "conversion_rate", "NULL")

                parts.append(
                    rf"""
                    SELECT
                        CAST('billingo' AS VARCHAR) AS source_system,
                        CAST(b.id AS VARCHAR) AS invoice_id,
                        CAST(ROW_NUMBER() OVER (PARTITION BY b.id ORDER BY it._dlt_id) AS INTEGER) AS line_idx,
                        COALESCE(TRY_CAST({c_ful} AS DATE), TRY_CAST(b.invoice_date AS DATE)) AS fulfillment_date,
                        CAST(UPPER(COALESCE(
                            NULLIF(regexp_extract(COALESCE(CAST(it.name AS VARCHAR), ''), '^\[([^\]]+)\]', 1), ''),
                            NULLIF(CAST({c_pid} AS VARCHAR), ''),
                            'UNKNOWN'
                        )) AS VARCHAR) AS sku,
                        CAST(it.name AS VARCHAR) AS item_name,
                        CAST(COALESCE(TRY_CAST(it.quantity AS DOUBLE), 0) AS DECIMAL(12, 3)) AS quantity,
                        CAST(ROUND(COALESCE(TRY_CAST(it.net_amount AS DOUBLE), 0) * COALESCE(NULLIF(TRY_CAST({c_rate} AS DOUBLE), 0), fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS net_amount_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(it.gross_amount AS DOUBLE), 0) * COALESCE(NULLIF(TRY_CAST({c_rate} AS DOUBLE), 0), fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS gross_amount_huf,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.billingo_documents__items it
                    JOIN pg.bronze_api.billingo_documents b ON b._dlt_id = it._dlt_parent_id
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    """
                )

            if "szamlazzhu_invoices__tetelek" in cat and "szamlazzhu_invoices" in cat:
                c_telj = _col(cat, "szamlazzhu_invoices", "s", "teljesites", "s.kelt")
                c_azon = _col(cat, "szamlazzhu_invoices__tetelek", "t", "azonosito", "'UNKNOWN'")
                c_dev = _col(cat, "szamlazzhu_invoices", "s", "devizanem", "'HUF'")
                c_arf = _col(cat, "szamlazzhu_invoices", "s", "arfolyam", "NULL")

                parts.append(
                    f"""
                    SELECT
                        CAST('szamlazzhu' AS VARCHAR) AS source_system,
                        CAST(s.szamlaszam AS VARCHAR) AS invoice_id,
                        CAST(ROW_NUMBER() OVER (PARTITION BY s.szamlaszam ORDER BY t._dlt_id) AS INTEGER) AS line_idx,
                        COALESCE(TRY_CAST({c_telj} AS DATE), TRY_CAST(s.kelt AS DATE)) AS fulfillment_date,
                        CAST(UPPER(COALESCE(NULLIF(TRIM(CAST({c_azon} AS VARCHAR)), ''), 'UNKNOWN')) AS VARCHAR) AS sku,
                        CAST(t.megnevezes AS VARCHAR) AS item_name,
                        CAST(COALESCE(TRY_CAST(t.mennyiseg AS DOUBLE), 0) AS DECIMAL(12, 3)) AS quantity,
                        CAST(ROUND(COALESCE(TRY_CAST(t.netto AS DOUBLE), 0) * COALESCE(NULLIF(TRY_CAST({c_arf} AS DOUBLE), 0), fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS net_amount_huf,
                        CAST(ROUND(COALESCE(TRY_CAST(t.brutto AS DOUBLE), 0) * COALESCE(NULLIF(TRY_CAST({c_arf} AS DOUBLE), 0), fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS gross_amount_huf,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.szamlazzhu_invoices__tetelek t
                    JOIN pg.bronze_api.szamlazzhu_invoices s ON s._dlt_id = t._dlt_parent_id
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_dev} AS VARCHAR), 'HUF'))
                    """
                )

            empty_inv_items = """
                SELECT
                    NULL::VARCHAR AS source_system, NULL::VARCHAR AS invoice_id, 0::INTEGER AS line_idx,
                    NULL::DATE AS fulfillment_date, NULL::VARCHAR AS sku, NULL::VARCHAR AS item_name,
                    0::DECIMAL(12, 3) AS quantity, 0::DECIMAL(14, 2) AS net_amount_huf,
                    0::DECIMAL(14, 2) AS gross_amount_huf, NOW() AS refreshed_at
                WHERE FALSE
            """
            summary["invoice_items"] = _build_silver_table(con, "invoice_items", parts, empty_inv_items, "fulfillment_date")

            # ----------------------------------------------------------------
            # 6. SILVER.CRM_DEALS
            # ----------------------------------------------------------------
            parts = []
            if "hubspot_deals" in cat:
                c_arch = _col(cat, "hubspot_deals", "h", "archived", "FALSE")
                c_dname = _col(cat, "hubspot_deals", "h", "properties__dealname", "''")
                c_owner = _col(cat, "hubspot_deals", "h", "properties__hubspot_owner_id")
                c_stage = _col(cat, "hubspot_deals", "h", "properties__dealstage", "''")
                c_prob = _col(cat, "hubspot_deals", "h", "properties__hs_deal_stage_probability", "0")
                c_src = _col(cat, "hubspot_deals", "h", "properties__hs_analytics_source", "'direct'")
                c_curr = _col(cat, "hubspot_deals", "h", "properties__deal_currency_code", "'HUF'")
                c_amt = _col(cat, "hubspot_deals", "h", "properties__amount", "0")
                c_cdat = _col(cat, "hubspot_deals", "h", "properties__createdate", "h.createdat")
                c_udat = _col(cat, "hubspot_deals", "h", "properties__hs_lastmodifieddate", "h.updatedat")
                c_cldat = _col(cat, "hubspot_deals", "h", "properties__closedate", "NULL")
                c_lost = _col(cat, "hubspot_deals", "h", "properties__closed_lost_reason")

                parts.append(
                    f"""
                    SELECT
                        CAST('hubspot' AS VARCHAR) AS source_system,
                        CAST(h.id AS VARCHAR) AS deal_id,
                        CAST({c_dname} AS VARCHAR) AS deal_name,
                        CAST(SPLIT_PART(COALESCE(CAST({c_dname} AS VARCHAR), ''), ' - ', 1) AS VARCHAR) AS company_name,
                        NULL::VARCHAR AS contact_name,
                        CAST({c_owner} AS VARCHAR) AS owner_name,
                        CAST(CASE
                            WHEN LOWER(COALESCE(CAST({c_stage} AS VARCHAR), '')) = 'closedwon' THEN 'won'
                            WHEN LOWER(COALESCE(CAST({c_stage} AS VARCHAR), '')) = 'closedlost' THEN 'lost'
                            ELSE 'open'
                        END AS VARCHAR) AS deal_status,
                        CAST({c_stage} AS VARCHAR) AS raw_stage,
                        CAST(ROUND(COALESCE(TRY_CAST({c_prob} AS DOUBLE), 0) * 100, 2) AS DECIMAL(5, 2)) AS win_probability,
                        CAST(LOWER(COALESCE(NULLIF(TRIM(CAST({c_src} AS VARCHAR)), ''), 'direct')) AS VARCHAR) AS lead_source,
                        CAST(UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF')) AS VARCHAR) AS currency,
                        CAST(ROUND(COALESCE(TRY_CAST({c_amt} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS amount_huf,
                        COALESCE(TRY_CAST({c_cdat} AS TIMESTAMPTZ), TRY_CAST(h.createdat AS TIMESTAMPTZ)) AS created_at,
                        COALESCE(TRY_CAST({c_udat} AS TIMESTAMPTZ), TRY_CAST(h.updatedat AS TIMESTAMPTZ)) AS updated_at,
                        CAST(TRY_CAST({c_cldat} AS TIMESTAMPTZ) AS DATE) AS expected_close_date,
                        CASE
                            WHEN LOWER(COALESCE(CAST({c_stage} AS VARCHAR), '')) IN ('closedwon', 'closedlost')
                                THEN CAST(TRY_CAST({c_cldat} AS TIMESTAMPTZ) AS DATE)
                            ELSE NULL
                        END AS closed_date,
                        CAST({c_lost} AS VARCHAR) AS lost_reason,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.hubspot_deals h
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    WHERE COALESCE(TRY_CAST({c_arch} AS BOOLEAN), FALSE) IS FALSE
                    """
                )

            if "pipedrive_deals" in cat:
                c_org = _col(cat, "pipedrive_deals", "p", "org_id__name")
                c_per = _col(cat, "pipedrive_deals", "p", "person_id__name")
                c_usr = _col(cat, "pipedrive_deals", "p", "user_id__name")
                c_stg = _col(cat, "pipedrive_deals", "p", "stage_id", "'0'")
                c_prob = _col(cat, "pipedrive_deals", "p", "probability", "0")
                c_curr = _col(cat, "pipedrive_deals", "p", "currency", "'HUF'")
                c_exp = _col(cat, "pipedrive_deals", "p", "expected_close_date", "NULL")
                c_won = _col(cat, "pipedrive_deals", "p", "won_time", "NULL")
                c_lost_t = _col(cat, "pipedrive_deals", "p", "lost_time", "NULL")
                c_lost_r = _col(cat, "pipedrive_deals", "p", "lost_reason")

                parts.append(
                    f"""
                    SELECT
                        CAST('pipedrive' AS VARCHAR) AS source_system,
                        CAST(p.id AS VARCHAR) AS deal_id,
                        CAST(p.title AS VARCHAR) AS deal_name,
                        CAST({c_org} AS VARCHAR) AS company_name,
                        CAST({c_per} AS VARCHAR) AS contact_name,
                        CAST({c_usr} AS VARCHAR) AS owner_name,
                        CAST(CASE
                            WHEN LOWER(COALESCE(CAST(p.status AS VARCHAR), '')) = 'won' THEN 'won'
                            WHEN LOWER(COALESCE(CAST(p.status AS VARCHAR), '')) = 'lost' THEN 'lost'
                            ELSE 'open'
                        END AS VARCHAR) AS deal_status,
                        CAST('stage_' || COALESCE(CAST({c_stg} AS VARCHAR), '0') AS VARCHAR) AS raw_stage,
                        CAST(COALESCE(TRY_CAST({c_prob} AS DOUBLE), 0) AS DECIMAL(5, 2)) AS win_probability,
                        CAST('direct' AS VARCHAR) AS lead_source,
                        CAST(UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF')) AS VARCHAR) AS currency,
                        CAST(ROUND(COALESCE(TRY_CAST(p.value AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS amount_huf,
                        TRY_CAST(p.add_time AS TIMESTAMP) AT TIME ZONE 'Europe/Budapest' AS created_at,
                        TRY_CAST(p.update_time AS TIMESTAMP) AT TIME ZONE 'Europe/Budapest' AS updated_at,
                        TRY_CAST({c_exp} AS DATE) AS expected_close_date,
                        COALESCE(TRY_CAST({c_won} AS DATE), TRY_CAST({c_lost_t} AS DATE)) AS closed_date,
                        CAST({c_lost_r} AS VARCHAR) AS lost_reason,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.pipedrive_deals p
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_curr} AS VARCHAR), 'HUF'))
                    """
                )

            if "minicrm_projects" in cat:
                c_del = _col(cat, "minicrm_projects", "m", "deleted", "'0'")
                c_ceg = _col(cat, "minicrm_projects", "m", "cegnev")
                c_kap = _col(cat, "minicrm_projects", "m", "kapcsolattarto")
                c_fel = _col(cat, "minicrm_projects", "m", "felelosnev")
                c_st = _col(cat, "minicrm_projects", "m", "statusz", "''")
                c_for = _col(cat, "minicrm_projects", "m", "forras", "'direct'")
                c_penz = _col(cat, "minicrm_projects", "m", "penznem", "'HUF'")
                c_ert = _col(cat, "minicrm_projects", "m", "ertek", "0")
                c_varh = _col(cat, "minicrm_projects", "m", "varhatozaras", "NULL")

                parts.append(
                    f"""
                    SELECT
                        CAST('minicrm' AS VARCHAR) AS source_system,
                        CAST(m.id AS VARCHAR) AS deal_id,
                        CAST(m.name AS VARCHAR) AS deal_name,
                        CAST({c_ceg} AS VARCHAR) AS company_name,
                        CAST({c_kap} AS VARCHAR) AS contact_name,
                        CAST({c_fel} AS VARCHAR) AS owner_name,
                        CAST(CASE
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%megrendel%'
                              OR LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%sikeres%' THEN 'won'
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%elutasít%'
                              OR LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%sikertelen%' THEN 'lost'
                            ELSE 'open'
                        END AS VARCHAR) AS deal_status,
                        CAST({c_st} AS VARCHAR) AS raw_stage,
                        CAST(CASE
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%megrendel%' THEN 100.00
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%elutasít%' THEN 0.00
                            ELSE 50.00
                        END AS DECIMAL(5, 2)) AS win_probability,
                        CAST(LOWER(COALESCE(NULLIF(TRIM(CAST({c_for} AS VARCHAR)), ''), 'direct')) AS VARCHAR) AS lead_source,
                        CAST(UPPER(COALESCE(CAST({c_penz} AS VARCHAR), 'HUF')) AS VARCHAR) AS currency,
                        CAST(ROUND(COALESCE(TRY_CAST({c_ert} AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS amount_huf,
                        TRY_CAST(m.createdat AS TIMESTAMP) AT TIME ZONE 'Europe/Budapest' AS created_at,
                        TRY_CAST(m.updatedat AS TIMESTAMP) AT TIME ZONE 'Europe/Budapest' AS updated_at,
                        TRY_CAST({c_varh} AS DATE) AS expected_close_date,
                        CASE
                            WHEN LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%megrendel%'
                              OR LOWER(COALESCE(CAST({c_st} AS VARCHAR), '')) LIKE '%elutasít%'
                                THEN TRY_CAST(m.updatedat AS DATE)
                            ELSE NULL
                        END AS closed_date,
                        NULL::VARCHAR AS lost_reason,
                        NOW() AS refreshed_at
                    FROM pg.bronze_api.minicrm_projects m
                    LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_penz} AS VARCHAR), 'HUF'))
                    WHERE COALESCE(CAST({c_del} AS VARCHAR), '0') = '0'
                    """
                )

            empty_crm = """
                SELECT
                    NULL::VARCHAR AS source_system, NULL::VARCHAR AS deal_id, NULL::VARCHAR AS deal_name,
                    NULL::VARCHAR AS company_name, NULL::VARCHAR AS contact_name, NULL::VARCHAR AS owner_name,
                    NULL::VARCHAR AS deal_status, NULL::VARCHAR AS raw_stage, 0::DECIMAL(5, 2) AS win_probability,
                    NULL::VARCHAR AS lead_source, NULL::VARCHAR AS currency, 0::DECIMAL(14, 2) AS amount_huf,
                    NULL::TIMESTAMPTZ AS created_at, NULL::TIMESTAMPTZ AS updated_at,
                    NULL::DATE AS expected_close_date, NULL::DATE AS closed_date,
                    NULL::VARCHAR AS lost_reason, NOW() AS refreshed_at
                WHERE FALSE
            """
            summary["crm_deals"] = _build_silver_table(con, "crm_deals", parts, empty_crm, "deal_status")

            # ----------------------------------------------------------------
            # 7. SILVER.PRODUCT_COGS
            # ----------------------------------------------------------------
            parts = []
            if "product_cogs" in cat:
                c_tnev = _col(cat, "product_cogs", "c", "termek_nev")
                c_besz = _col(cat, "product_cogs", "c", "beszallito")
                c_dev = _col(cat, "product_cogs", "c", "devizanem", "'HUF'")
                c_erv = _col(cat, "product_cogs", "c", "ervenyes_ettol", "DATE '2020-01-01'")
                c_ing = _col(cat, "product_cogs", "c", "ingested_at", "NOW()")

                parts.append(
                    f"""
                    WITH ranked AS (
                        SELECT
                            CAST(UPPER(TRIM(CAST(c.sku AS VARCHAR))) AS VARCHAR) AS sku,
                            CAST({c_tnev} AS VARCHAR) AS product_name,
                            CAST(ROUND(COALESCE(TRY_CAST(c.beszerzesi_netto_ar AS DOUBLE), 0) * COALESCE(fx.rate_to_huf, 1.0), 2) AS DECIMAL(14, 2)) AS cogs_net_huf,
                            CAST({c_besz} AS VARCHAR) AS supplier,
                            COALESCE(TRY_CAST({c_erv} AS DATE), DATE '2020-01-01') AS valid_from,
                            ROW_NUMBER() OVER (
                                PARTITION BY UPPER(TRIM(CAST(c.sku AS VARCHAR))), COALESCE(TRY_CAST({c_erv} AS DATE), DATE '2020-01-01')
                                ORDER BY TRY_CAST({c_ing} AS TIMESTAMPTZ) DESC NULLS LAST
                            ) AS rn
                        FROM pg.bronze_api.product_cogs c
                        LEFT JOIN pg.silver.fx_rates fx ON fx.currency = UPPER(COALESCE(CAST({c_dev} AS VARCHAR), 'HUF'))
                        WHERE NULLIF(TRIM(CAST(c.sku AS VARCHAR)), '') IS NOT NULL
                    ),
                    dedup AS (
                        SELECT sku, product_name, cogs_net_huf, supplier, valid_from
                        FROM ranked
                        WHERE rn = 1
                    )
                    SELECT
                        sku,
                        product_name,
                        cogs_net_huf,
                        supplier,
                        valid_from,
                        COALESCE(CAST(LEAD(valid_from) OVER (PARTITION BY sku ORDER BY valid_from) - INTERVAL 1 DAY AS DATE), DATE '2099-12-31') AS valid_to,
                        NOW() AS refreshed_at
                    FROM dedup
                    """
                )

            empty_cogs = """
                SELECT
                    NULL::VARCHAR AS sku, NULL::VARCHAR AS product_name, 0::DECIMAL(14, 2) AS cogs_net_huf,
                    NULL::VARCHAR AS supplier, NULL::DATE AS valid_from, NULL::DATE AS valid_to,
                    NOW() AS refreshed_at
                WHERE FALSE
            """
            summary["product_cogs"] = _build_silver_table(con, "product_cogs", parts, empty_cogs, "sku")

            logger.info("[SILVER-DUCKDB] Transzformáció kész: %s", summary)
            return summary

        finally:
            con.close()
