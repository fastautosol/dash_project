# 2026.11.26  14.00
import logging
import os
import threading
from typing import Any
import duckdb

logger = logging.getLogger("finance_silver")
SILVER_LOCK = threading.Lock()

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


def _has_table(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    n = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = 'pg' AND table_schema = ? AND table_name = ?",[schema, table]).fetchone()[0]
    return n > 0

def run_finance_silver_transformation() -> dict[str, Any]:
    """Összevezeti a Billingo, Számlázz.hu és COGS bronze_api táblákat a pg.silver sémába."""
    with SILVER_LOCK:
        attach_dsn = _get_attach_dsn()
        conn = duckdb.connect()
        try:
            try:
                conn.execute("LOAD postgres;")
            except duckdb.IOException:
                conn.execute("INSTALL postgres; LOAD postgres;")

            conn.execute(f"ATTACH '{attach_dsn}' AS pg (TYPE POSTGRES);")
            conn.execute("CREATE SCHEMA IF NOT EXISTS pg.silver;")

            has_billingo = _has_table(conn, "bronze_api", "billingo_documents")
            has_billingo_items = _has_table(conn, "bronze_api", "billingo_documents__items")
            has_szamlazzhu = _has_table(conn, "bronze_api", "szamlazzhu_invoices")
            has_szamlazzhu_items = _has_table(conn, "bronze_api", "szamlazzhu_invoices__tetelek")
            has_cogs = _has_table(conn, "bronze_api", "product_cogs")

            if not has_billingo and not has_szamlazzhu:
                logger.warning("Finance Silver kihagyva: nincs betöltött számlázó tábla a bronze_api sémában.")
                return {"status": "skipped", "reason": "no_bronze_tables"}

            # ---------------------------------------------------------
            # 1. SILVER.FINANCE_INVOICES (Egységesített számlafejek)
            # ---------------------------------------------------------
            header_sources: list[str] = []

            if has_billingo:
                header_sources.append("""
                    SELECT
                        'billingo:' || id::VARCHAR AS invoice_uid,
                        'billingo' AS source_system,
                        id::VARCHAR AS source_doc_id,
                        NULLIF(TRIM(invoice_number::VARCHAR), '') AS invoice_number,
                        LOWER(COALESCE(NULLIF(TRIM(type::VARCHAR), ''), 'invoice')) AS doc_type,
                        COALESCE(TRY_CAST(cancelled AS BOOLEAN), LOWER(TRIM(type::VARCHAR)) = 'cancellation', FALSE) AS is_cancelled,
                        (LOWER(TRIM(payment_status::VARCHAR)) = 'paid') AS is_paid,
                        CASE
                            WHEN COALESCE(TRY_CAST(cancelled AS BOOLEAN), FALSE) OR LOWER(TRIM(type::VARCHAR)) = 'cancellation' THEN 'cancelled'
                            WHEN LOWER(TRIM(payment_status::VARCHAR)) = 'paid' THEN 'paid'
                            WHEN TRY_CAST(due_date AS DATE) < CURRENT_DATE THEN 'expired'
                            ELSE 'outstanding'
                        END AS payment_status,
                        CASE LOWER(TRIM(payment_method::VARCHAR))
                            WHEN 'wire_transfer' THEN 'wire_transfer'
                            WHEN 'bankcard' THEN 'bankcard'
                            WHEN 'barion' THEN 'barion'
                            WHEN 'cod' THEN 'cod'
                            ELSE COALESCE(LOWER(NULLIF(TRIM(payment_method::VARCHAR), '')), 'other')
                        END AS payment_method,
                        UPPER(COALESCE(NULLIF(TRIM(currency::VARCHAR), ''), 'HUF')) AS currency,
                        COALESCE(TRY_CAST(conversion_rate AS DOUBLE), 1.0) AS conversion_rate,
                        COALESCE(TRY_CAST(net_total AS DECIMAL(14,2)), 0.00) AS net_amount,
                        ROUND(
                            COALESCE(TRY_CAST(gross_total AS DECIMAL(14,2)), 0.00) -
                            COALESCE(TRY_CAST(net_total AS DECIMAL(14,2)), 0.00), 2
                        ) AS vat_amount,
                        COALESCE(TRY_CAST(gross_total AS DECIMAL(14,2)), 0.00) AS gross_amount,
                        TRY_CAST(invoice_date AS DATE) AS invoice_date,
                        TRY_CAST(fulfillment_date AS DATE) AS fulfillment_date,
                        TRY_CAST(due_date AS DATE) AS due_date,
                        TRY_CAST(paid_date AS DATE) AS paid_date,
                        NULLIF(TRIM(partner__name::VARCHAR), '') AS partner_name,
                        NULLIF(TRIM(partner__taxcode::VARCHAR), '') AS partner_taxcode,
                        NULLIF(TRIM(partner__address__post_code::VARCHAR), '') AS partner_zip,
                        NULLIF(TRIM(partner__address__city::VARCHAR), '') AS partner_city,
                        NULLIF(TRIM(partner__address__address::VARCHAR), '') AS partner_address,
                        TRY_CAST(ingested_at AS TIMESTAMPTZ) AS ingested_at
                    FROM pg.bronze_api.billingo_documents
                    WHERE id IS NOT NULL
                """)

            if has_szamlazzhu:
                header_sources.append("""
                    SELECT
                        'szamlazzhu:' || szamlaszam::VARCHAR AS invoice_uid,
                        'szamlazzhu' AS source_system,
                        szamlaszam::VARCHAR AS source_doc_id,
                        NULLIF(TRIM(szamlaszam::VARCHAR), '') AS invoice_number,
                        CASE UPPER(TRIM(tipus::VARCHAR))
                            WHEN 'SS' THEN 'cancellation'
                            WHEN 'D' THEN 'proforma'
                            ELSE 'invoice'
                        END AS doc_type,
                        (UPPER(TRIM(tipus::VARCHAR)) = 'SS') AS is_cancelled,
                        COALESCE(TRY_CAST(fizetve AS BOOLEAN), FALSE) AS is_paid,
                        CASE
                            WHEN UPPER(TRIM(tipus::VARCHAR)) = 'SS' THEN 'cancelled'
                            WHEN COALESCE(TRY_CAST(fizetve AS BOOLEAN), FALSE) THEN 'paid'
                            WHEN TRY_CAST(fizhat AS DATE) < CURRENT_DATE THEN 'expired'
                            ELSE 'outstanding'
                        END AS payment_status,
                        CASE LOWER(TRIM(fizmod::VARCHAR))
                            WHEN 'átutalás' THEN 'wire_transfer'
                            WHEN 'bankkártya' THEN 'bankcard'
                            WHEN 'utánvét' THEN 'cod'
                            ELSE COALESCE(LOWER(NULLIF(TRIM(fizmod::VARCHAR), '')), 'other')
                        END AS payment_method,
                        UPPER(COALESCE(NULLIF(TRIM(devizanem::VARCHAR), ''), 'HUF')) AS currency,
                        COALESCE(TRY_CAST(arfolyam AS DOUBLE), 1.0) AS conversion_rate,
                        COALESCE(TRY_CAST(netto AS DECIMAL(14,2)), 0.00) AS net_amount,
                        COALESCE(TRY_CAST(afa AS DECIMAL(14,2)), 0.00) AS vat_amount,
                        COALESCE(TRY_CAST(brutto AS DECIMAL(14,2)), 0.00) AS gross_amount,
                        TRY_CAST(kelt AS DATE) AS invoice_date,
                        TRY_CAST(teljesites AS DATE) AS fulfillment_date,
                        TRY_CAST(fizhat AS DATE) AS due_date,
                        TRY_CAST(kifizetesdatum AS DATE) AS paid_date,
                        NULLIF(TRIM(vevo__nev::VARCHAR), '') AS partner_name,
                        NULLIF(TRIM(vevo__adoszam::VARCHAR), '') AS partner_taxcode,
                        NULLIF(TRIM(vevo__irsz::VARCHAR), '') AS partner_zip,
                        NULLIF(TRIM(vevo__telepules::VARCHAR), '') AS partner_city,
                        NULLIF(TRIM(vevo__cim::VARCHAR), '') AS partner_address,
                        TRY_CAST(ingested_at AS TIMESTAMPTZ) AS ingested_at
                    FROM pg.bronze_api.szamlazzhu_invoices
                    WHERE szamlaszam IS NOT NULL
                """)

            union_headers_sql = "\nUNION ALL\n".join(header_sources)

            conn.execute(f"""
                CREATE OR REPLACE TABLE pg.silver.kkv_finance_invoices AS
                WITH unified AS (
                    {union_headers_sql}
                ),
                deduped AS (
                    SELECT *,
                        NULLIF(regexp_extract(COALESCE(partner_taxcode, ''), '^(\\d{{8}})', 1), '') AS partner_tax_root,
                        ROW_NUMBER() OVER (
                            PARTITION BY invoice_uid
                            ORDER BY ingested_at DESC NULLS LAST
                        ) AS rn
                    FROM unified
                )
                SELECT
                    invoice_uid,
                    source_system,
                    source_doc_id,
                    invoice_number,
                    doc_type,
                    is_cancelled,
                    is_paid,
                    payment_status,
                    payment_method,
                    currency,
                    conversion_rate,
                    net_amount,
                    vat_amount,
                    gross_amount,
                    ROUND(net_amount * conversion_rate, 2) AS net_amount_huf,
                    ROUND(gross_amount * conversion_rate, 2) AS gross_amount_huf,
                    invoice_date,
                    fulfillment_date,
                    due_date,
                    paid_date,
                    CASE
                        WHEN is_paid AND paid_date IS NOT NULL AND invoice_date IS NOT NULL
                        THEN DATE_DIFF('day', invoice_date, paid_date)
                        ELSE NULL
                    END AS days_to_pay,
                    CASE
                        WHEN NOT is_paid AND NOT is_cancelled AND doc_type = 'invoice' AND due_date < CURRENT_DATE
                        THEN DATE_DIFF('day', due_date, CURRENT_DATE)
                        ELSE 0
                    END AS days_overdue,
                    CASE
                        WHEN partner_tax_root IS NOT NULL THEN 'tax:' || partner_tax_root
                        ELSE 'name:' || LOWER(COALESCE(partner_name, 'ismeretlen'))
                    END AS partner_key,
                    COALESCE(partner_name, 'Ismeretlen partner') AS partner_name,
                    partner_taxcode,
                    partner_tax_root,
                    partner_zip,
                    partner_city,
                    partner_address,
                    CURRENT_TIMESTAMP AS transformed_at
                FROM deduped
                WHERE rn = 1;
            """)

            # ---------------------------------------------------------
            # 2. SILVER.FINANCE_INVOICE_ITEMS (Tételek + COGS árrés)
            # ---------------------------------------------------------
            item_sources: list[str] = []

            if has_billingo and has_billingo_items:
                item_sources.append("""
                    SELECT
                        'billingo:' || d.id::VARCHAR || ':' || i._dlt_id::VARCHAR AS item_uid,
                        'billingo:' || d.id::VARCHAR AS invoice_uid,
                        'billingo' AS source_system,
                        NULLIF(TRIM(d.invoice_number::VARCHAR), '') AS invoice_number,
                        LOWER(COALESCE(NULLIF(TRIM(d.type::VARCHAR), ''), 'invoice')) AS doc_type,
                        COALESCE(TRY_CAST(d.cancelled AS BOOLEAN), LOWER(TRIM(d.type::VARCHAR)) = 'cancellation', FALSE) AS is_cancelled,
                        TRY_CAST(d.invoice_date AS DATE) AS invoice_date,
                        NULLIF(regexp_extract(COALESCE(i.name::VARCHAR, ''), '^\\[([^\\]]+)\\]', 1), '') AS sku,
                        TRIM(regexp_replace(COALESCE(i.name::VARCHAR, ''), '^\\[[^\\]]+\\]\\s*', '')) AS item_name,
                        COALESCE(TRY_CAST(i.quantity AS DECIMAL(14,3)), 0) AS quantity,
                        COALESCE(NULLIF(TRIM(i.unit::VARCHAR), ''), 'db') AS unit,
                        COALESCE(TRY_CAST(i.net_unit_amount AS DECIMAL(14,2)), 0.00) AS unit_net_price,
                        COALESCE(TRY_CAST(REPLACE(i.vat::VARCHAR, '%', '') AS DECIMAL(5,2)), 27.00) AS vat_rate,
                        COALESCE(TRY_CAST(i.net_amount AS DECIMAL(14,2)), 0.00) AS net_amount,
                        ROUND(
                            COALESCE(TRY_CAST(i.gross_amount AS DECIMAL(14,2)), 0.00) -
                            COALESCE(TRY_CAST(i.net_amount AS DECIMAL(14,2)), 0.00), 2
                        ) AS vat_amount,
                        COALESCE(TRY_CAST(i.gross_amount AS DECIMAL(14,2)), 0.00) AS gross_amount,
                        COALESCE(TRY_CAST(d.conversion_rate AS DOUBLE), 1.0) AS conversion_rate
                    FROM pg.bronze_api.billingo_documents d
                    JOIN pg.bronze_api.billingo_documents__items i
                      ON i._dlt_parent_id::VARCHAR = d._dlt_id::VARCHAR
                """)

            if has_szamlazzhu and has_szamlazzhu_items:
                item_sources.append("""
                    SELECT
                        'szamlazzhu:' || s.szamlaszam::VARCHAR || ':' || t._dlt_id::VARCHAR AS item_uid,
                        'szamlazzhu:' || s.szamlaszam::VARCHAR AS invoice_uid,
                        'szamlazzhu' AS source_system,
                        NULLIF(TRIM(s.szamlaszam::VARCHAR), '') AS invoice_number,
                        CASE UPPER(TRIM(s.tipus::VARCHAR))
                            WHEN 'SS' THEN 'cancellation'
                            WHEN 'D' THEN 'proforma'
                            ELSE 'invoice'
                        END AS doc_type,
                        (UPPER(TRIM(s.tipus::VARCHAR)) = 'SS') AS is_cancelled,
                        TRY_CAST(s.kelt AS DATE) AS invoice_date,
                        NULLIF(TRIM(t.azonosito::VARCHAR), '') AS sku,
                        NULLIF(TRIM(t.megnevezes::VARCHAR), '') AS item_name,
                        COALESCE(TRY_CAST(t.mennyiseg AS DECIMAL(14,3)), 0) AS quantity,
                        COALESCE(NULLIF(TRIM(t.mennyisegiegyseg::VARCHAR), ''), 'db') AS unit,
                        COALESCE(TRY_CAST(t.nettoegysegar AS DECIMAL(14,2)), 0.00) AS unit_net_price,
                        COALESCE(TRY_CAST(t.afakulcs AS DECIMAL(5,2)), 27.00) AS vat_rate,
                        COALESCE(TRY_CAST(t.netto AS DECIMAL(14,2)), 0.00) AS net_amount,
                        COALESCE(TRY_CAST(t.afa AS DECIMAL(14,2)), 0.00) AS vat_amount,
                        COALESCE(TRY_CAST(t.brutto AS DECIMAL(14,2)), 0.00) AS gross_amount,
                        COALESCE(TRY_CAST(s.arfolyam AS DOUBLE), 1.0) AS conversion_rate
                    FROM pg.bronze_api.szamlazzhu_invoices s
                    JOIN pg.bronze_api.szamlazzhu_invoices__tetelek t
                      ON t._dlt_parent_id::VARCHAR = s._dlt_id::VARCHAR
                """)

            if item_sources:
                union_items_sql = "\nUNION ALL\n".join(item_sources)
                cogs_cte = """
                    SELECT
                        NULLIF(TRIM(sku::VARCHAR), '') AS sku,
                        TRY_CAST(beszerzesi_netto_ar AS DECIMAL(14,2)) AS unit_cogs_net,
                        NULLIF(TRIM(beszallito::VARCHAR), '') AS supplier_name,
                        TRY_CAST(ervenyes_ettol AS DATE) AS valid_from
                    FROM pg.bronze_api.product_cogs
                    WHERE sku IS NOT NULL
                """ if has_cogs else """
                    SELECT
                        NULL::VARCHAR AS sku,
                        NULL::DECIMAL(14,2) AS unit_cogs_net,
                        NULL::VARCHAR AS supplier_name,
                        NULL::DATE AS valid_from
                    WHERE FALSE
                """

                conn.execute(f"""
                    CREATE OR REPLACE TABLE pg.silver.kkv_finance_invoice_items AS
                    WITH raw_items AS (
                        {union_items_sql}
                    ),
                    cogs_ref AS (
                        {cogs_cte}
                    ),
                    matched AS (
                        SELECT
                            r.*,
                            c.unit_cogs_net,
                            c.supplier_name,
                            ROW_NUMBER() OVER (
                                PARTITION BY r.item_uid
                                ORDER BY c.valid_from DESC NULLS LAST
                            ) AS cogs_rn
                        FROM raw_items r
                        LEFT JOIN cogs_ref c
                          ON r.sku = c.sku
                         AND c.valid_from <= r.invoice_date
                    )
                    SELECT
                        item_uid,
                        invoice_uid,
                        source_system,
                        invoice_number,
                        doc_type,
                        is_cancelled,
                        invoice_date,
                        sku,
                        item_name,
                        quantity,
                        unit,
                        unit_net_price,
                        vat_rate,
                        ROUND(net_amount * conversion_rate, 2) AS net_amount_huf,
                        ROUND(vat_amount * conversion_rate, 2) AS vat_amount_huf,
                        ROUND(gross_amount * conversion_rate, 2) AS gross_amount_huf,
                        unit_cogs_net,
                        supplier_name,
                        ROUND(quantity * COALESCE(unit_cogs_net, 0.00), 2) AS total_cogs_net_huf,
                        ROUND((net_amount * conversion_rate) - (quantity * COALESCE(unit_cogs_net, 0.00)), 2) AS gross_margin_net_huf,
                        CURRENT_TIMESTAMP AS transformed_at
                    FROM matched
                    WHERE cogs_rn = 1;
                """)

            # ---------------------------------------------------------
            # 3. SILVER.FINANCE_PARTNERS (Deduplikált partnertörzs)
            # ---------------------------------------------------------
            conn.execute("""
                CREATE OR REPLACE TABLE pg.silver.finance_partners AS
                WITH ranked_names AS (
                    SELECT
                        partner_key,
                        partner_name,
                        partner_taxcode,
                        partner_tax_root,
                        partner_zip,
                        partner_city,
                        partner_address,
                        ROW_NUMBER() OVER (
                            PARTITION BY partner_key
                            ORDER BY invoice_date DESC NULLS LAST, LENGTH(partner_name) DESC
                        ) AS rn
                    FROM pg.silver.finance_invoices
                ),
                agg AS (
                    SELECT
                        partner_key,
                        COUNT(*) FILTER (WHERE doc_type = 'invoice' AND NOT is_cancelled) AS invoice_count,
                        COALESCE(SUM(net_amount_huf) FILTER (WHERE doc_type <> 'proforma'), 0.00) AS total_net_revenue_huf,
                        COALESCE(SUM(gross_amount_huf) FILTER (WHERE doc_type <> 'proforma'), 0.00) AS total_gross_revenue_huf,
                        COALESCE(SUM(gross_amount_huf) FILTER (WHERE NOT is_paid AND NOT is_cancelled AND doc_type = 'invoice'), 0.00) AS open_debt_huf,
                        ROUND(AVG(days_to_pay), 1) AS avg_days_to_pay,
                        MIN(invoice_date) AS first_invoice_date,
                        MAX(invoice_date) AS last_invoice_date
                    FROM pg.silver.finance_invoices
                    GROUP BY partner_key
                )
                SELECT
                    a.partner_key,
                    n.partner_name,
                    n.partner_taxcode,
                    n.partner_tax_root,
                    n.partner_zip,
                    n.partner_city,
                    n.partner_address,
                    a.invoice_count,
                    a.total_net_revenue_huf,
                    a.total_gross_revenue_huf,
                    a.open_debt_huf,
                    a.avg_days_to_pay,
                    a.first_invoice_date,
                    a.last_invoice_date,
                    CURRENT_TIMESTAMP AS transformed_at
                FROM agg a
                JOIN ranked_names n ON a.partner_key = n.partner_key AND n.rn = 1;
            """)

            inv_cnt = conn.execute("SELECT COUNT(*) FROM pg.silver.finance_invoices").fetchone()[0]
            itm_cnt = conn.execute("SELECT COUNT(*) FROM pg.silver.finance_invoice_items").fetchone()[0] if item_sources else 0
            prt_cnt = conn.execute("SELECT COUNT(*) FROM pg.silver.finance_partners").fetchone()[0]

            result = {
                "status": "ok",
                "silver.finance_invoices": inv_cnt,
                "silver.finance_invoice_items": itm_cnt,
                "silver.finance_partners": prt_cnt,
            }
            logger.info("Finance Silver transzformáció sikeres: %s", result)
            return result

        except Exception:
            logger.exception("Finance silver transzformációs hiba")
            raise
        finally:
            conn.close()
