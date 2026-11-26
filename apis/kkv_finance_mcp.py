# 2026.11.26  18.00
import os
from datetime import date
from typing import Any
import psycopg
from psycopg.rows import dict_row
from fastmcp import FastMCP
import httpx

N8N_FEATURE_WEBHOOK = os.getenv("N8N_FEATURE_WEBHOOK", "")

mcp = FastMCP("FastAutoSol Finance Decision MCP")

def _get_readonly_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgresql"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "n8n"),
        user=os.getenv("POSTGRES_USER", "sql_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "sql_pass"),
        connect_timeout=10,
        options="-c default_transaction_read_only=on",
        row_factory=dict_row,
    )


@mcp.tool()
def get_overdue_invoices(min_days_overdue: int = 1) -> list[dict[str, Any]]:
    """Lejárt, még ki nem fizetett számlák lekérdezése a Billingo és Számlázz.hu rendszerekből."""
    sql = """
        SELECT
            source_system,
            invoice_number,
            partner_name,
            partner_taxcode,
            gross_amount,
            currency,
            due_date,
            (CURRENT_DATE - due_date) AS days_overdue
        FROM gold_finance.v_unified_invoices
        WHERE is_paid = FALSE
          AND is_cancelled = FALSE
          AND doc_type = 'invoice'
          AND (CURRENT_DATE - due_date) >= %s
        ORDER BY days_overdue DESC, gross_amount DESC
        LIMIT 100;
    """
    with _get_readonly_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (min_days_overdue,))
        return [dict(row) for row in cur.fetchall()]


@mcp.tool()
def get_margin_summary(date_from: date, date_to: date) -> dict[str, Any]:
    """Nettó árbevétel, beszerzési költség (COGS) és fedezeti árrés összesítése adott időszakra."""
    summary_sql = """
        SELECT
            COUNT(DISTINCT invoice_number) AS invoice_count,
            COALESCE(SUM(line_net_revenue), 0) AS total_net_revenue,
            COALESCE(SUM(total_cogs_net), 0) AS total_cogs_net,
            COALESCE(SUM(gross_margin_net), 0) AS total_margin_net,
            CASE
                WHEN COALESCE(SUM(line_net_revenue), 0) > 0
                THEN ROUND((SUM(gross_margin_net) / SUM(line_net_revenue)) * 100, 2)
                ELSE 0
            END AS margin_pct
        FROM gold_finance.v_invoice_margins
        WHERE invoice_date BETWEEN %s AND %s;
    """
    top_sku_sql = """
        SELECT
            sku,
            MAX(item_name) AS item_name,
            SUM(qty) AS sold_qty,
            SUM(line_net_revenue) AS net_revenue,
            SUM(gross_margin_net) AS gross_margin
        FROM gold_finance.v_invoice_margins
        WHERE invoice_date BETWEEN %s AND %s
        GROUP BY sku
        ORDER BY gross_margin DESC
        LIMIT 5;
    """
    with _get_readonly_conn() as conn, conn.cursor() as cur:
        cur.execute(summary_sql, (date_from, date_to))
        totals = dict(cur.fetchone() or {})
        cur.execute(top_sku_sql, (date_from, date_to))
        totals["top_products_by_margin"] = [dict(r) for r in cur.fetchall()]
        return totals


@mcp.tool()
def get_partner_financial_profile(partner_query: str) -> list[dict[str, Any]]:
    """Vevő/partner pénzügyi átvilágítása névrészlet vagy adószám alapján (forgalom, nyitott tartozás, fizetési fegyelem)."""
    sql = """
        SELECT
            partner_name,
            partner_taxcode,
            COUNT(*) FILTER (WHERE NOT is_cancelled AND doc_type = 'invoice') AS invoice_count,
            COALESCE(SUM(gross_amount) FILTER (WHERE NOT is_cancelled AND doc_type = 'invoice'), 0) AS total_gross_billed,
            COALESCE(SUM(gross_amount) FILTER (WHERE NOT is_paid AND NOT is_cancelled AND doc_type = 'invoice'), 0) AS open_debt_gross,
            ROUND(AVG(paid_date - invoice_date) FILTER (WHERE is_paid AND paid_date IS NOT NULL), 1) AS avg_days_to_pay
        FROM gold_finance.v_unified_invoices
        WHERE partner_name ILIKE %s OR partner_taxcode ILIKE %s
        GROUP BY partner_name, partner_taxcode
        ORDER BY total_gross_billed DESC
        LIMIT 10;
    """
    pattern = f"%{partner_query.strip()}%"
    with _get_readonly_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (pattern, pattern))
        return [dict(row) for row in cur.fetchall()]


@mcp.tool()
async def log_missing_feature(category: str, capability_gap: str) -> str:
    """Kizárólag akkor hívd meg, ha a felhasználó olyan üzleti, pénzügyi vagy
    operatív adatelemzést kér, amelyhez nincs elérhető eszköz vagy adatforrás.

    SZIGORÚ SZABÁLYOK:
    - Általános, nem üzleti kérdéseknél (pl. időjárás, általános csevegés) TILOS meghívni.
    - A 'capability_gap' mezőbe KIZÁRÓLAG általánosított funkcióleírást írj
      (pl. 'Webshopos készletinformációk lekérdezése').
    - TILOS ügyfélnevet, adószámot, számlaszámot vagy konkrét összeget beleírni!
    """
    if not N8N_FEATURE_WEBHOOK:
        return "A funkcióigény naplózása jelenleg nincs aktiválva."

    payload = {
        "category": category.strip()[:50],
        "capability_gap": capability_gap.strip()[:300],
    }

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            await client.post(N8N_FEATURE_WEBHOOK, json=payload)
    except httpx.HTTPError:
        # Nem akasztjuk meg a felhasználói beszélgetést, ha a webhook átmenetileg nem elérhető
        pass

    return "A hiányzó funkció igényét anonimizált formában rögzítettem a fejlesztő felé."
