# 2026.11.17  18.00
import json
import logging
import os
import random
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import dlt
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger("uvicorn.error")
pipeline_lock = threading.Lock()

os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "true"
os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "true"

INGEST_API_KEY = os.getenv("INGEST_API_KEY", "change-me-in-env")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgresql"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "n8n"),
    "username": os.getenv("POSTGRES_USER", "sql_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "sql_pass"),
    "connect_timeout": 15,
}

META_COLS = {"data_source_meta", "ingested_at", "_dlt_id", "_dlt_load_id"}


def verify_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if x_api_key != INGEST_API_KEY:
        raise HTTPException(status_code=401, detail="Érvénytelen vagy hiányzó X-API-Key.")


router = APIRouter(dependencies=[Depends(verify_api_key)])


# --- Közös segédfüggvények és dlt betöltő motor ---
def _sanitize_key(key: str, idx: int) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", (key or "").strip()).lower()
    if not clean:
        clean = f"unnamed_col_{idx}"
    if clean in META_COLS:
        clean = f"src_{clean}"
    return clean


def _prepare_records(records: list[dict[str, Any]], source_name: str) -> list[dict[str, Any]]:
    now_utc = datetime.now(timezone.utc).isoformat()
    prepared = []
    for row in records:
        clean_row = {_sanitize_key(str(k), idx): v for idx, (k, v) in enumerate(row.items())}
        clean_row["data_source_meta"] = source_name
        clean_row["ingested_at"] = now_utc
        prepared.append(clean_row)
    return prepared


def run_dlt_ingest(records: list[dict[str, Any]], source_name: str, table_name: str,
    write_disposition: Literal["merge", "append", "replace"] = "merge", primary_key: list[str] | str | None = "id") -> None:
    try:
        rows = _prepare_records(records, source_name)
        logger.info("[API-DLT] Betöltés indul (%d sor): %s -> bronze_api.%s", len(rows), source_name, table_name)

        with pipeline_lock:
            pipeline = dlt.pipeline(pipeline_name="rest_api_ingest", destination=dlt.destinations.postgres(credentials=DB_CONFIG), dataset_name="bronze_api")
            pipeline.drop_pending_packages()

            @dlt.resource(name=table_name, write_disposition=write_disposition, primary_key=primary_key if write_disposition == "merge" else None)
            def api_resource():
                yield rows

            info = pipeline.run(api_resource())
            info.raise_on_failed_jobs()

        logger.info("[API-DLT] KÉSZ (%d sor): bronze_api.%s | %s", len(rows), table_name, info)

    except Exception as e:
        logger.exception("[API-DLT] HIBA (%s -> %s): %s", source_name, table_name, e)


# --- Pydantic sémák ---
class ShopifyIngestRequest(BaseModel):
    source_name: str = "shopify_admin_api"
    table_name: str = "shopify_orders"
    write_disposition: Literal["merge", "append", "replace"] = "merge"
    primary_key: list[str] | str | None = "id"
    orders: list[dict[str, Any]] = Field(..., min_length=1)


class ShopifyMockRequest(BaseModel):
    count: int = Field(default=50, ge=1, le=1000)
    table_name: str = "shopify_orders"


# --- Mock generátor (crm_shopify_api.py alapjain, Shopify Admin REST sémára) ---
def _load_optional_tickets() -> list[dict[str, str]]:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base_dir, "generated_tickets.json")
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return [
        {"message": "Hol tart a rendelésem?", "intent": "order_status"},
        {"message": "Áfás számlát szeretnék kérni.", "intent": "invoice_request"},
        {"message": "Digitális letöltési link nem érkezett meg.", "intent": "digital_delivery"},
    ]


def _generate_mock_shopify_orders(count: int) -> list[dict[str, Any]]:
    tickets = _load_optional_tickets()
    catalog = [
        {"product_id": 881001, "title": "KKV Automatizációs Blueprint (PDF)", "price": 29900.0, "requires_shipping": False},
        {"product_id": 881002, "title": "n8n + PostgreSQL Sabloncsomag", "price": 59900.0, "requires_shipping": False},
        {"product_id": 881003, "title": "AI Adat-Audit Konzultáció", "price": 89000.0, "requires_shipping": False},
        {"product_id": 881004, "title": "Okos Raktár Vonalkód Olvasó", "price": 44900.0, "requires_shipping": True},
    ]

    orders = []
    for _ in range(count):
        selected_items = random.sample(catalog, k=random.randint(1, 3))
        line_items = []
        subtotal = 0.0

        for item in selected_items:
            qty = random.randint(1, 3)
            line_total = round(item["price"] * qty, 2)
            subtotal += line_total
            line_items.append({
                "id": random.randint(9000000000, 9999999999),
                "product_id": item["product_id"],
                "title": item["title"],
                "quantity": qty,
                "price": f"{item['price']:.2f}",
                "requires_shipping": item["requires_shipping"],
            })

        total_tax = round(subtotal * 0.27, 2)
        total_price = round(subtotal + total_tax, 2)
        created_dt = datetime.now(timezone.utc) - timedelta(hours=random.randint(0, 720))
        updated_dt = created_dt + timedelta(minutes=random.randint(1, 120))
        cust_id = random.randint(7000000000, 7999999999)
        email = f"vevo{random.randint(1, 500)}@{random.choice(['ceg.hu', 'gmail.com', 'vallalkozas.hu', 'example.com'])}"
        ticket_sample = random.choice(tickets)

        orders.append({
            "id": random.randint(5000000000, 5999999999),
            "order_number": random.randint(1001, 9999),
            "email": email,
            "financial_status": random.choice(["paid", "paid", "paid", "pending", "refunded", "partially_paid"]),
            "fulfillment_status": random.choice(["fulfilled", "unfulfilled", "partial", None]),
            "currency": "HUF",
            "subtotal_price": f"{subtotal:.2f}",
            "total_tax": f"{total_tax:.2f}",
            "total_price": f"{total_price:.2f}",
            "created_at": created_dt.isoformat(),
            "updated_at": updated_dt.isoformat(),
            "note": ticket_sample.get("message"),
            "tags": ticket_sample.get("intent"),
            "customer": {
                "id": cust_id,
                "email": email,
                "first_name": random.choice(["Anna", "Bence", "Csaba", "Dóra", "Eszter", "Gábor", "Katalin", "László", "Péter", "Zsófia"]),
                "last_name": random.choice(["Nagy", "Kovács", "Tóth", "Szabó", "Horváth", "Varga", "Kiss", "Molnár", "Németh", "Farkas"]),
                "orders_count": random.randint(1, 15),
                "total_spent": f"{round(random.uniform(30000, 800000), 2):.2f}",
                "default_address": {
                    "address1": f"{random.choice(['Fő utca', 'Kossuth tér', 'Petőfi sándor utca', 'Váci út', 'Rákóczi út'])} {random.randint(1, 120)}.",
                    "city": random.choice(["Budapest", "Debrecen", "Szeged", "Győr", "Pécs", "Miskolc"]),
                    "country_code": "HU",
                    "zip": str(random.randint(1011, 9985)),
                },
            },
            "line_items": line_items,
        })
    return orders


# --- Végpontok ---
@router.post("/shopify")
async def ingest_shopify_orders(request: ShopifyIngestRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(
        run_dlt_ingest,
        records=request.orders,
        source_name=request.source_name,
        table_name=request.table_name,
        write_disposition=request.write_disposition,
        primary_key=request.primary_key,
    )
    return {
        "status": "accepted",
        "records_received": len(request.orders),
        "target_table": f"bronze_api.{request.table_name}",
    }


@router.post("/shopify/mock")
async def ingest_shopify_mock_orders(request: ShopifyMockRequest, background_tasks: BackgroundTasks):
    mock_orders = _generate_mock_shopify_orders(request.count)
    background_tasks.add_task(
        run_dlt_ingest,
        records=mock_orders,
        source_name="shopify_mock_generator",
        table_name=request.table_name,
        write_disposition="merge",
        primary_key="id",
    )
    return {
        "status": "accepted",
        "generated_records": len(mock_orders),
        "target_table": f"bronze_api.{request.table_name}",
        "sample": mock_orders[:2],
    }
