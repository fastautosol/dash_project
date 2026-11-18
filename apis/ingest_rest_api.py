# 2026.11.18  10.00
import logging
import os
import re
import threading
from datetime import datetime, timezone
from typing import Any, Literal
import dlt
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from pydantic import BaseModel, Field

try:
    from apis.ingest_mock_shopify import generate_mock_shopify_orders
    from apis.ingest_mock_billingo import generate_mock_billingo_documents
    MOCK_ENABLED = True
except ImportError:
    MOCK_ENABLED = False

logger = logging.getLogger("uvicorn.error")
pipeline_lock = threading.Lock()

os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "true"
os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "true"

INGEST_API_KEY = os.getenv("INGEST_API_KEY", "FAS")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgresql"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "n8n"),
    "username": os.getenv("POSTGRES_USER", "sql_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "sql_pass"),
    "connect_timeout": 15}

META_COLS = {"data_source_meta", "ingested_at", "_dlt_id", "_dlt_load_id"}

def verify_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if x_api_key != INGEST_API_KEY:
        raise HTTPException(status_code=401, detail="Érvénytelen vagy hiányzó X-API-Key.")

router = APIRouter(dependencies=[Depends(verify_api_key)])


# --- Közös dlt betöltő motor ---
def _sanitize_key(key: str, idx: int) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", (key or "").strip()).lower()
    if not clean:
        clean = f"unnamed_col_{idx}"
    return f"src_{clean}" if clean in META_COLS else clean


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


# ---------- Pydantic modellek ----------
class ShopifyIngestRequest(BaseModel):
    source_name: str = "shopify_admin_api"
    table_name: str = "shopify_orders"
    write_disposition: Literal["merge", "append", "replace"] = "merge"
    primary_key: list[str] | str | None = "id"
    orders: list[dict[str, Any]] = Field(..., min_length=1)


class BillingoIngestRequest(BaseModel):
    source_name: str = "billingo_v3_api"
    table_name: str = "billingo_invoices"
    write_disposition: Literal["merge", "append", "replace"] = "merge"
    primary_key: list[str] | str | None = "id"
    data: list[dict[str, Any]] = Field(..., min_length=1)


class MockRequest(BaseModel):
    count: int = Field(default=50, ge=1, le=1000)


# ---------- Shopify végpontok ----------
@router.post("/shopify")
async def ingest_shopify_orders(request: ShopifyIngestRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(
        run_dlt_ingest,
        records=request.orders,
        source_name=request.source_name,
        table_name=request.table_name,
        write_disposition=request.write_disposition,
        primary_key=request.primary_key)
    return {"status": "accepted", "records_received": len(request.orders), "target_table": f"bronze_api.{request.table_name}"}


@router.post("/shopify/mock")
async def ingest_shopify_mock(request: MockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve ebben a környezetben.")
    mock_orders = generate_mock_shopify_orders(request.count)
    background_tasks.add_task(run_dlt_ingest, mock_orders, "shopify_mock_generator", "shopify_orders", "merge", "id")
    return {"status": "accepted", "generated_records": len(mock_orders), "target_table": "bronze_api.shopify_orders", "sample": mock_orders[:2]}


# --- Billingo végpontok ---
@router.post("/billingo")
async def ingest_billingo_documents(request: BillingoIngestRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(
        run_dlt_ingest,
        records=request.data,
        source_name=request.source_name,
        table_name=request.table_name,
        write_disposition=request.write_disposition,
        primary_key=request.primary_key)
    return {"status": "accepted", "records_received": len(request.data), "target_table": f"bronze_api.{request.table_name}"}


@router.post("/billingo/mock")
async def ingest_billingo_mock(request: MockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve ebben a környezetben.")
    mock_docs = generate_mock_billingo_documents(request.count)
    background_tasks.add_task(run_dlt_ingest, mock_docs, "billingo_mock_generator", "billingo_invoices", "merge", "id")
    return {"status": "accepted", "generated_records": len(mock_docs), "target_table": "bronze_api.billingo_invoices", "sample": mock_docs[:2]}
