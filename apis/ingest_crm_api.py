# 2026.11.23  18.00
import logging
import os
import re
import threading
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any

import dlt
import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from pydantic import BaseModel, Field

try:
    from apis.ingest_crm_mock import (generate_mock_hubspot_deals, generate_mock_minicrm_projects, generate_mock_pipedrive_deals)
    MOCK_ENABLED = True
except ImportError:
    MOCK_ENABLED = False

logger = logging.getLogger("uvicorn.error")

os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "true"
os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "true"

INGEST_API_KEY = os.getenv("INGEST_API_KEY", "FAS")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgresql"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "n8n"),
    "username": os.getenv("POSTGRES_USER", "sql_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "sql_pass"),
    "connect_timeout": 15,
}

META_COLS = {"data_source_meta", "ingested_at", "_dlt_id", "_dlt_load_id"}

# --- Táblaszintű zárolás ---
_locks_guard = threading.Lock()
_table_locks: dict[str, threading.Lock] = {}


def _get_table_lock(table_name: str) -> threading.Lock:
    with _locks_guard:
        if table_name not in _table_locks:
            _table_locks[table_name] = threading.Lock()
        return _table_locks[table_name]


def verify_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if x_api_key != INGEST_API_KEY:
        raise HTTPException(status_code=401, detail="Érvénytelen vagy hiányzó X-API-Key.")


router = APIRouter(dependencies=[Depends(verify_api_key)])


# --- Kulcs-normalizálás (MiniCRM ékezetes egyedi mezőkhöz is) ---
def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def _sanitize_key(key: str, idx: int) -> str:
    ascii_key = _strip_accents((key or "").strip())
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", ascii_key).lower()
    clean = re.sub(r"_+", "_", clean).strip("_")
    if not clean:
        clean = f"unnamed_col_{idx}"
    if clean[0].isdigit():
        clean = f"col_{clean}"
    return f"src_{clean}" if clean in META_COLS else clean


def _normalize_value(val: Any) -> Any:
    if isinstance(val, dict):
        return {_sanitize_key(str(k), i): _normalize_value(v) for i, (k, v) in enumerate(val.items())}
    if isinstance(val, list):
        return [_normalize_value(item) for item in val]
    return val


def _prepare_records(records: list[dict[str, Any]], source_name: str) -> list[dict[str, Any]]:
    now_utc = datetime.now(timezone.utc).isoformat()
    prepared = []
    for row in records:
        clean_row = {_sanitize_key(str(k), idx): _normalize_value(v) for idx, (k, v) in enumerate(row.items())}
        clean_row["data_source_meta"] = source_name
        clean_row["ingested_at"] = now_utc
        prepared.append(clean_row)
    return prepared


def run_crm_dlt_ingest(
    records: list[dict[str, Any]],
    source_name: str,
    table_name: str,
    primary_key: list[str] | str = "id",
) -> None:
    if not records:
        logger.info("[CRM-DLT] Nincs betöltendő rekord (%s -> %s).", source_name, table_name)
        return

    safe_table = _sanitize_key(table_name, 0)
    table_lock = _get_table_lock(safe_table)

    try:
        rows = _prepare_records(records, source_name)
        logger.info("[CRM-DLT] Betöltés indul (%d sor): %s -> bronze_api.%s", len(rows), source_name, safe_table)

        with table_lock:
            pipeline = dlt.pipeline(
                pipeline_name=f"crm_{safe_table}_ingest",
                destination=dlt.destinations.postgres(credentials=DB_CONFIG),
                dataset_name="bronze_api",
            )
            pipeline.abort_packages()

            @dlt.resource(
                name=safe_table,
                write_disposition="merge",
                primary_key=primary_key,
            )
            def crm_resource():
                yield rows

            info = pipeline.run(crm_resource())
            info.raise_on_failed_jobs()

        logger.info("[CRM-DLT] KÉSZ (%d sor): bronze_api.%s | %s", len(rows), safe_table, info)

    except Exception as e:
        logger.exception("[CRM-DLT] HIBA (%s -> %s): %s", source_name, safe_table, e)


# --- Pull kliensek (HubSpot v3, Pipedrive v1, MiniCRM R3) ---
def _fetch_hubspot_deals(access_token: str, days_back: int) -> list[dict[str, Any]]:
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp() * 1000)
    url = "https://api.hubapi.com/crm/v3/objects/deals/search"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    properties = [
        "dealname", "amount", "deal_currency_code", "pipeline", "dealstage",
        "hs_deal_stage_probability", "closedate", "createdate",
        "hs_lastmodifieddate", "hubspot_owner_id", "hs_analytics_source", "closed_lost_reason",
    ]
    after: str | None = None
    results: list[dict[str, Any]] = []

    with httpx.Client(timeout=45.0) as client:
        while True:
            body: dict[str, Any] = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "hs_lastmodifieddate",
                        "operator": "GTE",
                        "value": str(since_ms),
                    }]
                }],
                "properties": properties,
                "limit": 100,
            }
            if after:
                body["after"] = after

            resp = client.post(url, headers=headers, json=body)
            resp.raise_for_status()
            payload = resp.json()
            results.extend(payload.get("results", []))

            after = payload.get("paging", {}).get("next", {}).get("after")
            if not after:
                break
    return results


def _fetch_pipedrive_deals(api_token: str, company_domain: str, days_back: int) -> list[dict[str, Any]]:
    clean_domain = company_domain.replace("https://", "").replace("http://", "").replace(".pipedrive.com", "").strip("/")
    base_url = f"https://{clean_domain}.pipedrive.com/api/v1" if clean_domain else "https://api.pipedrive.com/v1"
    since_str = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d %H:%M:%S")
    start = 0
    limit = 100
    results: list[dict[str, Any]] = []

    with httpx.Client(timeout=45.0) as client:
        while True:
            params = {
                "api_token": api_token,
                "since": since_str,
                "start": start,
                "limit": limit,
            }
            resp = client.get(f"{base_url}/recents", params={**params, "items": "deal"})
            resp.raise_for_status()
            payload = resp.json()
            items = payload.get("data") or []
            for entry in items:
                deal_data = entry.get("data") if isinstance(entry, dict) and "data" in entry else entry
                if isinstance(deal_data, dict) and "id" in deal_data:
                    results.append(deal_data)

            pagination = payload.get("additional_data", {}).get("pagination", {})
            if not pagination.get("more_items_in_collection"):
                break
            start = pagination.get("next_start", start + limit)
    return results


def _fetch_minicrm_projects(system_id: str, api_key: str, category_id: int | None, days_back: int) -> list[dict[str, Any]]:
    """MiniCRM R3 API: módosult projekt ID-k listázása, majd részletes adatlapok lekérése."""
    updated_since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d %H:%M:%S")
    base_url = "https://r3.minicrm.hu/Api/R3"
    page = 1
    project_ids: list[str] = []

    with httpx.Client(timeout=45.0, auth=(system_id, api_key)) as client:
        while True:
            params: dict[str, Any] = {"UpdatedSince": updated_since, "Page": page}
            if category_id is not None:
                params["CategoryId"] = category_id

            resp = client.get(f"{base_url}/Project", params=params)
            resp.raise_for_status()
            payload = resp.json()
            batch_dict = payload.get("Results") or {}
            if not isinstance(batch_dict, dict) or not batch_dict:
                break

            project_ids.extend(batch_dict.keys())
            if len(batch_dict) < 100:
                break
            page += 1

        results: list[dict[str, Any]] = []
        for pid in project_ids:
            detail_resp = client.get(f"{base_url}/Project/{pid}")
            detail_resp.raise_for_status()
            detail = detail_resp.json()
            if isinstance(detail, dict) and detail.get("Id"):
                results.append(detail)
    return results


# --- Request modellek ---
class HubSpotSyncRequest(BaseModel):
    access_token: str = Field(default_factory=lambda: os.getenv("HUBSPOT_ACCESS_TOKEN", ""))
    days_back: int = Field(default=30, ge=1, le=365)
    table_name: str = "hubspot_deals"
    records: list[dict[str, Any]] | None = None


class PipedriveSyncRequest(BaseModel):
    api_token: str = Field(default_factory=lambda: os.getenv("PIPEDRIVE_API_TOKEN", ""))
    company_domain: str = Field(default_factory=lambda: os.getenv("PIPEDRIVE_DOMAIN", ""))
    days_back: int = Field(default=30, ge=1, le=365)
    table_name: str = "pipedrive_deals"
    records: list[dict[str, Any]] | None = None


class MiniCrmSyncRequest(BaseModel):
    system_id: str = Field(default_factory=lambda: os.getenv("MINICRM_SYSTEM_ID", ""))
    api_key: str = Field(default_factory=lambda: os.getenv("MINICRM_API_KEY", ""))
    category_id: int | None = Field(default=None)
    days_back: int = Field(default=30, ge=1, le=365)
    table_name: str = "minicrm_projects"
    records: list[dict[str, Any]] | None = None


class CrmMockRequest(BaseModel):
    count: int = Field(default=50, ge=1, le=1000)


# ---------- 1. HUBSPOT ----------
def _pull_and_ingest_hubspot(req: HubSpotSyncRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_hubspot_deals(req.access_token, req.days_back)
        run_crm_dlt_ingest(rows, "hubspot_crm_v3", req.table_name, "id")
    except Exception as e:
        logger.exception("[HUBSPOT-PULL] Hiba: %s", e)


@router.post("/hubspot")
async def sync_hubspot(request: HubSpotSyncRequest, background_tasks: BackgroundTasks):
    if request.records is None and not request.access_token:
        raise HTTPException(status_code=400, detail="Hiányzó HubSpot access_token vagy records.")
    background_tasks.add_task(_pull_and_ingest_hubspot, request)
    return {"status": "accepted", "provider": "hubspot", "target_table": f"bronze_api.{request.table_name}"}


@router.post("/hubspot/mock")
async def sync_hubspot_mock(request: CrmMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_hubspot_deals(request.count)
    background_tasks.add_task(run_crm_dlt_ingest, mock_rows, "hubspot_mock_generator", "hubspot_deals", "id")
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.hubspot_deals", "sample": mock_rows[:2]}


# ---------- 2. PIPEDRIVE ----------
def _pull_and_ingest_pipedrive(req: PipedriveSyncRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_pipedrive_deals(req.api_token, req.company_domain, req.days_back)
        run_crm_dlt_ingest(rows, "pipedrive_api_v1", req.table_name, "id")
    except Exception as e:
        logger.exception("[PIPEDRIVE-PULL] Hiba: %s", e)


@router.post("/pipedrive")
async def sync_pipedrive(request: PipedriveSyncRequest, background_tasks: BackgroundTasks):
    if request.records is None and not request.api_token:
        raise HTTPException(status_code=400, detail="Hiányzó Pipedrive api_token vagy records.")
    background_tasks.add_task(_pull_and_ingest_pipedrive, request)
    return {"status": "accepted", "provider": "pipedrive", "target_table": f"bronze_api.{request.table_name}"}


@router.post("/pipedrive/mock")
async def sync_pipedrive_mock(request: CrmMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_pipedrive_deals(request.count)
    background_tasks.add_task(run_crm_dlt_ingest, mock_rows, "pipedrive_mock_generator", "pipedrive_deals", "id")
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.pipedrive_deals", "sample": mock_rows[:2]}


# ---------- 3. MINICRM ----------
def _pull_and_ingest_minicrm(req: MiniCrmSyncRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_minicrm_projects(req.system_id, req.api_key, req.category_id, req.days_back)
        run_crm_dlt_ingest(rows, "minicrm_api_r3", req.table_name, "id")
    except Exception as e:
        logger.exception("[MINICRM-PULL] Hiba: %s", e)


@router.post("/minicrm")
async def sync_minicrm(request: MiniCrmSyncRequest, background_tasks: BackgroundTasks):
    if request.records is None and (not request.system_id or not request.api_key):
        raise HTTPException(status_code=400, detail="Hiányzó MiniCRM system_id / api_key vagy records.")
    background_tasks.add_task(_pull_and_ingest_minicrm, request)
    return {"status": "accepted", "provider": "minicrm", "target_table": f"bronze_api.{request.table_name}"}


@router.post("/minicrm/mock")
async def sync_minicrm_mock(request: CrmMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_minicrm_projects(request.count)
    background_tasks.add_task(run_crm_dlt_ingest, mock_rows, "minicrm_mock_generator", "minicrm_projects", "id")
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.minicrm_projects", "sample": mock_rows[:2]}
