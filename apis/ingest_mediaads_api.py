# 2026.11.23 12.00
import json
import logging
import os
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import dlt
import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from pydantic import BaseModel, Field

try:
    from apis.ingest_media_mock_ads import (generate_mock_google_ads_insights, generate_mock_meta_insights, generate_mock_tiktok_insights)
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

# --- Táblaszintű lockolás ---
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


# --- Közös dlt betöltő motor (izolált pipeline_name + táblaszintű lock) ---
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


def run_ads_dlt_ingest(records: list[dict[str, Any]], source_name: str, table_name: str, primary_key: list[str]) -> None:
    if not records:
        logger.info("[ADS-DLT] Nincs betöltendő rekord (%s -> %s).", source_name, table_name)
        return

    table_lock = _get_table_lock(table_name)
    try:
        rows = _prepare_records(records, source_name)
        logger.info("[ADS-DLT] Betöltés indul (%d sor): %s -> bronze_api.%s", len(rows), source_name, table_name)

        with table_lock:
            pipeline = dlt.pipeline(pipeline_name=f"ads_{table_name}_ingest",
                destination=dlt.destinations.postgres(credentials=DB_CONFIG),
                dataset_name="bronze_api")
            pipeline.drop_pending_packages()

            @dlt.resource(name=table_name, write_disposition="merge", primary_key=primary_key)
            def ads_resource():
                yield rows

            info = pipeline.run(ads_resource())
            info.raise_on_failed_jobs()

        logger.info("[ADS-DLT] KÉSZ (%d sor): bronze_api.%s | %s", len(rows), table_name, info)

    except Exception as e:
        logger.exception("[ADS-DLT] HIBA (%s -> %s): %s", source_name, table_name, e)


# --- Pull kliensek (Meta, Google/YouTube, TikTok) ---
def _fetch_meta_insights(account_id: str, access_token: str, days_back: int) -> list[dict[str, Any]]:
    clean_act = account_id if account_id.startswith("act_") else f"act_{account_id}"
    today = datetime.now(timezone.utc).date()
    since = (today - timedelta(days=days_back)).isoformat()
    until = today.isoformat()

    url = f"https://graph.facebook.com/v21.0/{clean_act}/insights"
    params: dict[str, Any] = {
        "access_token": access_token,
        "level": "adset",
        "breakdowns": "publisher_platform",  # facebook vs instagram szétválasztása
        "time_increment": 1,
        "time_range": json.dumps({"since": since, "until": until}),
        "fields": "account_id,account_currency,campaign_id,campaign_name,adset_id,adset_name,impressions,clicks,reach,spend,actions,action_values",
        "limit": 500}

    results: list[dict[str, Any]] = []
    with httpx.Client(timeout=45.0) as client:
        while url:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            results.extend(payload.get("data", []))
            url = payload.get("paging", {}).get("next")
            params = {}  # A paging.next URL már tartalmazza a paramétereket
    return results


def _fetch_google_ads_insights(customer_id: str, developer_token: str, access_token: str, login_customer_id: str | None, days_back: int) -> list[dict[str, Any]]:
    clean_cid = customer_id.replace("-", "")
    today = datetime.now(timezone.utc).date()
    since = (today - timedelta(days=days_back)).isoformat()
    until = today.isoformat()

    # Campaign szintű lekérdezés: Search, Shopping, PMax és YouTube (VIDEO / DEMAND_GEN) egyben
    gaql = f"""
        SELECT
            customer.id,
            customer.currency_code,
            campaign.id,
            campaign.name,
            campaign.advertising_channel_type,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            metrics.video_views
        FROM campaign
        WHERE segments.date BETWEEN '{since}' AND '{until}'
          AND metrics.impressions > 0
    """
    headers = {"Authorization": f"Bearer {access_token}", "developer-token": developer_token, "Content-Type": "application/json"}
    if login_customer_id:
        headers["login-customer-id"] = login_customer_id.replace("-", "")

    url = f"https://googleads.googleapis.com/v18/customers/{clean_cid}/googleAds:searchStream"
    results: list[dict[str, Any]] = []

    with httpx.Client(timeout=45.0) as client:
        resp = client.post(url, headers=headers, json={"query": gaql})
        resp.raise_for_status()
        for batch in resp.json():
            for row in batch.get("results", []):
                # Egységes struktúra: ha nincs adGroup, "0"-t adunk a composite PK stabilitásához
                row.setdefault("adGroup", {"id": "0", "name": "campaign_level"})
                results.append(row)
    return results


def _fetch_tiktok_insights(advertiser_id: str, access_token: str, currency: str, days_back: int) -> list[dict[str, Any]]:
    today = datetime.now(timezone.utc).date()
    start_date = (today - timedelta(days=days_back)).isoformat()
    end_date = today.isoformat()

    url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
    headers = {"Access-Token": access_token}
    page = 1
    results: list[dict[str, Any]] = []

    with httpx.Client(timeout=45.0) as client:
        while True:
            params = {
                "advertiser_id": advertiser_id,
                "report_type": "BASIC",
                "data_level": "AUCTION_ADGROUP",
                "dimensions": json.dumps(["stat_time_day", "campaign_id", "adgroup_id"]),
                "metrics": json.dumps(["campaign_name", "adgroup_name", "spend", "impressions","clicks", "conversion", "complete_payment", "total_complete_payment_rate"]),
                "start_date": start_date,
                "end_date": end_date,
                "page": page,
                "page_size": 500,
            }
            resp = client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json().get("data", {})
            batch = data.get("list", [])
            for item in batch:
                item["advertiser_id"] = advertiser_id
                item["currency"] = currency
                results.append(item)

            total_pages = data.get("page_info", {}).get("total_page", 1)
            if page >= total_pages or not batch:
                break
            page += 1
    return results


# --- Request modellek ---
class MetaPullRequest(BaseModel):
    account_id: str = Field(default_factory=lambda: os.getenv("META_AD_ACCOUNT_ID", ""))
    access_token: str = Field(default_factory=lambda: os.getenv("META_ACCESS_TOKEN", ""))
    days_back: int = Field(default=14, ge=1, le=90)
    table_name: str = "meta_ads_insights"
    records: list[dict[str, Any]] | None = None  # Opcionális Push támogatás


class GoogleAdsPullRequest(BaseModel):
    customer_id: str = Field(default_factory=lambda: os.getenv("GOOGLE_ADS_CUSTOMER_ID", ""))
    developer_token: str = Field(default_factory=lambda: os.getenv("GOOGLE_ADS_DEV_TOKEN", ""))
    access_token: str = Field(default_factory=lambda: os.getenv("GOOGLE_ADS_ACCESS_TOKEN", ""))
    login_customer_id: str | None = Field(default_factory=lambda: os.getenv("GOOGLE_ADS_MCC_ID"))
    days_back: int = Field(default=14, ge=1, le=90)
    table_name: str = "google_ads_insights"
    records: list[dict[str, Any]] | None = None


class TikTokPullRequest(BaseModel):
    advertiser_id: str = Field(default_factory=lambda: os.getenv("TIKTOK_ADVERTISER_ID", ""))
    access_token: str = Field(default_factory=lambda: os.getenv("TIKTOK_ACCESS_TOKEN", ""))
    currency: str = Field(default_factory=lambda: os.getenv("TIKTOK_ACCOUNT_CURRENCY", "EUR"))
    days_back: int = Field(default=14, ge=1, le=90)
    table_name: str = "tiktok_ads_insights"
    records: list[dict[str, Any]] | None = None


class AdsMockRequest(BaseModel):
    days: int = Field(default=14, ge=1, le=90)

# --- Összetett Primary Key definíciók ---
META_PK = ["date_start", "account_id", "campaign_id", "adset_id", "publisher_platform"]
GOOGLE_PK = ["segments__date", "customer__id", "campaign__id", "ad_group__id"]
TIKTOK_PK = ["dimensions__stat_time_day", "advertiser_id", "dimensions__campaign_id", "dimensions__adgroup_id"]

# ---------- 1. META ADS (Facebook + Instagram) ----------
def _pull_and_ingest_meta(req: MetaPullRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_meta_insights(req.account_id, req.access_token, req.days_back)
        run_ads_dlt_ingest(rows, "meta_graph_api", req.table_name, META_PK)
    except Exception as e:
        logger.exception("[META-PULL] Hiba: %s", e)


@router.post("/meta")
async def sync_meta_ads(request: MetaPullRequest, background_tasks: BackgroundTasks):
    if request.records is None and (not request.account_id or not request.access_token):
        raise HTTPException(status_code=400, detail="Hiányzó Meta account_id vagy access_token.")
    background_tasks.add_task(_pull_and_ingest_meta, request)
    return {"status": "accepted", "channel": "meta (facebook+instagram)", "target_table": f"bronze_api.{request.table_name}"}


@router.post("/meta/mock")
async def sync_meta_ads_mock(request: AdsMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_meta_insights(request.days)
    background_tasks.add_task(run_ads_dlt_ingest, mock_rows, "meta_mock_generator", "meta_ads_insights", META_PK)
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.meta_ads_insights", "sample": mock_rows[:2]}


# ---------- 2. GOOGLE ADS (Search/Shopping/PMax + YouTube) ----------
def _pull_and_ingest_google(req: GoogleAdsPullRequest) -> None:
    try:
        rows = (req.records if req.records is not None
            else _fetch_google_ads_insights(req.customer_id, req.developer_token, req.access_token, req.login_customer_id, req.days_back))
        run_ads_dlt_ingest(rows, "google_ads_api", req.table_name, GOOGLE_PK)
    except Exception as e:
        logger.exception("[GOOGLE-PULL] Hiba: %s", e)


@router.post("/google")
async def sync_google_ads(request: GoogleAdsPullRequest, background_tasks: BackgroundTasks):
    if request.records is None and (not request.customer_id or not request.developer_token or not request.access_token):
        raise HTTPException(status_code=400, detail="Hiányzó Google Ads hitelesítő adatok.")
    background_tasks.add_task(_pull_and_ingest_google, request)
    return {"status": "accepted", "channel": "google (search/pmax+youtube)", "target_table": f"bronze_api.{request.table_name}"}


@router.post("/google/mock")
async def sync_google_ads_mock(request: AdsMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_google_ads_insights(request.days)
    background_tasks.add_task(run_ads_dlt_ingest, mock_rows, "google_mock_generator", "google_ads_insights", GOOGLE_PK)
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.google_ads_insights", "sample": mock_rows[:2]}


# ---------- 3. TIKTOK ADS ----------
def _pull_and_ingest_tiktok(req: TikTokPullRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_tiktok_insights(req.advertiser_id, req.access_token, req.currency, req.days_back)
        run_ads_dlt_ingest(rows, "tiktok_business_api", req.table_name, TIKTOK_PK)
    except Exception as e:
        logger.exception("[TIKTOK-PULL] Hiba: %s", e)


@router.post("/tiktok")
async def sync_tiktok_ads(request: TikTokPullRequest, background_tasks: BackgroundTasks):
    if request.records is None and (not request.advertiser_id or not request.access_token):
        raise HTTPException(status_code=400, detail="Hiányzó TikTok advertiser_id vagy access_token.")
    background_tasks.add_task(_pull_and_ingest_tiktok, request)
    return {"status": "accepted", "channel": "tiktok", "target_table": f"bronze_api.{request.table_name}"}


@router.post("/tiktok/mock")
async def sync_tiktok_ads_mock(request: AdsMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_tiktok_insights(request.days)
    background_tasks.add_task(run_ads_dlt_ingest, mock_rows, "tiktok_mock_generator", "tiktok_ads_insights", TIKTOK_PK)
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.tiktok_ads_insights", "sample": mock_rows[:2]}
