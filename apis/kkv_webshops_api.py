# 2026.11.23  15.00
import logging
import os
import re
import threading
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any

import dlt
import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from pydantic import BaseModel, Field

try:
    from apis.kkv_webshops_mock import (generate_mock_shopify_orders, generate_mock_shoprenter_orders, generate_mock_unas_orders, generate_mock_woocommerce_orders)
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
    "connect_timeout": 15}

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


def run_webshop_dlt_ingest(records: list[dict[str, Any]], source_name: str, table_name: str, primary_key: list[str] | str) -> None:
    if not records:
        logger.info("[WEBSHOP-DLT] Nincs betöltendő rekord (%s -> %s).", source_name, table_name)
        return

    table_lock = _get_table_lock(table_name)
    try:
        rows = _prepare_records(records, source_name)
        logger.info("[WEBSHOP-DLT] Betöltés indul (%d sor): %s -> bronze_api.%s", len(rows), source_name, table_name)

        with table_lock:
            pipeline = dlt.pipeline(pipeline_name=f"webshop_{table_name}_ingest",
                destination=dlt.destinations.postgres(credentials=DB_CONFIG),
                dataset_name="bronze_api")
            pipeline.abort_packages()

            @dlt.resource(name=table_name, write_disposition="merge", primary_key=primary_key)
            def webshop_resource():
                yield rows

            info = pipeline.run(webshop_resource())
            info.raise_on_failed_jobs()

        logger.info("[WEBSHOP-DLT] KÉSZ (%d sor): bronze_api.%s | %s", len(rows), table_name, info)

    except Exception as e:
        logger.exception("[WEBSHOP-DLT] HIBA (%s -> %s): %s", source_name, table_name, e)


# --- Pull kliensek (Shopify, UNAS, Shoprenter, WooCommerce) ---
def _fetch_shopify_orders(shop_domain: str, access_token: str, days_back: int) -> list[dict[str, Any]]:
    clean_domain = shop_domain.replace("https://", "").replace("http://", "").strip("/")
    updated_min = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
    url: str | None = f"https://{clean_domain}/admin/api/2025-01/orders.json"
    headers = {"X-Shopify-Access-Token": access_token}
    params: dict[str, Any] = {"status": "any", "updated_at_min": updated_min, "limit": 250}
    results: list[dict[str, Any]] = []

    with httpx.Client(timeout=45.0) as client:
        while url:
            resp = client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            results.extend(resp.json().get("orders", []))
            link_header = resp.headers.get("Link", "")
            next_url = None
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    next_url = part.split(";")[0].strip().strip("<>")
                    break
            url = next_url
            params = {}
    return results


def _xml_element_to_dict(elem: ET.Element) -> Any:
    children = list(elem)
    if not children:
        return (elem.text or "").strip()
    result: dict[str, Any] = {}
    for child in children:
        val = _xml_element_to_dict(child)
        if child.tag in result:
            if not isinstance(result[child.tag], list):
                result[child.tag] = [result[child.tag]]
            result[child.tag].append(val)
        else:
            result[child.tag] = val
    return result


def _fetch_unas_orders(api_key: str, days_back: int) -> list[dict[str, Any]]:
    """UNAS XML API: login token lekérése, majd getOrder szűréssel."""
    date_start = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y.%m.%d")
    with httpx.Client(timeout=45.0) as client:
        login_xml = f"<Params><ApiKey>{api_key}</ApiKey></Params>"
        login_resp = client.post("https://api.unas.hu/shop/login", content=login_xml.encode("utf-8"))
        login_resp.raise_for_status()
        login_root = ET.fromstring(login_resp.text)
        token_el = login_root.find("Token")
        if token_el is None or not token_el.text:
            raise ValueError("UNAS autentikációs hiba: hiányzó Token.")
        token = token_el.text.strip()

        order_req_xml = f"""<?xml version="1.0" encoding="UTF-8" ?>
        <Params>
            <DateModStart>{date_start}</DateModStart>
        </Params>"""
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "text/xml; charset=utf-8"}
        orders_resp = client.post("https://api.unas.hu/shop/getOrder", headers=headers, content=order_req_xml.encode("utf-8"))
        orders_resp.raise_for_status()

        root = ET.fromstring(orders_resp.text)
        orders: list[dict[str, Any]] = []
        for order_el in root.findall("Order"):
            parsed = _xml_element_to_dict(order_el)
            # Biztosítjuk, hogy az Items/Item mindig lista legyen (1 tételes rendelésnél is)
            items_block = parsed.get("Items")
            if isinstance(items_block, dict) and "Item" in items_block:
                raw_items = items_block["Item"]
                parsed["Items"] = raw_items if isinstance(raw_items, list) else [raw_items]
            else:
                parsed["Items"] = []
            orders.append(parsed)
        return orders


def _fetch_shoprenter_orders(shop_name: str, username: str, password: str, days_back: int) -> list[dict[str, Any]]:
    """Shoprenter REST API full=1 paraméterrel, hogy a tételek is beágyazva érkezzenek."""
    clean_shop = shop_name.replace(".api.myshoprenter.hu", "").strip()
    base_url = f"http://{clean_shop}.api.myshoprenter.hu/orders"
    date_min = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d %H:%M:%S")
    page = 0
    limit = 200
    results: list[dict[str, Any]] = []

    with httpx.Client(timeout=45.0, auth=(username, password), headers={"Accept": "application/json"}) as client:
        while True:
            params = {"full": 1, "dateUpdatedMin": date_min, "page": page, "limit": limit}
            resp = client.get(base_url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            items = payload.get("items", [])
            if not items:
                break
            results.extend(items)
            if len(items) < limit:
                break
            page += 1
    return results


def _fetch_woocommerce_orders(store_url: str, consumer_key: str, consumer_secret: str, days_back: int) -> list[dict[str, Any]]:
    clean_url = store_url.rstrip("/")
    endpoint = f"{clean_url}/wp-json/wc/v3/orders"
    modified_after = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
    page = 1
    results: list[dict[str, Any]] = []

    with httpx.Client(timeout=45.0, auth=(consumer_key, consumer_secret)) as client:
        while True:
            params = {"modified_after": modified_after, "per_page": 100, "page": page}
            resp = client.get(endpoint, params=params)
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            results.extend(batch)
            if len(batch) < 100:
                break
            page += 1
    return results


# --- Request modellek ---
class ShopifySyncRequest(BaseModel):
    shop_domain: str = Field(default_factory=lambda: os.getenv("SHOPIFY_SHOP_DOMAIN", ""))
    access_token: str = Field(default_factory=lambda: os.getenv("SHOPIFY_ACCESS_TOKEN", ""))
    days_back: int = Field(default=7, ge=1, le=365)
    table_name: str = "shopify_orders"
    records: list[dict[str, Any]] | None = None


class UnasSyncRequest(BaseModel):
    api_key: str = Field(default_factory=lambda: os.getenv("UNAS_API_KEY", ""))
    days_back: int = Field(default=7, ge=1, le=365)
    table_name: str = "unas_orders"
    records: list[dict[str, Any]] | None = None


class ShoprenterSyncRequest(BaseModel):
    shop_name: str = Field(default_factory=lambda: os.getenv("SHOPRENTER_SHOP_NAME", ""))
    username: str = Field(default_factory=lambda: os.getenv("SHOPRENTER_USERNAME", ""))
    password: str = Field(default_factory=lambda: os.getenv("SHOPRENTER_PASSWORD", ""))
    days_back: int = Field(default=7, ge=1, le=365)
    table_name: str = "shoprenter_orders"
    records: list[dict[str, Any]] | None = None


class WooCommerceSyncRequest(BaseModel):
    store_url: str = Field(default_factory=lambda: os.getenv("WOOCOMMERCE_STORE_URL", ""))
    consumer_key: str = Field(default_factory=lambda: os.getenv("WOOCOMMERCE_CONSUMER_KEY", ""))
    consumer_secret: str = Field(default_factory=lambda: os.getenv("WOOCOMMERCE_CONSUMER_SECRET", ""))
    days_back: int = Field(default=7, ge=1, le=365)
    table_name: str = "woocommerce_orders"
    records: list[dict[str, Any]] | None = None


class WebshopMockRequest(BaseModel):
    count: int = Field(default=50, ge=1, le=1000)


# ---------- 1. SHOPIFY ----------
def _pull_and_ingest_shopify(req: ShopifySyncRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_shopify_orders(req.shop_domain, req.access_token, req.days_back)
        run_webshop_dlt_ingest(rows, "shopify_admin_api", req.table_name, "id")
    except Exception as e:
        logger.exception("[SHOPIFY-PULL] Hiba: %s", e)


@router.post("/shopify")
async def sync_shopify(request: ShopifySyncRequest, background_tasks: BackgroundTasks):
    if request.records is None and (not request.shop_domain or not request.access_token):
        raise HTTPException(status_code=400, detail="Hiányzó Shopify shop_domain vagy access_token.")
    background_tasks.add_task(_pull_and_ingest_shopify, request)
    return {"status": "accepted", "platform": "shopify", "target_table": f"bronze_api.{request.table_name}"}


@router.post("/shopify/mock")
async def sync_shopify_mock(request: WebshopMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_shopify_orders(request.count)
    background_tasks.add_task(run_webshop_dlt_ingest, mock_rows, "shopify_mock_generator", "shopify_orders", "id")
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.shopify_orders", "sample": mock_rows[:2]}


# ---------- 2. UNAS ----------
def _pull_and_ingest_unas(req: UnasSyncRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_unas_orders(req.api_key, req.days_back)
        run_webshop_dlt_ingest(rows, "unas_xml_api", req.table_name, "key")
    except Exception as e:
        logger.exception("[UNAS-PULL] Hiba: %s", e)


@router.post("/unas")
async def sync_unas(request: UnasSyncRequest, background_tasks: BackgroundTasks):
    if request.records is None and not request.api_key:
        raise HTTPException(status_code=400, detail="Hiányzó UNAS api_key.")
    background_tasks.add_task(_pull_and_ingest_unas, request)
    return {"status": "accepted", "platform": "unas", "target_table": f"bronze_api.{request.table_name}"}


@router.post("/unas/mock")
async def sync_unas_mock(request: WebshopMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_unas_orders(request.count)
    background_tasks.add_task(run_webshop_dlt_ingest, mock_rows, "unas_mock_generator", "unas_orders", "key")
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.unas_orders", "sample": mock_rows[:2]}


# ---------- 3. SHOPRENTER ----------
def _pull_and_ingest_shoprenter(req: ShoprenterSyncRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_shoprenter_orders(req.shop_name, req.username, req.password, req.days_back)
        run_webshop_dlt_ingest(rows, "shoprenter_rest_api", req.table_name, "id")
    except Exception as e:
        logger.exception("[SHOPRENTER-PULL] Hiba: %s", e)


@router.post("/shoprenter")
async def sync_shoprenter(request: ShoprenterSyncRequest, background_tasks: BackgroundTasks):
    if request.records is None and (not request.shop_name or not request.username or not request.password):
        raise HTTPException(status_code=400, detail="Hiányzó Shoprenter hitelesítő adatok.")
    background_tasks.add_task(_pull_and_ingest_shoprenter, request)
    return {"status": "accepted", "platform": "shoprenter", "target_table": f"bronze_api.{request.table_name}"}


@router.post("/shoprenter/mock")
async def sync_shoprenter_mock(request: WebshopMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_shoprenter_orders(request.count)
    background_tasks.add_task(run_webshop_dlt_ingest, mock_rows, "shoprenter_mock_generator", "shoprenter_orders", "id")
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.shoprenter_orders", "sample": mock_rows[:2]}


# ---------- 4. WOOCOMMERCE ----------
def _pull_and_ingest_woocommerce(req: WooCommerceSyncRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_woocommerce_orders(req.store_url, req.consumer_key, req.consumer_secret, req.days_back)
        run_webshop_dlt_ingest(rows, "woocommerce_rest_api", req.table_name, "id")
    except Exception as e:
        logger.exception("[WOOCOMMERCE-PULL] Hiba: %s", e)


@router.post("/woocommerce")
async def sync_woocommerce(request: WooCommerceSyncRequest, background_tasks: BackgroundTasks):
    if request.records is None and (not request.store_url or not request.consumer_key or not request.consumer_secret):
        raise HTTPException(status_code=400, detail="Hiányzó WooCommerce hitelesítő adatok.")
    background_tasks.add_task(_pull_and_ingest_woocommerce, request)
    return {"status": "accepted", "platform": "woocommerce", "target_table": f"bronze_api.{request.table_name}"}


@router.post("/woocommerce/mock")
async def sync_woocommerce_mock(request: WebshopMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_woocommerce_orders(request.count)
    background_tasks.add_task(run_webshop_dlt_ingest, mock_rows, "woocommerce_mock_generator", "woocommerce_orders", "id")
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.woocommerce_orders", "sample": mock_rows[:2]}
