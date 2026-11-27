import json
import os
import subprocess
import textwrap
import threading
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Body
from pydantic import BaseModel

# --- Környezeti változók ---
AIRBYTE_PYTHON = os.getenv("AIRBYTE_PYTHON", "/opt/airbyte-venv/bin/python")
DB_URL = os.getenv("DB_URL", "postgresql://sql_admin:sql_pass@postgresql:5432/n8n")
AIRBYTE_BASE_DIR = os.getenv("AIRBYTE_BASE_DIR", "/airbyte-data")

GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
GOOGLE_ADS_CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
GOOGLE_ADS_CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
GOOGLE_ADS_REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "")
GOOGLE_ADS_CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID", "")

META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID", "")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")

SHOPIFY_SHOP_NAME = os.getenv("SHOPIFY_SHOP_NAME", "")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "")

router = APIRouter()
SYNC_LOCK = threading.Lock()

# --- Izolált Python 3.12-ben futó inline ETL kód ---
INLINE_WORKER_CODE = textwrap.dedent(r"""
    import json, os, sys
    import airbyte as ab
    import dlt

    db_url = os.environ["DB_URL"]
    base_dir = os.environ.get("AIRBYTE_BASE_DIR", "/airbyte-data")

    payload = json.loads(sys.stdin.read())
    dataset = payload["dataset"]
    streams_meta = payload["streams_meta"]

    os.makedirs(f"{base_dir}/cache/{dataset}", exist_ok=True)
    os.makedirs(f"{base_dir}/dlt_pipelines", exist_ok=True)
    os.chdir(base_dir)

    cache = ab.new_local_cache(cache_dir=f"{base_dir}/cache/{dataset}")
    source = ab.get_source(
        payload["connector"],
        config=payload["config"],
        streams=list(streams_meta.keys()),
        install_if_missing=True,
    )
    source.check()
    read_result = source.read(cache=cache)

    def iter_batches(stream_name):
        yield from read_result[stream_name].to_arrow().to_batches(max_chunk_size=10_000)

    resources = [
        dlt.resource(
            iter_batches(name),
            name=name,
            write_disposition=meta["disposition"],
            primary_key=meta["primary_key"],
        )
        for name, meta in streams_meta.items()
    ]

    pipeline = dlt.pipeline(
        pipeline_name=f"pyairbyte_{dataset}",
        pipelines_dir=f"{base_dir}/dlt_pipelines",
        destination=dlt.destinations.postgres(credentials=db_url),
        dataset_name=dataset,
    )
    print(f"[{dataset}] SIKER:\n{pipeline.run(resources)}")
""").strip()


# --- Opcionális POST paraméter sémák ---
class GoogleAdsSyncParams(BaseModel):
    start_date: str = "2026-01-01"
    customer_id: Optional[str] = None
    streams: Optional[list[str]] = None


class MetaAdsSyncParams(BaseModel):
    start_date: str = "2026-01-01T00:00:00Z"
    account_id: Optional[str] = None
    streams: Optional[list[str]] = None


class ShopifySyncParams(BaseModel):
    start_date: str = "2026-01-01"
    shop_name: Optional[str] = None
    streams: Optional[list[str]] = None


def _run_isolated_sync(
    dataset: str,
    connector: str,
    config: dict[str, Any],
    streams_meta: dict[str, dict[str, Any]],
) -> None:
    payload = json.dumps({
        "dataset": dataset,
        "connector": connector,
        "config": config,
        "streams_meta": streams_meta,
    })
    env = {**os.environ, "DB_URL": DB_URL, "AIRBYTE_BASE_DIR": AIRBYTE_BASE_DIR}

    with SYNC_LOCK:
        proc = subprocess.run(
            [AIRBYTE_PYTHON, "-c", INLINE_WORKER_CODE],
            input=payload,
            env=env,
            text=True,
            capture_output=True,
        )
        if proc.stdout:
            print(proc.stdout.strip())
        if proc.returncode != 0:
            print(f"[{dataset}] HIBA (exit {proc.returncode}):\n{proc.stderr.strip()}")


def sync_google_ads_task(params: GoogleAdsSyncParams) -> None:
    customer_id = (params.customer_id or GOOGLE_ADS_CUSTOMER_ID).replace("-", "")
    if not all([GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_REFRESH_TOKEN, customer_id]):
        print("[raw_google_ads] Hiányzó Google Ads környezeti változók.")
        return

    all_streams = {
        "campaign": {"disposition": "merge", "primary_key": "campaign.id"},
        "campaign_performance_report": {"disposition": "append", "primary_key": None},
    }
    selected = {k: v for k, v in all_streams.items() if not params.streams or k in params.streams}

    config = {
        "credentials": {
            "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
            "client_id": GOOGLE_ADS_CLIENT_ID,
            "client_secret": GOOGLE_ADS_CLIENT_SECRET,
            "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
        },
        "customer_ids": [customer_id],
        "start_date": params.start_date,
    }
    _run_isolated_sync("raw_google_ads", "source-google-ads", config, selected)


def sync_meta_ads_task(params: MetaAdsSyncParams) -> None:
    account_id = (params.account_id or META_AD_ACCOUNT_ID).removeprefix("act_")
    if not all([account_id, META_ACCESS_TOKEN]):
        print("[raw_meta_ads] Hiányzó Meta Ads környezeti változók.")
        return

    all_streams = {
        "campaigns": {"disposition": "merge", "primary_key": "id"},
        "ads_insights": {"disposition": "append", "primary_key": None},
    }
    selected = {k: v for k, v in all_streams.items() if not params.streams or k in params.streams}

    config = {
        "account_ids": [account_id],
        "access_token": META_ACCESS_TOKEN,
        "start_date": params.start_date,
    }
    _run_isolated_sync("raw_meta_ads", "source-facebook-marketing", config, selected)


def sync_shopify_task(params: ShopifySyncParams) -> None:
    shop = (params.shop_name or SHOPIFY_SHOP_NAME).removesuffix(".myshopify.com")
    if not all([shop, SHOPIFY_ACCESS_TOKEN]):
        print("[raw_shopify] Hiányzó Shopify környezeti változók.")
        return

    all_streams = {
        "orders": {"disposition": "merge", "primary_key": "id"},
        "products": {"disposition": "merge", "primary_key": "id"},
        "customers": {"disposition": "merge", "primary_key": "id"},
    }
    selected = {k: v for k, v in all_streams.items() if not params.streams or k in params.streams}

    config = {
        "shop": shop,
        "credentials": {
            "auth_method": "api_password",
            "api_password": SHOPIFY_ACCESS_TOKEN,
        },
        "start_date": params.start_date,
    }
    _run_isolated_sync("raw_shopify", "source-shopify", config, selected)


@router.post("/google_ads_sync")
async def trigger_google_ads_sync(
    background_tasks: BackgroundTasks,
    params: GoogleAdsSyncParams = Body(default_factory=GoogleAdsSyncParams),
):
    background_tasks.add_task(sync_google_ads_task, params)
    return {"status": "google_ads_sync_started", "start_date": params.start_date}


@router.post("/meta_ads_sync")
async def trigger_meta_ads_sync(
    background_tasks: BackgroundTasks,
    params: MetaAdsSyncParams = Body(default_factory=MetaAdsSyncParams),
):
    background_tasks.add_task(sync_meta_ads_task, params)
    return {"status": "meta_ads_sync_started", "start_date": params.start_date}


@router.post("/shopify_sync")
async def trigger_shopify_sync(
    background_tasks: BackgroundTasks,
    params: ShopifySyncParams = Body(default_factory=ShopifySyncParams),
):
    background_tasks.add_task(sync_shopify_task, params)
    return {"status": "shopify_sync_started", "start_date": params.start_date}
