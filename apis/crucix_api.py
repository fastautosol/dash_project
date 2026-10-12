# crucix_pipeline.py
# Crucix OSINT -> DLT -> PostgreSQL
# FastAPI Router + BackgroundTasks
# 2026-10-12

import os
import time
import logging
from datetime import datetime, timezone
from typing import Any

import requests
import dlt

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()


# --------------------------------------------------
# 2. CONFIGURATION
# --------------------------------------------------

CRUCIX_API_URL = os.getenv("CRUCIX_API_URL", "http://crucix:3117/api/data")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgresql"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "n8n"),
    "username": os.getenv("DB_USER", "sql_admin"),
    "password": os.getenv("DB_PASSWORD", ""),
    "connect_timeout": 15}

REQUEST_TIMEOUT = 30
REQUEST_DELAY_SEC = 0.1

CRYPTO_KEYWORDS = [
    "crypto",
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "defi",
    "rwa",
    "tokenization",
    "fed",
    "federal reserve",
    "inflation",
    "interest rate",
    "treasury",
    "financial market",
    "stock market",
    "geopolitical",
    "oil price",
]


# --------------------------------------------------
# 3. API REQUEST MODEL
# --------------------------------------------------

class CrucixRequest(BaseModel):
    max_items: int = Field(default=200, ge=1, le=5000)
    filter_keywords: bool = True


# --------------------------------------------------
# 4. CRUCIX API CLIENT
# --------------------------------------------------

def crucix_get() -> Any:
    """Lekéri a Crucix API aktuális JSON-válaszát."""
    response = requests.get(CRUCIX_API_URL, timeout=REQUEST_TIMEOUT, headers={"Accept": "application/json"})
    response.raise_for_status()
    return response.json()


# --------------------------------------------------
# 5. NORMALIZE API RESPONSE
# --------------------------------------------------

def extract_items(data: Any) -> list[dict]:
    """
    Támogatott formák: JSON lista {"signals": [...]} {"feeds": [...]} {"events": [...]} {"results": [...]}
    Ha a Crucix más struktúrát ad vissza, a teljes objektumot egy rekordként mentjük.
    """

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if isinstance(data, dict):
        for key in ("signals", "feeds", "events", "results"):
            value = data.get(key)

            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

        return [data]

    raise ValueError(f"Nem támogatott Crucix JSON-típus: {type(data).__name__}")

# --------------------------------------------------
# 6. KEYWORD FILTER
# --------------------------------------------------

def is_relevant(item: dict) -> bool:
    """Kripto-, RWA-, pénzügyi és geopolitikai előszűrés."""

    text = " ".join(
        str(item.get(key, ""))
        for key in ("title", "content", "description", "summary", "source", "category")
    ).lower()

    return any(keyword in text for keyword in CRYPTO_KEYWORDS)


# --------------------------------------------------
# 7. DLT RESOURCE
# --------------------------------------------------

@dlt.resource(name="osint_feeds", write_disposition="append", columns={"raw_data": {"data_type": "json"}})
def fetch_signals(max_items: int = 200, filter_keywords: bool = True):
    """Egy futás során lekéri a Crucix adatokat, opcionálisan szűri őket, majd DLT-nek adja át."""

    data = crucix_get()
    items = extract_items(data)
    logger.info("Crucix API: %d elem érkezett", len(items))
    count = 0

    for item in items:
        if filter_keywords and not is_relevant(item):
            continue

        # A nyers rekord megőrzése mellett néhány általános mezőt is kinyerünk.
        yield {
            "record_id": str(item.get("id") or item.get("uuid") or item.get("url") or "") or None,
            "title": item.get("title"),
            "source": item.get("source"),
            "created_at": item.get("created_at"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "raw_data": item}

        count += 1
        if count >= max_items:
            break

    logger.info("DLT resource: %d rekord előkészítve", count)

# --------------------------------------------------
# 8. DLT PIPELINE
# --------------------------------------------------

def run_crucix_pipeline(max_items: int = 200, filter_keywords: bool = True):
    """DLT futtatása és PostgreSQL-be mentés."""

    try:
        pipeline = dlt.pipeline(pipeline_name="crucix_osint_pipeline", destination=dlt.destinations.postgres(credentials=DB_CONFIG),
            dataset_name="bronze")

        resource = fetch_signals(max_items=max_items, filter_keywords=filter_keywords)
        load_info = pipeline.run(resource)
        logger.info("Crucix DLT pipeline sikeres: %s", load_info)

        return str(load_info)

    except Exception:
        logger.exception("Crucix pipeline hiba")
        raise


# --------------------------------------------------
# 9. FASTAPI BACKGROUND TASK
# --------------------------------------------------

def run_pipeline_background(max_items: int, filter_keywords: bool):

    try:
        run_crucix_pipeline(max_items=max_items, filter_keywords=filter_keywords)
    except Exception:
        # A hiba részleteit a logger.exception már rögzíti.
        # A háttérfeladat nem küld HTTP-választ.
        pass


@router.post("/run")
async def trigger_crucix_pipeline(request: CrucixRequest, background_tasks: BackgroundTasks):

    background_tasks.add_task(run_pipeline_background, request.max_items, request.filter_keywords)

    return {
        "status": "accepted",
        "message": "Crucix OSINT pipeline elindítva a háttérben",
        "max_items": request.max_items,
        "filter_keywords": request.filter_keywords}

# --------------------------------------------------
# 10. API HEALTH / TEST
# --------------------------------------------------

@router.get("/health")
async def crucix_health():

    try:
        data = crucix_get()
        return {"status": "ok", "crucix_api_url": CRUCIX_API_URL, "response_type": type(data).__name__}

    except Exception as exc:
        logger.exception("Crucix API nem érhető el")
        raise HTTPException(status_code=502, detail=f"Crucix API hiba: {str(exc)}")
