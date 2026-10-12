# crucix_api.py
# Crucix OSINT -> DLT -> PostgreSQL (bronze.osint_feeds)
# FastAPI Router + BackgroundTasks
# 2026-10-12
#
# A Crucix /api/data valódi szerkezete (dashboard/inject.mjs synthesize()):
#   newsFeed: [{headline, source, type, timestamp, region, urgent, url}]
#   tg:       {posts, urgent: [{channel, text, views, date}], topPosts: [...]}
#   who:      [{title, date, summary}]
#   ideas:    [{title, text, type, confidence, horizon}]
#   + markets, fred, energy, acled, gdelt, space, ... (számszerű blokkok)

import os
import re
import hashlib
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

# A compose a DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD változókat adja át.
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgresql"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "n8n"),
    "username": os.getenv("DB_USER", "sql_admin"),
    "password": os.getenv("DB_PASSWORD", "sql_pass"),
    "connect_timeout": 15,
}

REQUEST_TIMEOUT = 30

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

# Szóhatáros keresés: a sima "in" szűrő az "eth"-et megtalálta a "together",
# "Ethiopia", a "fed"-et a "federal", az "rwa"-t a "Rwanda" szavakban is.
KEYWORD_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(k) for k in CRYPTO_KEYWORDS) + r")\b",
    re.IGNORECASE,
)


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
    """Lekéri a Crucix /api/data aktuális JSON-válaszát."""
    response = requests.get(
        CRUCIX_API_URL,
        timeout=REQUEST_TIMEOUT,
        headers={"Accept": "application/json"},
    )
    response.raise_for_status()
    return response.json()


# --------------------------------------------------
# 5. NORMALIZE API RESPONSE
# --------------------------------------------------

def _record_id(*parts: Any) -> str:
    """Stabil azonosító, hogy az ismétlődő sweep-ek ne duplázzanak (merge kulcs)."""
    joined = "|".join(str(p or "") for p in parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def extract_items(data: Any) -> list[dict]:
    """
    A Crucix szintetizált JSON-jából egységes rekordlistát készít:
    newsFeed (RSS + GDELT), Telegram posztok, WHO hírek, ötletek.
    Az első sweep (30-60 mp) végéig a válasz üres lehet -> üres lista.
    """

    if not isinstance(data, dict) or not data:
        return []

    items: list[dict] = []

    # RSS + GDELT (a Telegram elemeket a tg blokkból vesszük, teljes szöveggel)
    for n in data.get("newsFeed") or []:
        if not isinstance(n, dict) or n.get("type") == "telegram":
            continue
        items.append({
            "kind": n.get("type") or "news",
            "title": n.get("headline"),
            "content": None,
            "source": n.get("source"),
            "region": n.get("region"),
            "url": n.get("url"),
            "urgent": bool(n.get("urgent")),
            "published_at": n.get("timestamp"),
            "raw": n,
        })

    # Telegram OSINT csatornák
    tg = data.get("tg") or {}
    for key, urgent in (("urgent", True), ("topPosts", False)):
        for p in tg.get(key) or []:
            if not isinstance(p, dict):
                continue
            items.append({
                "kind": "telegram",
                "title": (p.get("text") or "")[:120],
                "content": p.get("text"),
                "source": p.get("channel"),
                "region": "OSINT",
                "url": None,
                "urgent": urgent,
                "published_at": p.get("date"),
                "raw": p,
            })

    # WHO járványügyi hírek
    for w in data.get("who") or []:
        if isinstance(w, dict):
            items.append({
                "kind": "who",
                "title": w.get("title"),
                "content": w.get("summary"),
                "source": "WHO",
                "region": None,
                "url": None,
                "urgent": False,
                "published_at": w.get("date"),
                "raw": w,
            })

    # Crucix által generált ötletek (LLM vagy szabályalapú)
    for i in data.get("ideas") or []:
        if isinstance(i, dict):
            items.append({
                "kind": "idea",
                "title": i.get("title"),
                "content": i.get("text"),
                "source": "crucix-ideas",
                "region": None,
                "url": None,
                "urgent": False,
                "published_at": None,
                "raw": i,
            })

    return items


# --------------------------------------------------
# 6. KEYWORD FILTER
# --------------------------------------------------

def is_relevant(item: dict) -> bool:
    """Kripto-, RWA-, pénzügyi és geopolitikai előszűrés (szóhatáros)."""

    text = " ".join(str(item.get(k) or "") for k in ("title", "content", "source", "region"))
    return bool(KEYWORD_RE.search(text))


# --------------------------------------------------
# 7. DLT RESOURCE
# --------------------------------------------------

@dlt.resource(
    name="osint_feeds",
    write_disposition="merge",
    primary_key="record_id",
    columns={"raw_data": {"data_type": "json"}},
)
def fetch_signals(max_items: int = 200, filter_keywords: bool = True):
    """Egy futás: Crucix lekérés -> normalizálás -> opcionális szűrés -> DLT (merge, nincs duplikáció)."""

    data = crucix_get()
    items = extract_items(data)
    logger.info("Crucix API: %d elem érkezett", len(items))

    if not items:
        logger.warning("Crucix: üres válasz (az első sweep még futhat, 30-60 mp)")

    fetched_at = datetime.now(timezone.utc).isoformat()
    seen: set[str] = set()
    count = 0

    for item in items:
        if filter_keywords and not is_relevant(item):
            continue

        record_id = _record_id(item["kind"], item["source"], item["title"], item["published_at"])
        if record_id in seen:
            continue
        seen.add(record_id)

        yield {
            "record_id": record_id,
            "kind": item["kind"],
            "title": item["title"],
            "content": item["content"],
            "source": item["source"],
            "region": item["region"],
            "url": item["url"],
            "urgent": item["urgent"],
            "published_at": item["published_at"],
            "fetched_at": fetched_at,
            "raw_data": item["raw"],
        }

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
        pipeline = dlt.pipeline(
            pipeline_name="crucix_osint_pipeline",
            destination=dlt.destinations.postgres(credentials=DB_CONFIG),
            dataset_name="bronze",
        )

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
        "filter_keywords": request.filter_keywords,
    }


# --------------------------------------------------
# 10. API HEALTH / PREVIEW
# --------------------------------------------------

@router.get("/health")
async def crucix_health():

    try:
        data = crucix_get()
        return {
            "status": "ok",
            "crucix_api_url": CRUCIX_API_URL,
            "response_type": type(data).__name__,
            "items": len(extract_items(data)),
        }

    except Exception as exc:
        logger.exception("Crucix API nem érhető el")
        raise HTTPException(status_code=502, detail=f"Crucix API hiba ({CRUCIX_API_URL}): {exc}")


@router.get("/preview")
async def crucix_preview(limit: int = 20, filter_keywords: bool = True):
    """Mentés nélkül megmutatja, mit írna a pipeline az adatbázisba (n8n / böngészős teszthez)."""

    try:
        items = extract_items(crucix_get())
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Crucix API hiba ({CRUCIX_API_URL}): {exc}")

    total = len(items)
    if filter_keywords:
        items = [i for i in items if is_relevant(i)]

    return {
        "total_items": total,
        "matched_items": len(items),
        "sample": [{k: v for k, v in i.items() if k != "raw"} for i in items[:limit]],
    }
