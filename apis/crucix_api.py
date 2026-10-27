"""
Crucix OSINT ingestion -> PostgreSQL bronze.osint_feeds

Only imports the Crucix newsFeed (RSS/GDELT) and Telegram OSINT items.
Markets, metals, energy and air-hotspot data are intentionally excluded.
"""

import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import dlt
import requests
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

CRUCIX_API_URL = os.getenv(
    "CRUCIX_API_URL",
    "http://crucix:3117/api/data",
)
REQUEST_TIMEOUT = 30

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgresql"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "n8n"),
    "username": os.getenv("DB_USER", "sql_admin"),
    "password": os.getenv("DB_PASSWORD", "sql_pass"),
    "connect_timeout": 15,
}

# Crucix dashboard typically exposes up to 50 news items and 50 Telegram/feed items.
MAX_NEWS_ITEMS = 50
MAX_TELEGRAM_ITEMS = 50

CRYPTO_KEYWORDS = [
    "crypto", "bitcoin", "btc", "eth", "ethereum", "sol", "sui",
    "meme", "defi", "rwa", "tokenization", "fed", "federal reserve",
    "inflation", "interest rate", "treasury", "financial market",
    "stock market", "oil price",
]
KEYWORD_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(word) for word in CRYPTO_KEYWORDS) + r")\b",
    re.IGNORECASE,
)


class CrucixRequest(BaseModel):
    # Kept for compatibility with existing API callers. The cap is applied
    # independently to news and Telegram items, so the total can be up to 100.
    max_items: int = Field(default=50, ge=1, le=5000)
    filter_keywords: bool = False


# ---------------------------------------------------------------------
# Crucix API and helpers
# ---------------------------------------------------------------------

def crucix_get() -> Any:
    """Fetch the current Crucix /api/data payload."""
    response = requests.get(
        CRUCIX_API_URL,
        timeout=REQUEST_TIMEOUT,
        headers={"Accept": "application/json"},
    )
    response.raise_for_status()
    return response.json()


def _hash_id(*parts: Any) -> str:
    """Stable ID for DLT merge/deduplication."""
    value = "|".join(str(part if part is not None else "") for part in parts)
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _to_iso(value: Any) -> str | None:
    """Convert ISO or RSS/RFC-822 timestamps to UTC ISO format."""
    if not value:
        return None

    text = str(value).strip()
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = parsedate_to_datetime(text)
        except (TypeError, ValueError, OverflowError):
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).isoformat()


def _is_relevant(item: dict) -> bool:
    text = " ".join(
        str(item.get(key) or "")
        for key in ("title", "content", "source", "region")
    )
    return bool(KEYWORD_RE.search(text))


# ---------------------------------------------------------------------
# Transform Crucix newsFeed + Telegram into bronze.osint_feeds rows
# ---------------------------------------------------------------------

def extract_feed_items(
    data: Any,
    max_items: int = 50,
) -> list[dict]:
    """
    Read up to max_items from newsFeed and independently up to max_items
    from Telegram. This preserves the two groups (up to 50 + 50 by default).
    """
    if not isinstance(data, dict):
        return []

    items: list[dict] = []

    # RSS/GDELT/newsFeed items. Telegram is read from the dedicated tg block.
    news_items = [
        item for item in (data.get("newsFeed") or [])
        if isinstance(item, dict) and item.get("type") != "telegram"
    ][:max_items]

    for item in news_items:
        items.append({
            "kind": item.get("type") or "news",
            "title": item.get("headline"),
            "content": None,
            "source": item.get("source"),
            "region": item.get("region"),
            "url": item.get("url"),
            "urgent": bool(item.get("urgent")),
            "published_at": _to_iso(item.get("timestamp")),
            "raw": item,
        })

    # Telegram OSINT posts, up to max_items across urgent + topPosts.
    tg = data.get("tg") or {}
    telegram_items = []
    for key, urgent in (("urgent", True), ("topPosts", False)):
        for post in tg.get(key) or []:
            if not isinstance(post, dict):
                continue
            telegram_items.append({
                "kind": "telegram",
                "title": (post.get("text") or "")[:120],
                "content": post.get("text"),
                "source": post.get("channel"),
                "region": "OSINT",
                "url": None,
                "urgent": urgent,
                "published_at": _to_iso(post.get("date")),
                "raw": post,
            })

    items.extend(telegram_items[:max_items])
    return items


def build_feed_rows(
    data: Any,
    fetched_at: str,
    max_items: int = 50,
    filter_keywords: bool = False,
) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()

    for item in extract_feed_items(data, max_items=max_items):
        if filter_keywords and not _is_relevant(item):
            continue

        record_id = _hash_id(
            item["kind"],
            item["source"],
            item["title"],
            item["published_at"],
        )
        if record_id in seen:
            continue
        seen.add(record_id)

        rows.append({
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
        })

    return rows


# ---------------------------------------------------------------------
# DLT pipeline: only bronze.osint_feeds
# ---------------------------------------------------------------------

def run_crucix_pipeline(
    max_items: int = 50,
    filter_keywords: bool = False,
    destination: Any = None,
) -> dict:
    """Fetch Crucix news/feed items and merge them into bronze.osint_feeds."""
    try:
        data = crucix_get()
        fetched_at = datetime.now(timezone.utc).isoformat()
        feed_rows = build_feed_rows(
            data=data,
            fetched_at=fetched_at,
            max_items=max_items,
            filter_keywords=filter_keywords,
        )

        counts = {"osint_feeds": len(feed_rows)}
        logger.info("Crucix API: sorok táblánként: %s", counts)

        if not feed_rows:
            logger.warning("Crucix: nincs mentendő news/feed adat")
            return {"counts": counts, "load_info": None}

        pipeline = dlt.pipeline(
            pipeline_name="crucix_osint_pipeline",
            destination=destination or dlt.destinations.postgres(
                credentials=DB_CONFIG
            ),
            dataset_name="bronze",
        )

        resource = dlt.resource(
            feed_rows,
            name="osint_feeds",
            write_disposition="merge",
            primary_key="record_id",
            columns={"raw_data": {"data_type": "json"}},
        )

        load_info = pipeline.run(resource)
        logger.info("Crucix DLT pipeline sikeres: %s", load_info)

        return {
            "counts": counts,
            "load_info": str(load_info),
        }

    except Exception:
        logger.exception("Crucix pipeline hiba")
        raise


def run_pipeline_background(max_items: int, filter_keywords: bool) -> None:
    try:
        run_crucix_pipeline(
            max_items=max_items,
            filter_keywords=filter_keywords,
        )
    except Exception:
        # The full exception has already been logged by run_crucix_pipeline.
        pass


# ---------------------------------------------------------------------
# FastAPI endpoints
# ---------------------------------------------------------------------

@router.post("/run")
async def trigger_crucix_pipeline(
    request: CrucixRequest,
    background_tasks: BackgroundTasks,
):
    background_tasks.add_task(
        run_pipeline_background,
        request.max_items,
        request.filter_keywords,
    )
    return {
        "status": "accepted",
        "message": "Crucix news/feed pipeline elindítva a háttérben",
        "max_items_per_group": request.max_items,
        "filter_keywords": request.filter_keywords,
        "destination": "bronze.osint_feeds",
    }


@router.get("/health")
async def crucix_health():
    try:
        data = crucix_get()
        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = build_feed_rows(
            data,
            fetched_at,
            max_items=MAX_NEWS_ITEMS,
            filter_keywords=False,
        )
        news_count = sum(1 for row in rows if row["kind"] != "telegram")
        telegram_count = sum(1 for row in rows if row["kind"] == "telegram")

        return {
            "status": "ok",
            "crucix_api_url": CRUCIX_API_URL,
            "response_type": type(data).__name__,
            "rows": {
                "osint_feeds": len(rows),
                "news": news_count,
                "telegram_feed_items": telegram_count,
            },
        }
    except Exception as exc:
        logger.exception("Crucix API nem érhető el")
        raise HTTPException(
            status_code=502,
            detail=f"Crucix API hiba ({CRUCIX_API_URL}): {exc}",
        ) from exc


@router.get("/preview")
async def crucix_preview(
    limit: int = 10,
    filter_keywords: bool = False,
):
    try:
        data = crucix_get()
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Crucix API hiba ({CRUCIX_API_URL}): {exc}",
        ) from exc

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = build_feed_rows(
        data,
        fetched_at,
        max_items=MAX_NEWS_ITEMS,
        filter_keywords=filter_keywords,
    )

    news_count = sum(1 for row in rows if row["kind"] != "telegram")
    telegram_count = sum(1 for row in rows if row["kind"] == "telegram")

    return {
        "rows": {
            "osint_feeds": len(rows),
            "news": news_count,
            "telegram_feed_items": telegram_count,
        },
        "osint_feeds": [
            {key: value for key, value in row.items() if key != "raw_data"}
            for row in rows[:max(0, limit)]
        ],
    }
