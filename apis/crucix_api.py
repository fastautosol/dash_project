# 2026-10-12 18.00
#   bronze.osint_feeds        hírek (RSS, GDELT) + Telegram OSINT posztok
#   bronze.osint_markets      kripto + arany, ezüst, olaj (WTI, Brent), földgáz (Yahoo Finance)
#   bronze.osint_air_hotspots repülési forgalom régiónként (OpenSky, aggregált)

# A /api/data szerkezete (Crucix dashboard/inject.mjs synthesize()):
#   meta:     {timestamp, ...}                      a sweep ideje
#   newsFeed: [{headline, source, type, timestamp, region, urgent, url}]
#   tg:       {urgent: [{channel, text, views, date, urgentFlags}], topPosts: [...]}
#   markets:  {crypto: [...], commodities: [...], timestamp}   elem: {symbol, name, price, change, changePct}
#   metals:   {gold, goldChange, goldChangePct, silver, ...}
#   energy:   {wti, brent, natgas, ...}
#   air:      [{region, total, noCallsign, highAlt, top: [[ország, db], ...]}]
#   airMeta:  {fallback, timestamp, source, error}

import os
import re
import hashlib
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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
    "password": os.getenv("DB_PASSWORD", "sql_pass"),
    "connect_timeout": 15}

REQUEST_TIMEOUT = 30

# Yahoo Finance szimbólumok -> rövid név (arany, ezüst, olaj, gáz)
COMMODITY_SYMBOLS = {"GC=F": "gold", "SI=F": "silver", "CL=F": "wti", "BZ=F": "brent", "NG=F": "natgas"}

# Opcionális szűrő a hírekre/Telegramra (alapból KI van kapcsolva, lásd CrucixRequest)
CRYPTO_KEYWORDS = ["crypto", "bitcoin", "btc", "eth", "ethereum", "sol", "sui", "meme", "defi", "rwa", "tokenization",
    "fed", "federal reserve", "inflation", "interest rate", "treasury", "financial market", "stock market", "oil price"]

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
    # max_items és filter_keywords csak a hírekre/Telegramra vonatkozik;
    # a piaci és repülési adat mindig teljes egészében bekerül.
    max_items: int = Field(default=500, ge=1, le=5000)
    filter_keywords: bool = False


# --------------------------------------------------
# 4. CRUCIX API CLIENT
# --------------------------------------------------

def crucix_get() -> Any:
    """Lekéri a Crucix /api/data aktuális JSON-válaszát."""
    response = requests.get(CRUCIX_API_URL, timeout=REQUEST_TIMEOUT, headers={"Accept": "application/json"})
    response.raise_for_status()
    return response.json()


# --------------------------------------------------
# 5. HELPERS
# --------------------------------------------------

def _hash_id(*parts: Any) -> str:
    """Stabil azonosító (merge kulcs): ugyanaz a bemenet -> ugyanaz a record_id."""
    joined = "|".join(str(p if p is not None else "") for p in parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def _to_iso(value: Any) -> str | None:
    """RSS (RFC 822) és ISO időbélyegek egységesítése UTC ISO formátumra."""
    if not value:
        return None

    text = str(value).strip()

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).isoformat()


def _num(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _sweep_at(data: dict, fallback: str) -> str:
    """A Crucix sweep ideje; ez azonosítja a pillanatfelvételt (dedup kulcs része)."""
    meta = data.get("meta") or {}
    markets = data.get("markets") or {}
    return _to_iso(meta.get("timestamp")) or _to_iso(markets.get("timestamp")) or fallback


# --------------------------------------------------
# 6. FEEDS: HÍREK + TELEGRAM
# --------------------------------------------------

def is_relevant(item: dict) -> bool:
    """Kripto-, pénzügyi és geopolitikai előszűrés (szóhatáros)."""
    text = " ".join(str(item.get(k) or "") for k in ("title", "content", "source", "region"))
    return bool(KEYWORD_RE.search(text))


def extract_feed_items(data: Any) -> list[dict]:
    """newsFeed (RSS + GDELT) és Telegram posztok egységes rekordlistává."""

    if not isinstance(data, dict) or not data:
        return []

    items: list[dict] = []

    # Telegram elemeket a tg blokkból vesszük (teljes szöveggel), a newsFeed-ből kihagyjuk
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
            "published_at": _to_iso(n.get("timestamp")),
            "raw": n,
        })

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
                "published_at": _to_iso(p.get("date")),
                "raw": p,
            })

    return items


def build_feed_rows(data: Any, fetched_at: str, max_items: int, filter_keywords: bool) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()

    for item in extract_feed_items(data):
        if filter_keywords and not is_relevant(item):
            continue

        record_id = _hash_id(item["kind"], item["source"], item["title"], item["published_at"])
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

        if len(rows) >= max_items:
            break

    return rows


# --------------------------------------------------
# 7. MARKETS: KRIPTO + ARANY, EZÜST, OLAJ, GÁZ
# --------------------------------------------------

def build_market_rows(data: Any, fetched_at: str) -> list[dict]:
    if not isinstance(data, dict) or not data:
        return []

    sweep_at = _sweep_at(data, fetched_at)
    markets = data.get("markets") or {}
    quotes: list[dict] = []

    # Kripto: minden, amit a Crucix ad
    for q in markets.get("crypto") or []:
        if isinstance(q, dict) and q.get("symbol"):
            quotes.append({
                "asset_class": "crypto",
                "asset": str(q["symbol"]).lower(),
                "symbol": q["symbol"],
                "name": q.get("name"),
                "price": _num(q.get("price")),
                "change": _num(q.get("change")),
                "change_pct": _num(q.get("changePct")),
            })

    # Nyersanyagok: csak arany, ezüst, WTI, Brent, földgáz
    commodities: dict[str, dict] = {}
    for q in markets.get("commodities") or []:
        if isinstance(q, dict) and q.get("symbol") in COMMODITY_SYMBOLS:
            commodities[q["symbol"]] = {
                "name": q.get("name"),
                "price": _num(q.get("price")),
                "change": _num(q.get("change")),
                "change_pct": _num(q.get("changePct")),
            }

    # Tartalék: ha a commodities tömbből hiányzik valami, a metals/energy blokkból pótoljuk
    metals = data.get("metals") or {}
    energy = data.get("energy") or {}
    fallback = {
        "GC=F": (metals.get("gold"), metals.get("goldChange"), metals.get("goldChangePct")),
        "SI=F": (metals.get("silver"), metals.get("silverChange"), metals.get("silverChangePct")),
        "CL=F": (energy.get("wti"), None, None),
        "BZ=F": (energy.get("brent"), None, None),
        "NG=F": (energy.get("natgas"), None, None),
    }
    for symbol, (price, change, change_pct) in fallback.items():
        if symbol not in commodities and _num(price) is not None:
            commodities[symbol] = {
                "name": COMMODITY_SYMBOLS[symbol],
                "price": _num(price),
                "change": _num(change),
                "change_pct": _num(change_pct),
            }

    for symbol, q in commodities.items():
        quotes.append({
            "asset_class": "commodity",
            "asset": COMMODITY_SYMBOLS[symbol],
            "symbol": symbol,
            **q,
        })

    rows = []
    for q in quotes:
        rows.append({
            "record_id": _hash_id("market", q["symbol"], sweep_at),
            **q,
            "sweep_at": sweep_at,
            "fetched_at": fetched_at,
        })

    return rows


# --------------------------------------------------
# 8. AIR: REPÜLÉSI FORGALOM RÉGIÓNKÉNT
# --------------------------------------------------

def build_air_rows(data: Any, fetched_at: str) -> list[dict]:
    if not isinstance(data, dict) or not data:
        return []

    sweep_at = _sweep_at(data, fetched_at)
    air_meta = data.get("airMeta") or {}
    rows = []

    for h in data.get("air") or []:
        if not isinstance(h, dict) or not h.get("region"):
            continue

        rows.append({
            "record_id": _hash_id("air", h["region"], sweep_at),
            "region": h["region"],
            "total_aircraft": h.get("total") or 0,
            "no_callsign": h.get("noCallsign") or 0,
            "high_altitude": h.get("highAlt") or 0,
            "top_countries": h.get("top") or [],
            # OpenSky 429 esetén a Crucix a legutóbbi nem üres adatot adja vissza:
            # ilyenkor is_fallback = true, és source_at a régi adat ideje.
            "is_fallback": bool(air_meta.get("fallback")),
            "source_at": _to_iso(air_meta.get("timestamp")),
            "source": air_meta.get("source"),
            "sweep_at": sweep_at,
            "fetched_at": fetched_at,
        })

    return rows


# --------------------------------------------------
# 9. DLT PIPELINE
# --------------------------------------------------

def build_resources(data: Any, max_items: int, filter_keywords: bool):

    fetched_at = datetime.now(timezone.utc).isoformat()
    feed_rows = build_feed_rows(data, fetched_at, max_items, filter_keywords)
    market_rows = build_market_rows(data, fetched_at)
    air_rows = build_air_rows(data, fetched_at)
    counts = {"osint_feeds": len(feed_rows), "osint_markets": len(market_rows), "osint_air_hotspots": len(air_rows)}
    resources = []

    if feed_rows:
        resources.append(dlt.resource(
            feed_rows, name="osint_feeds", write_disposition="merge",
            primary_key="record_id", columns={"raw_data": {"data_type": "json"}}))
    if market_rows:
        resources.append(dlt.resource(
            market_rows, name="osint_markets", write_disposition="merge", primary_key="record_id"))
    if air_rows:
        resources.append(dlt.resource(
            air_rows, name="osint_air_hotspots", write_disposition="merge",
            primary_key="record_id", columns={"top_countries": {"data_type": "json"}}))

    return resources, counts


def run_crucix_pipeline(max_items: int = 500, filter_keywords: bool = False, destination: Any = None):
    """Egy lekérés -> három tábla a bronze sémában. A destination csak teszthez írható felül."""

    try:
        data = crucix_get()
        resources, counts = build_resources(data, max_items, filter_keywords)
        logger.info("Crucix API: sorok táblánként: %s", counts)

        if not resources:
            logger.warning("Crucix: nincs mentendő adat (az első sweep 30-60 mp-ig tarthat)")
            return {"counts": counts, "load_info": None}

        pipeline = dlt.pipeline(
            pipeline_name="crucix_osint_pipeline",
            destination=destination or dlt.destinations.postgres(credentials=DB_CONFIG),
            dataset_name="bronze")

        load_info = pipeline.run(resources)
        logger.info("Crucix DLT pipeline sikeres: %s", load_info)

        return {"counts": counts, "load_info": str(load_info)}

    except Exception:
        logger.exception("Crucix pipeline hiba")
        raise


# --------------------------------------------------
# 10. FASTAPI BACKGROUND TASK
# --------------------------------------------------

def run_pipeline_background(max_items: int, filter_keywords: bool):

    try:
        run_crucix_pipeline(max_items=max_items, filter_keywords=filter_keywords)
    except Exception:
        # A hiba részleteit a logger.exception már rögzíti, háttérfeladat nem küld HTTP-választ.
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
# 11. API HEALTH / PREVIEW
# --------------------------------------------------

@router.get("/health")
async def crucix_health():

    try:
        data = crucix_get()
        _, counts = build_resources(data, max_items=5000, filter_keywords=False)
        return {
            "status": "ok",
            "crucix_api_url": CRUCIX_API_URL,
            "response_type": type(data).__name__,
            "rows": counts,
        }

    except Exception as exc:
        logger.exception("Crucix API nem érhető el")
        raise HTTPException(status_code=502, detail=f"Crucix API hiba ({CRUCIX_API_URL}): {exc}")


@router.get("/preview")
async def crucix_preview(limit: int = 10, filter_keywords: bool = False):

    try:
        data = crucix_get()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Crucix API hiba ({CRUCIX_API_URL}): {exc}")

    fetched_at = datetime.now(timezone.utc).isoformat()
    feeds = build_feed_rows(data, fetched_at, 5000, filter_keywords)
    markets = build_market_rows(data, fetched_at)
    air = build_air_rows(data, fetched_at)

    return {
        "rows": {"osint_feeds": len(feeds), "osint_markets": len(markets), "osint_air_hotspots": len(air)},
        "osint_feeds": [{k: v for k, v in r.items() if k != "raw_data"} for r in feeds[:limit]],
        "osint_markets": markets,
        "osint_air_hotspots": air}
