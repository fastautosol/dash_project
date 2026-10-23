# 2026.10.23  cryptonews_api.py  -  Crucix (OSINT) + Twitter egyetlen bronze.cryptonews táblába
#
# Bronze tábla (egyetlen):
#   bronze.cryptonews     közös hírtábla: Crucix (RSS, GDELT, Telegram) + Twitter posztok
#                         provider = 'crucix' | 'twitter'; kind = rss | gdelt | telegram | tweet
#
# Endpointok (a router prefixét a main.py-ban kell megadni, pl. /cryptonews):
#   POST /run      mindkét forrás (vagy a "sources" listában megadott), a Twitter önvédelemmel
#   GET  /health   Crucix elérhetőség + Twitter utolsó lekérés / következő engedélyezett időpont
#   GET  /preview  Crucix hírek előnézete (nem ír adatbázisba, nem hív Twittert)

import os
import re
import time
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Literal

import dlt
import requests
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

logger = logging.getLogger("cryptonews_api")
router = APIRouter()

# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgresql"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "n8n"),
    "username": os.getenv("DB_USER", "sql_admin"),
    "password": os.getenv("DB_PASSWORD", "sql_pass"),
    "connect_timeout": 15}

DB_URL = os.getenv(
    "DB_URL",
    f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
engine = create_engine(DB_URL, pool_pre_ping=True)

REQUEST_TIMEOUT = 30

# A pipeline neve a korábbi Crucix pipeline neve maradt (a dlt állapotát megtartja).
PIPELINE_NAME = "crucix_osint_pipeline"
DATASET_NAME = "bronze"

# ---- Crucix ----
CRUCIX_API_URL = os.getenv("CRUCIX_API_URL", "http://crucix:3117/api/data")

# Csak azok a hírek/Telegram posztok kerülnek be, amelyek ezek közül legalább egyet tartalmaznak
CRYPTO_KEYWORDS = ["crypto", "bitcoin", "btc", "eth", "ethereum", "sol", "sui", "meme", "defi", "rwa", "tokenization",
    "fed", "federal reserve", "inflation", "interest rate", "treasury", "financial market", "stock market", "oil price"]

# Szóhatáros keresés ("eth" ne találja meg a "together" szót)
KEYWORD_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(k) for k in CRYPTO_KEYWORDS) + r")\b",
    re.IGNORECASE)

# ---- Twitter (twitterapi.io) ----
# A kulcs CSAK környezeti változóból jön (Coolify secret), a kódban nincs alapértelmezett érték.
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY", "")
TWITTER_LAST_TWEETS_URL = "https://api.twitterapi.io/twitter/user/last_tweets"

# Szűkített, crypto-központú lista. Felülírható: TWITTER_ACCOUNTS="A,B,C" környezeti változóval.
DEFAULT_TWITTER_ACCOUNTS = [
    "VitalikButerin", "cz_binance", "Binance", "Solana",
    "Pentosh1", "GCRClassic", "Cobie", "Ansem",
    "MilesDeutscher", "BenjaminCowen", "glassnode", "AltcoinSherpa"]
# Kivéve (ide visszavehetők): "KobeissiLetter" (fizetős, nem jön), "ElonMusk" (nem crypto-specifikus),
# "SolyTrading", "MacnBTC", "ColdBloodedShill", "IncomeSharks", "crypto_rand", "SatoshiFlipper"
TWITTER_ACCOUNTS = [a.strip() for a in os.getenv("TWITTER_ACCOUNTS", "").split(",") if a.strip()] or DEFAULT_TWITTER_ACCOUNTS

TWEETS_PER_ACCOUNT = int(os.getenv("TWEETS_PER_ACCOUNT", "5"))
TWITTER_REQUEST_DELAY_SEC = float(os.getenv("TWITTER_REQUEST_DELAY_SEC", "5.5"))     # ingyenes csomag: ~1 kérés / 5 mp
TWITTER_MIN_INTERVAL_HOURS = float(os.getenv("TWITTER_MIN_INTERVAL_HOURS", "3"))     # ennél gyakrabban nem hív Twittert
SKIP_REPLIES_AND_RETWEETS = os.getenv("TWITTER_SKIP_REPLIES_RT", "true").lower() == "true"
# A fiókok már crypto-központúak, ezért a tweetekre alapból NINCS kulcsszószűrés
TWITTER_KEYWORD_FILTER = os.getenv("TWITTER_KEYWORD_FILTER", "false").lower() == "true"

# A közös tábla oszloptípusai: így az oszlopok akkor is létrejönnek, ha az első betöltésben csak NULL van bennük
CRYPTONEWS_COLUMNS = {
    "urgent": {"data_type": "bool"},
    "published_at": {"data_type": "timestamp"},
    "fetched_at": {"data_type": "timestamp"},
    "like_count": {"data_type": "bigint"},
    "retweet_count": {"data_type": "bigint"},
    "reply_count": {"data_type": "bigint"},
    "view_count": {"data_type": "bigint"},
    "raw_data": {"data_type": "json"}}


# --------------------------------------------------
# 2. API REQUEST MODEL
# --------------------------------------------------

class RunRequest(BaseModel):
    # Melyik forrást futtassa. Alapból mindkettőt; a Twitter a minimum-intervallal önmagát védi,
    # így az n8n bátran hívhatja gyakrabban is (pl. 30 percenként) a /run-t.
    sources: list[Literal["crucix", "twitter"]] = Field(default_factory=lambda: ["crucix", "twitter"])
    # max_items és filter_keywords a Crucix hírekre/Telegramra vonatkozik
    max_items: int = Field(default=500, ge=1, le=5000)
    filter_keywords: bool = True
    # True: a minimum-intervallumot figyelmen kívül hagyja (kézi teszthez)
    force_twitter: bool = False


# --------------------------------------------------
# 3. COMMON HELPERS
# --------------------------------------------------

def _hash_id(*parts: Any) -> str:
    """Stabil azonosító (merge kulcs): ugyanaz a bemenet -> ugyanaz a record_id."""
    joined = "|".join(str(p if p is not None else "") for p in parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def _to_iso(value: Any) -> str | None:
    """RSS (RFC 822) és ISO időbélyegek egységesítése UTC ISO formátumra."""
    if not value:
        return None

    text_value = str(value).strip()

    try:
        dt = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = parsedate_to_datetime(text_value)
        except (TypeError, ValueError):
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc).isoformat()


def _twitter_time_to_iso(value: Any) -> str | None:
    """Twitter formátum ('Tue Dec 10 07:00:30 +0000 2024') -> UTC ISO; más formátumnál _to_iso."""
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc).isoformat()
    except ValueError:
        return _to_iso(value)


def is_relevant(item: dict) -> bool:
    """Kulcsszó-előszűrés (szóhatáros)."""
    text_value = " ".join(str(item.get(k) or "") for k in ("title", "content", "source", "region"))
    return bool(KEYWORD_RE.search(text_value))


def get_pipeline():
    return dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dlt.destinations.postgres(credentials=DB_CONFIG),
        dataset_name=DATASET_NAME)


def cryptonews_resource(rows: list[dict]):
    return dlt.resource(rows, name="cryptonews", write_disposition="merge",
                        primary_key="record_id", columns=CRYPTONEWS_COLUMNS)


# --------------------------------------------------
# 4. CRUCIX: API CLIENT
# --------------------------------------------------

def crucix_get() -> Any:
    """Lekéri a Crucix /api/data aktuális JSON-válaszát."""
    response = requests.get(CRUCIX_API_URL, timeout=REQUEST_TIMEOUT, headers={"Accept": "application/json"})
    response.raise_for_status()
    return response.json()


# --------------------------------------------------
# 5. CRUCIX: HÍREK + TELEGRAM  ->  bronze.cryptonews
# --------------------------------------------------

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


def build_crucix_news_rows(data: Any, fetched_at: str, max_items: int, filter_keywords: bool) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()

    for item in extract_feed_items(data):
        if filter_keywords and not is_relevant(item):
            continue

        record_id = _hash_id("crucix", item["kind"], item["source"], item["title"], item["published_at"])
        if record_id in seen:
            continue
        seen.add(record_id)

        rows.append({
            "record_id": record_id,
            "provider": "crucix",
            "kind": item["kind"],
            "external_id": None,
            "title": item["title"],
            "content": item["content"],
            "source": item["source"],
            "region": item["region"],
            "url": item["url"],
            "urgent": item["urgent"],
            "published_at": item["published_at"],
            "like_count": None,
            "retweet_count": None,
            "reply_count": None,
            "view_count": None,
            "fetched_at": fetched_at,
            "raw_data": item["raw"],
        })

        if len(rows) >= max_items:
            break

    return rows


# --------------------------------------------------
# 6. CRUCIX: PIPELINE
# --------------------------------------------------

def run_crucix_pipeline(max_items: int = 500, filter_keywords: bool = True):
    """Egy lekérés -> bronze.cryptonews (kulcsszóra szűrt hírek és Telegram posztok)."""

    try:
        data = crucix_get()
        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = build_crucix_news_rows(data, fetched_at, max_items, filter_keywords)
        logger.info("Crucix: %d sor a bronze.cryptonews táblába", len(rows))

        if not rows:
            logger.warning("Crucix: nincs mentendő adat (az első sweep 30-60 mp-ig tarthat, vagy nincs kulcsszó-találat)")
            return {"rows": 0, "load_info": None}

        load_info = get_pipeline().run(cryptonews_resource(rows))
        logger.info("Crucix DLT pipeline sikeres: %s", load_info)
        return {"rows": len(rows), "load_info": str(load_info)}

    except Exception:
        logger.exception("Crucix pipeline hiba")
        raise


# --------------------------------------------------
# 7. TWITTER: THROTTLE + FETCH + LOAD  ->  bronze.cryptonews
# --------------------------------------------------

def twitter_last_scrape() -> datetime | None:
    """A legutóbbi Twitter-lekérés ideje (a közös tábla twitter sorainak fetched_at maximuma)."""
    try:
        with engine.connect() as conn:
            value = conn.execute(
                text("SELECT MAX(fetched_at) FROM bronze.cryptonews WHERE provider = 'twitter'")).scalar()
    except Exception:
        return None   # a tábla még nem létezik

    if value is None:
        return None
    if isinstance(value, str):
        iso = _to_iso(value)
        value = datetime.fromisoformat(iso) if iso else None
    if value is not None and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value


def twitter_next_allowed() -> datetime | None:
    last = twitter_last_scrape()
    return last + timedelta(hours=TWITTER_MIN_INTERVAL_HOURS) if last else None


def extract_tweets(payload: dict) -> list:
    """A válasz formátuma: {"status": "success", "data": {"tweets": [...]}}.
    Régebbi/más formátumra is felkészülünk (top-level 'tweets')."""
    data = payload.get("data")
    if isinstance(data, dict):
        return data.get("tweets") or []
    return payload.get("tweets") or []


def _is_reply_or_retweet(tweet: dict) -> bool:
    if tweet.get("isReply"):
        return True
    if tweet.get("retweeted_tweet"):
        return True
    return str(tweet.get("text") or "").startswith("RT @")


def fetch_twitter_rows() -> list[dict]:
    """Végigmegy a kiválasztott fiókokon, és bronze.cryptonews-formátumú sorokat ad vissza."""
    rows: list[dict] = []
    headers = {"X-API-Key": TWITTER_API_KEY, "Accept": "application/json"}

    for i, username in enumerate(TWITTER_ACCOUNTS):
        if i > 0:
            time.sleep(TWITTER_REQUEST_DELAY_SEC)
        try:
            response = requests.get(TWITTER_LAST_TWEETS_URL, headers=headers,
                                    params={"userName": username}, timeout=REQUEST_TIMEOUT)

            if response.status_code in (401, 402):
                # Hibás kulcs vagy elfogyott kredit: felesleges a többi fiókot is végigpróbálni
                logger.error("Twitter API %s -> megszakítva (kulcs/kredit?): %s",
                             response.status_code, response.text[:200])
                break

            if response.status_code != 200:
                logger.warning("Twitter API hiba %s: %s %s", username, response.status_code, response.text[:200])
                continue

            if response.text.strip().lower().startswith("<!doctype html"):
                logger.warning("HTML jött JSON helyett %s esetén. Rossz végpont?", username)
                continue

            try:
                payload = response.json()
            except Exception as json_err:
                logger.warning("JSON dekódolási hiba %s: %s", username, json_err)
                continue

            if payload.get("status") not in (None, "success"):
                logger.warning("Twitter API üzenet %s: %s", username, payload.get("msg") or payload)
                continue

            tweets = extract_tweets(payload)
            if SKIP_REPLIES_AND_RETWEETS:
                tweets = [t for t in tweets if not _is_reply_or_retweet(t)]

            fetched_at = datetime.now(timezone.utc).isoformat()
            kept = 0
            for tweet in tweets:
                if kept >= TWEETS_PER_ACCOUNT:
                    break
                tweet_id = str(tweet.get("id"))
                text_value = tweet.get("text")

                if TWITTER_KEYWORD_FILTER and not is_relevant(
                        {"title": text_value, "content": text_value, "source": username}):
                    continue

                rows.append({
                    "record_id": _hash_id("twitter", tweet_id),
                    "provider": "twitter",
                    "kind": "tweet",
                    "external_id": tweet_id,
                    "title": (text_value or "")[:120],
                    "content": text_value,
                    "source": username,
                    "region": None,
                    "url": tweet.get("url"),
                    "urgent": False,
                    "published_at": _twitter_time_to_iso(tweet.get("createdAt") or tweet.get("created_at")),
                    "like_count": tweet.get("likeCount"),
                    "retweet_count": tweet.get("retweetCount"),
                    "reply_count": tweet.get("replyCount"),
                    "view_count": tweet.get("viewCount"),
                    "fetched_at": fetched_at,
                    "raw_data": tweet,
                })
                kept += 1

        except Exception as e:
            logger.warning("Hálózati hiba %s feldolgozásakor: %s", username, e)

    return rows


def run_twitter_pipeline(force: bool = False) -> dict:
    if not TWITTER_API_KEY:
        logger.warning("Twitter: hiányzik a TWITTER_API_KEY környezeti változó, kihagyva")
        return {"status": "no_api_key"}

    if not force:
        next_allowed = twitter_next_allowed()
        if next_allowed and datetime.now(timezone.utc) < next_allowed:
            logger.info("Twitter: kihagyva, a következő lekérés ekkor engedélyezett: %s", next_allowed.isoformat())
            return {"status": "skipped_recent", "next_allowed": next_allowed.isoformat()}

    rows = fetch_twitter_rows()
    if not rows:
        logger.warning("Twitter: nem sikerült adatot kinyerni az API-ból")
        return {"status": "no_data", "rows": 0}

    load_info = get_pipeline().run(cryptonews_resource(rows))
    logger.info("Twitter: %d tweet a bronze.cryptonews táblába, DLT: %s", len(rows), load_info)
    return {"status": "loaded", "rows": len(rows)}


# --------------------------------------------------
# 8. FASTAPI BACKGROUND TASK
# --------------------------------------------------

def run_sources_background(request: RunRequest):
    # Minden forrás külön try-ban fut, hogy az egyik hibája ne állítsa meg a másikat.
    # Egymás után futnak (ugyanaz a dlt pipeline), nem párhuzamosan.
    if "crucix" in request.sources:
        try:
            run_crucix_pipeline(max_items=request.max_items, filter_keywords=request.filter_keywords)
        except Exception:
            pass   # a részleteket a logger.exception már rögzítette

    if "twitter" in request.sources:
        try:
            run_twitter_pipeline(force=request.force_twitter)
        except Exception:
            logger.exception("Twitter pipeline hiba")


@router.post("/run")
async def trigger_cryptonews_pipeline(request: RunRequest, background_tasks: BackgroundTasks):

    background_tasks.add_task(run_sources_background, request)
    return {
        "status": "accepted",
        "message": "Cryptonews pipeline elindítva a háttérben",
        "sources": request.sources,
        "filter_keywords": request.filter_keywords,
        "twitter_min_interval_hours": TWITTER_MIN_INTERVAL_HOURS,
        "force_twitter": request.force_twitter}


# --------------------------------------------------
# 9. HEALTH / PREVIEW
# --------------------------------------------------

@router.get("/health")
async def cryptonews_health():

    last = twitter_last_scrape()
    result: dict[str, Any] = {"twitter": {
        "api_key_set": bool(TWITTER_API_KEY),
        "accounts": len(TWITTER_ACCOUNTS),
        "min_interval_hours": TWITTER_MIN_INTERVAL_HOURS,
        "last_scrape": last.isoformat() if last else None,
        "next_allowed": twitter_next_allowed().isoformat() if last else None}}

    try:
        data = crucix_get()
        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = build_crucix_news_rows(data, fetched_at, 5000, True)
        result["crucix"] = {"status": "ok", "url": CRUCIX_API_URL, "cryptonews_rows": len(rows)}
    except Exception as exc:
        logger.exception("Crucix API nem érhető el")
        raise HTTPException(status_code=502, detail=f"Crucix API hiba ({CRUCIX_API_URL}): {exc}")

    return result


@router.get("/preview")
async def cryptonews_preview(limit: int = 10, filter_keywords: bool = True):

    try:
        data = crucix_get()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Crucix API hiba ({CRUCIX_API_URL}): {exc}")

    fetched_at = datetime.now(timezone.utc).isoformat()
    news = build_crucix_news_rows(data, fetched_at, 5000, filter_keywords)

    return {
        "rows": {"cryptonews": len(news)},
        "cryptonews": [{k: v for k, v in r.items() if k != "raw_data"} for r in news[:limit]]}
