# 2026.10.05  18.00
import requests
import dlt
import time
import os
import re
import html
import logging
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel

logger = logging.getLogger(__name__)

BASE_URL = "https://api.themoviedb.org/3"
router = APIRouter()
DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}
TMDB_API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI2NmNhZGRhZmFlZTExMGU4ZDZiNzEzNjkxZTA4N2E5NiIsIm5iZiI6MTc5MTIwNjk4OC40NTIsInN1YiI6IjZhYzNhNjRjODU4MTM4MmU0OTQ1NWI2NSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.sJPOOZ-NQNlYCDPMqqe3ikQUxjK6USitksUuCB6qyFI"

# --- beállítások ---
TMDB_MAX_CHANGE_DAYS = 14        # a /movie/changes legfeljebb 14 napos időszakot enged
REQUEST_DELAY_SEC = 0.1          # TMDB rate limit biztonsági tartalék
MAX_REVIEW_CHARS = 1500          # egy kritika max hossza a bronze rétegben

EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001FAFF"
    "\U00002600-\U000027BF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002B00-\U00002BFF"
    "\U0000FE0F"
    "]+",
    flags=re.UNICODE)


def clean_text(text: str) -> str:
    if not text:
        return text
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = EMOJI_PATTERN.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


class MovieRequest(BaseModel):
    max_movies: int = 20
    days_back: int = 7           # csak az első futásra számít, utána a dlt incremental állapota dönt


def tmdb_get(endpoint: str, params: dict | None = None) -> dict | None:
    """GET a TMDB-re. 404 -> None (pl. törölt film), 429 -> várakozás és újrapróbálás."""
    headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_API_TOKEN}"}
    for attempt in range(3):
        response = requests.get(f"{BASE_URL}/{endpoint}", headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        if response.status_code == 404:
            return None
        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 2))
            logger.warning("TMDB rate limit, várakozás %ss", wait)
            time.sleep(wait)
            continue
        raise Exception(f"TMDB API error {response.status_code} on /{endpoint}: {response.text}")
    raise Exception(f"TMDB API: túl sok újrapróbálás on /{endpoint}")


def get_changed_movie_ids(start_date: str, end_date: str, max_movies: int) -> list[int]:
    """Lapozva (az API oldalanként 100 elemet ad)."""
    ids, page = [], 1
    while len(ids) < max_movies:
        data = tmdb_get("movie/changes", {"start_date": start_date, "end_date": end_date, "page": page})
        if not data:
            break
        ids += [item["id"] for item in data.get("results", [])]
        if page >= data.get("total_pages", 1):
            break
        page += 1
    return ids[:max_movies]


def get_movie_details(movie_id: int) -> dict | None:
    """Részletes adatok + user reviewk egy hívásban (append_to_response)."""
    return tmdb_get(f"movie/{movie_id}", {"append_to_response": "reviews", "language": "en-US"})


@dlt.resource(name="tmdb_movies_raw", write_disposition="merge", primary_key="movie_id", columns={"user_reviews": {"data_type": "json"}})

def fetch_movies(max_movies: int = 20, updated_at=dlt.sources.incremental("updated_at")):
    """Filmenként EGY sor: metaadatok + user_reviews (jsonb)."""
    now = datetime.now(timezone.utc)
    ingested_at = now.isoformat()

    # kezdő dátum: az utolsó sikeres futás, de max. 14 nap vissza (TMDB korlát)
    start_dt = updated_at.last_value[:10]
    earliest = (now - timedelta(days=TMDB_MAX_CHANGE_DAYS)).strftime("%Y-%m-%d")
    start_dt = max(start_dt, earliest)
    end_dt = now.strftime("%Y-%m-%d")

    movie_ids = get_changed_movie_ids(start_dt, end_dt, max_movies)
    logger.info("%d módosult film a TMDB-n %s és %s között", len(movie_ids), start_dt, end_dt)

    for movie_id in movie_ids:
        m = get_movie_details(movie_id)
        time.sleep(REQUEST_DELAY_SEC)
        if not m:
            logger.info("Film %s nem érhető el (törölt?), kihagyva", movie_id)
            continue

        reviews = m.get("reviews", {}).get("results", [])
        credits = m.get("credits", {})
        yield {
            "movie_id": m.get("id"),
            "title": m.get("title"),
            "budget": m.get("budget", 0),
            "revenue": m.get("revenue", 0),
            "release_date": m.get("release_date"),
            "vote_average": m.get("vote_average", 0.0),
            "overview": clean_text(m.get("overview", "")),   
            "user_reviews": [clean_text(r["content"])[:MAX_REVIEW_CHARS] for r in reviews if r.get("content")],
            "original_title": m.get("original_title"),
            "original_language": m.get("original_language"),
            "tagline": m.get("tagline"),
            "runtime": m.get("runtime"),
            "status": m.get("status"),
            "popularity": m.get("popularity"),
            "vote_count": m.get("vote_count"),
            "imdb_id": m.get("imdb_id"),
            "poster_path": m.get("poster_path"),
            "collection": (m.get("belongs_to_collection") or {}).get("name"),
            "genres": [g["name"] for g in m.get("genres", [])],
            "keywords": [k["name"] for k in m.get("keywords", {}).get("keywords", [])],
            "director": next((c["name"] for c in credits.get("crew", []) if c.get("job") == "Director"), None),
            "top_cast": [c["name"] for c in credits.get("cast", [])[:10]],
            "processed": False,
            "updated_at": ingested_at,
        }


def run_dlt_pipeline(max_movies: int, days_back: int):
    """dlt futtatása és Postgresbe mentés"""
    try:
        pipeline = dlt.pipeline(
            pipeline_name="tmdb_data_pipeline",
            destination=dlt.destinations.postgres(credentials=DB_CONFIG),
            dataset_name="bronze")

        initial = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
        resource = fetch_movies(
            max_movies=max_movies,
            updated_at=dlt.sources.incremental("updated_at", initial_value=initial))

        info = pipeline.run(resource)
        logger.info("dlt sikeresen végrehajtva: %s", info)

    except Exception as e:
        logger.exception("pipeline hiba: %s", e)


@router.post("/")
async def trigger_movie_fetch(request: MovieRequest, background_tasks: BackgroundTasks):

    background_tasks.add_task(run_dlt_pipeline, request.max_movies, request.days_back)

    return {"status": "success", "message": f"TMDB: max {request.max_movies} movie data gathering in background"}


# --- kézi futtatás teszthez: python tmdb_api.py ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_dlt_pipeline(max_movies=20, days_back=7)
