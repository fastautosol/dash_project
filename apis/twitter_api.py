# 2026.10.15  18.00
import os
import time
import dlt
import requests
from fastapi import APIRouter, BackgroundTasks
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

# A kulcsot NE tárold a kódban: állítsd be env változóként (Coolify -> Environment Variables)
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY", "new1_62292ead9a31403a8625219d85e689ee")

BASE_URL = "https://api.twitterapi.io"
LAST_TWEETS_URL = f"{BASE_URL}/twitter/user/last_tweets"

DB_URL = os.getenv("DB_URL", "postgresql://sql_admin:sql_pass@postgresql:5432/n8n")
engine = create_engine(DB_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)

router = APIRouter()

TOP_20_ACCOUNTS = [
    "ElonMusk", "VitalikButerin", "Binance", "cz_binance", "Solana",
    "SolyTrading", "Pentosh1", "GCRClassic", "Cobie", "MacnBTC",
    "ColdBloodedShill", "Ansem", "IncomeSharks", "MilesDeutscher", "crypto_rand",
    "AltcoinSherpa", "SatoshiFlipper", "BenjaminCowen", "glassnode"]

TWEETS_PER_ACCOUNT = 5
REQUEST_DELAY_SEC = 5.5  # a twitterapi.io ingyenes csomag ~1 kérés / 5 mp


def get_last_tweet_id_from_db(username: str):
    try:
        query = """SELECT tweet_id FROM crypto_signals.twitter_trends WHERE username = :username
            ORDER BY created_at DESC LIMIT 1;
        """
        with engine.begin() as conn:
            row = conn.execute(text(query), {"username": username}).fetchone()
            return row[0] if row else None
    except Exception as e:
        if "UndefinedTable" in str(e) or "does not exist" in str(e):
            return None
        print(f"Adatbázis hiba ({username}): {str(e)}")
        return None


def extract_tweets(payload: dict) -> list:
    """A válasz formátuma: {"status": "success", "data": {"tweets": [...]}}.
    Régebbi/más formátumra is felkészülünk (top-level 'tweets')."""
    data = payload.get("data")
    if isinstance(data, dict):
        return data.get("tweets") or []
    return payload.get("tweets") or []


def fetch_tweets_and_load_optimized():
    if not TWITTER_API_KEY:
        print("❌ Hiányzik a TWITTER_API_KEY környezeti változó.")
        return

    all_tweets = []
    headers = {"X-API-Key": TWITTER_API_KEY, "Accept": "application/json"}

    for i, username in enumerate(TOP_20_ACCOUNTS):
        if i > 0:
            time.sleep(REQUEST_DELAY_SEC)
        try:
            params = {"userName": username}
            response = requests.get(LAST_TWEETS_URL, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                print(f"API Hiba {username} esetén: Status: {response.status_code} {response.text[:200]}")
                continue

            if response.text.strip().lower().startswith("<!doctype html"):
                print(f"⚠️ HTML jött JSON helyett {username} esetén. Rossz végpont?")
                continue

            try:
                payload = response.json()
            except Exception as json_err:
                print(f"JSON dekódolási hiba {username} esetén: {str(json_err)}")
                continue

            if payload.get("status") not in (None, "success"):
                print(f"API üzenet {username} esetén: {payload.get('msg') or payload}")
                continue

            tweets_list = extract_tweets(payload)[:TWEETS_PER_ACCOUNT]
            scraped_at = datetime.now(timezone.utc).isoformat()

            for tweet in tweets_list:
                all_tweets.append({
                    "username": username,
                    "tweet_id": str(tweet.get("id")),
                    "text": tweet.get("text"),
                    "created_at": tweet.get("createdAt") or tweet.get("created_at"),
                    "url": tweet.get("url"),
                    "like_count": tweet.get("likeCount"),
                    "retweet_count": tweet.get("retweetCount"),
                    "reply_count": tweet.get("replyCount"),
                    "view_count": tweet.get("viewCount"),
                    "scraped_at": scraped_at,
                })

        except Exception as e:
            print(f"Hálózati hiba történt {username} feldolgozásakor: {str(e)}")

    if all_tweets:
        pipeline = dlt.pipeline(
            pipeline_name="twitter_ingestion",
            destination="postgres",
            dataset_name="crypto_signals"
        )
        pipeline.run(
            all_tweets,
            table_name="twitter_trends",
            write_disposition="merge",
            primary_key="tweet_id"
        )
        print(f"🚀 SIKER: Összesen {len(all_tweets)} tweet feldolgozva. Az újak bekerültek a DB-be!")
    else:
        print("❌ Nem sikerült adatot kinyerni az API-ból.")


@router.post("/tweet_sync")
async def trigger_twitter_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(fetch_tweets_and_load_optimized)
    return {"status": "optimized_twitter_sync_started"}
