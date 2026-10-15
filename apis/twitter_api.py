# 2026.10.15  18.00
import os
import dlt
import requests
import psycopg2
from fastapi import APIRouter, BackgroundTasks, HTTPException
from datetime import datetime
from sqlalchemy import create_engine, text

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY", "new1_62292ead9a31403a8625219d85e689ee")
BASE_URL = "https://twitterapi.io"
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
#engine = create_engine(DB_URL)
engine = create_engine(DB_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)

router = APIRouter()

TOP_20_ACCOUNTS = [
    "ElonMusk", "VitalikButerin", "Binance", "cz_binance", "Solana", 
    "SolyTrading", "Pentosh1", "GCRClassic", "Cobie", "MacnBTC",
    "ColdBloodedShill", "Ansem", "IncomeSharks", "MilesDeutscher", "crypto_rand",
    "AltcoinSherpa", "SatoshiFlipper", "BenjaminCowen", "glassnode"]


def get_last_tweet_id_from_db(username: str) -> str:
    try:
        query = """SELECT tweet_id FROM crypto_signals.twitter_trends WHERE username = :username 
            ORDER BY created_at DESC LIMIT 1;
        """
        with engine.begin() as conn:
            result = conn.execute(text(query), {"username": username})
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        if "UndefinedTable" in str(e) or "does not exist" in str(e):
            return None
        print(f"Adatbázis hiba ({username}): {str(e)}")
        return None

def fetch_tweets_and_load_optimized():
    all_tweets = []
    headers = {
        "X-API-Key": TWITTER_API_KEY,
        "Accept": "application/json"
    }
    
    for username in TOP_20_ACCOUNTS:
        try:
            # JAVÍTÁS 1: A paraméter pontos neve 'userName' (nagy N-nel)
            # JAVÍTÁS 2: A 'since_id'-t kivettük, mert az API nem támogatja ezen a végponton
            params = {
                "userName": username, 
                "limit": 5
            }
            
            response = requests.get(BASE_URL, headers=headers, params=params)
            
            if response.status_code == 200 and response.text:
                # Biztonsági ellenőrzés: ha mégis HTML-t kapnánk, ne omoljon össze
                if response.text.strip().startswith("<!DOCTYPE html>"):
                    print(f"⚠️ Az API HTML-t adott vissza JSON helyett {username} esetén. Rossz végpont vagy konfiguráció.")
                    continue
                    
                try:
                    data = response.json()
                    tweets_list = data.get("tweets", []) 
                    
                    for tweet in tweets_list:
                        all_tweets.append({
                            "username": username,
                            "tweet_id": str(tweet.get("id")),
                            "text": tweet.get("text"),
                            "created_at": tweet.get("created_at"),
                            "scraped_at": datetime.utcnow().isoformat()
                        })
                except Exception as json_err:
                    print(f"JSON dekódolási hiba {username} esetén: {str(json_err)}")
            else:
                print(f"API Hiba {username} esetén: Status: {response.status_code}")
                
        except Exception as e:
            print(f"Hálózati hiba történt {username} feldolgozásakor: {str(e)}")

    # --- JAVÍTÁS 3: A DLT MENTÉS MOST MÁR BIZTOSAN LEFUT ---
    if all_tweets:
        pipeline = dlt.pipeline(
            pipeline_name="twitter_ingestion",
            destination="postgres",
            dataset_name="crypto_signals"
        )
        
        # A dlt a színfalak mögött összehasonlítja a tweet_id-kat a DB-vel.
        # Csak azokat szúrja be, amik MÉG NINCSENEK a PostgreSQL-ben!
        load_info = pipeline.run(
            all_tweets, 
            table_name="twitter_trends", 
            write_disposition="merge", # Ez végzi az okos összefésülést
            primary_key="tweet_id"     # Ez alapján szűri a duplikációt
        )
        print(f"🚀 SIKER: Összesen {len(all_tweets)} tweet feldolgozva. Az újak bekerültek a DB-be!")
    else:
        print("❌ Nem sikerült adatot kinyerni az API-ból.")




@router.post("/tweet_sync")
async def trigger_twitter_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(fetch_tweets_and_load_optimized)
    return {"status": "optimized_twitter_sync_started"}
