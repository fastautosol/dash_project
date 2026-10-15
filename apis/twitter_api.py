# 2026.10.15  18.00
import os
import dlt
import requests
import psycopg2
from fastapi import FastAPI, BackgroundTasks, HTTPException
from datetime import datetime

app = FastAPI(title="Optimized TwitterAPI.io Pipeline")

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY", "new1_62292ead9a31403a8625219d85e689ee")
BASE_URL = "https://twitterapi.io"

# A te PostgreSQL elérhetőséged (a dlt secrets-ből vagy környezeti változóból)
DB_CONN_STRING = "dbname='vector_db' user='username' password='password' host='localhost' port='5432'"

TOP_20_ACCOUNTS = [
    "ElonMusk", "VitalikButerin", "Binance", "cz_binance", "Solana", 
    "SolyTrading", "Pentosh1", "GCRClassic", "Cobie", "MacnBTC",
    "ColdBloodedShill", "Ansem", "IncomeSharks", "MilesDeutscher", "crypto_rand",
    "AltcoinSherpa", "SatoshiFlipper", "BenjaminCowen", "CryptoMichNL", "glassnode"
]

# Függvény a legutolsó letöltött tweet ID lekérésére a PostgreSQL-ből
def get_last_tweet_id_from_db(username: str) -> str:
    try:
        conn = psycopg2.connect(DB_CONN_STRING)
        cur = conn.cursor()
        # Lekérjük a legfrissebb tweet ID-t az adott felhasználóhoz
        # Megjegyzés: A tábla és oszlopnevek a dlt alapértelmezett elnevezéseit követik (kisbetűsítés)
        cur.execute(
            "SELECT tweet_id FROM crypto_signals.twitter_trends WHERE username = %s ORDER BY created_at DESC LIMIT 1;", 
            (username,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        print(f"Nem sikerült lekérni az utolsó ID-t a DB-ből ({username}): {str(e)}")
        return None

def fetch_tweets_and_load_optimized():
    all_tweets = []
    headers = {"X-API-Key": TWITTER_API_KEY}
    
    for username in TOP_20_ACCOUNTS:
        try:
            params = {"username": username, "limit": 5}
            
            # KÖLTSÉGOPTIMALIZÁLÁS: Megnézzük, van-e már korábbi tweetünk tőle
            last_id = get_last_tweet_id_from_db(username)
            if last_id:
                # Ha van, az API-nak megmondjuk, hogy csak az ezutániakat kérjük!
                # Ezáltal az üres vagy ismétlődő kérésekért minimális összeget fizetsz (vagy semmit)
                params["since_id"] = last_id
            
            response = requests.get(BASE_URL, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                tweets_list = data.get("tweets", []) 
                
                for tweet in tweets_list:
                    all_tweets.append({
                        "username": username,
                        "tweet_id": str(tweet.get("id")), # Biztonság kedvéért stringként kezeljük az ID-t
                        "text": tweet.get("text"),
                        "created_at": tweet.get("created_at"),
                        "scraped_at": datetime.utcnow().isoformat()
                    })
            else:
                print(f"Hiba {username} lekérésekor: {response.status_code}")
                
        except Exception as e:
            print(f"Kivétel történt {username} feldolgozásakor: {str(e)}")

    # ADATBÁZIS OPTIMALIZÁLÁS (UPSERT)
    if all_tweets:
        pipeline = dlt.pipeline(
            pipeline_name="twitter_ingestion",
            destination="postgres",
            dataset_name="crypto_signals"
        )
        
        # Szigorú duplikáció szűrés: Ha a tweet_id már létezik, nem szúrja be újra!
        load_info = pipeline.run(
            all_tweets, 
            table_name="twitter_trends", 
            write_disposition="merge", # A merge végzi az upsert-et dlt-ben
            primary_key="tweet_id"     # Ez alapján azonosítja a duplikációkat
        )
        print(f"Sikeresen feldolgozva {len(all_tweets)} tweet (Upsert mód).")
    else:
        print("Nincsenek új tweetek az elmúlt 2 órában. Zéró felesleges adatbázis művelet.")

@app.post("/api/v1/trigger-twitter-sync")
async def trigger_twitter_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(fetch_tweets_and_load_optimized)
    return {"status": "optimized_twitter_sync_started"}
