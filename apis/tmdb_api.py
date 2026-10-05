# 2026.10.05  18.00
import dlt
import requests
from datetime import datetime, timedelta
import time

# TMDB API Hosszú Bearer Token (ami eyJ-vel kezdődik)
TMDB_API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI2NmNhZGRhZmFlZTExMGU4ZDZiNzEzNjkxZTA4N2E5NiIsIm5iZiI6MTc5MTIwNjk4OC40NTIsInN1YiI6IjZhYzNhNjRjODU4MTM4MmU0OTQ1NWI2NSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.sJPOOZ-NQNlYCDPMqqe3ikQUxjK6USitksUuCB6qyFI"

POSTGRES_CONN_STR = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
# ==============================================================================

@dlt.source(name="tmdb")
def tmdb_source(api_token: str):

    # Inkrementális kapuőr: Megjegyzi a Postgresben az utolsó sikeres futás dátumát.
    # Első futáskor az elmúlt 7 nap változásait nézi, utána már csak a legfrissebbet!
    incremental_gate = dlt.sources.incremental("change_date", initial_value=(datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d"))

    # 1. Erőforrás: A megváltozott filmek listája és azok részletes adatai
    @dlt.resource(name="movies", write_disposition="merge", primary_key="movie_id")
    def get_movies(change_date=incremental_gate):
        headers = {"accept": "application/json", "Authorization": f"Bearer {api_token}"}
        
        start_dt = change_date.last_value
        end_dt = datetime.utcnow().strftime("%Y-%m-%d")
        
        # Lekérjük a TMDB-től, melyik filmek változtak az időszakban
        url = f"https://themoviedb.org{start_dt}&end_date={end_dt}"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Hiba a TMDB API elérésekor: {response.status_code}")
            return

        changes = response.json().get("results", [])
        print(f"Összesen {len(changes)} módosult film található a TMDB-n {start_dt} óta.")
        
        # A gyors teszteléshez limitáljuk a kört a top 20-ra, hogy ne fusson sokáig
        for item in changes[:20]:
            movie_id = item["id"]
            
            # Részletes filmadatok lekérése + hozzácsapjuk a felhasználói kritikákat (append_to_response)
            movie_url = f"https://themoviedb.org{movie_id}?append_to_response=reviews&language=en-US"
            movie_resp = requests.get(movie_url, headers=headers)
            
            if movie_resp.status_code == 200:
                m = movie_resp.json()
                reviews_list = m.get("reviews", {}).get("results", [])
                
                # Átadjuk az adatot a dlt-nek, ami automatikusan legenerálja/frissíti a táblát
                yield {
                    "movie_id": m.get("id"),
                    "title": m.get("title"),
                    "budget": m.get("budget", 0),
                    "revenue": m.get("revenue", 0),
                    "release_date": m.get("release_date"),
                    "vote_average": m.get("vote_average", 0.0),
                    "overview": m.get("overview", ""), # <-- Szöveg RAG-hoz (1. szint)
                    # A kritikák szövegeit egy tiszta listává alakítjuk a RAG-nak (2. szint)
                    "user_reviews": [r.get("content", "") for r in reviews_list if r.get("content")],
                    "updated_at": datetime.utcnow().isoformat()
                }
            
            # Betartjuk a TMDB rate limitet (biztonsági játék, másodpercenként max 40 kérés)
            time.sleep(0.1)

    return get_movies

# --- PIPELINE FUTTATÁS ---
if __name__ == "__main__":
    # Létrehozzuk a dlt pipeline-t, és közvetlenül átadjuk neki a Postgres kapcsolati stringet credentials-ként
    pipeline = dlt.pipeline(
        pipeline_name="tmdb_data_pipeline",
        #destination="postgres",
        destination=dlt.destinations.postgres(credentials=DB_CONFIG), 
        #credentials=POSTGRES_CONN_STR, # Így nem kell a secrets.toml fájl a teszthez
        dataset_name="bronze_tmdb" # Ebbe a sémába fog pakolni a Postgresen belül
    )
    
    print("dlt pipeline indul: TMDB -> PostgreSQL 18 (TOML nélkül)...")
    load_info = pipeline.run(tmdb_source(api_token=TMDB_API_TOKEN))
    
    print("Adatbetöltés sikeres!")
    print(load_info)
