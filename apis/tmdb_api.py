import dlt
import requests
from datetime import datetime, timedelta

# --- CONFIG ---
TMDB_API_KEY = "66caddafaee110e8d6b713691e087a96"
headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_API_KEY}"}

# --- DLT SOURCE DEFINÍCIÓ ---
@dlt.source
def tmdb_source(start_date=dlt.secrets.value):
    
    incremental_gate = dlt.sources.incremental("change_date", initial_value=(datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d"))

    @dlt.resource(name="movies", write_disposition="merge", primary_key="id")
    def get_changed_movies(change_date=incremental_gate):
        current_date = datetime.utcnow().strftime("%Y-%m-%d")
        url = f"https://themoviedb.org{change_date.last_value}&end_date={current_date}"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            changes = response.json().get("results", [])
            
            # Végigmegyünk a módosult filmek listáján (max 50-et kérünk be tesztként, hogy ne fusson sokáig)
            for item in changes[:50]:
                movie_id = item["id"]
                movie_url = f"https://api.themoviedb.org/3/movie/{movie_id}?append_to_response=reviews"
                movie_resp = requests.get(movie_url, headers=headers)
                
                if movie_resp.status_code == 200:
                    movie_data = movie_resp.json()
                    
                    # Előkészítjük a dlt-nek átadandó struktúrát (Strukturált + Szöveges adatok együtt)
                    yield {
                        "id": movie_data.get("id"),
                        "title": movie_data.get("title"),
                        "budget": movie_data.get("budget"),
                        "revenue": movie_data.get("revenue"),
                        "release_date": movie_data.get("release_date"),
                        "overview": movie_data.get("overview"), # <- Ez megy a RAG-ba!
                        "vote_average": movie_data.get("vote_average"),
                        "reviews": [r.get("content") for r in movie_data.get("reviews", {}).get("results", [])], 
                        "last_fetched_at": datetime.utcnow().isoformat()
                    }

    return get_changed_movies

# --- PIPELINE FUTTATÁS ---
if __name__ == "__main__":
    
    pipeline = dlt.pipeline(pipeline_name="tmdb_to_postgres", destination="postgres", dataset_name="tmdb_data")        
    load_info = pipeline.run(tmdb_source())
    print(load_info)
