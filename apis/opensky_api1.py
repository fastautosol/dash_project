# 2026.10.08  18.00
import dlt
import requests
import time
from datetime import datetime, timezone
from dlt.sources.helpers import requests as dlt_requests
from fastapi import APIRouter, BackgroundTasks

# ----- Config -----
AIRPORTS = ["OMDB", "OMAA", "EDDF"]  # OMDB: Dubai, OMAA: Abu Dhabi, EDDF: Frankfurt
CLIENT_ID = "fastautosol@gmail.com-api-client"
CLIENT_SECRET = "1Fk2Xga7e85duhpQYbjNAseMt2Qn5gcF"
AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
DB_CONFIG = {"host": "postgresql","port": 5432,"database": "n8n","username": "sql_admin","password": "sql_pass","connect_timeout": 15}

# 1. Létrehozzuk a FastAPI routert
router = APIRouter()

def get_auth_token():
    payload = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
    response = requests.post(AUTH_URL, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

@dlt.resource(name="uae_flights", write_disposition="replace") 
def fetch_airport_flights():
    token = get_auth_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    time_end = int(time.time())
    time_start = time_end - 2* 86400 
    
    for airport in AIRPORTS:
        url_departure = f"https://opensky-network.org{airport}&begin={time_start}&end={time_end}"
        
        try:
            response = dlt_requests.get(url_departure, headers=headers, timeout=15)
            if response.status_code == 404: 
                continue
            response.raise_for_status()
            flights = response.json()
            
            for f in flights:
                callsign = (f.get("callsign") or "").strip()
                icao24 = f.get("icao24")
                
                if callsign.startswith("UAE") and icao24:
                    yield {
                        "icao24": icao24,
                        "callsign": callsign,
                        "est_departure_airport": f.get("estDepartureAirport"),
                        "est_arrival_airport": f.get("estArrivalAirport"),
                        "first_seen": f.get("firstSeen"),
                        "last_seen": f.get("lastSeen"),
                        "updated_at": datetime.now(timezone.utc).isoformat() #datetime.now()
                    }
        except Exception as e:
            print(f"Hiba a(z) {airport} reptér lekérdezésekor: {str(e)}")

def run_dlt_pipeline():
    """Belső függvény a dlt pipeline szinkron futtatásához."""
    pipeline = dlt.pipeline(
        pipeline_name="opensky_airport_tracker",
        destination="postgres",
        dataset_name="bronze"
    )
    load_info = pipeline.run(fetch_airport_flights())
    return str(load_info)

# ----- 2. ÚJ FASTAPI API VÉGPONT -----
@router.get("/airport_flights")
def trigger_airport_flights_sync(background_tasks: BackgroundTasks):
    # A dlt futtatását áttesszük egy háttérfolyamatba (Background Task), 
    background_tasks.add_task(run_dlt_pipeline)
    
    return {
        "status": "pending",
        "message": "Az OpenSky reptéri menetrend szinkronizálása elindult a háttérben.",
        "timestamp": datetime.now().isoformat()
    }
