# 2026.10.09  17.00
import dlt
import requests
import time
import httpx
from datetime import datetime, timezone
from dlt.sources.helpers import requests as dlt_requests
from fastapi import APIRouter, BackgroundTasks
from sqlalchemy import create_engine, text

# ----- Config -----
AIRPORTS = ["OMDB", "OMAA", "EDDF"]  # OMDB: Dubai, OMAA: Abu Dhabi, EDDF: Frankfurt
CLIENT_ID = "fastautosol@gmail.com-api-client"
CLIENT_SECRET = "1Fk2Xga7e85duhpQYbjNAseMt2Qn5gcF"
AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}
DB_URL = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)

API = "https://opensky-network.org/api"

router = APIRouter()

def get_auth_token():
    payload = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
    response = requests.post(AUTH_URL, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]


@dlt.resource(name="uae_flights", write_disposition="merge", primary_key=["icao24", "first_seen"])
def fetch_airport_flights():
    
    token = get_auth_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    time_end = int(time.time())
    time_start = int(time_end - 1.5 * 86400) 
    
    for airport in AIRPORTS:
        for direction in ("departure", "arrival"):
            url_flights = f"{API}/flights/{direction}?airport={airport}&begin={time_start}&end={time_end}"
                         
            try:
                response = dlt_requests.get(url_flights, headers=headers, timeout=15)
                if response.status_code == 404: 
                    continue
                response.raise_for_status()
                flights = response.json()
                
                for f in flights:
                    callsign = (f.get("callsign") or "").strip()
                    icao24 = f.get("icao24")
                    est_departure_airport = f.get("estDepartureAirport")
                    est_arrival_airport = f.get("estArrivalAirport")
                    
                    if callsign.startswith("UAE") and icao24 and est_departure_airport and est_arrival_airport:
                        yield {
                            "icao24": icao24,
                            "callsign": callsign,
                            "est_departure_airport": est_departure_airport,
                            "est_arrival_airport": est_arrival_airport,
                            "first_seen": f.get("firstSeen"),
                            "last_seen": f.get("lastSeen"),
                            "est_dep_horiz_dist": f.get("estDepartureAirportHorizDistance"),
                            "est_dep_vert_dist":  f.get("estDepartureAirportVertDistance"),
                            "est_arr_horiz_dist": f.get("estArrivalAirportHorizDistance"),
                            "est_arr_vert_dist":  f.get("estArrivalAirportVertDistance"),
                            "updated_at": datetime.now(timezone.utc).isoformat() #datetime.now()
                        }
            except Exception as e:
                print(f"Hiba a(z) {airport} reptér lekérdezésekor: {str(e)}")

def run_dlt_pipeline():

    pipeline = dlt.pipeline(
        pipeline_name="opensky_airport_tracker",
        destination=dlt.destinations.postgres(credentials=DB_CONFIG),
        dataset_name="bronze")
    
    load_info = pipeline.run(fetch_airport_flights())
    return str(load_info)

# ----- 2. ÚJ FASTAPI API VÉGPONT -----
@router.get("/airport_flights")
def trigger_airport_flights_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dlt_pipeline)   
    return {"message": "Az OpenSky reptéri menetrend szinkronizálása elindult a háttérben.", "timestamp": datetime.now().isoformat()}

def get_tracked_icao_codes() -> list[str]:
    query = text("SELECT DISTINCT icao24 FROM bronze.uae_flights WHERE icao24 IS NOT NULL")
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            return [row[0].strip() for row in result if row[0]]
            
    except Exception as e:
        print(f"Adatbázis hiba az ICAO kódok lekérésekor: {e}")
        return []

async def fetch_live_states_from_opensky(icao_list: list[str]) -> list[dict]:
    if not icao_list:
        return []

    params = [("icao24", icao_3) for icao_3 in icao_list]
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(f"{API}/states/all", params=params)
            if response.status_code != 200:
                return []
                
            data = response.json()
            states = data.get("states", [])
            
            live_flights = []
            if states:
                for s in states:
                    # Szűrés és validálás: csak a valós koordinátákkal rendelkező, levegőben lévő gépek
                    if s[5] is not None and s[6] is not None:
                        live_flights.append({
                            "icao24": s[0].strip() if s[0] else "N/A",
                            "callsign": s[1].strip() if s[1] else "UNKNOWN",
                            "origin_country": s[2],
                            "longitude": float(s[5]),
                            "latitude": float(s[6]),
                            "altitude_m": float(s[7]) if s[7] else 0.0,
                            "on_ground": bool(s[8]),
                            "velocity_mps": float(s[9]) if s[9] else 0.0,
                            "heading_deg": float(s[10]) if s[10] else 0.0,
                            "vertical_rate_mps": float(s[11]) if s[11] else 0.0,
                        })
            return live_flights
        except Exception as e:
            print(f"Hiba az OpenSky API hívásakor: {e}")
            return []

# ----- 1. FASTAPI ENDPOINT (A Dash térkép aszinkron AJAX hívásaihoz) -----
@router.get("/live-positions")
async def get_live_positions():
    icao_codes = get_tracked_icao_codes()
    if not icao_codes:
        return {"flights": []}
    
    live_data = await fetch_live_states_from_opensky(icao_codes[:50])
    return {"flights": live_data}
