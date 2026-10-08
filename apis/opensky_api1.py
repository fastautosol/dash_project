# pipelines/opensky_airport_pipeline.py
import dlt
import requests
import time
from datetime import datetime, timedelta
from dlt.sources.helpers import requests as dlt_requests

# ----- REPTÉR KONFIGURÁCIÓ -----
AIRPORTS = ["OMDB", "OMAA", "EDDF"]  # OMDB: Dubai, OMAA: Abu Dhabi, EDDF: Frankfurt
CLIENT_ID = "A_TE_OPENSKY_CLIENT_ID"
CLIENT_SECRET = "A_TE_OPENSKY_CLIENT_SECRET"
AUTH_URL = "https://opensky-network.org"

def get_auth_token():
    """Lekéri az OAuth2 Bearer tokent (30 percig érvényes)"""
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    response = requests.post(AUTH_URL, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

@dlt.resource(name="scheduled_a380_flights", write_disposition="replace") # Mindig frissítjük az aktuális aktív listát
def fetch_airport_flights():
    token = get_auth_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    # Időablak beállítása (például az elmúlt 24 óra indításai)
    time_end = int(time.time())
    time_start = time_end - 86400  # 1 nap másodpercekben (maximum 7 nap engedélyezett)
    
    for airport in AIRPORTS:
        url_departure = f"https://opensky-network.org/api/flights/departure?airport={airport}&begin={time_start}&end={time_end}"
        
        try:
            response = dlt_requests.get(url_departure, headers=headers, timeout=15)
            if response.status_code == 404: # Ha nincs járat az adott idősávban, ugorjunk
                continue
            response.raise_for_status()
            flights = response.json()
            
            for f in flights:
                callsign = (f.get("callsign") or "").strip()
                icao24 = f.get("icao24")
                
                # SZŰRÉS: Emirates járatok (UAE)
                # (Mivel Dubai-ból szinte csak A380-as és B777-es Emirates gépek indulnak, az icao24 alapján a 
                # következő lépésben a states-nél fogjuk pontosan látni, hogy melyik az aktív A380-as óriásgép)
                if callsign.startswith("UAE") and icao24:
                    yield {
                        "icao24": icao24,
                        "callsign": callsign,
                        "est_departure_airport": f.get("estDepartureAirport"),
                        "est_arrival_airport": f.get("estArrivalAirport"),
                        "first_seen": f.get("firstSeen"),
                        "last_seen": f.get("lastSeen"),
                        "updated_at": datetime.now()
                    }
        except Exception as e:
            print(f"Hiba a(z) {airport} reptér lekérdezésekor: {str(e)}")

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="opensky_airport_tracker",
        destination="postgres",
        dataset_name="sky_monitor"
    )
    
    load_info = pipeline.run(fetch_airport_flights())
    print(load_info)
