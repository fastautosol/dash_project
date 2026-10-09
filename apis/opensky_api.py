# 2026.10.09  18.00
import dlt
import requests
import time
import httpx
import asyncio
from datetime import datetime, timezone
from dlt.sources.helpers import requests as dlt_requests
from fastapi import APIRouter, BackgroundTasks
from sqlalchemy import create_engine, text

# ----- Config -----
AIRPORTS = ["OMDB", "OMAA", "EDDF", "VHHH", "YSSY", "KLAX", "EHAM"]
CLIENT_ID = "fastautosol@gmail.com-api-client"
CLIENT_SECRET = "1Fk2Xga7e85duhpQYbjNAseMt2Qn5gcF"
AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}
DB_URL = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)
API = "https://opensky-network.org/api"
log = logging.getLogger("opensky")
LIVE_WINDOW_MINUTES = 10   # positions older than this are not returned

router = APIRouter()

# ----- Auth (cached token) -----
_token = {"value": None, "exp": 0.0}

def get_cached_token() -> str:
    if _token["value"] is None or time.time() > _token["exp"] - 60:
        payload = {"grant_type": "client_credentials",
                   "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
        r = requests.post(AUTH_URL, data=payload, timeout=15)
        r.raise_for_status()
        j = r.json()
        _token["value"] = j["access_token"]
        _token["exp"] = time.time() + j.get("expires_in", 1500)
    return _token["value"]

# ----- dlt: airport flights (run 1-2x a day, e.g. from n8n) -----
@dlt.resource(name="uae_flights", write_disposition="merge", primary_key=["icao24", "first_seen"])
def fetch_airport_flights():
    token = get_cached_token()
    headers = {"Authorization": f"Bearer {token}"}

    time_end = int(time.time())
    time_start = int(time_end - 1.5 * 86400)  # API max window is 2 days

    for airport in AIRPORTS:
        for direction in ("departure", "arrival"):
            url = f"{API}/flights/{direction}"
            params = {"airport": airport, "begin": time_start, "end": time_end}
            try:
                response = dlt_requests.get(url, headers=headers, params=params, timeout=15)
                if response.status_code == 404:  # no flights found
                    continue
                response.raise_for_status()

                for f in response.json():
                    callsign = (f.get("callsign") or "").strip()
                    icao24 = f.get("icao24")
                    dep = f.get("estDepartureAirport")
                    arr = f.get("estArrivalAirport")

                    if callsign.startswith("UAE") and icao24 and dep and arr:
                        yield {
                            "icao24": icao24,
                            "callsign": callsign,
                            "est_departure_airport": dep,
                            "est_arrival_airport": arr,
                            "first_seen": f.get("firstSeen"),
                            "last_seen": f.get("lastSeen"),
                            "est_dep_horiz_dist": f.get("estDepartureAirportHorizDistance"),
                            "est_dep_vert_dist": f.get("estDepartureAirportVertDistance"),
                            "est_arr_horiz_dist": f.get("estArrivalAirportHorizDistance"),
                            "est_arr_vert_dist": f.get("estArrivalAirportVertDistance"),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        }
            except Exception as e:
                log.error("Hiba a(z) %s %s lekérdezésekor: %s", airport, direction, e)


def run_dlt_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="opensky_airport_tracker",
        destination=dlt.destinations.postgres(credentials=DB_CONFIG),
        dataset_name="bronze")
    load_info = pipeline.run(fetch_airport_flights())
    log.info("dlt load OK: %s", load_info)
    return str(load_info)


@router.get("/airport_flights")
def trigger_airport_flights_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dlt_pipeline)
    return {"message": "Az OpenSky reptéri menetrend szinkronizálása elindult a háttérben.",
            "timestamp": datetime.now().isoformat()}

# ----- /live-positions: latest row per aircraft from the DB (no OpenSky call here) -----
def load_latest_positions() -> list[dict]:
    query = text("""
        SELECT DISTINCT ON (icao24)
               icao24, callsign, origin_country, latitude, longitude,
               altitude_m, on_ground, velocity_mps, heading_deg,
               vertical_rate_mps, snapshot_time
        FROM bronze.a380_positions
        WHERE snapshot_time > now() - make_interval(mins => :m)
        ORDER BY icao24, snapshot_time DESC
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"m": LIVE_WINDOW_MINUTES}).mappings().all()
    except Exception as e:  # e.g. table does not exist before the tracker's first run
        log.error("Adatbázis hiba a pozíciók lekérésekor: %s", e)
        return []

    now = datetime.now(timezone.utc)
    flights = []
    for r in rows:
        ts = r["snapshot_time"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        flights.append({
            "icao24": r["icao24"],
            "callsign": r["callsign"] or "UNKNOWN",
            "origin_country": r["origin_country"],
            "longitude": r["longitude"],
            "latitude": r["latitude"],
            "altitude_m": r["altitude_m"] or 0.0,
            "on_ground": bool(r["on_ground"]),
            "velocity_mps": r["velocity_mps"] or 0.0,
            "heading_deg": r["heading_deg"] or 0.0,
            "vertical_rate_mps": r["vertical_rate_mps"] or 0.0,
            "snapshot_time": ts.isoformat(),
            "age_seconds": round((now - ts).total_seconds()),
        })
    return flights


@router.get("/live-positions")
async def get_live_positions():
    return {"flights": await asyncio.to_thread(load_latest_positions)}
