# 2026.10.09 - authenticated live-positions, token cache, response cache, non-blocking calls
import asyncio
import logging
import os
import time
from datetime import datetime, timezone

import dlt
import httpx
import requests
from dlt.sources.helpers import requests as dlt_requests
from fastapi import APIRouter, BackgroundTasks
from sqlalchemy import create_engine, text

log = logging.getLogger("opensky")

# ----- Config -----
AIRPORTS = ["OMDB", "OMAA", "EDDF"]  # OMDB: Dubai, OMAA: Abu Dhabi, EDDF: Frankfurt
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID", "fastautosol@gmail.com-api-client")
CLIENT_SECRET = os.environ["OPENSKY_CLIENT_SECRET"]  # set in Coolify env, never in code
AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API = "https://opensky-network.org/api"

DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n",
             "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}
DB_URL = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)

LIVE_TTL_SECONDS = 45      # how long a /live-positions response is reused
ICAO_TTL_SECONDS = 600     # how long the tracked ICAO list is reused
MAX_TRACKED = 50

router = APIRouter()


# ----- Auth (cached token) -----
_token = {"value": None, "exp": 0.0}


def get_cached_token() -> str:
    """Blocking; call via asyncio.to_thread from async code."""
    if _token["value"] is None or time.time() > _token["exp"] - 60:
        payload = {"grant_type": "client_credentials",
                   "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
        r = requests.post(AUTH_URL, data=payload, timeout=15)
        r.raise_for_status()
        j = r.json()
        _token["value"] = j["access_token"]
        _token["exp"] = time.time() + j.get("expires_in", 1500)
    return _token["value"]


# ----- dlt: airport flights -----
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
        dataset_name="bronze",
    )
    load_info = pipeline.run(fetch_airport_flights())
    log.info("dlt load OK: %s", load_info)
    return str(load_info)


@router.get("/airport_flights")
def trigger_airport_flights_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_dlt_pipeline)
    return {"message": "Az OpenSky reptéri menetrend szinkronizálása elindult a háttérben.",
            "timestamp": datetime.now().isoformat()}


# ----- Tracked aircraft list (cached in memory) -----
_icao_cache = {"codes": [], "ts": 0.0}


def get_tracked_icao_codes() -> list[str]:
    """Blocking SQL; call via asyncio.to_thread from async code."""
    if _icao_cache["codes"] and time.time() - _icao_cache["ts"] < ICAO_TTL_SECONDS:
        return _icao_cache["codes"]

    query = text("""SELECT DISTINCT icao24 FROM bronze.uae_flights
                    WHERE icao24 IS NOT NULL ORDER BY icao24""")
    try:
        with engine.connect() as conn:
            codes = [row[0].strip() for row in conn.execute(query) if row[0]]
        _icao_cache["codes"] = codes
        _icao_cache["ts"] = time.time()
        return codes
    except Exception as e:
        log.error("Adatbázis hiba az ICAO kódok lekérésekor: %s", e)
        return _icao_cache["codes"]  # stale list is better than nothing


# ----- Live states -----
async def fetch_live_states_from_opensky(icao_list: list[str]) -> list[dict] | None:
    """Returns a list (possibly empty) on success, None on failure."""
    if not icao_list:
        return []

    try:
        token = await asyncio.to_thread(get_cached_token)
        headers = {"Authorization": f"Bearer {token}"}
        params = [("icao24", code) for code in icao_list]

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{API}/states/all", params=params, headers=headers)

        if response.status_code == 401:  # expired token -> refresh next time
            _token["exp"] = 0.0
            log.warning("OpenSky 401, token törölve")
            return None
        if response.status_code != 200:
            log.warning("OpenSky states HTTP %s", response.status_code)
            return None

        states = response.json().get("states") or []
        live_flights = []
        for s in states:
            # only aircraft with valid coordinates
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
        log.error("Hiba az OpenSky API hívásakor: %s", e)
        return None


# ----- /live-positions with response cache -----
_live_cache = {"flights": [], "ts": 0.0}
_live_lock = asyncio.Lock()


@router.get("/live-positions")
async def get_live_positions():
    # Fresh enough -> serve from cache, no OpenSky call
    if _live_cache["ts"] and time.time() - _live_cache["ts"] < LIVE_TTL_SECONDS:
        return {"flights": _live_cache["flights"],
                "age_seconds": round(time.time() - _live_cache["ts"])}

    async with _live_lock:  # only one request refreshes; others wait and reuse
        if _live_cache["ts"] and time.time() - _live_cache["ts"] < LIVE_TTL_SECONDS:
            return {"flights": _live_cache["flights"],
                    "age_seconds": round(time.time() - _live_cache["ts"])}

        icao_codes = await asyncio.to_thread(get_tracked_icao_codes)
        if not icao_codes:
            return {"flights": [], "age_seconds": None}

        live_data = await fetch_live_states_from_opensky(icao_codes[:MAX_TRACKED])
        if live_data is not None:
            _live_cache["flights"] = live_data
            _live_cache["ts"] = time.time()
            return {"flights": live_data, "age_seconds": 0}

        # failure -> last good answer, marked with its age
        age = round(time.time() - _live_cache["ts"]) if _live_cache["ts"] else None
        return {"flights": _live_cache["flights"], "age_seconds": age}
