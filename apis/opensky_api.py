# 2026.10.10  18.00
import dlt
import requests
import time
import httpx
import asyncio
import os
import logging
from datetime import datetime, timezone
from dlt.sources.helpers import requests as dlt_requests
from fastapi import APIRouter, BackgroundTasks
from sqlalchemy import bindparam, create_engine, text


# ----- Config -----
AIRPORTS = ["OMDB", "OMAA", "EDDF", "VHHH", "YSSY", "KLAX", "EHAM", "LHBP", "KJFK"]
AIRLINES = ["UAE", "QTR", "ETD", "DLH", "QFA", "WZZ", "KLM", "BAW", "ARF", "AAL", "DAL"]
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
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


@dlt.resource(name="opensky_flights", write_disposition="merge", primary_key=["icao24", "first_seen"])
def fetch_airport_flights():
    
    token = get_auth_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    time_end = int(time.time())
    time_start = int(time_end - 86400) 
    
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
                    dep = f.get("estDepartureAirport")
                    arr = f.get("estArrivalAirport")
                    
                    if callsign[:3] in AIRLINES and icao24 and dep and arr:
                        yield {
                            "icao24": icao24,
                            "callsign": callsign,
                            "est_departure_airport": dep,
                            "est_arrival_airport": arr,
                            "first_seen": f.get("firstSeen"),
                            "last_seen": f.get("lastSeen"),
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

_token = {"value": None, "exp": 0.0}
def get_cached_token() -> str:
    if time.time() > _token["exp"] - 60:
        payload = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
        r = requests.post(AUTH_URL, data=payload, timeout=15)
        r.raise_for_status()
        j = r.json()
        _token["value"] = j["access_token"]
        _token["exp"] = time.time() + j.get("expires_in", 1500)
    return _token["value"]


def get_tracked_icao_codes() -> list[str]:
    query = text("SELECT DISTINCT icao24 FROM bronze.uae_flights WHERE icao24 IS NOT NULL ORDER BY icao24")
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            return [row[0].strip() for row in result if row[0]]
            
    except Exception as e:
        print(f"Adatbázis hiba az ICAO kódok lekérésekor: {e}")
        return []


async def fetch_live_states_from_opensky(icao_list):
    if not icao_list:
        return []

    token = await asyncio.to_thread(get_cached_token)
    headers = {"Authorization": f"Bearer {token}"}
    params = [("icao24", c) for c in icao_list]
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(f"{API}/states/all", params=params, headers=headers)
            if response.status_code == 401: # lejárt token
                _token["exp"] = 0
                return []
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


async def fetch_live_states_batched(icao_codes: list[str], chunk_size: int = 25, max_concurrency: int = 5) -> list[dict]:

    if not icao_codes:
        return []

    semaphore = asyncio.Semaphore(max_concurrency)
    chunks = [icao_codes[i:i + chunk_size] for i in range(0, len(icao_codes), chunk_size)]

    async def fetch_chunk(chunk):
        async with semaphore:
            return await fetch_live_states_from_opensky(chunk)

    results = await asyncio.gather(*(fetch_chunk(chunk) for chunk in chunks), return_exceptions=True)
    live_data = []

    for result in results:
        if isinstance(result, Exception):
            logging.error("OpenSky chunk lekérdezési hiba: %s", result)
            continue
        live_data.extend(result)
    return live_data



# ----- Útvonal-dúsítás: hívójel -> indulási/érkezési reptér koordinátákkal (adsbdb.com), DB-ben cache-elve -----
ROUTE_API = "https://api.adsbdb.com/v0/callsign/{callsign}"
ROUTE_HIT_TTL_DAYS = 14      # a megtalált útvonalat ennyi napig használjuk újra
ROUTE_MISS_TTL_DAYS = 3      # az ismeretlen hívójelet ennyi nap múlva kérdezzük újra
MAX_ROUTE_LOOKUPS = 150      # egy /live-positions hívásban legfeljebb ennyi új hívójelet keresünk
ROUTE_KEYS = ("origin_icao", "origin_lat", "origin_lon", "dest_icao", "dest_lat", "dest_lon")


def _ensure_route_table():
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze.callsign_routes (
                callsign    text PRIMARY KEY,
                found       boolean NOT NULL,
                origin_icao text, origin_lat double precision, origin_lon double precision,
                dest_icao   text, dest_lat   double precision, dest_lon   double precision,
                fetched_at  timestamptz NOT NULL DEFAULT now())"""))


def load_route_cache(callsigns: list[str]) -> dict[str, dict]:
    """Blocking; a még érvényes cache-sorok hívójel szerint."""
    if not callsigns:
        return {}
    _ensure_route_table()
    query = text("""
        SELECT callsign, found, origin_icao, origin_lat, origin_lon, dest_icao, dest_lat, dest_lon
        FROM bronze.callsign_routes
        WHERE callsign IN :c
          AND fetched_at > now() - CASE WHEN found THEN make_interval(days => :hit)
                                        ELSE make_interval(days => :miss) END
    """).bindparams(bindparam("c", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(query, {"c": callsigns, "hit": ROUTE_HIT_TTL_DAYS,
                                    "miss": ROUTE_MISS_TTL_DAYS}).mappings().all()
    return {r["callsign"]: dict(r) for r in rows}


def save_route_cache(rows: list[dict]) -> None:
    """Blocking; upsert a cache-táblába."""
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bronze.callsign_routes
                (callsign, found, origin_icao, origin_lat, origin_lon, dest_icao, dest_lat, dest_lon, fetched_at)
            VALUES
                (:callsign, :found, :origin_icao, :origin_lat, :origin_lon, :dest_icao, :dest_lat, :dest_lon, now())
            ON CONFLICT (callsign) DO UPDATE SET
                found = EXCLUDED.found,
                origin_icao = EXCLUDED.origin_icao, origin_lat = EXCLUDED.origin_lat, origin_lon = EXCLUDED.origin_lon,
                dest_icao = EXCLUDED.dest_icao, dest_lat = EXCLUDED.dest_lat, dest_lon = EXCLUDED.dest_lon,
                fetched_at = now()"""), rows)


async def _fetch_route(client, semaphore: asyncio.Semaphore, callsign: str) -> dict | None:
    """Cache-sort ad vissza; None, ha a lekérdezés technikai okból sikertelen (azt nem cache-eljük)."""
    async with semaphore:
        try:
            response = await client.get(ROUTE_API.format(callsign=callsign))
        except Exception as e:
            logging.warning("adsbdb hiba (%s): %s", callsign, e)
            return None
        await asyncio.sleep(0.2)   # udvarias tempó a közösségi API felé

    row = {"callsign": callsign, "found": False, **{k: None for k in ROUTE_KEYS}}
    if response.status_code == 404:          # ismeretlen hívójel
        return row
    if response.status_code != 200:
        logging.warning("adsbdb HTTP %s (%s)", response.status_code, callsign)
        return None
    try:
        route = response.json()["response"]["flightroute"]
        origin, dest = route["origin"], route["destination"]
        row.update(found=True,
                   origin_icao=origin["icao_code"], origin_lat=float(origin["latitude"]), origin_lon=float(origin["longitude"]),
                   dest_icao=dest["icao_code"], dest_lat=float(dest["latitude"]), dest_lon=float(dest["longitude"]))
    except Exception:                        # váratlan válasz-alak -> nincs útvonal
        row["found"] = False
    return row


async def enrich_with_routes(flights: list[dict]) -> None:
    """Minden járathoz hozzáadja az origin_*/dest_* mezőket (None, ha nincs útvonal)."""
    for f in flights:
        for k in ROUTE_KEYS:
            f[k] = None
    try:
        callsigns = sorted({f["callsign"] for f in flights if f.get("callsign") and f["callsign"] != "UNKNOWN"})
        if not callsigns:
            return

        cache = await asyncio.to_thread(load_route_cache, callsigns)
        missing = [c for c in callsigns if c not in cache][:MAX_ROUTE_LOOKUPS]

        if missing:
            semaphore = asyncio.Semaphore(3)
            async with httpx.AsyncClient(timeout=10.0) as client:
                results = await asyncio.gather(*(_fetch_route(client, semaphore, c) for c in missing))
            fresh = [r for r in results if r is not None]
            if fresh:
                await asyncio.to_thread(save_route_cache, fresh)
                cache.update({r["callsign"]: r for r in fresh})
            logging.info("Útvonal-keresés: %d új hívójel, %d sikeres lekérdezés", len(missing), len(fresh))

        for f in flights:
            route = cache.get(f["callsign"])
            if route and route["found"]:
                for k in ROUTE_KEYS:
                    f[k] = route[k]
    except Exception as e:
        logging.error("Útvonal-dúsítási hiba: %s", e)


# ----- Géptípus-dúsítás: icao24 -> típus (adsbdb.com), DB-ben cache-elve -----
AIRCRAFT_API = "https://api.adsbdb.com/v0/aircraft/{icao24}"
TYPE_HIT_TTL_DAYS = 180      # a gép típusa gyakorlatilag nem változik
TYPE_MISS_TTL_DAYS = 14      # az ismeretlen gépet ennyi nap múlva kérdezzük újra
MAX_TYPE_LOOKUPS = 150       # egy /live-positions hívásban legfeljebb ennyi új gépet keresünk
TYPE_KEYS = ("aircraft_icao_type", "aircraft_type")   # pl. "A388" és a modell neve


def _ensure_type_table():
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze.aircraft_types (
                icao24     text PRIMARY KEY,
                found      boolean NOT NULL,
                icao_type  text,
                model      text,
                fetched_at timestamptz NOT NULL DEFAULT now())"""))


def load_type_cache(icao24s: list[str]) -> dict[str, dict]:
    """Blocking; a még érvényes cache-sorok icao24 szerint."""
    if not icao24s:
        return {}
    _ensure_type_table()
    query = text("""
        SELECT icao24, found, icao_type, model
        FROM bronze.aircraft_types
        WHERE icao24 IN :c
          AND fetched_at > now() - CASE WHEN found THEN make_interval(days => :hit)
                                        ELSE make_interval(days => :miss) END
    """).bindparams(bindparam("c", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(query, {"c": icao24s, "hit": TYPE_HIT_TTL_DAYS,
                                    "miss": TYPE_MISS_TTL_DAYS}).mappings().all()
    return {r["icao24"]: dict(r) for r in rows}


def save_type_cache(rows: list[dict]) -> None:
    """Blocking; upsert a cache-táblába."""
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bronze.aircraft_types (icao24, found, icao_type, model, fetched_at)
            VALUES (:icao24, :found, :icao_type, :model, now())
            ON CONFLICT (icao24) DO UPDATE SET
                found = EXCLUDED.found, icao_type = EXCLUDED.icao_type,
                model = EXCLUDED.model, fetched_at = now()"""), rows)


async def _fetch_aircraft_type(client, semaphore: asyncio.Semaphore, icao24: str) -> dict | None:
    """Cache-sort ad vissza; None, ha a lekérdezés technikai okból sikertelen (azt nem cache-eljük)."""
    async with semaphore:
        try:
            response = await client.get(AIRCRAFT_API.format(icao24=icao24))
        except Exception as e:
            logging.warning("adsbdb gép-hiba (%s): %s", icao24, e)
            return None
        await asyncio.sleep(0.2)   # udvarias tempó a közösségi API felé

    row = {"icao24": icao24, "found": False, "icao_type": None, "model": None}
    if response.status_code == 404:          # ismeretlen gép
        return row
    if response.status_code != 200:
        logging.warning("adsbdb gép HTTP %s (%s)", response.status_code, icao24)
        return None
    try:
        aircraft = response.json()["response"]["aircraft"]
        row.update(found=True, icao_type=aircraft.get("icao_type"), model=aircraft.get("type"))
    except Exception:                        # váratlan válasz-alak -> nincs típus
        row["found"] = False
    return row


async def enrich_with_aircraft_types(flights: list[dict]) -> None:
    """Minden járathoz hozzáadja az aircraft_icao_type és aircraft_type mezőt (None, ha ismeretlen)."""
    for f in flights:
        for k in TYPE_KEYS:
            f[k] = None
    try:
        icao24s = sorted({f["icao24"].lower() for f in flights if f.get("icao24") and f["icao24"] != "N/A"})
        if not icao24s:
            return

        cache = await asyncio.to_thread(load_type_cache, icao24s)
        missing = [c for c in icao24s if c not in cache][:MAX_TYPE_LOOKUPS]

        if missing:
            semaphore = asyncio.Semaphore(3)
            async with httpx.AsyncClient(timeout=10.0) as client:
                results = await asyncio.gather(*(_fetch_aircraft_type(client, semaphore, c) for c in missing))
            fresh = [r for r in results if r is not None]
            if fresh:
                await asyncio.to_thread(save_type_cache, fresh)
                cache.update({r["icao24"]: r for r in fresh})
            logging.info("Géptípus-keresés: %d új gép, %d sikeres lekérdezés", len(missing), len(fresh))

        for f in flights:
            row = cache.get((f.get("icao24") or "").lower())
            if row and row["found"]:
                f["aircraft_icao_type"] = row["icao_type"]
                f["aircraft_type"] = row["model"]
    except Exception as e:
        logging.error("Géptípus-dúsítási hiba: %s", e)


@dlt.resource(name="opensky_live_positions", write_disposition="merge", primary_key=["icao24", "snapshot_at"])
def fetch_live_positions_resource(rows: list[dict]):
    yield from rows


def run_dlt_live_positions(live_data: list[dict]):
    if not live_data:
        logging.warning("Nincs menthető OpenSky pozíció.")
        return None

    pipeline = dlt.pipeline(
        pipeline_name="opensky_live_positions",
        destination=dlt.destinations.postgres(credentials=DB_CONFIG),
        dataset_name="bronze")

    load_info = pipeline.run(fetch_live_positions_resource(live_data))
    return str(load_info)


# ----- 1. FASTAPI ENDPOINT (A Dash térkép aszinkron AJAX hívásaihoz) -----
@router.get("/live-positions")
async def get_live_positions():

    icao_codes = await asyncio.to_thread(get_tracked_icao_codes)

    if not icao_codes:
        return {"flights": []}

    live_data = await fetch_live_states_batched(icao_codes, chunk_size=50, max_concurrency=5)

    await enrich_with_routes(live_data)    # indulási/érkezési reptér a hívójel alapján
    await enrich_with_aircraft_types(live_data)    # géptípus az icao24 alapján

    if not live_data:
        return {"flights": []}

    snapshot_at = datetime.now(timezone.utc).isoformat()    # Közös időbélyeg az adott pillanatfelvételhez

    for flight in live_data:
        flight["snapshot_at"] = snapshot_at

    await asyncio.to_thread(run_dlt_live_positions, live_data)

    return {"flights": live_data}
