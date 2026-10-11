# 2026.10.11  17.00
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
# Trailing space-ek eltávolítva a megbízható egyezés érdekében
AIRPORTS = ["OMDB", "OMAA", "EDDF", "VHHH", "YSSY", "KLAX", "EHAM", "LHBP", "KJFK"]
AIRLINES = ["UAE", "QTR", "ETD", "DLH", "QFA", "WZZ", "KLM", "BAW", "AFR", "AAL", "DAL"]

CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}
DB_URL = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)

API = "https://opensky-network.org/api"
router = APIRouter()

# Limitálók az adsbdb.com védelmére
MAX_LOOKUPS_PER_RUN = 150
ROUTE_HIT_TTL_DAYS = 14
ROUTE_MISS_TTL_DAYS = 3
TYPE_HIT_TTL_DAYS = 180
TYPE_MISS_TTL_DAYS = 14
AIRPORT_TTL_DAYS = 365

def get_auth_token():
    payload = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
    response = requests.post(AUTH_URL, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

# ==============================================================================
# 1. PIPELINE: Történelmi járatok lekérése (OpenSky)
# ==============================================================================
@dlt.resource(name="opensky_flights", write_disposition="merge", primary_key=["icao24", "first_seen"])
def fetch_airport_flights():
    token = get_auth_token()
    headers = {"Authorization": f"Bearer {token}"}
    time_end = int(time.time())
    time_start = int(time_end - 1.5 * 86400)  # 36 óra (biztonságos kompromisszum)
    
    for airport in AIRPORTS:
        for direction in ("departure", "arrival"):
            url_flights = f"{API}/flights/{direction}?airport={airport}&begin={time_start}&end={time_end}"
            try:
                response = dlt_requests.get(url_flights, headers=headers, timeout=30) # Timeout növelve
                if response.status_code == 404: 
                    continue
                if response.status_code != 200:
                    logging.warning(f"OpenSky API hiba {response.status_code} a(z) {airport} reptérnél")
                    continue
                
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
                            "updated_at": datetime.now(timezone.utc).isoformat()
                        }
            except Exception as e:
                logging.error(f"Hiba a(z) {airport} reptér lekérdezésekor: {str(e)}")

        time.sleep(2) 

# ==============================================================================
# SEGÉDFÜGGVÉNYEK: Hiányzó elemek lekérdezése az adatbázisból
# ==============================================================================
def _ensure_airports_table():
    """Létrehozza a bronze.airports táblát, ha még nem létezik."""
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze.airports (icao text PRIMARY KEY, name text, city text,
            lat double precision, lon double precision, fetched_at timestamptz NOT NULL DEFAULT now())
        """))

def get_missing_callsigns() -> list[str]:
    _ensure_airports_table()
    query = text("""
        SELECT DISTINCT callsign FROM bronze.opensky_flights
        WHERE callsign IS NOT NULL AND callsign != 'UNKNOWN'
        EXCEPT
        SELECT callsign FROM bronze.callsign_routes 
        WHERE fetched_at > now() - CASE WHEN found THEN make_interval(days => :hit) ELSE make_interval(days => :miss) END
        LIMIT :limit
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"hit": ROUTE_HIT_TTL_DAYS, "miss": ROUTE_MISS_TTL_DAYS, "limit": MAX_LOOKUPS_PER_RUN})
        return [row[0] for row in result]

def get_missing_icao24s() -> list[str]:
    query = text("""
        SELECT DISTINCT icao24 FROM bronze.opensky_flights
        WHERE icao24 IS NOT NULL
        EXCEPT
        SELECT icao24 FROM bronze.aircraft_types 
        WHERE fetched_at > now() - CASE WHEN found THEN make_interval(days => :hit) ELSE make_interval(days => :miss) END
        LIMIT :limit
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"hit": TYPE_HIT_TTL_DAYS, "miss": TYPE_MISS_TTL_DAYS, "limit": MAX_LOOKUPS_PER_RUN})
        return [row[0] for row in result]

def get_missing_airports() -> list[str]:
    query = text("""
        SELECT DISTINCT airport_code FROM (
            SELECT est_departure_airport AS airport_code FROM bronze.opensky_flights
            UNION
            SELECT est_arrival_airport AS airport_code FROM bronze.opensky_flights
        ) AS airports
        WHERE airport_code IS NOT NULL
        EXCEPT
        SELECT icao FROM bronze.airports WHERE fetched_at > now() - make_interval(days => :ttl)
        LIMIT :limit
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"ttl": AIRPORT_TTL_DAYS, "limit": MAX_LOOKUPS_PER_RUN})
        return [row[0] for row in result]

# ==============================================================================
# 2. PIPELINE: Útvonalak dúsítása (adsbdb.com callsign)
# ==============================================================================
@dlt.resource(name="callsign_routes", write_disposition="merge", primary_key="callsign")
def fetch_missing_routes():
    missing_callsigns = get_missing_callsigns()
    logging.info(f"Útvonal dúsítás: {len(missing_callsigns)} hiányzó callsign")
    
    for callsign in missing_callsigns:
        time.sleep(0.2)  # Udvarias tempó
        try:
            resp = requests.get(f"https://api.adsbdb.com/v0/callsign/{callsign}", timeout=10)
            row = {"callsign": callsign, "found": False, "origin_icao": None, "origin_lat": None, "origin_lon": None, 
                   "dest_icao": None, "dest_lat": None, "dest_lon": None, "fetched_at": datetime.now(timezone.utc)}
            
            if resp.status_code == 200:
                data = resp.json().get("response", {}).get("flightroute", {})
                if data and "origin" in data and "destination" in data:
                    row.update({
                        "found": True,
                        "origin_icao": data["origin"].get("icao_code"),
                        "origin_lat": float(data["origin"].get("latitude", 0)),
                        "origin_lon": float(data["origin"].get("longitude", 0)),
                        "dest_icao": data["destination"].get("icao_code"),
                        "dest_lat": float(data["destination"].get("latitude", 0)),
                        "dest_lon": float(data["destination"].get("longitude", 0))
                    })
            yield row
        except Exception as e:
            logging.warning(f"Útvonal lekérdezési hiba ({callsign}): {e}")

# ==============================================================================
# 3. PIPELINE: Géptípusok dúsítása (adsbdb.com icao24)
# ==============================================================================
@dlt.resource(name="aircraft_types", write_disposition="merge", primary_key="icao24")
def fetch_missing_aircraft():
    missing_icao24s = get_missing_icao24s()
    logging.info(f"Géptípus dúsítás: {len(missing_icao24s)} hiányzó icao24")
    
    for icao24 in missing_icao24s:
        time.sleep(0.2)  # Udvarias tempó
        try:
            resp = requests.get(f"https://api.adsbdb.com/v0/aircraft/{icao24}", timeout=10)
            row = {"icao24": icao24, "found": False, "icao_type": None, "model": None, "fetched_at": datetime.now(timezone.utc)}
            
            if resp.status_code == 200:
                data = resp.json().get("response", {}).get("aircraft", {})
                if data:
                    row.update({
                        "found": True,
                        "icao_type": data.get("icao_type"),
                        "model": data.get("type")
                    })
            yield row
        except Exception as e:
            logging.warning(f"Géptípus lekérdezési hiba ({icao24}): {e}")

# ==============================================================================
# 4. PIPELINE: Repterek dúsítása (adsbdb.com airport)
# ==============================================================================
@dlt.resource(name="airports", write_disposition="merge", primary_key="icao")
def fetch_missing_airports():
    missing_airports = get_missing_airports()
    logging.info(f"Reptér dúsítás: {len(missing_airports)} hiányzó reptér")
    
    for icao in missing_airports:
        time.sleep(0.2)  # Udvarias tempó
        try:
            resp = requests.get(f"https://api.adsbdb.com/v0/airport/{icao}", timeout=10)
            row = {"icao": icao, "name": None, "city": None, "lat": None, "lon": None, "fetched_at": datetime.now(timezone.utc)}
            
            if resp.status_code == 200:
                data = resp.json().get("response", {}).get("airport", {})
                if data:
                    row.update({
                        "name": data.get("name"),
                        "city": data.get("city"),
                        "lat": float(data.get("latitude", 0)),
                        "lon": float(data.get("longitude", 0))
                    })
            yield row
        except Exception as e:
            logging.warning(f"Reptér lekérdezési hiba ({icao}): {e}")

# ==============================================================================
# ÖSSZEFONT HÁTTÉRFOLYAMAT
# ==============================================================================
def run_full_sync_pipeline():
    """Egymás után futtatja a 4 pipeline-t, hogy az adatok függőségi sorrendben frissüljenek."""
    logging.info("=== TELJES SZINKRONIZÁCIÓ ELINDULT ===")
    
    pipeline = dlt.pipeline(
        pipeline_name="opensky_full_tracker",
        destination=dlt.destinations.postgres(credentials=DB_CONFIG),
        dataset_name="bronze"
    )
    
    try:
        # 1. Történelmi adatok
        logging.info("1/4: Történelmi járatok letöltése...")
        pipeline.run(fetch_airport_flights())
        
        # 2. Útvonalak
        logging.info("2/4: Hiányzó útvonalak dúsítása...")
        pipeline.run(fetch_missing_routes())
        
        # 3. Géptípusok
        logging.info("3/4: Hiányzó géptípusok dúsítása...")
        pipeline.run(fetch_missing_aircraft())
        
        # 4. Repterek
        logging.info("4/4: Hiányzó repterek dúsítása...")
        pipeline.run(fetch_missing_airports())
        
        logging.info("=== TELJES SZINKRONIZÁCIÓ SIKERES ===")
    except Exception as e:
        logging.error(f"Hiba a teljes szinkronizáció során: {e}")

@router.get("/flights")
def trigger_full_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_full_sync_pipeline)
    return {
        "message": "Teljes szinkronizáció elindult: történelmi adatok + 3 cache tábla frissítése.", 
        "timestamp": datetime.now().isoformat()
    }

# ==============================================================================
# ÉLŐ POZÍCIÓK KEZELÉSE (A korábbi just-in-time logika maradványa, de most már a cache-re támaszkodik)
# ==============================================================================
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
    query = text("""
        SELECT DISTINCT icao24 FROM bronze.opensky_flights 
        WHERE icao24 IS NOT NULL
        AND last_seen > extract(epoch from now() - interval '24 hours')::bigint 
        ORDER BY icao24
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            return [row[0].strip() for row in result if row[0]]            
    except Exception as e:
        logging.error(f"Adatbázis hiba az ICAO kódok lekérésekor: {e}")
        return []

async def fetch_live_states_from_opensky(icao_list):
    if not icao_list:
        return []
    token = await asyncio.to_thread(get_cached_token)
    headers = {"Authorization": f"Bearer {token}"}
    params = [("icao24", c) for c in icao_list]
    
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(f"{API}/states/all", params=params, headers=headers)
            if response.status_code == 401:
                _token["exp"] = 0
                return []
            if response.status_code != 200:
                return []
            
            data = response.json()
            states = data.get("states", [])
            live_flights = []
            if states:
                for s in states:
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
            logging.error(f"Hiba az OpenSky API hívásakor: {e}")
            return []

async def fetch_live_states_batched(icao_codes: list[str], chunk_size: int = 50, max_concurrency: int = 5) -> list[dict]:
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

# Itt már NINCS SZÜKSÉG just-in-time enrich_with_routes/aircraft függvényekre, 
# mert a /live endpoint most már közvetlenül JOIN-olhatja a cache táblákat, 
# vagy a dlt pipeline már előre feltöltötte őket. 
# Az egyszerűség kedvéért most a korábbi enrich függvényeket meghagyhatod fallbackként, 
# de a háttérfolyamat miatt 99%-ban cache-hit lesz.

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
        dataset_name="bronze"
    )
    load_info = pipeline.run(fetch_live_positions_resource(live_data))
    return str(load_info)

@router.get("/live")
async def get_live_positions():
    icao_codes = await asyncio.to_thread(get_tracked_icao_codes)
    if not icao_codes:
        return {"flights": []}
    
    live_data = await fetch_live_states_batched(icao_codes, chunk_size=50, max_concurrency=5)
    
    # Mivel a háttérfolyamat már feltöltötte a cache-t, itt már csak minimális fallback kell, 
    # vagy akár el is hagyható, ha a Dash oldal SQL JOIN-nal olvassa ki az adatokat.
    # A kompatibilitás miatt meghagyjuk a hívást, de most már gyors lesz (cache hit).
    await enrich_with_routes(live_data)    
    await enrich_with_aircraft_types(live_data)    
    
    if not live_data:
        return {"flights": []}
    
    snapshot_at = datetime.now(timezone.utc).isoformat()
    for flight in live_data:
        flight["snapshot_at"] = snapshot_at
        
    await asyncio.to_thread(run_dlt_live_positions, live_data)
    return {"flights": live_data}


# ==============================================================================
# SEGÉDFÜGGVÉNYEK ÉS ENRICH LOGIKA (A /live végponthoz)
# ==============================================================================
# --- Útvonal-dúsítás ---
ROUTE_API = "https://api.adsbdb.com/v0/callsign/{callsign}"
ROUTE_HIT_TTL_DAYS = 14
ROUTE_MISS_TTL_DAYS = 3
MAX_ROUTE_LOOKUPS = 150
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
    if not callsigns: return {}
    _ensure_route_table()
    query = text("""
        SELECT callsign, found, origin_icao, origin_lat, origin_lon, dest_icao, dest_lat, dest_lon
        FROM bronze.callsign_routes
        WHERE callsign IN :c
        AND fetched_at > now() - CASE WHEN found THEN make_interval(days => :hit)
                                      ELSE make_interval(days => :miss) END
    """).bindparams(bindparam("c", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(query, {"c": callsigns, "hit": ROUTE_HIT_TTL_DAYS, "miss": ROUTE_MISS_TTL_DAYS}).mappings().all()
    return {r["callsign"]: dict(r) for r in rows}

def save_route_cache(rows: list[dict]) -> None:
    if not rows: return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bronze.callsign_routes
            (callsign, found, origin_icao, origin_lat, origin_lon, dest_icao, dest_lat, dest_lon, fetched_at)
            VALUES (:callsign, :found, :origin_icao, :origin_lat, :origin_lon, :dest_icao, :dest_lat, :dest_lon, now())
            ON CONFLICT (callsign) DO UPDATE SET
                found = EXCLUDED.found, origin_icao = EXCLUDED.origin_icao, origin_lat = EXCLUDED.origin_lat, origin_lon = EXCLUDED.origin_lon,
                dest_icao = EXCLUDED.dest_icao, dest_lat = EXCLUDED.dest_lat, dest_lon = EXCLUDED.dest_lon, fetched_at = now()
        """), rows)

async def _fetch_route(client, semaphore: asyncio.Semaphore, callsign: str) -> dict | None:
    async with semaphore:
        try:
            response = await client.get(ROUTE_API.format(callsign=callsign))
        except Exception as e:
            logging.warning("adsbdb hiba (%s): %s", callsign, e)
            return None
        await asyncio.sleep(0.2)
        row = {"callsign": callsign, "found": False, **{k: None for k in ROUTE_KEYS}}
        if response.status_code == 404: return row
        if response.status_code != 200:
            logging.warning("adsbdb HTTP %s (%s)", response.status_code, callsign)
            return None
        try:
            route = response.json()["response"]["flightroute"]
            origin, dest = route["origin"], route["destination"]
            row.update(found=True,
                       origin_icao=origin["icao_code"], origin_lat=float(origin["latitude"]), origin_lon=float(origin["longitude"]),
                       dest_icao=dest["icao_code"], dest_lat=float(dest["latitude"]), dest_lon=float(dest["longitude"]))
        except Exception:
            row["found"] = False
        return row

async def enrich_with_routes(flights: list[dict]) -> None:
    for f in flights:
        for k in ROUTE_KEYS: f[k] = None
    try:
        callsigns = sorted({f["callsign"] for f in flights if f.get("callsign") and f["callsign"] != "UNKNOWN"})
        if not callsigns: return
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
                for k in ROUTE_KEYS: f[k] = route[k]
    except Exception as e:
        logging.error("Útvonal-dúsítási hiba: %s", e)


# --- Géptípus-dúsítás ---
AIRCRAFT_API = "https://api.adsbdb.com/v0/aircraft/{icao24}"
TYPE_HIT_TTL_DAYS = 180
TYPE_MISS_TTL_DAYS = 14
MAX_TYPE_LOOKUPS = 150
TYPE_KEYS = ("aircraft_icao_type", "aircraft_type")

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
    if not icao24s: return {}
    _ensure_type_table()
    query = text("""
        SELECT icao24, found, icao_type, model
        FROM bronze.aircraft_types
        WHERE icao24 IN :c
        AND fetched_at > now() - CASE WHEN found THEN make_interval(days => :hit)
                                      ELSE make_interval(days => :miss) END
    """).bindparams(bindparam("c", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(query, {"c": icao24s, "hit": TYPE_HIT_TTL_DAYS, "miss": TYPE_MISS_TTL_DAYS}).mappings().all()
    return {r["icao24"]: dict(r) for r in rows}

def save_type_cache(rows: list[dict]) -> None:
    if not rows: return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bronze.aircraft_types (icao24, found, icao_type, model, fetched_at)
            VALUES (:icao24, :found, :icao_type, :model, now())
            ON CONFLICT (icao24) DO UPDATE SET
                found = EXCLUDED.found, icao_type = EXCLUDED.icao_type,
                model = EXCLUDED.model, fetched_at = now()"""), rows)

async def _fetch_aircraft_type(client, semaphore: asyncio.Semaphore, icao24: str) -> dict | None:
    async with semaphore:
        try:
            response = await client.get(AIRCRAFT_API.format(icao24=icao24))
        except Exception as e:
            logging.warning("adsbdb gép-hiba (%s): %s", icao24, e)
            return None
        await asyncio.sleep(0.2)
        row = {"icao24": icao24, "found": False, "icao_type": None, "model": None}
        if response.status_code == 404: return row
        if response.status_code != 200:
            logging.warning("adsbdb gép HTTP %s (%s)", response.status_code, icao24)
            return None
        try:
            aircraft = response.json()["response"]["aircraft"]
            row.update(found=True, icao_type=aircraft.get("icao_type"), model=aircraft.get("type"))
        except Exception:
            row["found"] = False
        return row

async def enrich_with_aircraft_types(flights: list[dict]) -> None:
    for f in flights:
        for k in TYPE_KEYS: f[k] = None
    try:
        icao24s = sorted({f["icao24"].lower() for f in flights if f.get("icao24") and f["icao24"] != "N/A"})
        if not icao24s: return
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
