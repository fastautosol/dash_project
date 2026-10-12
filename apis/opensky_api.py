# 2026.10.12  9.00
import asyncio
import csv
import io
import logging
import os
import time
from datetime import datetime, timezone

import dlt
import httpx
import requests
from fastapi import APIRouter, BackgroundTasks
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import URL

# ----- Config -----
AIRPORTS = ["OMDB", "OMAA", "EDDF", "VHHH", "YSSY", "KLAX", "EHAM", "LHBP", "KJFK"]
AIRLINES = ["UAE", "QTR", "ETD", "DLH", "QFA", "WZZ", "KLM", "BAW", "AFR", "AAL", "DAL"]

CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API = "https://opensky-network.org/api"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "DB_POSTGRESDB_HOST"),
    "port": os.getenv("DB_PORT", "DB_POSTGRESDB_PORT"),
    "database": os.getenv("DB_NAME", "DB_POSTGRESDB_DATABASE"),
    "username": os.getenv("DB_USER", "DB_POSTGRESDB_USER"),
    "password": os.getenv("DB_PASSWORD", "DB_POSTGRESDB_PASSWORD"),
    "connect_timeout": 15}

if not DB_CONFIG["username"] or not DB_CONFIG["password"]:
    logging.error("Hiányzik a DB_USER / DB_PASSWORD környezeti változó az app konténerében.")

DB_URL = URL.create(
    "postgresql+psycopg",
    username=DB_CONFIG["username"],
    password=DB_CONFIG["password"],
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    database=DB_CONFIG["database"],
)
engine = create_engine(DB_URL, pool_size=5, max_overflow=10, pool_pre_ping=True)

router = APIRouter()

# OpenSky /flights/* : az adatok éjszakai batch-ben frissülnek, csak a TEGNAPI és
# korábbi napok érhetők el, az intervallum pedig max. 2 nap lehet.
# Ezért lezárt UTC napokat kérdezünk le (a "most" végű ablak 400-as hibát ad).
FLIGHTS_LOOKBACK_DAYS = 1          # 1 vagy 2 (max. 2 nap!)
TRACKED_WINDOW_HOURS = 48          # a batch késése miatt 24 óra túl szűk

# adsbdb.com védelme
MAX_LOOKUPS_PER_RUN = 150
ROUTE_HIT_TTL_DAYS = 14
ROUTE_MISS_TTL_DAYS = 3
TYPE_HIT_TTL_DAYS = 180
TYPE_MISS_TTL_DAYS = 14
AIRPORT_TTL_DAYS = 365
ROUTE_API = "https://api.adsbdb.com/v0/callsign/{callsign}"
AIRCRAFT_API = "https://api.adsbdb.com/v0/aircraft/{icao24}"
# Az adsbdb-nek NINCS /airport végpontja (404), ezért az OurAirports adatbázist használjuk.
AIRPORTS_CSV_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"

ROUTE_KEYS = ("origin_icao", "origin_lat", "origin_lon", "dest_icao", "dest_lat", "dest_lon")
TYPE_KEYS = ("aircraft_icao_type", "aircraft_type")

# Futásonkénti számláló: a "SIKERES" log ne hazudjon, ha közben hibák voltak
_stats = {"errors": 0, "flights": 0}


# ==============================================================================
# Token kezelés (egyetlen, megosztott cache)
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


class OpenSkyRateLimited(Exception):
    pass


def opensky_get(path: str, params: dict) -> requests.Response:
    """OpenSky GET: token-frissítés 401-nél, 429-nél kivétel. NEM dob kivételt 4xx-re."""
    r = None
    for _ in range(2):
        headers = {"Authorization": f"Bearer {get_cached_token()}"}
        r = requests.get(f"{API}{path}", params=params, headers=headers, timeout=30)
        if r.status_code == 401:
            _token["exp"] = 0
            continue
        break
    if r.status_code == 429:
        retry = r.headers.get("X-Rate-Limit-Retry-After-Seconds", "?")
        raise OpenSkyRateLimited(f"429 – elfogyott a kredit, újrapróbálás {retry} mp múlva")
    return r


def flight_window() -> tuple[int, int]:
    """Az utolsó FLIGHTS_LOOKBACK_DAYS lezárt UTC nap (tegnap 23:59:59-ig)."""
    today_midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end = int(today_midnight.timestamp()) - 1
    days = max(1, min(FLIGHTS_LOOKBACK_DAYS, 2))
    begin = end - days * 86400 + 1          # < 2 nap
    return begin, end


# ==============================================================================
# Táblák biztosítása
# ==============================================================================
def _table_exists(name: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(text("SELECT to_regclass(:n) IS NOT NULL"), {"n": name}).scalar()


def _ensure_airports_table():
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze.airports (icao text PRIMARY KEY, name text, city text,
            lat double precision, lon double precision, fetched_at timestamptz NOT NULL DEFAULT now())
        """))


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


# ==============================================================================
# 1. PIPELINE: Történelmi járatok (OpenSky)
# ==============================================================================
@dlt.resource(name="opensky_flights", write_disposition="merge", primary_key=["icao24", "first_seen"])
def fetch_airport_flights():
    begin, end = flight_window()
    logging.info(
        "OpenSky ablak: %s → %s (UTC)",
        datetime.fromtimestamp(begin, timezone.utc).isoformat(),
        datetime.fromtimestamp(end, timezone.utc).isoformat(),
    )

    for airport in AIRPORTS:
        for direction in ("departure", "arrival"):
            try:
                response = opensky_get(f"/flights/{direction}", {"airport": airport, "begin": begin, "end": end})
            except OpenSkyRateLimited as e:
                _stats["errors"] += 1
                logging.error("OpenSky: %s – a letöltés leáll", e)
                return
            except Exception as e:
                _stats["errors"] += 1
                logging.error("Hiba a(z) %s reptér (%s) lekérdezésekor: %s", airport, direction, e)
                continue

            if response.status_code == 404:      # nincs járat az időszakban – normális
                logging.info("%s %s: nincs adat (404)", airport, direction)
            elif response.status_code != 200:
                _stats["errors"] += 1
                logging.error("OpenSky %s a(z) %s %s hívásnál: %s",
                              response.status_code, airport, direction, response.text[:300])
            else:
                for f in response.json():
                    callsign = (f.get("callsign") or "").strip()
                    icao24 = f.get("icao24")
                    dep = f.get("estDepartureAirport")
                    arr = f.get("estArrivalAirport")
                    if callsign[:3] in AIRLINES and icao24 and dep and arr:
                        _stats["flights"] += 1
                        yield {
                            "icao24": icao24,
                            "callsign": callsign,
                            "est_departure_airport": dep,
                            "est_arrival_airport": arr,
                            "first_seen": f.get("firstSeen"),
                            "last_seen": f.get("lastSeen"),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        }
            time.sleep(0.5)


# ==============================================================================
# Hiányzó elemek lekérdezése az adatbázisból
# ==============================================================================
def get_missing_callsigns() -> list[str]:
    if not _table_exists("bronze.opensky_flights"):
        return []
    _ensure_route_table()
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
    if not _table_exists("bronze.opensky_flights"):
        return []
    _ensure_type_table()
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
    if not _table_exists("bronze.opensky_flights"):
        return []
    _ensure_airports_table()
    # name IS NOT NULL: a korábbi (hibás adsbdb-s) üres sorok újra feldolgozásra kerülnek
    query = text("""
        SELECT DISTINCT airport_code FROM (
            SELECT est_departure_airport AS airport_code FROM bronze.opensky_flights
            UNION
            SELECT est_arrival_airport AS airport_code FROM bronze.opensky_flights
        ) AS airports
        WHERE airport_code IS NOT NULL
        EXCEPT
        SELECT icao FROM bronze.airports
        WHERE name IS NOT NULL AND fetched_at > now() - make_interval(days => :ttl)
        LIMIT :limit
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"ttl": AIRPORT_TTL_DAYS, "limit": MAX_LOOKUPS_PER_RUN})
        return [row[0] for row in result]


# ==============================================================================
# 2. PIPELINE: Útvonalak dúsítása (adsbdb.com callsign)
#    Csak 200 / 404 kerül a cache-be; 429/5xx/hálózati hiba újrapróbálódik.
# ==============================================================================
@dlt.resource(name="callsign_routes", write_disposition="merge", primary_key="callsign")
def fetch_missing_routes():
    missing_callsigns = get_missing_callsigns()
    logging.info("Útvonal dúsítás: %d hiányzó callsign", len(missing_callsigns))

    for callsign in missing_callsigns:
        time.sleep(0.2)
        try:
            resp = requests.get(ROUTE_API.format(callsign=callsign), timeout=10)
        except Exception as e:
            _stats["errors"] += 1
            logging.warning("Útvonal lekérdezési hiba (%s): %s", callsign, e)
            continue

        row = {"callsign": callsign, "found": False, **{k: None for k in ROUTE_KEYS},
               "fetched_at": datetime.now(timezone.utc)}
        if resp.status_code == 200:
            try:
                data = resp.json().get("response", {}).get("flightroute", {})
                if data and "origin" in data and "destination" in data:
                    row.update({
                        "found": True,
                        "origin_icao": data["origin"].get("icao_code"),
                        "origin_lat": float(data["origin"]["latitude"]),
                        "origin_lon": float(data["origin"]["longitude"]),
                        "dest_icao": data["destination"].get("icao_code"),
                        "dest_lat": float(data["destination"]["latitude"]),
                        "dest_lon": float(data["destination"]["longitude"]),
                    })
            except Exception as e:
                logging.warning("Útvonal feldolgozási hiba (%s): %s", callsign, e)
        elif resp.status_code != 404:
            _stats["errors"] += 1
            logging.warning("adsbdb HTTP %s (%s) – kihagyva, nem cache-eljük", resp.status_code, callsign)
            continue
        yield row


# ==============================================================================
# 3. PIPELINE: Géptípusok dúsítása (adsbdb.com icao24)
# ==============================================================================
@dlt.resource(name="aircraft_types", write_disposition="merge", primary_key="icao24")
def fetch_missing_aircraft():
    missing_icao24s = get_missing_icao24s()
    logging.info("Géptípus dúsítás: %d hiányzó icao24", len(missing_icao24s))

    for icao24 in missing_icao24s:
        time.sleep(0.2)
        try:
            resp = requests.get(AIRCRAFT_API.format(icao24=icao24), timeout=10)
        except Exception as e:
            _stats["errors"] += 1
            logging.warning("Géptípus lekérdezési hiba (%s): %s", icao24, e)
            continue

        row = {"icao24": icao24, "found": False, "icao_type": None, "model": None,
               "fetched_at": datetime.now(timezone.utc)}
        if resp.status_code == 200:
            data = resp.json().get("response", {}).get("aircraft", {})
            if data:
                row.update({"found": True, "icao_type": data.get("icao_type"), "model": data.get("type")})
        elif resp.status_code != 404:
            _stats["errors"] += 1
            logging.warning("adsbdb gép HTTP %s (%s) – kihagyva, nem cache-eljük", resp.status_code, icao24)
            continue
        yield row


# ==============================================================================
# 4. PIPELINE: Repterek dúsítása (OurAirports CSV)
# ==============================================================================
_airport_db: dict | None = None


def _load_airport_db() -> dict:
    global _airport_db
    if _airport_db is None:
        r = requests.get(AIRPORTS_CSV_URL, timeout=60)
        r.raise_for_status()
        db = {}
        for rec in csv.DictReader(io.StringIO(r.text)):
            for key in (rec.get("ident"), rec.get("icao_code"), rec.get("gps_code")):
                if key:
                    db.setdefault(key.strip().upper(), rec)
        _airport_db = db
    return _airport_db


@dlt.resource(name="airports", write_disposition="merge", primary_key="icao")
def fetch_missing_airports():
    missing_airports = get_missing_airports()
    logging.info("Reptér dúsítás: %d hiányzó reptér", len(missing_airports))
    if not missing_airports:
        return

    try:
        db = _load_airport_db()
    except Exception as e:
        _stats["errors"] += 1
        logging.error("Repülőtér-adatbázis letöltési hiba: %s", e)
        return

    for icao in missing_airports:
        rec = db.get(icao.upper())
        if not rec:
            logging.warning("Ismeretlen reptér: %s – kihagyva", icao)
            continue
        try:
            yield {
                "icao": icao,
                "name": rec.get("name"),
                "city": rec.get("municipality"),
                "lat": float(rec["latitude_deg"]),
                "lon": float(rec["longitude_deg"]),
                "fetched_at": datetime.now(timezone.utc),
            }
        except Exception as e:
            logging.warning("Reptér feldolgozási hiba (%s): %s", icao, e)


# ==============================================================================
# ÖSSZEFŰZÖTT HÁTTÉRFOLYAMAT
# ==============================================================================
def run_full_sync_pipeline():
    """Egymás után futtatja a 4 pipeline-t, függőségi sorrendben."""
    _stats["errors"] = 0
    _stats["flights"] = 0
    logging.info("=== TELJES SZINKRONIZÁCIÓ ELINDULT ===")

    pipeline = dlt.pipeline(
        pipeline_name="opensky_full_tracker",
        destination=dlt.destinations.postgres(credentials=DB_CONFIG),
        dataset_name="bronze",
    )

    steps = [
        ("1/4: Történelmi járatok letöltése...", fetch_airport_flights),
        ("2/4: Hiányzó útvonalak dúsítása...", fetch_missing_routes),
        ("3/4: Hiányzó géptípusok dúsítása...", fetch_missing_aircraft),
        ("4/4: Hiányzó repterek dúsítása...", fetch_missing_airports),
    ]
    for label, resource in steps:
        logging.info(label)
        try:
            pipeline.run(resource())
        except Exception as e:
            _stats["errors"] += 1
            logging.error("Hiba a(z) '%s' lépésben: %s", label, e)   # a következő lépés lefut

    if _stats["errors"]:
        logging.warning("=== SZINKRONIZÁCIÓ BEFEJEZVE HIBÁKKAL: %d hiba, %d járat ===",
                        _stats["errors"], _stats["flights"])
    else:
        logging.info("=== TELJES SZINKRONIZÁCIÓ SIKERES: %d járat ===", _stats["flights"])


@router.get("/flights")
def trigger_full_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_full_sync_pipeline)
    return {
        "message": "Teljes szinkronizáció elindult: történelmi adatok + 3 cache tábla frissítése.",
        "timestamp": datetime.now().isoformat(),
    }


# ==============================================================================
# ÉLŐ POZÍCIÓK
# ==============================================================================
def get_tracked_icao_codes() -> list[str]:
    query = text("""
        SELECT DISTINCT icao24 FROM bronze.opensky_flights
        WHERE icao24 IS NOT NULL
        AND last_seen > extract(epoch from now() - make_interval(hours => :h))::bigint
        ORDER BY icao24
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"h": TRACKED_WINDOW_HOURS})
            return [row[0].strip() for row in result if row[0]]
    except Exception as e:
        logging.error("Adatbázis hiba az ICAO kódok lekérésekor: %s", e)
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
                logging.warning("OpenSky states/all HTTP %s", response.status_code)
                return []

            states = response.json().get("states", []) or []
            live_flights = []
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
            logging.error("Hiba az OpenSky API hívásakor: %s", e)
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
        dataset_name="bronze",
    )
    load_info = pipeline.run(fetch_live_positions_resource(live_data))
    return str(load_info)


@router.get("/live")
async def get_live_positions():
    icao_codes = await asyncio.to_thread(get_tracked_icao_codes)
    if not icao_codes:
        return {"flights": []}

    live_data = await fetch_live_states_batched(icao_codes, chunk_size=50, max_concurrency=5)
    if not live_data:
        return {"flights": []}

    await enrich_with_routes(live_data)
    await enrich_with_aircraft_types(live_data)

    snapshot_at = datetime.now(timezone.utc).isoformat()
    for flight in live_data:
        flight["snapshot_at"] = snapshot_at

    await asyncio.to_thread(run_dlt_live_positions, live_data)
    return {"flights": live_data}


# ==============================================================================
# DÚSÍTÁS A /live VÉGPONTHOZ (cache-first)
# ==============================================================================
def load_route_cache(callsigns: list[str]) -> dict[str, dict]:
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
        rows = conn.execute(query, {"c": callsigns, "hit": ROUTE_HIT_TTL_DAYS, "miss": ROUTE_MISS_TTL_DAYS}).mappings().all()
    return {r["callsign"]: dict(r) for r in rows}


def save_route_cache(rows: list[dict]) -> None:
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bronze.callsign_routes
            (callsign, found, origin_icao, origin_lat, origin_lon, dest_icao, dest_lat, dest_lon, fetched_at)
            VALUES (:callsign, :found, :origin_icao, :origin_lat, :origin_lon, :dest_icao, :dest_lat, :dest_lon, now())
            ON CONFLICT (callsign) DO UPDATE SET
                found = EXCLUDED.found, origin_icao = EXCLUDED.origin_icao, origin_lat = EXCLUDED.origin_lat,
                origin_lon = EXCLUDED.origin_lon, dest_icao = EXCLUDED.dest_icao, dest_lat = EXCLUDED.dest_lat,
                dest_lon = EXCLUDED.dest_lon, fetched_at = now()
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
        if response.status_code == 404:
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
        except Exception:
            row["found"] = False
        return row


async def enrich_with_routes(flights: list[dict]) -> None:
    for f in flights:
        for k in ROUTE_KEYS:
            f[k] = None
    try:
        callsigns = sorted({f["callsign"] for f in flights if f.get("callsign") and f["callsign"] != "UNKNOWN"})
        if not callsigns:
            return
        cache = await asyncio.to_thread(load_route_cache, callsigns)
        missing = [c for c in callsigns if c not in cache][:MAX_LOOKUPS_PER_RUN]
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


def load_type_cache(icao24s: list[str]) -> dict[str, dict]:
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
        rows = conn.execute(query, {"c": icao24s, "hit": TYPE_HIT_TTL_DAYS, "miss": TYPE_MISS_TTL_DAYS}).mappings().all()
    return {r["icao24"]: dict(r) for r in rows}


def save_type_cache(rows: list[dict]) -> None:
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
    async with semaphore:
        try:
            response = await client.get(AIRCRAFT_API.format(icao24=icao24))
        except Exception as e:
            logging.warning("adsbdb gép-hiba (%s): %s", icao24, e)
            return None
        await asyncio.sleep(0.2)
        row = {"icao24": icao24, "found": False, "icao_type": None, "model": None}
        if response.status_code == 404:
            return row
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
        for k in TYPE_KEYS:
            f[k] = None
    try:
        icao24s = sorted({f["icao24"].lower() for f in flights if f.get("icao24") and f["icao24"] != "N/A"})
        if not icao24s:
            return
        cache = await asyncio.to_thread(load_type_cache, icao24s)
        missing = [c for c in icao24s if c not in cache][:MAX_LOOKUPS_PER_RUN]
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
