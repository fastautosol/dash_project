# 2026.10.08  - Emirates A380 tracker: dlt pipeline + FastAPI router
#   python opensky_api.py flights              # flotta/járat felderítés (naponta elég)
#   python opensky_api.py positions            # egyszeri pozíció-snapshot
#   python opensky_api.py positions --loop 120 # folyamatos gyűjtés 120 mp-enként
#
# API (prefix az app.py-ban: /api/opensky, záró perjel NÉLKÜL hívd):
#   GET /api/opensky/flights?hours=48&callsign=UAE1&limit=200
#   GET /api/opensky/live-positions
#   GET /api/opensky/paths?hours=24

import argparse
import time
from datetime import datetime, timezone

import dlt
import requests
from fastapi import APIRouter
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# ----- KONFIGURÁCIÓ (teszt alatt hardcode; élesben környezeti változóba, a secretet pedig cseréld le) -----
API = "https://opensky-network.org/api"
AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CLIENT_ID = "fastautosol@gmail.com-api-client"
CLIENT_SECRET = "1Fk2Xga7e85duhpQYbjNAseMt2Qn5gcF"

DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
engine = create_engine(DB_URL, pool_pre_ping=True)

AIRPORTS = ["OMDB", "EDDF"]      # OMDB: Dubai, EDDF: Frankfurt (az OMAA Etihad bázis, Emirates A380 ott nem jellemző)
CALLSIGN_PREFIX = "UAE"          # Emirates
AIRCRAFT_TYPECODE = "A388"       # ICAO típuskód: Airbus A380-800
DATASET = "bronze"
FLIGHT_LOOKBACK_H = 36           # az airport endpoint max. 2 napos ablakot enged
FLEET_MAX_AGE_DAYS = 14          # ennyi napnál régebben nem látott gépet kihagyunk a lekérdezésből
POSITIONS_RETENTION_DAYS = 7

router = APIRouter()


# ----- OPENSKY HTTP -----
_token = {"value": None, "exp": 0.0}


def _auth_headers() -> dict:
    """OAuth2 Bearer token cache-elve (30 percig érvényes). Kulcs nélkül anonim hívás."""
    if not CLIENT_ID:
        return {}
    if time.time() > _token["exp"] - 60:
        r = requests.post(
            AUTH_URL,
            data={"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
            timeout=15,
        )
        r.raise_for_status()
        j = r.json()
        _token["value"] = j["access_token"]
        _token["exp"] = time.time() + j.get("expires_in", 1800)
    return {"Authorization": f"Bearer {_token['value']}"}


def _get(url: str, params=None, retries: int = 3) -> requests.Response:
    r = None
    for _ in range(retries):
        r = requests.get(url, params=params, headers=_auth_headers(), timeout=20)
        if r.status_code == 429:
            wait = int(r.headers.get("X-Rate-Limit-Retry-After-Seconds", 30))
            if wait > 120:  # napi kredit elfogyott, nincs értelme várni
                print(f"OpenSky rate limit, a következő próbálkozás {wait} mp múlva lehetséges")
                return r
            time.sleep(wait)
            continue
        return r
    return r


_type_cache: dict[str, str | None] = {}


def is_a380(icao24: str) -> bool:
    """Az OpenSky aircraft metadata alapján megmondja, hogy A380-ról van-e szó.
    Csak a sikeres (200) és a 'nem létezik' (404) válasz kerül a cache-be,
    átmeneti hibánál (429, 5xx) a következő futás újra megpróbálja."""
    if icao24 not in _type_cache:
        r = _get(f"{API}/metadata/aircraft/icao/{icao24}")
        if r.status_code == 200:
            _type_cache[icao24] = r.json().get("typecode")
        elif r.status_code == 404:
            _type_cache[icao24] = None
        else:
            return False
    return _type_cache[icao24] == AIRCRAFT_TYPECODE


def _ts(epoch: int | None):
    return datetime.fromtimestamp(epoch, tz=timezone.utc) if epoch else None


# ----- 1. DLT RESOURCE: Emirates A380 járatok (flotta felderítés) -----
@dlt.resource(name="a380_flights", write_disposition="merge", primary_key=("icao24", "first_seen"))
def a380_flights():
    """Indulások ÉS érkezések a figyelt repterekről; csak az A380-as UAE járatok maradnak.
    Fontos: az OpenSky /flights végpontok éjszakai batch-ből frissülnek, a mai járatok
    általában csak másnap jelennek meg. Élő követésre a /states/all szolgál (lásd lent)."""
    end = int(time.time())
    begin = end - FLIGHT_LOOKBACK_H * 3600

    for airport in AIRPORTS:
        for direction in ("departure", "arrival"):
            r = _get(f"{API}/flights/{direction}", params={"airport": airport, "begin": begin, "end": end})
            if r.status_code == 404:  # nincs járat az ablakban
                continue
            if r.status_code != 200:
                print(f"Hiba {airport}/{direction}: HTTP {r.status_code}")
                continue

            for f in r.json():
                callsign = (f.get("callsign") or "").strip()
                icao24 = (f.get("icao24") or "").lower()
                if not (icao24 and callsign.startswith(CALLSIGN_PREFIX)):
                    continue
                if not is_a380(icao24):
                    continue
                yield {
                    "icao24": icao24,
                    "callsign": callsign,
                    "est_departure_airport": f.get("estDepartureAirport"),
                    "est_arrival_airport": f.get("estArrivalAirport"),
                    "first_seen": f.get("firstSeen"),
                    "last_seen": f.get("lastSeen"),
                    "first_seen_at": _ts(f.get("firstSeen")),
                    "last_seen_at": _ts(f.get("lastSeen")),
                }


# ----- DB OLVASÁS (közös helper: hiányzó táblánál csendben üres lista) -----
def _read(sql, params: dict | None = None) -> list[dict]:
    try:
        with engine.connect() as conn:
            return [dict(r) for r in conn.execute(sql, params or {}).mappings().all()]
    except ProgrammingError as e:
        if "does not exist" in str(e):  # a dlt még nem hozta létre a táblát (nem futott a pipeline)
            return []
        print(f"Adatbázis hiba: {e}")
        return []
    except Exception as e:
        print(f"Adatbázis hiba: {e}")
        return []


def _jsonable(rows: list[dict]) -> list[dict]:
    """datetime -> ISO string, hogy a válasz JSON-szerializálható legyen."""
    return [{k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items()} for r in rows]


def fleet_icao24() -> list[str]:
    """A dlt által korábban összegyűjtött egyedi A380 icao24 kódok."""
    rows = _read(
        text(
            f"SELECT DISTINCT icao24 FROM {DATASET}.a380_flights "
            f"WHERE first_seen_at > now() - make_interval(days => :d)"
        ),
        {"d": FLEET_MAX_AGE_DAYS},
    )
    return [r["icao24"].lower() for r in rows if r["icao24"]]


# ----- 2. DLT RESOURCE: élő pozíciók (append -> ebből rajzolódik az útvonal) -----
@dlt.resource(name="a380_positions", write_disposition="append")
def a380_positions():
    icaos = fleet_icao24()
    if not icaos:
        print("Üres a flotta lista – előbb futtasd: python opensky_api.py flights")
        return

    # Egyetlen hívás az egész flottára (kevesebb API kredit, mint több kisebb hívás)
    params = [("icao24", i) for i in icaos[:300]]
    r = _get(f"{API}/states/all", params=params)
    if r.status_code != 200:
        print(f"Hiba az OpenSky states hívásakor: HTTP {r.status_code}")
        return

    data = r.json()
    snapshot = _ts(data.get("time")) or datetime.now(timezone.utc)
    for s in data.get("states") or []:
        if s[5] is None or s[6] is None:  # nincs koordináta
            continue
        yield {
            "icao24": s[0],
            "callsign": (s[1] or "").strip() or None,
            "origin_country": s[2],
            "snapshot_time": snapshot,
            "longitude": s[5],
            "latitude": s[6],
            "altitude_m": s[7] if s[7] is not None else s[13],  # barometrikus, ennek hiányában geometrikus
            "on_ground": s[8],
            "velocity_mps": s[9],
            "heading_deg": s[10],
            "vertical_rate_mps": s[11],
        }


# ----- FUTTATÁS -----
def _pipeline():
    return dlt.pipeline(
        pipeline_name="opensky_a380",
        destination="postgres",
        dataset_name=DATASET,
    )


def prune_positions():
    try:
        with engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {DATASET}.a380_positions WHERE snapshot_time < now() - make_interval(days => :d)"),
                {"d": POSITIONS_RETENTION_DAYS},
            )
    except ProgrammingError:
        pass  # a tábla még nem létezik
    except Exception as e:
        print(f"Takarítási hiba: {e}")


# ----- 3. FASTAPI ENDPOINTOK (csak DB-ből olvasnak, nem hívják az OpenSky-t) -----
FLIGHTS_SQL = """
    SELECT icao24, callsign, est_departure_airport, est_arrival_airport,
           first_seen_at, last_seen_at
    FROM {schema}.a380_flights
    WHERE first_seen_at > now() - make_interval(hours => :h)
    {callsign_filter}
    ORDER BY first_seen_at DESC
    LIMIT :n
"""

LATEST_SQL = text(
    f"""
    SELECT DISTINCT ON (icao24)
           icao24, callsign, longitude, latitude, altitude_m, on_ground,
           velocity_mps, heading_deg, vertical_rate_mps, snapshot_time
    FROM {DATASET}.a380_positions
    WHERE snapshot_time > now() - interval '5 minutes'
    ORDER BY icao24, snapshot_time DESC
    """
)

PATHS_SQL = text(
    f"""
    SELECT icao24, callsign, latitude, longitude, snapshot_time
    FROM {DATASET}.a380_positions
    WHERE snapshot_time > now() - make_interval(hours => :h)
    ORDER BY icao24, snapshot_time
    """
)


@router.get("/flights")
def get_flights(hours: int = 48, callsign: str | None = None, limit: int = 200):
    """A380-as Emirates járatok (indulás/érkezés) az elmúlt N órából.
    Példa: /api/opensky/flights?hours=72&callsign=UAE1"""
    hours = max(1, min(hours, 24 * FLEET_MAX_AGE_DAYS))
    limit = max(1, min(limit, 1000))
    params = {"h": hours, "n": limit}
    callsign_filter = ""
    if callsign:
        callsign_filter = "AND callsign = :cs"
        params["cs"] = callsign.strip().upper()
    sql = text(FLIGHTS_SQL.format(schema=DATASET, callsign_filter=callsign_filter))
    return {"flights": _jsonable(_read(sql, params))}


@router.get("/live-positions")
def get_live_positions():
    """A legutóbbi (max. 5 perces) pozíció gépenként."""
    return {"flights": _jsonable(_read(LATEST_SQL))}


@router.get("/paths")
def get_paths(hours: int = 24):
    """Útvonal-pontok az elmúlt N órából."""
    hours = max(1, min(hours, 24 * POSITIONS_RETENTION_DAYS))
    return {"points": _jsonable(_read(PATHS_SQL, {"h": hours}))}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["flights", "positions"])
    ap.add_argument("--loop", type=int, default=0, help="másodperc; 0 = egyszeri futás")
    args = ap.parse_args()

    pipeline = _pipeline()
    if args.mode == "flights":
        print(pipeline.run(a380_flights()))
    else:
        while True:
            try:
                print(pipeline.run(a380_positions()))
                prune_positions()
            except Exception as e:  # egy hiba ne állítsa le a ciklust
                print(f"Futási hiba: {e}")
            if not args.loop:
                break
            time.sleep(args.loop)
