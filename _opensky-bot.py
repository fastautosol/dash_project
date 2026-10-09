# 2026.10.09 - OpenSky position collector: UAE* aircraft -> bronze.a380_positions
# Separate long-running process (like candles-bot.py). Run:  python opensky_tracker.py
import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import dlt
import requests
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger("opensky_tracker")

# =========================
# CONFIGURATION
# =========================
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID", "fastautosol@gmail.com-api-client")
CLIENT_SECRET = os.environ["OPENSKY_CLIENT_SECRET"]  # set in Coolify env
AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API = "https://opensky-network.org/api"

PG_URL = os.getenv("DB_URL", "postgresql://sql_admin:sql_pass@postgresql:5432/n8n")   # dlt (psycopg2)
SA_URL = PG_URL.replace("postgresql://", "postgresql+psycopg://", 1)                  # SQLAlchemy (psycopg 3)
engine = create_engine(SA_URL, pool_size=2, max_overflow=2, pool_pre_ping=True)

POLL_SECONDS = 120          # seconds between OpenSky position fetches
ICAO_REFRESH_SECONDS = 600  # how often the tracked aircraft list is reloaded
CHUNK_SIZE = 100            # icao24 values per /states/all request
CLEANUP_HOURS = 60          # hours of positions to retain
TABLE = "uae_positions"    # name kept for the Dash page (opensky_charts.py)


# =========================
# AUTH (cached token)
# =========================
_token = {"value": None, "exp": 0.0}

def get_cached_token() -> str:
    if _token["value"] is None or time.time() > _token["exp"] - 60:
        payload = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
        r = requests.post(AUTH_URL, data=payload, timeout=15)
        r.raise_for_status()
        j = r.json()
        _token["value"] = j["access_token"]
        _token["exp"] = time.time() + j.get("expires_in", 1500)
    return _token["value"]


# =========================
# TRACKED AIRCRAFT (UAE* callsigns from bronze.uae_flights)
# =========================
def load_tracked_icao() -> list[str]:
    query = text("""SELECT DISTINCT icao24 FROM bronze.uae_flights WHERE icao24 IS NOT NULL ORDER BY icao24""")
    with engine.connect() as conn:
        return [row[0].strip() for row in conn.execute(query) if row[0]]


# =========================
# FETCH + PARSE
# =========================
def fetch_states(icao_chunk: list[str]) -> tuple[list[dict], float | None]:
    """Blocking. Returns (records, retry_after_seconds_or_None)."""
    headers = {"Authorization": f"Bearer {get_cached_token()}"}
    params = [("icao24", c) for c in icao_chunk]
    r = requests.get(f"{API}/states/all", params=params, headers=headers, timeout=20)

    remaining = r.headers.get("X-Rate-Limit-Remaining")
    if remaining is not None:
        log.info("[API] rate limit remaining: %s", remaining)

    if r.status_code == 401:
        _token["exp"] = 0.0  # force a new token next time
        log.warning("[API] 401 - token reset")
        return [], None
    if r.status_code == 429:
        retry = float(r.headers.get("X-Rate-Limit-Retry-After-Seconds", 300))
        log.warning("[API] 429 - waiting %ss", retry)
        return [], retry
    if r.status_code != 200:
        log.warning("[API] HTTP %s", r.status_code)
        return [], None

    records = []
    for s in r.json().get("states") or []:
        if s[5] is None or s[6] is None:       # no coordinates
            continue
        ts = s[3] or s[4]                      # time_position, else last_contact
        if not ts:
            continue
        records.append({
            "icao24": s[0].strip() if s[0] else None,
            "snapshot_time": datetime.fromtimestamp(ts, tz=timezone.utc),
            "callsign": s[1].strip() if s[1] else None,
            "origin_country": s[2],
            "longitude": float(s[5]),
            "latitude": float(s[6]),
            "altitude_m": float(s[7]) if s[7] is not None else None,
            "on_ground": bool(s[8]),
            "velocity_mps": float(s[9]) if s[9] is not None else None,
            "heading_deg": float(s[10]) if s[10] is not None else None,
            "vertical_rate_mps": float(s[11]) if s[11] is not None else None,
        })
    return records, None


# =========================
# MAIN LOOP
# =========================
async def tracker_loop(pipeline) -> None:
    icao_list: list[str] = []
    last_icao_load = 0.0
    last_cleanup = 0.0

    while True:
        started = time.time()
        wait = POLL_SECONDS

        # 1. Refresh tracked aircraft list
        if not icao_list or started - last_icao_load > ICAO_REFRESH_SECONDS:
            try:
                icao_list = await asyncio.to_thread(load_tracked_icao)
                last_icao_load = started
                log.info("[ICAO] tracking %d aircraft", len(icao_list))
            except Exception as e:
                log.error("[ICAO] load failed: %s", e)

        # 2. Fetch positions in chunks
        records: list[dict] = []
        for i in range(0, len(icao_list), CHUNK_SIZE):
            chunk = icao_list[i:i + CHUNK_SIZE]
            try:
                recs, retry_after = await asyncio.to_thread(fetch_states, chunk)
                records.extend(recs)
                if retry_after:
                    wait = max(wait, retry_after)
                    break
            except Exception as e:
                log.error("[API] fetch failed: %s", e)

        # 3. Upsert
        if records:
            try:
                await asyncio.to_thread(
                    pipeline.run, records, table_name=TABLE,
                    write_disposition="merge", primary_key=["icao24", "snapshot_time"],
                )
                log.info("[DB] upserted %d positions", len(records))
            except Exception as e:
                log.error("[DB] upsert failed: %r", getattr(e, "__cause__", None) or e)

        # 4. Hourly cleanup
        if started - last_cleanup >= 3600:
            threshold = datetime.now(timezone.utc) - timedelta(hours=CLEANUP_HOURS)
            try:
                def _cleanup():
                    with pipeline.sql_client() as client:
                        tname = client.make_qualified_table_name(TABLE)
                        client.execute_sql(f"DELETE FROM {tname} WHERE snapshot_time < %s", (threshold,))
                await asyncio.to_thread(_cleanup)
                log.info("[CLEANUP] removed positions older than %dh", CLEANUP_HOURS)
            except Exception as e:  # table may not exist before the first insert
                log.warning("[CLEANUP] skipped: %s", e)
            last_cleanup = started

        await asyncio.sleep(max(5.0, wait - (time.time() - started)))


async def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="opensky_positions_tracker",
        destination=dlt.destinations.postgres(credentials=PG_URL),
        dataset_name="bronze",
    )
    await tracker_loop(pipeline)


if __name__ == "__main__":
    asyncio.run(main())
