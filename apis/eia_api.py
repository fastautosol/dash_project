# 2026.10.31 9.00 EIA + ISO LMP - POST /hourly & /daily
import os
import time
import logging
from datetime import date, timedelta

import dlt
import requests
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)
router = APIRouter()

EIA_KEY = os.getenv("EIA_API_KEY", "YNAIaZXieTAUvQjk3JhfehqZW44ucqOzF9jutPhB")
BASE = "https://api.eia.gov/v2"
PAGE = 500

DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 25}
DEFAULT_RESPONDENTS = os.getenv("EIA_RESPONDENTS", "CISO,ERCO,PJM,MISO,SWPP,SOCO,DUK,TVA,FPL,FPC,TEC,NYIS,ISNE,AZPS,SRP,TEPC,NEVP,PGE,PACW,PACE,BPAT").split(",")
DEFAULT_TIMEZONE = os.getenv("EIA_TIMEZONE", "Eastern").split(",")

session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(total=5, connect=5, read=5, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])))

# ---------------------------------------------------------------- EIA ----------------------------------------------------------------
# uses_respondent: respondent facet; uses_timezone: timezone facet (csak a daily-* route-okon letezik)
ENDPOINTS = {
    "eia_petroleum_spot": dict(route="petroleum/pri/spt", pk=["period", "series"], data=["value"]),
    "eia_natgas_futures": dict(route="natural-gas/pri/fut", pk=["period", "series"], data=["value"]),
    "eia_elec_daily_region": dict(route="electricity/rto/daily-region-data", pk=["period", "respondent", "type", "timezone"], data=["value"], uses_respondent=True, uses_timezone=True),
    "eia_elec_daily_fuel_type": dict(route="electricity/rto/daily-fuel-type-data", pk=["period", "respondent", "fueltype", "timezone"], data=["value"], uses_respondent=True, uses_timezone=True),
    "eia_nuclear_outages": dict(route="nuclear-outages/us-nuclear-outages", pk=["period"], data=["capacity", "outage", "percentOutage"]),
    "eia_elec_daily_demand": dict(route="electricity/rto/daily-region-data", pk=["period", "respondent", "type", "timezone"], data=["value"], uses_respondent=True, uses_timezone=True, facets={"type": ["D"]}, frequency="daily"),
    "eia_elec_hourly_demand": dict(route="electricity/rto/region-data", pk=["period", "respondent", "type"], data=["value"], uses_respondent=True, facets={"type": ["D"]}, frequency="hourly"),
}

# melyik vegpont melyik tablait futtatja
HOURLY_EIA = ["eia_elec_hourly_demand"]
DAILY_EIA = [k for k in ENDPOINTS if k not in HOURLY_EIA]


def build_facets(cfg: dict, respondents: list[str], timezone: list[str]) -> dict:
    facets = dict(cfg.get("facets", {}))
    if cfg.get("uses_respondent"):
        facets["respondent"] = respondents
    if cfg.get("uses_timezone"):
        facets["timezone"] = timezone
    return facets


def eia_fetch(cfg: dict, facets: dict, start: str, end: str):
    offset = 0
    freq = cfg.get("frequency", "daily")
    if freq == "hourly":  # orás adatnal YYYY-MM-DDTHH formatum kell (UTC)
        start, end = f"{start}T00", f"{end}T23"
    while True:
        params = [("api_key", EIA_KEY), ("frequency", freq), ("start", start), ("end", end), ("sort[0][column]", "period"), ("sort[0][direction]", "asc"), ("offset", offset), ("length", PAGE)]
        params += [(f"data[{i}]", c) for i, c in enumerate(cfg["data"])]
        for facet, values in facets.items():
            params += [(f"facets[{facet}][]", v) for v in values]

        r = session.get(f"{BASE}/{cfg['route']}/data/", params=params, timeout=(10, 240))
        if not r.ok:
            raise Exception(f"EIA error {r.status_code} on {cfg['route']}: {r.text}")

        rows = r.json()["response"].get("data", [])

        for row in rows:
            for c in cfg["data"]:  # az EIA stringkent adja az ertekeket
                row[c] = float(row[c]) if row.get(c) not in (None, "") else None
            yield row

        if len(rows) < PAGE:
            break
        offset += PAGE
        time.sleep(1)


def make_resource(name: str, cfg: dict, facets: dict, start: str, end: str):
    @dlt.resource(name=name, write_disposition="merge", primary_key=cfg["pk"])
    def _res():
        yield from eia_fetch(cfg, facets, start, end)
    return _res


def run_eia_pipeline(pipeline_name: str, names: list[str], days_back: int, respondents: list[str] | None, timezone: list[str] | None):
    if not EIA_KEY:
        logger.error("Hianyzik az EIA_API_KEY kornyezeti valtozo")
        return

    respondents = respondents or DEFAULT_RESPONDENTS
    timezone = timezone or DEFAULT_TIMEZONE
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=days_back)).isoformat()

    # tablankent kulon futas, hogy egy hibas route ne vigye el a tobbit
    for name in names:
        cfg = ENDPOINTS[name]
        try:
            facets = build_facets(cfg, respondents, timezone)
            pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination=dlt.destinations.postgres(credentials=DB_CONFIG), dataset_name="bronze")
            info = pipeline.run(make_resource(name, cfg, facets, start, end))
            logger.info("EIA %s kesz: %s", name, info)
        except Exception as e:
            logger.exception("EIA %s hiba: %s", name, e)


# ------------------------------------------------- ISO day-ahead orás LMP (gridstatus) -------------------------------------------------
LMP_TABLE = "iso_lmp_hourly"
LMP_PK = ["interval_start_utc", "iso", "location", "market"]
LMP_MARKET = "DAY_AHEAD_HOURLY"

# cls: gridstatus osztaly; respondent: megfelelo EIA respondent (osszekapcsolashoz);
# pass_locations: a get_lmp kapja-e a helyszinlistat (CAISO-nal kotelezo), kulonben utolag szurunk
ISO_LMP = {
    "CAISO": dict(cls="CAISO", respondent="CISO", pass_locations=True, locations=["TH_NP15_GEN-APND", "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"]),
    "NYISO": dict(cls="NYISO", respondent="NYIS", pass_locations=False, locations=None),
    # kesobb: "ERCOT" (ingyenes regisztracio + kornyezeti valtozok kellenek), "PJM", "ISONE"
}
DEFAULT_ISOS = os.getenv("LMP_ISOS", "CAISO,NYISO").split(",")


def normalize_lmp(df, iso: str, cfg: dict) -> list[dict]:
    """gridstatus DataFrame -> egyseges sorok (snake_case oszlopok, ido UTC-ben)."""
    import pandas as pd

    if df is None or len(df) == 0:
        return []
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    ts_col = "interval_start" if "interval_start" in df.columns else "time"
    if ts_col not in df.columns or "location" not in df.columns:
        raise ValueError(f"{iso}: varatlan oszlopok: {list(df.columns)}")

    out = pd.DataFrame()
    out["interval_start_utc"] = pd.to_datetime(df[ts_col], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    out["iso"] = iso
    out["eia_respondent"] = cfg["respondent"]
    out["location"] = df["location"].astype(str)
    out["location_type"] = df["location_type"] if "location_type" in df.columns else None
    out["market"] = LMP_MARKET
    for col in ("lmp", "energy", "congestion", "loss"):
        out[col] = pd.to_numeric(df[col], errors="coerce") if col in df.columns else None

    out = out.drop_duplicates(subset=LMP_PK, keep="last")
    out = out.astype(object).where(out.notna(), None)
    return out.to_dict("records")


def fetch_iso_lmp(iso: str, cfg: dict, start: str, end: str, locations: list[str] | None):
    # lazy import: ha a gridstatus nem telepitheto/inkompatibilis, az EIA vegpontok ettol meg mukodnek
    import gridstatus as gs
    import pandas as pd

    client = getattr(gs, cfg["cls"])()
    kwargs = dict(date=pd.Timestamp(start), end=pd.Timestamp(end), market=gs.Markets.DAY_AHEAD_HOURLY)
    if cfg["pass_locations"]:
        kwargs["locations"] = locations
    df = client.get_lmp(**kwargs)

    if locations and not cfg["pass_locations"] and df is not None and "Location" in df.columns:
        df = df[df["Location"].isin(locations)]

    rows = normalize_lmp(df, iso, cfg)
    logger.info("LMP %s: %d sor (%s -> %s)", iso, len(rows), start, end)
    yield from rows


def make_lmp_resource(iso: str, cfg: dict, start: str, end: str, locations: list[str] | None):
    @dlt.resource(name=LMP_TABLE, write_disposition="merge", primary_key=LMP_PK)
    def _res():
        yield from fetch_iso_lmp(iso, cfg, start, end, locations)
    return _res


def run_lmp_pipeline(days_back: int, isos: list[str] | None, locations: dict[str, list[str]] | None):
    isos = isos or DEFAULT_ISOS
    locations = locations or {}
    end = date.today().isoformat()  # a holnapi DAM-ot a kovetkezo futas hozza be
    start = (date.today() - timedelta(days=days_back)).isoformat()

    # ISO-nkent kulon futas, hogy egy hibas ISO ne vigye el a tobbit
    for iso in isos:
        cfg = ISO_LMP.get(iso)
        if not cfg:
            logger.error("LMP: ismeretlen ISO: %s (elerheto: %s)", iso, list(ISO_LMP))
            continue
        try:
            locs = locations.get(iso) or cfg["locations"]
            pipeline = dlt.pipeline(pipeline_name="iso_lmp", destination=dlt.destinations.postgres(credentials=DB_CONFIG), dataset_name="bronze")
            info = pipeline.run(make_lmp_resource(iso, cfg, start, end, locs))
            logger.info("LMP %s kesz: %s", iso, info)
        except Exception as e:
            logger.exception("LMP %s hiba: %s", iso, e)


class HourlyRequest(BaseModel):
    days_back: int = 3                                  # 3 oranként futtatva ez bőven elég
    respondents: list[str] | None = None                # None = DEFAULT_RESPONDENTS
    timezone: list[str] | None = None                   # None = DEFAULT_TIMEZONE
    isos: list[str] | None = None                       # None = DEFAULT_ISOS
    locations: dict[str, list[str]] | None = None       # ISO-nkent helyszinlista


class DailyRequest(BaseModel):
    days_back: int = 14                                 # az EIA utolag revidealja az adatot
    tables: list[str] | None = None                     # None = mind a napi tablak
    respondents: list[str] | None = None
    timezone: list[str] | None = None


def run_hourly_job(days_back: int, respondents, timezone, isos, locations):
    run_eia_pipeline("eia_hourly", HOURLY_EIA, days_back, respondents, timezone)
    run_lmp_pipeline(days_back, isos, locations)


def run_daily_job(days_back: int, tables: list[str] | None, respondents, timezone):
    names = [t for t in (tables or DAILY_EIA) if t in DAILY_EIA]
    ignored = [t for t in (tables or []) if t not in DAILY_EIA]
    if ignored:
        logger.warning("/daily: ezek nem napi tablak, kihagyva (hasznald a /hourly vegpontot): %s", ignored)
    if names:
        run_eia_pipeline("eia_daily", names, days_back, respondents, timezone)


@router.post("/hourly")
async def trigger_hourly(request: HourlyRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(run_hourly_job, request.days_back, request.respondents, request.timezone, request.isos, request.locations)
    return {"status": "success", "message": "hourly EIA + ISO LMP gathering in background", "tables": HOURLY_EIA + ["iso_lmp_hourly"]}


@router.post("/daily")
async def trigger_daily(request: DailyRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(run_daily_job, request.days_back, request.tables, request.respondents, request.timezone)
    return {"status": "success", "message": "daily EIA gathering in background", "tables": DAILY_EIA}
