# 2026.10.30 14.00 EIA
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
DEFAULT_RESPONDENTS = ("CISO,ERCO,PJM,MISO,SWPP,SOCO,DUK,TVA,FPL,FPC,TEC,NYIS,ISNE,AZPS,SRP,TEPC,NEVP,PGE,PACW,PACE,BPAT").split(",")
DEFAULT_TIMEZONE = os.getenv("EIA_TIMEZONE", "Eastern").split(",")

session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(total=5, connect=5, read=5, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])))

ENDPOINTS = {
    "eia_petroleum_spot": dict(route="petroleum/pri/spt", pk=["period", "series"], data=["value"]),
    "eia_natgas_futures": dict(route="natural-gas/pri/fut", pk=["period", "series"], data=["value"]),
    "eia_elec_daily_region": dict(route="electricity/rto/daily-region-data", pk=["period", "respondent", "type", "timezone"], data=["value"], uses_respondent=True),
    "eia_elec_daily_fuel_type": dict(route="electricity/rto/daily-fuel-type-data", pk=["period", "respondent", "fueltype", "timezone"], data=["value"], uses_respondent=True),
    "eia_nuclear_outages": dict(route="nuclear-outages/us-nuclear-outages", pk=["period"], data=["capacity", "outage", "percentOutage"]),
    "eia_elec_daily_demand": dict(route="electricity/rto/daily-region-data", pk=["period", "respondent", "timezone"], data=["value"], uses_respondent=True, frequency="daily"),    
    "eia_elec_hourly_demand": dict(route="electricity/rto/region-data",  pk=["period", "respondent", "timezone"], data=["value"], uses_respondent=True, frequency="hourly"),    
    "eia_elec_wholesale_prices_daily": dict(route="electricity/wholesale/data", pk=["period", "price-node", "market"], data=["price"], frequency="daily"),
    "eia_elec_wholesale_prices_hourly": dict(route="electricity/wholesale/data", pk=["period", "price-node", "market"], data=["price"], frequency="hourly")
}


def build_facets(cfg: dict, respondents: list[str], timezone: list[str]) -> dict:
    facets = dict(cfg.get("facets", {}))
    if cfg.get("uses_respondent"):
        facets["respondent"] = respondents
        facets["timezone"] = timezone
    return facets


def eia_fetch(cfg: dict, facets: dict, start: str, end: str):
    offset = 0
    freq = cfg.get("frequency", "daily")  # Dinamikus frekvencia kezelés
    while True:
        params = [("api_key", EIA_KEY), ("frequency", freq), ("start", start), ("end", end), ("sort[0][column]", "period"), ("sort[0][direction]", "asc"), ("offset", offset), ("length", PAGE)]
        params += [(f"data[{i}]", c) for i, c in enumerate(cfg["data"])]
        for facet, values in facets.items():
            params += [(f"facets[{facet}][]", v) for v in values]

        r = session.get(f"{BASE}/{cfg['route']}/data/", params=params, timeout=(10, 240))
        if not r.ok:
            raise Exception(f"EIA error {r.status_code} on {cfg['route']}: {r.text}")

        body = r.json()["response"]
        rows = body.get("data", [])

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


class EiaRequest(BaseModel):
    days_back: int = 14                    # az EIA utolag revidealja az adatot, ezert mindig visszatoltunk
    tables: list[str] | None = None        # None = mind
    respondents: list[str] | None = None   # None = DEFAULT_RESPONDENTS
    timezone: list[str] | None = None      # None = DEFAULT_TIMEZONE


def run_eia_pipeline(days_back: int, tables: list[str] | None, respondents: list[str] | None, timezone: list[str] | None):
    if not EIA_KEY:
        logger.error("Hianyzik az EIA_API_KEY kornyezeti valtozo")
        return

    respondents = respondents or DEFAULT_RESPONDENTS
    timezone = timezone or DEFAULT_TIMEZONE
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=days_back)).isoformat()
    selected = {k: v for k, v in ENDPOINTS.items() if not tables or k in tables}

    # route-onkent kulon futas, hogy egy hibas route ne vigye el a tobbit
    for name, cfg in selected.items():
        try:
            facets = build_facets(cfg, respondents, timezone)
            pipeline = dlt.pipeline(pipeline_name="eia_daily", destination=dlt.destinations.postgres(credentials=DB_CONFIG), dataset_name="bronze")
            info = pipeline.run(make_resource(name, cfg, facets, start, end))
            logger.info("EIA %s kesz: %s", name, info)
        except Exception as e:
            logger.exception("EIA %s hiba: %s", name, e)


@router.post("/")
async def trigger_eia_fetch(request: EiaRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(run_eia_pipeline, request.days_back, request.tables, request.respondents, request.timezone)
    return {"status": "success", "message": "EIA daily data gathering in background"}
