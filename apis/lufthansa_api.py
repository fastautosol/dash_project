# 2026.05.12  9.00
import os
import httpx
import asyncio
import pandas as pd
import requests
from fastapi.responses import StreamingResponse, FileResponse
import io
from fastapi import APIRouter
import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from datetime import datetime

router = APIRouter()

# ----- CONFIGURATION -----
DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}

def get_lufthansa_token():
    CLIENT_ID = os.getenv("LH_CLIENT_ID")
    CLIENT_SECRET = os.getenv("LH_CLIENT_SECRET")
    token_url = "https://api.lufthansa.com/v1/oauth/token"
    payload = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
    resp = requests.post(token_url, data=payload)
    resp.raise_for_status()
    return resp.json()["access_token"]

async def fetch_route(client, token, origin, dest, flight_date, sem):
    url = f"https://api.lufthansa.com/v1/operations/customerflightinformation/route/{origin}/{dest}/{flight_date}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    async with sem:
        try:
            resp = await client.get(url, headers=headers, timeout=30)
            await asyncio.sleep(0.5)

            if resp.status_code != 200:
                return []

            json_data = resp.json()
            flights = json_data.get("FlightInformation", {}).get("Flights", {}).get("Flight", [])

            if not flights:
                return []

            # ensure list
            if isinstance(flights, dict):
                flights = [flights]

            for f in flights:
                f["route_key"] = f"{origin}-{dest}"

            return flights

        except Exception:
            return []

@dlt.resource(name="lh_flights") #max_table_nesting=0
def flights_resource(rows: list[dict]):
    for r in rows:
        yield r

@router.get("/flights/{flight_date}")
async def get_flightroute_details(flight_date: str):

    token = get_lufthansa_token()

    ROUTES_FULL = [
    # FRA Routes - Long Haul
    ("FRA", "SIN"), ("FRA", "HND"), ("FRA", "LAX"), ("FRA", "JFK"), ("FRA", "EWR"), ("FRA", "ORD"), ("FRA", "IAD"), ("FRA", "BOS"), ("FRA", "DEN"), ("FRA", "SFO"),
    ("FRA", "MIA"), ("FRA", "YYZ"), ("FRA", "MEX"), ("FRA", "DEL"), ("FRA", "BOM"), ("FRA", "BLR"), ("FRA", "HYD"), ("FRA", "ICN"), ("FRA", "GRU"), ("FRA", "DXB"),
    ("FRA", "CAI"), ("FRA", "TLV"), ("FRA", "BEY"), 
    # FRA Routes - European
    ("FRA", "LHR"), ("FRA", "LCY"), ("FRA", "CDG"), ("FRA", "AMS"), ("FRA", "MAD"), ("FRA", "BCN"), ("FRA", "LIS"), ("FRA", "ATH"), ("FRA", "IST"), ("FRA", "BER"),
    ("FRA", "HAM"), ("FRA", "DUS"), ("FRA", "MUC"), ("FRA", "VIE"), ("FRA", "ZRH"), ("FRA", "CPH"), ("FRA", "OSL"), ("FRA", "HEL"), ("FRA", "WAW"), ("FRA", "PRG"),
    ("FRA", "BUD"), ("FRA", "MXP"), ("FRA", "TLS"), ("FRA", "MAN"), ("FRA", "DUB"),
    # MUC Routes - Long Haul
    ("MUC", "LAX"), ("MUC", "SFO"), ("MUC", "DEN"), ("MUC", "ORD"), ("MUC", "EWR"), ("MUC", "JFK"), ("MUC", "BOS"), ("MUC", "DEL"), ("MUC", "BOM"), ("MUC", "BLR"),
    ("MUC", "BKK"), ("MUC", "JNB"), ("MUC", "CPT"), ("MUC", "DXB"),
    # MUC Routes - European
    ("MUC", "LHR"), ("MUC", "CDG"), ("MUC", "AMS"), ("MUC", "MAD"), ("MUC", "BCN"), ("MUC", "LIS"), ("MUC", "ATH"), ("MUC", "BER"), ("MUC", "HAM"), ("MUC", "DUS"),
    ("MUC", "FRA"), ("MUC", "VIE"), ("MUC", "ZRH"), ("MUC", "CPH"), ("MUC", "OSL"), ("MUC", "WAW"), ("MUC", "PRG"), ("MUC", "BUD"), ("MUC", "FCO"), ("MUC", "MXP"),
    ("MUC", "MAN"), ("MUC", "DUB"), ("MUC", "TLV"),
    ] 

    sem = asyncio.Semaphore(4)

    async with httpx.AsyncClient(timeout=90) as client:
        tasks = [fetch_route(client, token, o, d, flight_date, sem) for o, d in ROUTES_FULL]
        results = await asyncio.gather(*tasks)

    data = []
    for r in results:
        if r:
            data.extend(r)

    if not data:
        return {"status": "no_data"}

    for row in data:
        row["_ingested_at"] = datetime.utcnow().isoformat()
    # --------------------------------------------------------------------------------------------------

    ALLOWED_FIELDS = {"Status", "Equipment", "Departure", "Arrival", "AircraftDetails", "route_key",  "_ingested_at"}
    clean_data = []    
    for row in data:
        filtered = {k: v for k, v in row.items() if k in ALLOWED_FIELDS}
        clean_data.append(filtered)
    
    # --------------------------------------------------------------------------------------------------
    pipeline = dlt.pipeline(pipeline_name="lufthansa_ingest", 
                            destination=dlt.destinations.postgres(credentials=DB_CONFIG), 
                            dataset_name="bronze")
    try:
        load_info = pipeline.run(flights_resource(clean_data), write_disposition="merge", 
                                 primary_key=["route_key", "departure__scheduled__date", "departure__scheduled__time"])

    except PipelineStepFailed as e:    
        if e.step == "load" or "does not exist" in str(e).lower():
            pipeline.drop_pending_packages()
            load_info = pipeline.run(flights_resource(clean_data), write_disposition="append")
        else:
            raise

    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

    return {"rows": len(data), "status": "loaded", "load_info": str(load_info), "sample": data[:3]}
