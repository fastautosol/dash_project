# 2026.05.19  15:00
from fastapi import FastAPI, APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List
import aiohttp
import asyncio
import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from datetime import datetime
import os

# --- CONFIG ---
META_ACCESS_TOKEN = os.getenv("META_API_KEY")
BASE_URL = "https://graph.facebook.com/v18.0"
semaphore = asyncio.Semaphore(3)
router = APIRouter()

DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}

# --- PYDANTIC MODEL ---
class MetaLeadRequest(BaseModel):
    page_ids: List[str] = Field(..., min_length=1, description="List of Facebook Page IDs to scan")
    max_leads: int = Field(20, ge=1, le=100)

# --- GENERIC META REQUEST ---
async def meta_get(session, endpoint, params):
    params["access_token"] = META_ACCESS_TOKEN
    async with semaphore:
        async with session.get(f"{BASE_URL}/{endpoint}", params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise Exception(f"Meta API error {resp.status}: {text}")
            return await resp.json()

@dlt.resource(name="facebook_leads", max_table_nesting=0)
def meta_resource(rows: list[dict]):
    for r in rows:
        yield r

@router.post("/")
async def get_meta_leads_api(req: MetaLeadRequest):
    data = await fetch_meta_multipage(req.page_ids, req.max_leads)
    if not data:
        return {"status": "no_data"}

    for row in data:
        row["_ingested_at"] = datetime.utcnow().isoformat()

    pipeline = dlt.pipeline(
        pipeline_name="facebook_ingest",
        destination=dlt.destinations.postgres(credentials=DB_CONFIG),
        dataset_name="bronze")

    try:
        # Merge ensures if a lead updates, it overwrites old records instead of duplicating
        load_info = pipeline.run(meta_resource(data), write_disposition="merge", primary_key="lead_id")

    except PipelineStepFailed as e:
        if e.step == "load" or e.step == "normalize" or "does not exist" in str(e).lower():
            pipeline.drop_pending_packages()
            load_info = pipeline.run(meta_resource(data), write_disposition="append")
        else:
            raise
    except Exception as e:
        print(f"Unexpected pipeline error: {e}")
        raise

    return {"rows_loaded": len(data),  "status": "loaded",  "load_info": str(load_info),  "sample": data[:5]}


# --------- MULTI FETCH (CONCURRENT PAGES) ---------
async def fetch_meta_multipage(page_ids, max_leads):
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [fetch_single_page_leads(session, pid, max_leads) for pid in page_ids]
        results = await asyncio.gather(*tasks)
        return [item for sublist in results for item in sublist]

      
# --------- SINGLE PAGE PROCESSING ---------
async def fetch_single_page_leads(session, page_id, max_leads):
    try:
        # 1. Grab lead IDs registered to the page
        lead_logs = await meta_get(session, f"{page_id}/leadgen_forms", {"fields": "leads{id,created_time}", "limit": 1})
        
        forms = lead_logs.get("data", [])
        if not forms:
            return []

        results = []
        # Loop through active forms on the page
        for form in forms:
            leads_data = form.get("leads", {}).get("data", [])[:max_leads]
            
            for raw_lead in leads_data:
                lead_id = raw_lead["id"]
                
                # 2. Extract full profile detail parameters from individual Lead ID
                detail = await meta_get(session, lead_id, {"fields": "created_time,id,field_data,form_id"})

                # --- STRATEGIC ASYNC SLEEP ---
                await asyncio.sleep(0.5)
                
                # Normalize key/value structure into a flattened dictionary
                field_map = {}
                for field in detail.get("field_data", []):
                    name = field["name"]
                    values = field.get("values", [None])[0]
                    field_map[name] = values

                results.append({
                    "error": None,
                    "page_id": page_id,
                    "lead_id": lead_id,
                    "form_id": detail.get("form_id"),
                    "created_at": detail.get("created_time"),
                    "email": field_map.get("email"),
                    "full_name": field_map.get("full_name") or field_map.get("first_name"),
                    "phone_number": field_map.get("phone_number"),
                    "raw_fields": field_map  # Captures custom questionnaire items dynamically
                })

        return results

    except Exception as e:
        return [{"page_id": page_id, "lead_id": f"ERROR_{page_id}", "error": str(e), "email": None, "full_name": None}]
