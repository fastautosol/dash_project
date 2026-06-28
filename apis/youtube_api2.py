# 2026.05.14  18.00
from fastapi import FastAPI, APIRouter, Query
from pydantic import BaseModel, Field
from typing import List
import aiohttp
import asyncio
import isodate
import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from datetime import datetime
import json
import re
import os

# --- CONFIG ---
YOUTUBE_KEY = os.getenv("YOUTUBE_API_KEY")
BASE_URL = "https://www.googleapis.com/youtube/v3"
semaphore = asyncio.Semaphore(5)
router = APIRouter()

DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}

import airbyte as ab
from fastapi import FastAPI, BackgroundTasks

app = FastAPI()

POSTGRES_CONN_STR = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"

def sync_youtube_incremental():
    try:
        # 1. Kapcsolódás a Postgres-hez. 
        # A PyAirbyte ebben a sémában egy külön '_airbyte_state' táblában fogja 
        # megjegyezni, hogy melyik videónál és kommentnél járt legutóbb!
        db_cache = ab.caches.get_postgres_cache(
            connection_string=POSTGRES_CONN_STR,
            schema_name="youtube_raw"
        )

        source = ab.get_source(
            "source-youtube-data",
            config={
                "api_key": "YOUTUBE_KEY",
                "channel_ids": ["UC_x5XG1OV2P6uZZ5FSM9Ttw","UCcjk85TZJfmvBRpL1qJjChA","UCwqB3JaGWAXgBtf59nWVf_w"]
            }
        )

        source.check()
        source.select_all_streams()

        # 2. AZ INKREMENTÁLIS TRÜKK:
        # A force_full_refresh=False (ez az alapértelmezett) arra kényszeríti a PyAirbyte-ot,
        # hogy olvassa ki a Postgres-ből a legutóbbi állapotot (State-et).
        # Így a Google API-tól CSAK az új videókat és az új kommenteket fogja elkérni!
        source.read(cache=db_cache, force_full_refresh=False)
        print("Inkrementális YouTube szinkronizáció sikeres.")

    except Exception as e:
        print(f"Hiba történt: {str(e)}")

#@router.post("/sync/youtube/incremental", tags=["YouTube"])
@router.post("/")
async def trigger_youtube_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(sync_youtube_incremental)
    return {"status": "Incremental YouTube sync started"}

