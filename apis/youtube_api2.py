# 2026.05.28  11.00
import asyncio
import os
import airbyte as ab
from airbyte.caches import PostgresCache
from fastapi import APIRouter, BackgroundTasks

YOUTUBE_KEY = os.getenv("YOUTUBE_API_KEY")

POSTGRES_PARAMS = {
    "host":     "postgresql",
    "port":     5432,
    "database": "n8n",
    "username": "sql_admin",
    "password": "sql_pass",
}

semaphore = asyncio.Semaphore(5)
router = APIRouter()


def sync_youtube_incremental():
    try:
        db_cache = PostgresCache(
            **POSTGRES_PARAMS,
            schema_name="youtube_raw",
        )

        source = ab.get_source(
            "source-youtube-data",
            config={
                # FIX: api_key must be nested inside a "credentials" object
                # with an auth_type discriminator field
                "credentials": {
                    "auth_type": "api_key",
                    "api_key": YOUTUBE_KEY,
                },
                "channel_ids": [
                    "UC_x5XG1OV2P6uZZ5FSM9Ttw",
                    "UCcjk85TZJfmvBRpL1qJjChA",
                    "UCwqB3JaGWAXgBtf59nWVf_w",
                ],
            },
        )
        source.check()
        source.select_all_streams()
        source.read(cache=db_cache, force_full_refresh=False)
        print("Inkrementális YouTube szinkronizáció sikeres.")
    except Exception as e:
        print(f"Hiba történt: {str(e)}")


@router.post("/", tags=["YouTube"])
async def trigger_youtube_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(sync_youtube_incremental)
    return {"status": "Incremental YouTube sync started"}
