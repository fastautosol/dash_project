# 2026.11.24
import os
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from apis.kkv_silver import run_silver_transformation

INGEST_API_KEY = os.getenv("INGEST_API_KEY", "FAS")

def verify_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if x_api_key != INGEST_API_KEY:
        raise HTTPException(status_code=401, detail="Érvénytelen vagy hiányzó X-API-Key.")

router = APIRouter(dependencies=[Depends(verify_api_key)])

@router.post("/silver")
async def trigger_silver_transform(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_silver_transformation)
    return {"status": "accepted", "layer": "silver"}
