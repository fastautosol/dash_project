import os
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query
from apis.finance_silver import run_finance_silver_transformation

INGEST_API_KEY = os.getenv("INGEST_API_KEY", "FAS")


def verify_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if x_api_key != INGEST_API_KEY:
        raise HTTPException(status_code=401, detail="Érvénytelen vagy hiányzó X-API-Key.")


router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.post("/silver")
async def trigger_finance_silver(
    background_tasks: BackgroundTasks,
    sync: bool = Query(default=False, description="Ha true, megvárja a futást és visszaadja a sorok számát."),
):
    if sync:
        stats = run_finance_silver_transformation()
        return {"status": "completed", "layer": "silver", "stats": stats}

    background_tasks.add_task(run_finance_silver_transformation)
    return {"status": "accepted", "layer": "silver"}
