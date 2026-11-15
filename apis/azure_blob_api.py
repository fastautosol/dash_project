# 2026.11.15  12.00
import io
import json
import logging
import os
import threading
import dlt
import pandas as pd
from azure.storage.blob import BlobServiceClient
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()
pipeline_lock = threading.Lock()

AZURE_BLOB_KEY = "NnrfB+Oq71lv1fad3Az37bmZGLUt36VlEEbdUC9t1Gwy1fs/4AQuZaWpesteQtiwuIm4tKML2QP7+ASt4BHAzg=="
AZURE_CONNECTION_STRING = f"DefaultEndpointsProtocol=https;AccountName=azurestoragekcs1;AccountKey={AZURE_BLOB_KEY};EndpointSuffix=core.windows.net"
DEFAULT_CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "external-catalog")

DB_CONFIG = {
    "host": "postgresql",
    "port": 5432,
    "database": "n8n",
    "username": "sql_admin",
    "password": "sql_pass",
    "connect_timeout": 15}

SUPPORTED_EXTENSIONS = (".csv", ".xls", ".xlsx", ".json")
blob_service_client = (BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING) if AZURE_CONNECTION_STRING else None)

class AzureBlobRequest(BaseModel):
    blob_name: str = "Online_Retail.xlsx"
    table_name: str = "online_retail"
    source_name: str = "azure_external_catalog"
    write_disposition: str = "replace"       # "replace", "append" vagy "merge"
    primary_key: list[str] | str | None = None
    container_name: str = DEFAULT_CONTAINER
    dtype_overrides: dict[str, str] | None = {"InvoiceNo": "str", "StockCode": "str"}


def _read_stream_to_df(blob_name: str, stream: io.BytesIO, dtype_overrides: dict[str, str] | None) -> pd.DataFrame:
    
    lower_name = blob_name.lower()
    if lower_name.endswith(".csv"):
        return pd.read_csv(stream, dtype=dtype_overrides)
    if lower_name.endswith((".xls", ".xlsx")):
        return pd.read_excel(stream, engine="openpyxl", dtype=dtype_overrides)
    if lower_name.endswith(".json"):
        raw = json.loads(stream.getvalue().decode("utf-8"))
        if isinstance(raw, dict):
            raw = [raw]
        return pd.json_normalize(raw)

    raise ValueError(f"Nem támogatott formátum: {blob_name}")


def run_azure_blob_pipeline(req: AzureBlobRequest):
    try:
        logger.info("Azure Blob feldolgozás indult: %s -> bronze.%s", req.blob_name, req.table_name)
        blob_client = blob_service_client.get_blob_client(container=req.container_name, blob=req.blob_name)

        stream = io.BytesIO()
        blob_client.download_blob().readinto(stream)
        stream.seek(0)

        df = _read_stream_to_df(req.blob_name, stream, req.dtype_overrides)
        if df.empty:
            logger.warning("Üres fájl, kihagyva: %s", req.blob_name)
            return

        df["data_source_meta"] = req.source_name

        with pipeline_lock:
            pipeline = dlt.pipeline(pipeline_name="azure_blob_ingest", destination=dlt.destinations.postgres(credentials=DB_CONFIG), dataset_name="bronze_azure_blob")
            run_kwargs = {"table_name": req.table_name, "write_disposition": req.write_disposition}
            if req.primary_key and req.write_disposition == "merge":
                run_kwargs["primary_key"] = req.primary_key

            info = pipeline.run(df, **run_kwargs)
            info.raise_on_failed_jobs()

        logger.info("Azure Blob kész (%d sor): %s -> bronze.%s | %s", len(df), req.blob_name, req.table_name, info)

    except Exception as e:
        logger.exception("Azure Blob pipeline hiba (%s): %s", req.blob_name, e)


@router.post("/")
async def trigger_azure_blob_ingest(request: AzureBlobRequest, background_tasks: BackgroundTasks):
    if not blob_service_client:
        raise HTTPException(status_code=500, detail="Hiányzik az AZURE_CONNECTION_STRING környezeti változó.")

    if not request.blob_name.lower().endswith(SUPPORTED_EXTENSIONS):
        raise HTTPException(status_code=400, detail=f"Csak ezek támogatottak: {SUPPORTED_EXTENSIONS}")

    blob_client = blob_service_client.get_blob_client(container=request.container_name, blob=request.blob_name)
    if not blob_client.exists():
        raise HTTPException(status_code=404, detail=f"A blob nem található: {request.blob_name}")

    background_tasks.add_task(run_azure_blob_pipeline, request)
    return {"status": "accepted", "message": f"Azure blob ingestion started in background for {request.blob_name}", "target_table": f"bronze.{request.table_name}"}
