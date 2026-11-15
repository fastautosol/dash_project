# 2026.11.15 18.00
import io
import json
import logging
import os
import threading
from datetime import datetime, timezone
import dlt
import polars as pl
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()
pipeline_lock = threading.Lock()

# Kényszeríti a dlt-t, hogy Arrow betöltésnél is generáljon _dlt_id és _dlt_load_id oszlopokat
os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "true"
os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "true"

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING", "")
DEFAULT_CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "external-catalog")

DB_CONFIG = {
    "host": "postgresql",
    "port": 5432,
    "database": "n8n",
    "username": "sql_admin",
    "password": "sql_pass",
    "connect_timeout": 15,
}

SUPPORTED_EXTENSIONS = (".csv", ".xls", ".xlsx", ".json", ".parquet")

blob_service_client = (
    BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    if AZURE_CONNECTION_STRING
    else None
)


class AzureBlobRequest(BaseModel):
    blob_name: str = "Online_Retail.xlsx"
    table_name: str = "online_retail"
    source_name: str = "azure_external_catalog"
    write_disposition: str = "replace"       # "replace", "append" vagy "merge"
    primary_key: list[str] | str | None = None
    container_name: str = DEFAULT_CONTAINER


def _read_stream_to_arrow(blob_name: str, stream: io.BytesIO) -> pa.Table:
    """
    CSV, JSON és Parquet esetén DataFrame nélkül közvetlenül PyArrow Table-t készít.
    Excel esetén a Rust-alapú Polars + calamine motort használja, majd zero-copy módon Arrow-ra alakítja.
    """
    lower_name = blob_name.lower()

    if lower_name.endswith(".parquet"):
        return pq.read_table(stream)

    if lower_name.endswith(".csv"):
        return pv.read_csv(stream)

    if lower_name.endswith(".json"):
        text = stream.getvalue().decode("utf-8").strip()
        if not text:
            return pa.table({})
        try:
            raw = json.loads(text)
            if isinstance(raw, dict):
                raw = [raw]
        except json.JSONDecodeError:
            # NDJSON (soronkénti JSON) támogatás
            raw = [json.loads(line) for line in text.splitlines() if line.strip()]
        return pa.Table.from_pylist(raw)

    if lower_name.endswith((".xls", ".xlsx")):
        # A magas infer_schema_length megelőzi a vegyes szöveg/szám oszlopok (pl. Description) miatti típusütközést
        df = pl.read_excel(stream, engine="calamine", infer_schema_length=10000)
        return df.to_arrow()

    raise ValueError(f"Nem támogatott formátum: {blob_name}")


def _append_metadata(table: pa.Table, source_name: str, blob_name: str) -> pa.Table:
    """Metaadat oszlopok hozzáfűzése közvetlenül PyArrow szinten."""
    n = table.num_rows
    now_utc = datetime.now(timezone.utc)

    table = table.append_column("data_source_meta", pa.repeat(pa.scalar(source_name, type=pa.string()), n))
    table = table.append_column("source_file_name", pa.repeat(pa.scalar(blob_name, type=pa.string()), n))
    table = table.append_column(
        "ingested_at",
        pa.repeat(pa.scalar(now_utc, type=pa.timestamp("us", tz="UTC")), n),
    )
    return table


def run_azure_blob_pipeline(req: AzureBlobRequest):
    try:
        logger.info("Azure Blob feldolgozás indult: %s -> bronze.%s", req.blob_name, req.table_name)
        blob_client = blob_service_client.get_blob_client(container=req.container_name, blob=req.blob_name)

        stream = io.BytesIO()
        blob_client.download_blob().readinto(stream)
        stream.seek(0)

        arrow_table = _read_stream_to_arrow(req.blob_name, stream)
        if arrow_table.num_rows == 0:
            logger.warning("Üres fájl, kihagyva: %s", req.blob_name)
            return

        arrow_table = _append_metadata(arrow_table, req.source_name, req.blob_name)

        with pipeline_lock:
            pipeline = dlt.pipeline(
                pipeline_name="azure_blob_ingest",
                destination=dlt.destinations.postgres(credentials=DB_CONFIG),
                dataset_name="bronze",
            )

            @dlt.resource(
                name=req.table_name,
                write_disposition=req.write_disposition,
                primary_key=req.primary_key if req.write_disposition == "merge" else None,
            )
            def blob_resource():
                yield arrow_table

            info = pipeline.run(blob_resource())
            info.raise_on_failed_jobs()

        logger.info(
            "Azure Blob kész (%d sor): %s -> bronze.%s | %s",
            arrow_table.num_rows,
            req.blob_name,
            req.table_name,
            info,
        )

    except Exception as e:
        logger.exception("Azure Blob pipeline hiba (%s): %s", req.blob_name, e)


@router.post("/")
async def trigger_azure_blob_ingest(request: AzureBlobRequest, background_tasks: BackgroundTasks):
    if not blob_service_client:
        raise HTTPException(status_code=500, detail="Hiányzik az AZURE_CONNECTION_STRING környezeti változó.")

    if not request.blob_name.lower().endswith(SUPPORTED_EXTENSIONS):
        raise HTTPException(status_code=400, detail=f"Csak ezek támogatottak: {SUPPORTED_EXTENSIONS}")

    if request.write_disposition == "merge" and not request.primary_key:
        raise HTTPException(status_code=400, detail="A 'merge' módhoz kötelező megadni a 'primary_key' mezőt.")

    blob_client = blob_service_client.get_blob_client(container=request.container_name, blob=request.blob_name)
    if not blob_client.exists():
        raise HTTPException(status_code=404, detail=f"A blob nem található: {request.blob_name}")

    background_tasks.add_task(run_azure_blob_pipeline, request)
    return {
        "status": "accepted",
        "message": f"Azure blob ingestion started in background for {request.blob_name}",
        "target_table": f"bronze.{request.table_name}",
    }
