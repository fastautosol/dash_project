# 2026.11.15 18.00
import io
import json
import logging
import os
import re
import tarfile
import threading
import zipfile
from datetime import datetime, timezone
from typing import Iterator
import dlt
import polars as pl
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

logger = logging.getLogger("uvicorn.error")
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

router = APIRouter()
pipeline_lock = threading.Lock()

os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "true"
os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "true"

AZURE_BLOB_KEY = "NnrfB+Oq71lv1fad3Az37bmZGLUt36VlEEbdUC9t1Gwy1fs/4AQuZaWpesteQtiwuIm4tKML2QP7+ASt4BHAzg=="
AZURE_CONNECTION_STRING = f"DefaultEndpointsProtocol=https;AccountName=azurestoragekcs1;AccountKey={AZURE_BLOB_KEY};EndpointSuffix=core.windows.net"
DEFAULT_CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "external-catalog")

DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}

SUPPORTED_EXTENSIONS = (".csv", ".xls", ".xlsx", ".json", ".jsonl", ".parquet", ".tar", ".tar.gz", ".tgz", ".zip")

blob_service_client = (BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING) if AZURE_CONNECTION_STRING else None)

class AzureBlobRequest(BaseModel):
    blob_name: str = "Online_Retail.xlsx"
    table_name: str = "online_retail"
    source_name: str = "azure_external_catalog"
    write_disposition: str = "replace"       # "replace", "append" vagy "merge"
    primary_key: list[str] | str | None = None
    container_name: str = DEFAULT_CONTAINER


def _decode_csv_bytes_to_utf8(raw_bytes: bytes) -> io.BytesIO:
    for encoding in ("utf-8-sig", "cp1252", "latin1"):
        try:
            return io.BytesIO(raw_bytes.decode(encoding).encode("utf-8"))
        except UnicodeDecodeError:
            continue
    return io.BytesIO(raw_bytes.decode("utf-8", errors="replace").encode("utf-8"))


def _json_bytes_to_arrow(raw_bytes: bytes) -> pa.Table:
    text = raw_bytes.decode("utf-8-sig", errors="replace").strip()
    if not text:
        return pa.table({})
    try:
        raw = json.loads(text)
        if isinstance(raw, dict):
            raw = [raw]
    except json.JSONDecodeError:
        raw = [json.loads(line) for line in text.splitlines() if line.strip()]
    return pa.Table.from_pylist(raw) if raw else pa.table({})


def _sanitize_arrow_table(table: pa.Table) -> pa.Table:
    new_arrays = []
    new_fields = []
    seen = set()

    for idx, field in enumerate(table.schema):
        col = table.column(idx)
        if pa.types.is_null(field.type):
            col = pa.nulls(table.num_rows, type=pa.string())

        clean = re.sub(r"[^0-9a-zA-Z]+", "_", (field.name or "").strip()).strip("_").lower()
        if not clean or clean[0].isdigit():
            clean = f"col_{clean or idx}"
        if clean in seen:
            clean = f"{clean}_{idx}"
        seen.add(clean)

        new_fields.append(pa.field(clean, col.type))
        new_arrays.append(col)

    return pa.Table.from_arrays(new_arrays, schema=pa.schema(new_fields))


def _append_metadata(table: pa.Table, source_name: str, blob_name: str, now_utc: datetime, archive_member: str | None = None) -> pa.Table:
    table = _sanitize_arrow_table(table)
    n = table.num_rows
    table = table.append_column("data_source_meta", pa.repeat(pa.scalar(source_name, type=pa.string()), n))
    table = table.append_column("source_file_name", pa.repeat(pa.scalar(blob_name, type=pa.string()), n))
    table = table.append_column("archive_member", pa.repeat(pa.scalar(archive_member, type=pa.string()), n))
    table = table.append_column("ingested_at", pa.repeat(pa.scalar(now_utc, type=pa.timestamp("us", tz="UTC")), n))
    return table


def _is_valid_json_member(filename: str) -> bool:
    base = os.path.basename(filename)
    if not base or base.startswith((".", "._")) or "__MACOSX" in filename:
        return False
    return base.lower().endswith((".json", ".jsonl", ".ndjson"))


def _iter_arrow_batches(blob_name: str, stream: io.BytesIO, source_name: str) -> Iterator[pa.Table]:
    """Fájltípustól függően 1 vagy több (archívum esetén fájlonként) PyArrow táblát ad vissza."""
    lower_name = blob_name.lower()
    now_utc = datetime.now(timezone.utc)

    # 1. TAR / TAR.GZ / TGZ archívumok (pl. jsons.tar)
    if lower_name.endswith((".tar", ".tar.gz", ".tgz")):
        with tarfile.open(fileobj=stream, mode="r:*") as tar:
            for member in tar:
                if not member.isfile() or not _is_valid_json_member(member.name):
                    continue
                extracted = tar.extractfile(member)
                if not extracted:
                    continue
                table = _json_bytes_to_arrow(extracted.read())
                if table.num_rows > 0:
                    yield _append_metadata(table, source_name, blob_name, now_utc, archive_member=member.name)
        return

    # 2. ZIP archívumok
    if lower_name.endswith(".zip"):
        with zipfile.ZipFile(stream, mode="r") as zf:
            for member_name in zf.namelist():
                if member_name.endswith("/") or not _is_valid_json_member(member_name):
                    continue
                table = _json_bytes_to_arrow(zf.read(member_name))
                if table.num_rows > 0:
                    yield _append_metadata(table, source_name, blob_name, now_utc, archive_member=member_name)
        return

    # 3. Egyedi fájlok (Parquet, CSV, JSON, Excel)
    if lower_name.endswith(".parquet"):
        table = pq.read_table(stream)
    elif lower_name.endswith(".csv"):
        utf8_stream = _decode_csv_bytes_to_utf8(stream.getvalue())
        table = pv.read_csv(utf8_stream, read_options=pv.ReadOptions(block_size=16 * 1024 * 1024), parse_options=pv.ParseOptions(newlines_in_values=True))
    elif lower_name.endswith((".json", ".jsonl", ".ndjson")):
        table = _json_bytes_to_arrow(stream.getvalue())
    elif lower_name.endswith((".xls", ".xlsx")):
        df = pl.read_excel(stream, engine="calamine", infer_schema_length=None)
        table = df.to_arrow()
    else:
        raise ValueError(f"Nem támogatott formátum: {blob_name}")

    if table.num_rows > 0:
        yield _append_metadata(table, source_name, blob_name, now_utc)


def run_azure_blob_pipeline(req: AzureBlobRequest):
    try:
        logger.info("[AZURE-DLT] Letöltés indul: %s -> bronze.%s", req.blob_name, req.table_name)
        blob_client = blob_service_client.get_blob_client(container=req.container_name, blob=req.blob_name)

        stream = io.BytesIO()
        blob_client.download_blob().readinto(stream)
        stream.seek(0)

        total_rows = 0

        with pipeline_lock:
            pipeline = dlt.pipeline(pipeline_name="azure_blob_ingest",
                destination=dlt.destinations.postgres(credentials=DB_CONFIG),
                dataset_name="bronze_azure_blob")

            @dlt.resource(name=req.table_name, write_disposition=req.write_disposition,
                primary_key=req.primary_key if req.write_disposition == "merge" else None)
            def blob_resource():
                nonlocal total_rows
                for batch in _iter_arrow_batches(req.blob_name, stream, req.source_name):
                    total_rows += batch.num_rows
                    yield batch

            info = pipeline.run(blob_resource())
            info.raise_on_failed_jobs()

        stream.close()
        logger.info("[AZURE-DLT] KÉSZ (%d sor): %s -> bronze.%s | %s", total_rows, req.blob_name, req.table_name, info)

    except Exception as e:
        logger.exception("[AZURE-DLT] HIBA (%s): %s", req.blob_name, e)


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
        "target_table": f"bronze_azure_blob.{request.table_name}",
    }
