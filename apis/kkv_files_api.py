# 2026.11.26  18.00
import gzip
import io
import json
import logging
import math
import os
import re
import tarfile
import threading
import unicodedata
import zipfile
from collections.abc import Iterator
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import dlt
import polars as pl
from azure.storage.blob import BlobServiceClient
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, Header, HTTPException, UploadFile
from pydantic import BaseModel, Field

logger = logging.getLogger("uvicorn.error")
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "true"
os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "true"

INGEST_API_KEY = os.getenv("INGEST_API_KEY", "FAS")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgresql"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "n8n"),
    "username": os.getenv("POSTGRES_USER", "sql_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "sql_pass"),
    "connect_timeout": 15,
}

AZURE_BLOB_KEY = os.getenv("AZURE_BLOB_SECRET", "")
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING") or (
    f"DefaultEndpointsProtocol=https;AccountName=azurestoragekcs1;AccountKey={AZURE_BLOB_KEY};EndpointSuffix=core.windows.net"
    if AZURE_BLOB_KEY
    else ""
)
DEFAULT_CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "external-catalog")

blob_service_client = (
    BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    if AZURE_CONNECTION_STRING
    else None
)

META_COLS = {"data_source_meta", "source_file_name", "archive_member", "ingested_at", "_dlt_id", "_dlt_load_id"}
MAX_UNCOMPRESSED_BYTES = 100 * 1024 * 1024  # 100 MB limit kicsomagolás után
SUPPORTED_DATA_EXTS = (".csv", ".xlsx", ".xls", ".json", ".jsonl", ".ndjson", ".parquet")
SUPPORTED_ARCHIVE_EXTS = (".tar", ".tar.gz", ".tgz", ".gz", ".zip")
ALL_SUPPORTED_EXTS = SUPPORTED_DATA_EXTS + SUPPORTED_ARCHIVE_EXTS
VALID_WRITE_MODES = {"merge", "append", "replace"}

# --- Táblaszintű zárolás ---
_locks_guard = threading.Lock()
_table_locks: dict[str, threading.Lock] = {}


def _get_table_lock(table_name: str) -> threading.Lock:
    with _locks_guard:
        if table_name not in _table_locks:
            _table_locks[table_name] = threading.Lock()
        return _table_locks[table_name]


def verify_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if x_api_key != INGEST_API_KEY:
        raise HTTPException(status_code=401, detail="Érvénytelen vagy hiányzó X-API-Key.")


router = APIRouter(dependencies=[Depends(verify_api_key)])


# --- Kulcs-normalizálás és adattisztítás ---
def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def _sanitize_key(key: str, idx: int = 0) -> str:
    ascii_key = _strip_accents((key or "").strip())
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", ascii_key).lower()
    clean = re.sub(r"_+", "_", clean).strip("_")
    if not clean:
        clean = f"unnamed_col_{idx}"
    if clean[0].isdigit():
        clean = f"col_{clean}"
    return f"src_{clean}" if clean in META_COLS else clean


def _normalize_primary_key(primary_key: list[str] | str | None) -> list[str] | None:
    if not primary_key:
        return None
    raw_cols = primary_key.split(",") if isinstance(primary_key, str) else primary_key
    cleaned = [_sanitize_key(col, i) for i, col in enumerate(raw_cols) if col and col.strip()]
    return cleaned or None


def _normalize_value(val: Any) -> Any:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, dict):
        return {_sanitize_key(str(k), i): _normalize_value(v) for i, (k, v) in enumerate(val.items())}
    if isinstance(val, list):
        return [_normalize_value(item) for item in val]
    return val


def _prepare_records(
    records: list[dict[str, Any]],
    source_name: str,
    file_name: str,
    archive_member: str | None = None,
) -> list[dict[str, Any]]:
    now_utc = datetime.now(timezone.utc).isoformat()
    prepared: list[dict[str, Any]] = []
    for row in records:
        clean_row = {_sanitize_key(str(k), idx): _normalize_value(v) for idx, (k, v) in enumerate(row.items())}
        clean_row["data_source_meta"] = source_name
        clean_row["source_file_name"] = file_name
        if archive_member is not None:
            clean_row["archive_member"] = archive_member
        clean_row["ingested_at"] = now_utc
        prepared.append(clean_row)
    return prepared


# --- Polars + Calamine + Parquet + JSON + Archívum feldolgozó motor ---
def _extract_json_records(raw_text: str, is_lines: bool = False) -> list[dict[str, Any]]:
    if is_lines:
        items = [json.loads(line) for line in raw_text.splitlines() if line.strip()]
    else:
        parsed = json.loads(raw_text)
        if isinstance(parsed, list):
            items = parsed
        elif isinstance(parsed, dict):
            list_candidates = [
                v for k, v in parsed.items()
                if k.lower() in {"data", "items", "records", "results", "rows"} and isinstance(v, list)
            ]
            if len(list_candidates) == 1:
                items = list_candidates[0]
            elif len(parsed) == 1 and isinstance(next(iter(parsed.values())), list):
                items = next(iter(parsed.values()))
            else:
                items = [parsed]
        else:
            items = [{"value": parsed}]

    return [item if isinstance(item, dict) else {"value": item} for item in items]


def _detect_csv_separator(decoded_text: str) -> str:
    first_line = decoded_text.splitlines()[0] if decoded_text.strip() else ""
    counts = {sep: first_line.count(sep) for sep in (";", ",", "\t")}
    best_sep = max(counts, key=lambda k: counts[k])
    return best_sep if counts[best_sep] > 0 else ","


def _clean_polars_df(df: pl.DataFrame) -> list[dict[str, Any]]:
    if df.is_empty():
        return []
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))
    float_cols = [col for col, dtype in df.schema.items() if dtype in (pl.Float32, pl.Float64)]
    if float_cols:
        df = df.with_columns([pl.col(c).fill_nan(None) for c in float_cols])
    return df.to_dicts()


def _parse_single_file(content: bytes, filename: str, sheet_name: str = "") -> list[dict[str, Any]]:
    lower_name = filename.lower()

    if lower_name.endswith(".csv"):
        for enc in ("utf-8-sig", "utf-8", "cp1250", "cp1252", "latin1"):
            try:
                text = content.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError(f"Nem támogatott CSV karakterkódolás: {filename}")

        if not text.strip():
            return []

        sep = _detect_csv_separator(text)
        df = pl.read_csv(
            io.BytesIO(text.encode("utf-8")),
            separator=sep,
            infer_schema_length=10000,
            truncate_ragged_lines=True,
        )
        return _clean_polars_df(df)

    if lower_name.endswith((".xlsx", ".xls")):
        excel_kwargs: dict[str, Any] = {"engine": "calamine"}
        clean_sheet = sheet_name.strip()
        if clean_sheet:
            if clean_sheet.isdigit():
                idx = int(clean_sheet)
                excel_kwargs["sheet_id"] = idx if idx >= 1 else 1
            else:
                excel_kwargs["sheet_name"] = clean_sheet
        else:
            excel_kwargs["sheet_id"] = 1

        df = pl.read_excel(io.BytesIO(content), **excel_kwargs)
        return _clean_polars_df(df)

    if lower_name.endswith(".parquet"):
        df = pl.read_parquet(io.BytesIO(content))
        return _clean_polars_df(df)

    if lower_name.endswith((".json", ".jsonl", ".ndjson")):
        for enc in ("utf-8-sig", "utf-8", "cp1250", "latin1"):
            try:
                text = content.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError(f"Nem támogatott JSON karakterkódolás: {filename}")
        return _extract_json_records(text, is_lines=lower_name.endswith((".jsonl", ".ndjson")))

    raise ValueError(f"Nem támogatott fájlkiterjesztés: {filename}")


def _is_ignored_archive_entry(entry_name: str) -> bool:
    p = Path(entry_name)
    return "__MACOSX" in p.parts or p.name.startswith(".") or not p.name.lower().endswith(SUPPORTED_DATA_EXTS)


def _iter_parsed_batches(
    content: bytes,
    filename: str,
    source_name: str,
    sheet_name: str = "",
) -> Iterator[list[dict[str, Any]]]:
    """Fájlonként vagy archívum-tagként előkészített rekordkötegeket (batch) ad vissza."""
    lower_name = filename.lower()

    # ----- .tar / .tar.gz / .tgz archívum -----
    if lower_name.endswith((".tar", ".tar.gz", ".tgz")):
        total_unpacked = 0
        with tarfile.open(fileobj=io.BytesIO(content), mode="r:*") as tf:
            for member in tf.getmembers():
                if not member.isfile() or _is_ignored_archive_entry(member.name):
                    continue
                total_unpacked += member.size
                if total_unpacked > MAX_UNCOMPRESSED_BYTES:
                    raise ValueError("A kicsomagolt archívum mérete meghaladja a 100 MB-os limitet.")
                extracted = tf.extractfile(member)
                if extracted:
                    raw_rows = _parse_single_file(extracted.read(), Path(member.name).name, sheet_name)
                    if raw_rows:
                        yield _prepare_records(raw_rows, source_name, filename, archive_member=member.name)
        return

    # ----- Egyedi .gz fájl (pl. export.csv.gz vagy dump.json.gz) -----
    if lower_name.endswith(".gz"):
        decompressed = gzip.decompress(content)
        if len(decompressed) > MAX_UNCOMPRESSED_BYTES:
            raise ValueError("A kicsomagolt GZIP fájl mérete meghaladja a 100 MB-os limitet.")
        inner_name = filename[:-3]
        raw_rows = _parse_single_file(decompressed, inner_name, sheet_name)
        if raw_rows:
            yield _prepare_records(raw_rows, source_name, filename, archive_member=inner_name)
        return

    # ----- .zip archívum -----
    if lower_name.endswith(".zip"):
        total_unpacked = 0
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for info in zf.infolist():
                if info.is_dir() or _is_ignored_archive_entry(info.filename):
                    continue
                total_unpacked += info.file_size
                if total_unpacked > MAX_UNCOMPRESSED_BYTES:
                    raise ValueError("A kicsomagolt ZIP mérete meghaladja a 100 MB-os limitet.")
                raw_rows = _parse_single_file(zf.read(info.filename), Path(info.filename).name, sheet_name)
                if raw_rows:
                    yield _prepare_records(raw_rows, source_name, filename, archive_member=info.filename)
        return

    # ----- Közvetlen adatfájl (.csv, .xlsx, .xls, .json, .jsonl, .ndjson, .parquet) -----
    raw_rows = _parse_single_file(content, filename, sheet_name)
    if raw_rows:
        yield _prepare_records(raw_rows, source_name, filename)


# --- Közös DLT betöltő motor (bronze_api) ---
def run_files_dlt_ingest(
    batches: Iterator[list[dict[str, Any]]] | list[list[dict[str, Any]]],
    source_label: str,
    table_name: str,
    write_disposition: str = "merge",
    primary_key: list[str] | str | None = None,
) -> int:
    safe_table = _sanitize_key(table_name, 0)
    pk_list = _normalize_primary_key(primary_key)
    effective_disposition = write_disposition.lower().strip()

    if effective_disposition == "merge" and not pk_list:
        logger.warning("[FILES-DLT] Merge módhoz nincs primary_key megadva (%s) -> append módra váltás.", safe_table)
        effective_disposition = "append"

    table_lock = _get_table_lock(safe_table)
    total_rows = 0

    try:
        with table_lock:
            pipeline = dlt.pipeline(
                pipeline_name=f"files_{safe_table}_ingest",
                destination=dlt.destinations.postgres(credentials=DB_CONFIG),
                dataset_name="bronze_api",
            )
            pipeline.abort_packages()

            resource_kwargs: dict[str, Any] = {
                "name": safe_table,
                "write_disposition": effective_disposition,
            }
            if pk_list and effective_disposition == "merge":
                resource_kwargs["primary_key"] = pk_list

            @dlt.resource(**resource_kwargs)
            def file_resource():
                nonlocal total_rows
                for batch in batches:
                    if batch:
                        total_rows += len(batch)
                        yield batch

            info = pipeline.run(file_resource())
            if total_rows > 0:
                info.raise_on_failed_jobs()

        if total_rows == 0:
            logger.info("[FILES-DLT] Nincs betöltendő rekord (%s -> bronze_api.%s).", source_label, safe_table)
        else:
            logger.info("[FILES-DLT] KÉSZ (%d sor, mód=%s): %s -> bronze_api.%s | %s", total_rows, effective_disposition, source_label, safe_table, info)
        return total_rows

    except Exception as e:
        logger.exception("[FILES-DLT] HIBA (%s -> bronze_api.%s): %s", source_label, safe_table, e)
        raise


# --- Request modellek ---
class AzureBlobRequest(BaseModel):
    blob_name: str = Field(default="Online_Retail.xlsx")
    table_name: str = Field(default="online_retail")
    source_name: str = Field(default="azure_external_catalog")
    write_disposition: str = Field(default="replace")  # "replace", "append" vagy "merge"
    primary_key: list[str] | str | None = None
    container_name: str = Field(default=DEFAULT_CONTAINER)
    sheet_name: str = Field(default="")


# ---------- 1. HTTP FÁJLFELTÖLTÉS (/upload) ----------
@router.post("/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    table_name: str = Form(...),
    primary_key: str = Form(default=""),
    write_mode: str = Form(default="merge"),
    sheet_name: str = Form(default=""),
    source_label: str = Form(default="file_upload"),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Hiányzó fájlnév.")

    if not file.filename.lower().endswith(ALL_SUPPORTED_EXTS):
        raise HTTPException(status_code=400, detail=f"Támogatott formátumok: {ALL_SUPPORTED_EXTS}")

    clean_mode = write_mode.lower().strip()
    if clean_mode not in VALID_WRITE_MODES:
        raise HTTPException(status_code=400, detail=f"Érvénytelen write_mode ({write_mode}). Választható: {VALID_WRITE_MODES}")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Üres fájl.")

    try:
        batches = list(_iter_parsed_batches(content, file.filename, source_label, sheet_name))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Fájlfeldolgozási hiba: {e}") from e

    total_rows = sum(len(b) for b in batches)
    if total_rows == 0:
        raise HTTPException(status_code=400, detail="A fájl nem tartalmaz feldolgozható rekordokat.")

    safe_table = _sanitize_key(table_name, 0)
    pk_list = _normalize_primary_key(primary_key)

    background_tasks.add_task(
        run_files_dlt_ingest,
        batches,
        f"{source_label}:{file.filename}",
        safe_table,
        clean_mode,
        pk_list,
    )

    return {
        "status": "accepted",
        "filename": file.filename,
        "parsed_rows": total_rows,
        "target_table": f"bronze_api.{safe_table}",
        "primary_key": pk_list,
        "sample": batches[0][:2],
    }


# ---------- 2. AZURE BLOB STORAGE BETÖLTŐ (/azure-blob) ----------
def _download_and_ingest_azure_blob(req: AzureBlobRequest) -> None:
    try:
        logger.info("[AZURE-DLT] Letöltés indul: %s/%s -> bronze_api.%s", req.container_name, req.blob_name, req.table_name)
        blob_client = blob_service_client.get_blob_client(container=req.container_name, blob=req.blob_name)
        raw_bytes = blob_client.download_blob().readall()

        batches = _iter_parsed_batches(raw_bytes, req.blob_name, req.source_name, req.sheet_name)
        run_files_dlt_ingest(
            batches,
            f"{req.source_name}:{req.blob_name}",
            req.table_name,
            req.write_disposition,
            req.primary_key,
        )
    except Exception as e:
        logger.exception("[AZURE-DLT] HIBA (%s): %s", req.blob_name, e)


@router.post("/azure-blob")
async def trigger_azure_blob_ingest(request: AzureBlobRequest, background_tasks: BackgroundTasks):
    if not blob_service_client:
        raise HTTPException(status_code=500, detail="Hiányzik az AZURE_BLOB_SECRET vagy AZURE_CONNECTION_STRING környezeti változó.")

    if not request.blob_name.lower().endswith(ALL_SUPPORTED_EXTS):
        raise HTTPException(status_code=400, detail=f"Támogatott formátumok: {ALL_SUPPORTED_EXTS}")

    clean_mode = request.write_disposition.lower().strip()
    if clean_mode not in VALID_WRITE_MODES:
        raise HTTPException(status_code=400, detail=f"Érvénytelen write_disposition ({request.write_disposition}). Választható: {VALID_WRITE_MODES}")

    pk_list = _normalize_primary_key(request.primary_key)
    if clean_mode == "merge" and not pk_list:
        raise HTTPException(status_code=400, detail="A 'merge' módhoz kötelező megadni a 'primary_key' mezőt.")

    blob_client = blob_service_client.get_blob_client(container=request.container_name, blob=request.blob_name)
    if not blob_client.exists():
        raise HTTPException(status_code=404, detail=f"A blob nem található: {request.container_name}/{request.blob_name}")

    safe_table = _sanitize_key(request.table_name, 0)
    background_tasks.add_task(_download_and_ingest_azure_blob, request)

    return {
        "status": "accepted",
        "message": f"Azure blob betöltés elindítva: {request.blob_name}",
        "target_table": f"bronze_api.{safe_table}",
        "primary_key": pk_list,
    }
