# 2026.11.23  18.00
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
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import dlt
import httpx
import polars as pl
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, Header, HTTPException, UploadFile
from pydantic import BaseModel, Field

try:
    from apis.ingest_finance_mock import (generate_mock_billingo_documents, generate_mock_product_cogs, generate_mock_szamlazzhu_invoices)
    MOCK_ENABLED = True
except ImportError:
    MOCK_ENABLED = False

logger = logging.getLogger("uvicorn.error")

os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "true"
os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "true"

INGEST_API_KEY = os.getenv("INGEST_API_KEY", "FAS")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgresql"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "n8n"),
    "username": os.getenv("POSTGRES_USER", "sql_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "sql_pass"),
    "connect_timeout": 15}

META_COLS = {"data_source_meta", "ingested_at", "_dlt_id", "_dlt_load_id"}
MAX_UNCOMPRESSED_BYTES = 100 * 1024 * 1024  # 100 MB limit kicsomagolás után
SUPPORTED_DATA_EXTS = (".csv", ".xlsx", ".xls", ".json", ".jsonl", ".ndjson")

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


def _sanitize_key(key: str, idx: int) -> str:
    ascii_key = _strip_accents((key or "").strip())
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", ascii_key).lower()
    clean = re.sub(r"_+", "_", clean).strip("_")
    if not clean:
        clean = f"unnamed_col_{idx}"
    if clean[0].isdigit():
        clean = f"col_{clean}"
    return f"src_{clean}" if clean in META_COLS else clean


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


def _prepare_records(records: list[dict[str, Any]], source_name: str) -> list[dict[str, Any]]:
    now_utc = datetime.now(timezone.utc).isoformat()
    prepared = []
    for row in records:
        clean_row = {_sanitize_key(str(k), idx): _normalize_value(v) for idx, (k, v) in enumerate(row.items())}
        clean_row["data_source_meta"] = source_name
        clean_row["ingested_at"] = now_utc
        prepared.append(clean_row)
    return prepared


def run_finance_dlt_ingest(records: list[dict[str, Any]], source_name: str, table_name: str, write_disposition: str = "merge", primary_key: list[str] | str | None = None) -> None:
    if not records:
        logger.info("[FINANCE-DLT] Nincs betöltendő rekord (%s -> %s).", source_name, table_name)
        return

    safe_table = _sanitize_key(table_name, 0)
    table_lock = _get_table_lock(safe_table)

    try:
        rows = _prepare_records(records, source_name)
        effective_disposition = write_disposition
        if effective_disposition == "merge" and not primary_key:
            logger.warning("[FINANCE-DLT] Merge módhoz nincs primary_key megadva (%s) -> append módra váltás.", safe_table)
            effective_disposition = "append"

        logger.info("[FINANCE-DLT] Betöltés indul (%d sor, mód=%s): %s -> bronze_api.%s", len(rows), effective_disposition, source_name, safe_table)

        with table_lock:
            pipeline = dlt.pipeline(pipeline_name=f"finance_{safe_table}_ingest",
                destination=dlt.destinations.postgres(credentials=DB_CONFIG),
                dataset_name="bronze_api")
            pipeline.abort_packages()

            resource_kwargs: dict[str, Any] = {"name": safe_table, "write_disposition": effective_disposition}
            if primary_key and effective_disposition == "merge":
                resource_kwargs["primary_key"] = primary_key

            @dlt.resource(**resource_kwargs)
            def finance_resource():
                yield rows

            info = pipeline.run(finance_resource())
            info.raise_on_failed_jobs()

        logger.info("[FINANCE-DLT] KÉSZ (%d sor): bronze_api.%s | %s", len(rows), safe_table, info)

    except Exception as e:
        logger.exception("[FINANCE-DLT] HIBA (%s -> %s): %s", source_name, safe_table, e)


# --- Pull kliensek (Billingo v3 + Számlázz.hu Számla Agent) ---
def _fetch_billingo_documents(api_key: str, days_back: int) -> list[dict[str, Any]]:
    start_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).date().isoformat()
    end_date = datetime.now(timezone.utc).date().isoformat()
    url = "https://api.billingo.hu/v3/documents"
    headers = {"X-API-KEY": api_key, "Accept": "application/json"}
    page = 1
    results: list[dict[str, Any]] = []

    with httpx.Client(timeout=45.0) as client:
        while True:
            params = {"page": page, "per_page": 100, "start_date": start_date, "end_date": end_date}
            resp = client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            payload = resp.json()
            batch = payload.get("data", [])
            results.extend(batch)
            if page >= payload.get("last_page", 1) or not batch:
                break
            page += 1
    return results


def _xml_to_dict(elem: ET.Element) -> Any:
    children = list(elem)
    if not children:
        return (elem.text or "").strip()
    out: dict[str, Any] = {}
    for child in children:
        tag = child.tag.split("}", 1)[-1]
        val = _xml_to_dict(child)
        if tag in out:
            if not isinstance(out[tag], list):
                out[tag] = [out[tag]]
            out[tag].append(val)
        else:
            out[tag] = val
    return out


def _fetch_szamlazzhu_by_numbers(agent_key: str, invoice_numbers: list[str]) -> list[dict[str, Any]]:
    """Számlázz.hu Számla Agent XML lekérdezés konkrét számlaszámokra."""
    url = "https://www.szamlazz.hu/szamla/"
    results: list[dict[str, Any]] = []

    with httpx.Client(timeout=45.0) as client:
        for inv_num in invoice_numbers:
            query_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
            <xmlszamlaxml xmlns="http://www.szamlazz.hu/xmlszamlaxml" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <szamlaagentkulcs>{agent_key}</szamlaagentkulcs>
                <szamlaszam>{inv_num}</szamlaszam>
                <rendelesSzam></rendelesSzam>
                <pdf>false</pdf>
            </xmlszamlaxml>"""
            files = {"action-szamla_agent_xml": ("query.xml", query_xml.encode("utf-8"), "text/xml")}
            resp = client.post(url, files=files)
            resp.raise_for_status()

            root = ET.fromstring(resp.text)
            parsed = _xml_to_dict(root)
            if isinstance(parsed, dict) and parsed.get("sikeres") == "false":
                logger.warning("[SZAMLAZZHU] Sikertelen lekérdezés (%s): %s", inv_num, parsed.get("hibauzenet"))
                continue

            if isinstance(parsed, dict) and isinstance(parsed.get("tetelek"), dict):
                raw_items = parsed["tetelek"].get("tetel", [])
                parsed["tetelek"] = raw_items if isinstance(raw_items, list) else [raw_items]
            results.append(parsed)
    return results


# --- Polars + Calamine + JSON + Archívum feldolgozó motor ---
def _extract_json_records(raw_text: str, is_lines: bool = False) -> list[dict[str, Any]]:
    if is_lines:
        items = [json.loads(line) for line in raw_text.splitlines() if line.strip()]
    else:
        parsed = json.loads(raw_text)
        if isinstance(parsed, list):
            items = parsed
        elif isinstance(parsed, dict):
            list_candidates = [v for k, v in parsed.items() if k.lower() in {"data", "items", "records", "results", "rows"} and isinstance(v, list)]
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
        for enc in ("utf-8-sig", "utf-8", "cp1250", "latin1"):
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
        df = pl.read_csv(io.BytesIO(text.encode("utf-8")), separator=sep, infer_schema_length=10000, truncate_ragged_lines=True)
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

    if lower_name.endswith((".json", ".jsonl", ".ndjson")):
        for enc in ("utf-8-sig", "utf-8", "cp1250"):
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
    return ("__MACOSX" in p.parts or p.name.startswith(".") or not p.name.lower().endswith(SUPPORTED_DATA_EXTS))


def _parse_uploaded_payload(content: bytes, filename: str, sheet_name: str = "") -> list[dict[str, Any]]:
    lower_name = filename.lower()

    # ----- .tar / .tar.gz / .tgz archívum -----
    if lower_name.endswith((".tar", ".tar.gz", ".tgz")):
        records: list[dict[str, Any]] = []
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
                    records.extend(_parse_single_file(extracted.read(), Path(member.name).name, sheet_name))
        return records

    # ----- Egyedi .gz fájl (pl. export.csv.gz vagy dump.json.gz) -----
    if lower_name.endswith(".gz"):
        decompressed = gzip.decompress(content)
        if len(decompressed) > MAX_UNCOMPRESSED_BYTES:
            raise ValueError("A kicsomagolt GZIP fájl mérete meghaladja a 100 MB-os limitet.")
        inner_name = filename[:-3]
        return _parse_single_file(decompressed, inner_name, sheet_name)

    # ----- .zip archívum -----
    if lower_name.endswith(".zip"):
        records = []
        total_unpacked = 0
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for info in zf.infolist():
                if info.is_dir() or _is_ignored_archive_entry(info.filename):
                    continue
                total_unpacked += info.file_size
                if total_unpacked > MAX_UNCOMPRESSED_BYTES:
                    raise ValueError("A kicsomagolt ZIP mérete meghaladja a 100 MB-os limitet.")
                records.extend(_parse_single_file(zf.read(info.filename), Path(info.filename).name, sheet_name))
        return records

    # 4. Közvetlen adatfájl (.csv, .xlsx, .xls, .json, .jsonl, .ndjson)
    return _parse_single_file(content, filename, sheet_name)


# --- Request modellek ---
class BillingoSyncRequest(BaseModel):
    api_key: str = Field(default_factory=lambda: os.getenv("BILLINGO_API_KEY", ""))
    days_back: int = Field(default=30, ge=1, le=365)
    table_name: str = "billingo_documents"
    records: list[dict[str, Any]] | None = None


class SzamlazzHuSyncRequest(BaseModel):
    agent_key: str = Field(default_factory=lambda: os.getenv("SZAMLAZZHU_AGENT_KEY", ""))
    invoice_numbers: list[str] = Field(default_factory=list)
    table_name: str = "szamlazzhu_invoices"
    records: list[dict[str, Any]] | None = None


class FinanceMockRequest(BaseModel):
    count: int = Field(default=50, ge=1, le=1000)


# ---------- 1. BILLINGO API v3 ----------
def _pull_and_ingest_billingo(req: BillingoSyncRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_billingo_documents(req.api_key, req.days_back)
        run_finance_dlt_ingest(rows, "billingo_api_v3", req.table_name, "merge", "id")
    except Exception as e:
        logger.exception("[BILLINGO-PULL] Hiba: %s", e)

@router.post("/billingo")
async def sync_billingo(request: BillingoSyncRequest, background_tasks: BackgroundTasks):
    if request.records is None and not request.api_key:
        raise HTTPException(status_code=400, detail="Hiányzó Billingo api_key vagy records.")
    background_tasks.add_task(_pull_and_ingest_billingo, request)
    return {"status": "accepted", "provider": "billingo", "target_table": f"bronze_api.{request.table_name}"}

@router.post("/billingo/mock")
async def sync_billingo_mock(request: FinanceMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_billingo_documents(request.count)
    background_tasks.add_task(run_finance_dlt_ingest, mock_rows, "billingo_mock_generator", "billingo_documents", "merge", "id")
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.billingo_documents", "sample": mock_rows[:2]}


# ---------- 2. SZÁMLÁZZ.HU ----------
def _pull_and_ingest_szamlazzhu(req: SzamlazzHuSyncRequest) -> None:
    try:
        rows = req.records if req.records is not None else _fetch_szamlazzhu_by_numbers(req.agent_key, req.invoice_numbers)
        run_finance_dlt_ingest(rows, "szamlazzhu_agent", req.table_name, "merge", "szamlaszam")
    except Exception as e:
        logger.exception("[SZAMLAZZHU-PULL] Hiba: %s", e)

@router.post("/szamlazzhu")
async def sync_szamlazzhu(request: SzamlazzHuSyncRequest, background_tasks: BackgroundTasks):
    if request.records is None and (not request.agent_key or not request.invoice_numbers):
        raise HTTPException(status_code=400, detail="Add meg a records listát (webhook/export) vagy az agent_key + invoice_numbers mezőket.")
    background_tasks.add_task(_pull_and_ingest_szamlazzhu, request)
    return {"status": "accepted", "provider": "szamlazz.hu", "target_table": f"bronze_api.{request.table_name}"}

@router.post("/szamlazzhu/mock")
async def sync_szamlazzhu_mock(request: FinanceMockRequest, background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_szamlazzhu_invoices(request.count)
    background_tasks.add_task(run_finance_dlt_ingest, mock_rows, "szamlazzhu_mock_generator", "szamlazzhu_invoices", "merge", "szamlaszam")
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.szamlazzhu_invoices", "sample": mock_rows[:2]}


# ---------- 3. UNIVERZÁLIS FÁJLFELDOLGOZÓ (Polars + Calamine + JSON + Archívumok) ----------
@router.post("/upload-file")
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...), table_name: str = Form(...), primary_key: str = Form(default=""),
    write_mode: str = Form(default="merge"), sheet_name: str = Form(default=""), source_label: str = Form(default="file_upload")):
        
    if not file.filename:
        raise HTTPException(status_code=400, detail="Hiányzó fájlnév.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Üres fájl.")

    try:
        records = _parse_uploaded_payload(content, file.filename, sheet_name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Fájlfeldolgozási hiba: {e}") from e

    if not records:
        raise HTTPException(status_code=400, detail="A fájl nem tartalmaz feldolgozható rekordokat.")

    pk_list = [_sanitize_key(col, i) for i, col in enumerate(primary_key.split(",")) if col.strip()]
    safe_table = _sanitize_key(table_name, 0)

    background_tasks.add_task(run_finance_dlt_ingest, records, f"{source_label}:{file.filename}", safe_table, write_mode, pk_list if pk_list else None)

    return {
        "status": "accepted",
        "filename": file.filename,
        "parsed_rows": len(records),
        "target_table": f"bronze_api.{safe_table}",
        "primary_key": pk_list or None,
        "sample": records[:2]}


# ---------- 4. BESZERZÉSI ÁRAK (COGS) MOCK ----------
@router.post("/cogs/mock")
async def sync_cogs_mock(background_tasks: BackgroundTasks):
    if not MOCK_ENABLED:
        raise HTTPException(status_code=404, detail="Mock modul nincs telepítve.")
    mock_rows = generate_mock_product_cogs()
    background_tasks.add_task(run_finance_dlt_ingest, mock_rows, "cogs_mock_generator", "product_cogs", "merge", ["sku", "ervenyes_ettol"])
    return {"status": "accepted", "generated_records": len(mock_rows), "target_table": "bronze_api.product_cogs", "sample": mock_rows}
