# 2026.11.15  12.00
import io
import json
import os
from typing import Optional

import dlt
import pandas as pd
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Azure Blob to PostgreSQL (dlt) API")

# Konfigurációk Coolify Environment Variables-ből
DB_URL = os.getenv("DB_URL", "postgresql://sql_admin:sql_pass@postgresql:5432/n8n")

AZURE_BLOB_KEY = "NnrfB+Oq71lv1fad3Az37bmZGLUt36VlEEbdUC9t1Gwy1fs/4AQuZaWpesteQtiwuIm4tKML2QP7+ASt4BHAzg=="
AZURE_CONNECTION_STRING = f"DefaultEndpointsProtocol=https;AccountName=azurestoragekcs1;AccountKey={AZURE_BLOB_KEY};EndpointSuffix=core.windows.net"
#AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING", "")

DEFAULT_CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "external-catalog")

blob_service_client = (BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING) if AZURE_CONNECTION_STRING else None)


def read_blob_to_dataframe(blob_name: str, stream: io.BytesIO) -> pd.DataFrame:

    lower_name = blob_name.lower()
    if lower_name.endswith(".csv"):
        return pd.read_csv(stream)
    if lower_name.endswith((".xls", ".xlsx")):
        df = pd.read_excel(stream, engine="openpyxl", dtype={"InvoiceNo": str, "StockCode": str})
        return df
    if lower_name.endswith(".json"):
        raw_data = json.loads(stream.getvalue().decode("utf-8"))
        if isinstance(raw_data, dict):
            raw_data = [raw_data]
        return pd.json_normalize(raw_data)

    raise HTTPException(status_code=400, detail="Nem támogatott formátum. Csak .csv, .xls, .xlsx vagy .json engedélyezett.")


@app.post("/process-azure-blob/")
def process_azure_blob(
    blob_name: str = "Online_Retail.xlsx",
    table_name: str = "online_retail",
    source_name: str = "azure_external_catalog",
    write_disposition: str = "replace",
    primary_key: Optional[str] = None,
    container_name: str = DEFAULT_CONTAINER,
):
    """
    Beolvassa a megadott fájlt az Azure Blob Storage-ból és betölti PostgreSQL-be dlt-vel.
    Megjegyzés: szinkron `def` végpontot használunk, így a FastAPI külön threadpoolban
    futtatja a blokkoló I/O (Azure letöltés + Pandas + dlt) műveleteket.
    """
    if not blob_service_client:
        raise HTTPException(status_code=500, detail="Hiányzó AZURE_CONNECTION_STRING.")

    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        if not blob_client.exists():
            raise HTTPException(status_code=404, detail=f"Blob nem található: {blob_name}")
        stream = io.BytesIO()
        blob_client.download_blob().readinto(stream)
        stream.seek(0)
        df = read_blob_to_dataframe(blob_name, stream)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Azure letöltési/olvasási hiba: {str(e)}")

    if df.empty:
        return {"status": "ok", "message": "A fájl üres volt, nem történt betöltés."}

    # Metaadat oszlop hozzáadása
    df["data_source_meta"] = source_name

    try:
        pipeline = dlt.pipeline(pipeline_name="azure_blob_pipeline", destination=dlt.destinations.postgres(credentials=DB_URL), dataset_name="bronze_azure_blob")

        run_kwargs = {"table_name": table_name, "write_disposition": write_disposition}
        if primary_key and write_disposition == "merge":
            run_kwargs["primary_key"] = primary_key

        load_info = pipeline.run(df, **run_kwargs)
        load_info.raise_on_failed_jobs()

        return {
            "status": "success",
            "container": container_name,
            "blob_name": blob_name,
            "rows_processed": len(df),
            "target_table": f"raw_external_data.{table_name}",
            "dlt_load_ids": load_info.loads_ids,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dlt betöltési hiba: {str(e)}")
