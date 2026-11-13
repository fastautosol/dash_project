from fastapi import FastAPI, HTTPException
import pandas as pd
import json
import io
import dlt
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient

app = FastAPI(title="Azure Blob Stream to dlt Pipeline API")

# 1. Konfigurációk (Ezeket érdemes a Coolify Környezeti Változói közé (Environment Variables) tenni)
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=your_account;AccountKey=your_key;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "client-raw-data"

# SQLAlchemy engine az adatbázis kapcsolatokhoz
engine = create_engine(DB_URL, pool_pre_ping=True)

# Azure Blob Service kliens inicializálása
try:
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
except Exception as e:
    print(f"Súlyos hiba az Azure Blob kliens inicializálásakor: {str(e)}")

@app.post("/process-azure-blob/")
async def process_azure_blob(blob_name: str, source_name: str):
    """
    Közvetlenül az Azure Blob Storage-ból olvassa be a fájlt streamelve, 
    majd dlt-vel betölti a PostgreSQL-be.
    :param blob_name: A fájl pontos elérése a containeren belül (pl: 'incoming/sales_2026_11.csv')
    :param source_name: Metaadat a dlt-nek az adat eredetéről (pl: 'Shopify_Kaggle')
    """
    try:
        # 2. Kapcsolódás a specifikus blobhoz a konténeren belül
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        
        if not blob_client.exists():
            raise HTTPException(status_code=404, detail=f"A megadott blob nem található: {blob_name}")
            
        # 3. Streamelés indítása: Nem töltjük le az egész fájlt a VPS lemezére, hanem memóriastreamként kezeljük
        blob_data = blob_client.download_blob()
        stream = io.BytesIO()
        blob_data.readinto(stream)
        stream.seek(0)  # Visszaállítjuk a stream elejére a mutatót az olvasáshoz
        
        # 4. Fájltípus detektálása a kiterjesztés alapján és beolvasás Pandas segítségével
        if blob_name.endswith('.csv'):
            df = pd.read_csv(stream)
            data_to_load = df.to_dict(orient="records")
            
        elif blob_name.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(stream)
            data_to_load = df.to_dict(orient="records")
            
        elif blob_name.endswith('.json'):
            # JSON esetében szöveges streamként olvassuk be
            stream_text = io.StringIO(stream.getvalue().decode('utf-8'))
            data_to_load = json.load(stream_text)
            if isinstance(data_to_load, dict):
                data_to_load = [data_to_load]
        else:
            raise HTTPException(status_code=400, detail="Nem támogatott fájlformátum az Azure-ban. Csak CSV, Excel vagy JSON feldolgozható.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Hiba az Azure Blob letöltése/streamelése során: {str(e)}")

    if not data_to_load:
        return {"status": "Sikeres", "üzenet": "A fájl üres volt, nem történt betöltés."}

    # Metaadat hozzáadása
    for record in data_to_load:
        record["data_source_meta"] = source_name

    # 5. dlt Pipeline futtatása MERGE stratégiával
    try:
        pipeline = dlt.pipeline(
            pipeline_name="azure_kkv_pipeline",
            destination=dlt.destinations.postgres(credentials=DB_URL),
            dataset_name="sales_data"
        )

        # Összefésülés order_id alapján
        load_info = pipeline.run(
            data_to_load,
            table_name="orders",
            write_disposition="merge",
            primary_key="order_id"
        )
        
        return {
            "status": "Sikeresen feldolgozva",
            "feldolgozott_blob": blob_name,
            "dlt_load_id": load_info.loads.load_id if load_info.loads else "N/A",
            "üzenet": "Az Azure-ból streamelt fájl sikeresen bekerült a PostgreSQL-be."
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dlt csővezeték hiba: {str(e)}")
