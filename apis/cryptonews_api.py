# 2026.05.18 18.00
from fastapi import APIRouter
import requests
import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from datetime import datetime
from pydantic import BaseModel
from typing import List

class CryptoNewsRequest(BaseModel):
    categories: List[str]

BASE_URL = "https://min-api.cryptocompare.com/data/v2/news/"
DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}

router = APIRouter()

@dlt.resource(name="cryptonews", max_table_nesting=0)
def cryptonews_resource(data):
    yield from data

# -----------------------------------------------------------------------------
@router.post("/")
def get_cryptonews(req: CryptoNewsRequest):
    categories_str = ",".join(req.categories) 
    response = requests.get(BASE_URL, params={"categories": categories_str, "excludeCategories": "Sponsored", "lang": "EN"})
    response.raise_for_status()
    raw = response.json()
    articles = raw.get("Data", [])

    data = []
    for article in articles:
        article["_ingested_at"] = datetime.utcnow().isoformat()
        article["_categories_queried"] = categories_str
        data.append(article)

    # -----------------------------------------------------------------------------
    pipeline = dlt.pipeline(
        pipeline_name="cryptonews_ingest",
        destination=dlt.destinations.postgres(credentials=DB_CONFIG),
        dataset_name="bronze")

    try:
        load_info = pipeline.run(cryptonews_resource(data), write_disposition="merge", primary_key=["id"])

    except PipelineStepFailed as e:
        if e.step == "load" or "does not exist" in str(e).lower():
            pipeline.drop_pending_packages()
            load_info = pipeline.run(cryptonews_resource(data), write_disposition="append")
        else:
            raise

    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

    return {"rows": len(data), "status": "loaded", "load_info": str(load_info), "sample": data[:3]}
