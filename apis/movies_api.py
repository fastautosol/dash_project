# 2026.05.18 18.00
from fastapi import APIRouter
import requests
import json
import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from datetime import datetime
from pydantic import BaseModel
from typing import List

class MoviesRequest(BaseModel):
    imdb_ids: List[str]

API_KEY = "86fa3341"
BASE_URL = "http://www.omdbapi.com/"
DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}

router = APIRouter()

@dlt.resource(name="omdb_movies", max_table_nesting=0)
def movies_resource(data):
    yield from data

# -----------------------------------------------------------------------------
@router.post("/")
def get_omdb_movies(req: MoviesRequest):
    imdb_ids = req.imdb_ids
    data = []
    for imdb_id in imdb_ids:
        response = requests.get(BASE_URL, params={
            "i": imdb_id,
            "apikey": API_KEY
        })
        response.raise_for_status()
        movie = response.json()
        movie["_ingested_at"] = datetime.utcnow().isoformat()
        data.append(movie)

    # -----------------------------------------------------------------------------
    pipeline = dlt.pipeline(
        pipeline_name="omdb_movies_ingest", 
        destination=dlt.destinations.postgres(credentials=DB_CONFIG), 
        dataset_name="bronze")
    
    try:
        load_info = pipeline.run(movies_resource(clean_data), write_disposition="merge", primary_key=["movies_id"])

    except PipelineStepFailed as e:    
        if e.step == "load" or "does not exist" in str(e).lower():
            pipeline.drop_pending_packages()
            load_info = pipeline.run(movies_resource(clean_data), write_disposition="append")
        else:
            raise

    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    
    return {"rows": len(data), "status": "loaded", "load_info": str(load_info), "sample": data[:3]}
