# 2026.05.11  15.00
from fastapi import APIRouter
import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
import random
import uuid
import json
import os
from datetime import datetime, timezone, timedelta

DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}
#access_token = "shpat_your_admin_api_token"
#shop_url = "https://your-shop-name.myshopify.com"

router = APIRouter()

# ---------------------------------------------------------------------------------------------
@dlt.resource(name="tickets", max_table_nesting=0)
def tickets_resource(rows: list[dict]):
    for r in rows:
        yield r

@router.get("/")
def generate_crm():

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    JSON_PATH = os.path.join(BASE_DIR, "generated_tickets.json")

    with open(JSON_PATH, "r", encoding="utf-8") as f:
        generated_data = json.load(f)
    
    def fake_ticket():
        return {
        "ticket_id": random.randint(1000000000, 9999999999), 
        "total_price": round(random.uniform(10, 200), 5),
        "subtotal_price": round(random.uniform(10, 400), 2),
        "total_tax": round(random.uniform(1, 50), 2),       
        "currency": "USD",
        "financial_status": random.choice(["paid", "refunded", "pending", "partially_paid"]),
        "fulfillment_status": random.choice(["fulfilled", "unfulfilled", "partial", None]),
        "order_number": random.randint(1001, 9999),
        "customer": {
            "id": str(uuid.uuid4()),
            "email": f"customer{random.randint(1, 999)}@{random.choice(["example.com", "yahoo.com", "gmail.com", "telnet.com"])}",
            "first_name": random.choice(["Alice", "Bob", "Carol", "Dave", "John", "Jane", "Mark", "Sarah", "Emily", "Michael"]),
            "last_name": random.choice(["Smith", "Jones", "Brown", "Wilson", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris"]),
            "orders_count": random.randint(1, 25),
            "total_spent": str(round(random.uniform(50, 5000), 2)),
            "address": f"{random.randint(1, 999)} {random.choice(["Main St", "Park Ave", "Center St", "High St", "Cedar St", "Park Rd", "Center Rd"])}",
            "city": random.choice(["New York", "London", "Berlin", "Budapest", "Paris", "Tokyo", "Sydney", "Prague", "Mumbai", "Beijing"]),
            "country": random.choice(["US", "GB", "DE", "HU", "FR", "JP", "AU", "RU", "IN", "CN"]),
            "zip": str(random.randint(10000, 99999)),
        },
        "message": random.choice(generated_data)["message"],
        "intent": random.choice(generated_data)["intent"],
        "created_at": (datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 10800))).isoformat(),
        "processed": False
    }

    data = [fake_ticket() for _ in range(250)]

    if not data:
        return {"status": "no_data"}

    for row in data:
        row["_ingested_at"] = datetime.utcnow().isoformat()

    # -----------------------------------------------------------------------------
    pipeline = dlt.pipeline(
        pipeline_name="fake_shopify",
        destination=dlt.destinations.postgres(credentials=DB_CONFIG),
        dataset_name="crm_shopify")

    try:
        load_info = pipeline.run(tickets_resource(data), write_disposition="merge", primary_key=["ticket_id", "created_at"])

    except PipelineStepFailed as e:    
        if e.step == "load" or "does not exist" in str(e).lower():
            load_info = pipeline.run(tickets_resource(data), write_disposition="append")
        else:
            raise

    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

    return {"loaded": len(data), "load_info": str(load_info), "sample": data[:5]}
        
    #load_info = pipeline.run([ dlt.resource(fake_tickets(), name="tickets") ])
    #load_info = pipeline.run(dlt.resource(tickets2, name="tickets"))
