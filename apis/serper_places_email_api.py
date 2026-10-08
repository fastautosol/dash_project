# 2026.10.08 15.00

from fastapi import APIRouter, BackgroundTasks
import asyncio
import aiohttp
import re
import os
import json
import dlt
import logging

from datetime import datetime
from pydantic import BaseModel
from bs4 import BeautifulSoup

logger = logging.getLogger("serper_places_email")

class SerperRequest(BaseModel):
    city: str
    limit: int = 10


SERPER_KEY = os.getenv("SERPER_API_KEY")
DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}

router = APIRouter()

EMAIL_REGEX = r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"

# -----------------------------------------------------------------------------
# DLT RESOURCE
# -----------------------------------------------------------------------------

@dlt.resource(name="companies_email", max_table_nesting=0)
def companies_resource(data):
    yield from data


# -----------------------------------------------------------------------------
# EMAIL HELPERS
# -----------------------------------------------------------------------------

async def scrape_emails_from_url(session: aiohttp.ClientSession, url: str) -> list[str]:

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=7), headers={"User-Agent": "Mozilla/5.0"}) as resp:

            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            emails = re.findall(EMAIL_REGEX, soup.get_text())

            return list(set(emails))

    except Exception:
        return []


async def search_email_via_serper(session: aiohttp.ClientSession, company_name: str, address: str) -> list[str]:

    try:
        payload = {"q": f"{company_name} {address} email contact"}

        async with session.post("https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"}, json=payload, timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:

            data = await resp.json()
            emails = re.findall(EMAIL_REGEX, json.dumps(data))

            return list(set(emails))

    except Exception:
        return []


async def find_emails(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    website: str,
    company_name: str,
    address: str
) -> str:

    async with semaphore:

        emails = []

        if website and website != "UNKNOWN":
            emails = await scrape_emails_from_url(
                session,
                website
            )

        if not emails:
            emails = await search_email_via_serper(
                session,
                company_name,
                address
            )

        return ", ".join(emails) if emails else "UNKNOWN"


# -----------------------------------------------------------------------------
# ENRICH COMPANY
# -----------------------------------------------------------------------------

async def enrich_company(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    record: dict
) -> dict:

    record["email"] = await find_emails(
        session,
        semaphore,
        record["website"],
        record["name"],
        record["address"]
    )

    return record


# -----------------------------------------------------------------------------
# FETCH SERPER DATA
# -----------------------------------------------------------------------------

async def fetch_serper_async(
    city: str,
    limit: int
):

    url = "https://google.serper.dev/places"

    variations = [
        "shipping",
        "transport",
        "logistics",
        "freight forwarding",
        "warehouse",
        "shopping",
        "delivery",
        "factory",
        "producing",
        "transportation",
        "apartment",
        "hotel",
        "guesthouse",
        "car service",
        "restaurant",
        "company",
        "business",
        "office",
        "IT company"
    ]

    results_map = {}

    async with aiohttp.ClientSession() as session:

        # ---------------------------------------------------------
        # STEP 1 - SERPER PLACES
        # ---------------------------------------------------------

        for v in variations:

            if len(results_map) >= limit:
                break

            payload = {
                "q": f"{v} in {city}",
                "gl": "hu",
                "hl": "hu"
            }

            async with session.post(
                url,
                headers={
                    "X-API-KEY": SERPER_KEY,
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:

                resp.raise_for_status()

                places = (
                    await resp.json()
                ).get("places", [])

            for p in places:

                key = (
                    p.get("title"),
                    p.get("address")
                )

                if key not in results_map:

                    name = p.get("title") or "UNKNOWN"
                    address = p.get("address") or "UNKNOWN"
                    website = p.get("website") or "UNKNOWN"

                    results_map[key] = {
                        "cid": p.get("cid"),
                        "name": name,
                        "address": address,
                        "rating": p.get("rating") or "UNKNOWN",
                        "reviews": p.get("ratingCount") or "UNKNOWN",
                        "latitude": p.get("latitude") or "UNKNOWN",
                        "longitude": p.get("longitude") or "UNKNOWN",
                        "category": p.get("category") or "UNKNOWN",
                        "phoneNumber": p.get("phoneNumber") or "UNKNOWN",
                        "website": website,
                        "email": "UNKNOWN",
                        "city": city,
                        "_ingested_at": datetime.utcnow().isoformat()
                    }

        records = list(results_map.values())[:limit]

        logger.info(
            "Collected %s companies for city=%s",
            len(records),
            city
        )

        # ---------------------------------------------------------
        # STEP 2 - EMAIL ENRICHMENT
        # ---------------------------------------------------------

        semaphore = asyncio.Semaphore(5)

        tasks = [
            enrich_company(
                session,
                semaphore,
                r
            )
            for r in records
        ]

        enriched = await asyncio.gather(*tasks)

        logger.info(
            "Email enrichment finished. Records=%s",
            len(enriched)
        )

        return enriched


# -----------------------------------------------------------------------------
# BACKGROUND PIPELINE
# -----------------------------------------------------------------------------

def run_dlt_pipeline(
    city: str,
    limit: int
):

    try:

        logger.info(
            "Starting background pipeline for city=%s limit=%s",
            city,
            limit
        )

        data = asyncio.run(fetch_serper_async(city, limit))

        pipeline = dlt.pipeline(
            pipeline_name="serper_companies",
            destination=dlt.destinations.postgres(credentials=DB_CONFIG),
            dataset_name="serper")

        load_info = pipeline.run(companies_resource(data), write_disposition="merge", primary_key=["name", "address"])
        logger.info("DLT pipeline finished successfully: %s", load_info)

    except Exception as e:

        logger.exception("Pipeline failed: %s", e)


# -----------------------------------------------------------------------------
# API ENDPOINT
# -----------------------------------------------------------------------------

@router.post("/")
async def get_companies(req: SerperRequest, background_tasks: BackgroundTasks):

    background_tasks.add_task(run_dlt_pipeline, req.city, req.limit)

    return {
        "status": "success",
        "message": f"Company search started in background for {req.city}"
    }
