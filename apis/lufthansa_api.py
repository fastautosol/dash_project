# ============================================================
# Lufthansa API -> DLT -> PostgreSQL
#
# Bronze tables:
#   bronze.lh_flights
#   bronze.lh_schedule
#
# APIs:
#   1. Customer Flight Information
#   2. Flight Schedules
# ============================================================

import os
import json
import httpx
import asyncio
import requests
import dlt
import logging
from datetime import datetime, timezone
from fastapi import APIRouter, BackgroundTasks
from dlt.pipeline.exceptions import PipelineStepFailed

logger = logging.getLogger("lufthansa_api")

router = APIRouter()

DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15}
LH_BASE_URL = "https://api.lufthansa.com/v1"
MAX_CONCURRENT_REQUESTS = 5
REQUEST_DELAY = 0.5

# =========================== ROUTES =================================

ROUTES_FULL = [

    ("FRA", "SIN"),        ("MUC", "LAX"),        ("HAM", "FRA"),        ("BER", "FRA"),
    ("FRA", "HND"),        ("MUC", "SFO"),        ("HAM", "MUC"),        ("BER", "MUC"),
    ("FRA", "LAX"),        ("MUC", "DEN"),        ("HAM", "LHR"),        ("BER", "LHR"),
    ("FRA", "JFK"),        ("MUC", "ORD"),        ("HAM", "CDG"),        ("BER", "CDG"),
    ("FRA", "EWR"),        ("MUC", "EWR"),        ("HAM", "AMS"),        ("BER", "AMS"),
    ("FRA", "ORD"),        ("MUC", "JFK"),        ("HAM", "MAD"),        ("BER", "MAD"),
    ("FRA", "IAD"),        ("MUC", "BOS"),        ("HAM", "BCN"),        ("BER", "BCN"),
    ("FRA", "BOS"),        ("MUC", "DEL"),        ("HAM", "LIS"),        ("BER", "LIS"),
    ("FRA", "DEN"),        ("MUC", "BOM"),        ("HAM", "ATH"),        ("BER", "ATH"),
    ("FRA", "SFO"),        ("MUC", "BLR"),        ("HAM", "BER"),        ("BER", "VIE"),
    ("FRA", "MIA"),        ("MUC", "BKK"),        ("HAM", "VIE"),        ("BER", "ZRH"),
    ("FRA", "YYZ"),        ("MUC", "JNB"),        ("HAM", "ZRH"),        ("BER", "CPH"),
    ("FRA", "MEX"),        ("MUC", "CPT"),        ("HAM", "CPH"),        ("BER", "OSL"),
    ("FRA", "DEL"),        ("MUC", "DXB"),        ("HAM", "OSL"),        ("BER", "HEL"),
    ("FRA", "BOM"),        ("MUC", "LHR"),        ("HAM", "HEL"),        ("BER", "WAW"),
    ("FRA", "BLR"),        ("MUC", "CDG"),        ("HAM", "WAW"),        ("BER", "PRG"),
    ("FRA", "HYD"),        ("MUC", "AMS"),        ("HAM", "PRG"),        ("BER", "BUD"),
    ("FRA", "ICN"),        ("MUC", "MAD"),        ("HAM", "BUD"),        ("BER", "FCO"),
    ("FRA", "GRU"),        ("MUC", "BCN"),        ("HAM", "FCO"),        ("BER", "MXP"),
    ("FRA", "DXB"),        ("MUC", "LIS"),        ("HAM", "MXP"),        ("BER", "MAN"),
    ("FRA", "CAI"),        ("MUC", "ATH"),        ("HAM", "MAN"),        ("BER", "DUB"),
    ("FRA", "TLV"),        ("MUC", "BER"),        ("HAM", "DUB"),
    ("FRA", "BEY"),        ("MUC", "HAM"),
    ("FRA", "LHR"),        ("MUC", "DUS"),
    ("FRA", "LCY"),        ("MUC", "FRA"),
    ("FRA", "CDG"),        ("MUC", "VIE"),
    ("FRA", "AMS"),        ("MUC", "ZRH"),
    ("FRA", "MAD"),        ("MUC", "CPH"),
    ("FRA", "BCN"),        ("MUC", "OSL"),
    ("FRA", "LIS"),        ("MUC", "WAW"),
    ("FRA", "ATH"),        ("MUC", "PRG"),
    ("FRA", "IST"),        ("MUC", "BUD"),
    ("FRA", "BER"),        ("MUC", "FCO"),
    ("FRA", "HAM"),        ("MUC", "MXP"),
    ("FRA", "DUS"),        ("MUC", "MAN"),
    ("FRA", "MUC"),        ("MUC", "DUB"),
    ("FRA", "VIE"),        ("MUC", "TLV"),
    ("FRA", "ZRH"),
    ("FRA", "CPH"),
    ("FRA", "OSL"),
    ("FRA", "HEL"),
    ("FRA", "WAW"),
    ("FRA", "PRG"),
    ("FRA", "BUD"),
    ("FRA", "MXP"),
    ("FRA", "TLS"),
    ("FRA", "MAN"),
    ("FRA", "DUB"),
]


# ============================================================
# AUTHENTICATION
# ============================================================

def get_lufthansa_token():

    client_id = os.getenv("LH_CLIENT_ID")
    client_secret = os.getenv("LH_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError("LH_CLIENT_ID / LH_CLIENT_SECRET environment variables are missing.")

    token_url = f"{LH_BASE_URL}/oauth/token"
    payload = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    response = requests.post(token_url, data=payload, timeout=30)
    response.raise_for_status()

    return response.json()["access_token"]

# ============================================================
# HELPERS
# ============================================================

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize_date_lh(value):
    if not value:
        return None

    value = value.strip()

    try:
        return datetime.strptime(value, "%d%b%y").date().isoformat()

    except ValueError:
        return value
        
def minutes_to_hhmm(minutes):
    if minutes is None:
        return None

    try:
        minutes = int(minutes)

        days = minutes // 1440
        mins = minutes % 1440

        hours = mins // 60
        minutes = mins % 60

        time_string = f"{hours:02d}:{minutes:02d}"

        if days > 0:
            return f"{time_string}+{days}d"

        return time_string

    except (ValueError, TypeError):
        return None


def extract_data_elements(data_elements):
    result = {
        "marketing_flights": None,
        "arrival_terminal": None,
        "departure_terminal": None,
    }

    raw_elements = []

    for element in data_elements or []:

        element_id = element.get("id")
        value = element.get("value")

        raw_elements.append(element)

        if element_id == 10:
            result["marketing_flights"] = value

        elif element_id == 98:
            result["arrival_terminal"] = value

        elif element_id == 99:
            result["departure_terminal"] = value

    result["data_elements_raw"] = raw_elements

    return result


# ============================================================
# FLIGHT INFORMATION API
# ============================================================

async def fetch_route(
    client,
    token,
    origin,
    dest,
    flight_date,
    sem,
):

    url = (
        f"{LH_BASE_URL}"
        f"/operations/customerflightinformation"
        f"/route/{origin}/{dest}/{flight_date}"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    async with sem:

        try:

            response = await client.get(
                url,
                headers=headers,
                timeout=30,
            )

            await asyncio.sleep(REQUEST_DELAY)

            if response.status_code != 200:
                logger.warning(
                    f"Flight API error {origin}-{dest}: {response.status_code}"
                )
                return []

            json_data = response.json()

            flights = (
                json_data
                .get("FlightInformation", {})
                .get("Flights", {})
                .get("Flight", [])
            )

            if not flights:
                return []

            if isinstance(flights, dict):
                flights = [flights]

            for flight in flights:

                flight["route_key"] = (
                    f"{origin}-{dest}"
                )

            return flights

        except Exception as e:
            logger.warning(
                f"Flight API exception {origin}-{dest}: {e}"
            )
            return []


# ============================================================
# FLIGHT SCHEDULE API
# ============================================================

async def fetch_schedule(
    client,
    token,
    origin,
    dest,
    start_date,
    end_date,
    sem,
):

    url = (
        f"{LH_BASE_URL}"
        f"/flight-schedules/flightschedules/passenger"
    )

    params = {
        "airlines": "LH",
        "startDate": start_date,
        "endDate": end_date,
        "daysOfOperation": "1234567",
        "timeMode": "UTC",
        "origin": origin,
        "destination": dest,
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    async with sem:

        try:

            response = await client.get(
                url,
                headers=headers,
                params=params,
                timeout=30,
            )

            await asyncio.sleep(REQUEST_DELAY)

            if response.status_code != 200:
                logger.warning(
                    f"Schedule API error {origin}-{dest}: {response.status_code}"
                )
                return []

            data = response.json()

            if isinstance(data, list):
                schedules = data

            elif isinstance(data, dict):

                schedules = (
                    data.get("flightSchedules")
                    or data.get("FlightSchedules")
                    or data.get("schedules")
                    or data.get("data")
                    or []
                )

            else:
                schedules = []

            return normalize_schedule_response(
                schedules=schedules,
                origin=origin,
                destination=dest,
                requested_start=start_date,
                requested_end=end_date,
            )

        except Exception as e:
            logger.warning(
                f"Schedule API exception {origin}-{dest}: {e}"
            )
            return []


# ============================================================
# NORMALIZE SCHEDULE RESPONSE
# ============================================================

def normalize_schedule_response(
    schedules,
    origin,
    destination,
    requested_start,
    requested_end,
):

    rows = []

    ingestion_time = utc_now_iso()

    for schedule in schedules:

        airline = schedule.get("airline")
        flight_number = schedule.get("flightNumber")
        suffix = schedule.get("suffix", "")

        period_utc = schedule.get(
            "periodOfOperationUTC",
            {},
        )

        period_lt = schedule.get(
            "periodOfOperationLT",
            {},
        )

        period_start = normalize_date_lh(
            period_utc.get("startDate")
        )

        period_end = normalize_date_lh(
            period_utc.get("endDate")
        )

        days_of_operation = (
            period_utc
            .get("daysOfOperation", "")
            .strip()
        )

        legs = schedule.get("legs", [])

        data_elements = schedule.get(
            "dataElements",
            [],
        )

        element_info = extract_data_elements(
            data_elements
        )

        for leg in legs:

            leg_origin = leg.get(
                "origin",
                origin,
            )

            leg_destination = leg.get(
                "destination",
                destination,
            )

            sequence_number = leg.get(
                "sequenceNumber"
            )

            flight_key = (
                f"{airline}_"
                f"{flight_number}"
                f"{suffix}_"
                f"{period_start}_"
                f"{leg_origin}_"
                f"{leg_destination}_"
                f"{sequence_number}"
            )

            marketing_flights = (
                element_info[
                    "marketing_flights"
                ]
            )

            row = {
                "flight_key": flight_key,
                "airline": airline,
                "flight_number": flight_number,
                "flight_suffix": suffix,
                "operating_carrier": leg.get("aircraftOwner"),
                "origin": leg_origin,
                "destination": leg_destination,
                "route_key": f"{leg_origin}-{leg_destination}",
                "sequence_number": sequence_number,
                "schedule_start_date": period_start,
                "schedule_end_date": period_end,
                "days_of_operation": days_of_operation,
                "schedule_start_date_lt": normalize_date_lh(period_lt.get("startDate")),
                "schedule_end_date_lt": normalize_date_lh(period_lt.get("endDate")),
                "aircraft_type": leg.get("aircraftType"),
                "aircraft_owner": leg.get("aircraftOwner"),
                "aircraft_configuration": leg.get("aircraftConfigurationVersion"),
                "registration": leg.get("registration"),
                "service_type": leg.get("serviceType"),
                "operating": leg.get("op"),
                "departure_time_utc": minutes_to_hhmm(leg.get("aircraftDepartureTimeUTC")),
                "departure_date_diff_utc": leg.get("aircraftDepartureTimeDateDiffUTC"),
                "departure_time_lt": minutes_to_hhmm(leg.get("aircraftDepartureTimeLT")),
                "departure_date_diff_lt": leg.get("aircraftDepartureTimeDateDiffLT"),
                "arrival_time_utc": minutes_to_hhmm(leg.get("aircraftArrivalTimeUTC")),
                "arrival_date_diff_utc": leg.get("aircraftArrivalTimeDateDiffUTC"),
                "arrival_time_lt": minutes_to_hhmm(leg.get("aircraftArrivalTimeLT")),
                "arrival_date_diff_lt": leg.get("aircraftArrivalTimeDateDiffLT"),
                "departure_variation_min": leg.get("aircraftDepartureTimeVariation"),
                "arrival_variation_min": leg.get("aircraftArrivalTimeVariation"),
                "marketing_flights": marketing_flights,
                "departure_terminal": element_info["departure_terminal"],
                "arrival_terminal": element_info["arrival_terminal"],
                "data_elements_raw": json.dumps(element_info["data_elements_raw"]),
                "requested_start_date": normalize_date_lh(requested_start),
                "requested_end_date": normalize_date_lh(requested_end),
                "_ingested_at": ingestion_time,
            }

            rows.append(row)

    return rows


# ============================================================
# DLT RESOURCES & PIPELINE
# ============================================================

@dlt.resource(
    name="lh_flights",
    write_disposition="merge",
)
def flights_resource(rows: list[dict]):
    for row in rows:
        yield row


@dlt.resource(
    name="lh_schedule",
    write_disposition="merge",
)
def schedule_resource(rows: list[dict]):
    for row in rows:
        yield row


def get_pipeline():
    return dlt.pipeline(
        pipeline_name="lufthansa_ingest",
        destination=dlt.destinations.postgres(
            credentials=DB_CONFIG
        ),
        dataset_name="bronze",
    )


# ============================================================
# ASYNC FETCH WORKER
# ============================================================

async def fetch_lufthansa_data_async(flight_date: str):
    token = get_lufthansa_token()
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    parsed_date = datetime.strptime(flight_date, "%Y-%m-%d")
    schedule_start = parsed_date.strftime("%d%b%y").upper()
    schedule_end = schedule_start

    async with httpx.AsyncClient(timeout=90) as client:
        flight_tasks = [
            fetch_route(client, token, origin, destination, flight_date, sem)
            for origin, destination in ROUTES_FULL
        ]
        schedule_tasks = [
            fetch_schedule(client, token, origin, destination, schedule_start, schedule_end, sem)
            for origin, destination in ROUTES_FULL
        ]

        flight_results, schedule_results = await asyncio.gather(
            asyncio.gather(*flight_tasks),
            asyncio.gather(*schedule_tasks)
        )

    flight_data = [item for sublist in flight_results if sublist for item in sublist]
    schedule_data = [item for sublist in schedule_results if sublist for item in sublist]

    for row in flight_data:
        row["_ingested_at"] = utc_now_iso()

    ALLOWED_FIELDS = {
        "Status", "Equipment", "Departure", "Arrival",
        "AircraftDetails", "MarketingCarrierList", "OperatingCarrier",
        "route_key", "_ingested_at",
    }

    clean_flights = []
    for row in flight_data:
        filtered = {k: v for k, v in row.items() if k in ALLOWED_FIELDS}
        operating = row.get("OperatingCarrier", {})
        departure = row.get("Departure", {})
        scheduled = departure.get("Scheduled", {})

        airline = operating.get("AirlineID")
        flight_number = operating.get("FlightNumber")
        flight_date_value = scheduled.get("Date")

        if airline and flight_number and flight_date_value:
            filtered["flight_key"] = f"{airline}_{flight_number}_{flight_date_value}"

        clean_flights.append(filtered)

    return clean_flights, schedule_data


# ============================================================
# BACKGROUND TASK PIPELINE RUNNER
# ============================================================

def run_dlt_pipeline(flight_date: str):
    try:
        logger.info("Starting background Lufthansa pipeline for date=%s", flight_date)
        clean_flights, schedule_data = asyncio.run(fetch_lufthansa_data_async(flight_date))

        if not clean_flights and not schedule_data:
            logger.info("No data collected for date=%s", flight_date)
            return

        pipeline = get_pipeline()

        if clean_flights:
            try:
                load_info = pipeline.run(
                    flights_resource(clean_flights),
                    write_disposition="merge",
                    primary_key=["flight_key"],
                )
                logger.info("Flights DLT pipeline finished: %s", load_info)
            except PipelineStepFailed as e:
                logger.error("Flight DLT pipeline error, falling back to append: %s", e)
                pipeline.drop_pending_packages()
                load_info = pipeline.run(
                    flights_resource(clean_flights),
                    write_disposition="append",
                )
                logger.info("Flights DLT pipeline fallback finished: %s", load_info)

        if schedule_data:
            try:
                load_info = pipeline.run(
                    schedule_resource(schedule_data),
                    write_disposition="merge",
                    primary_key=["flight_key"],
                )
                logger.info("Schedule DLT pipeline finished: %s", load_info)
            except PipelineStepFailed as e:
                logger.error("Schedule DLT pipeline error, falling back to append: %s", e)
                pipeline.drop_pending_packages()
                load_info = pipeline.run(
                    schedule_resource(schedule_data),
                    write_disposition="append",
                )
                logger.info("Schedule DLT pipeline fallback finished: %s", load_info)

    except Exception as e:
        logger.exception("Lufthansa pipeline failed for date=%s: %s", flight_date, e)


# ============================================================
# FASTAPI ENDPOINT
# ============================================================

@router.get("/flights/{flight_date}")
async def get_flightroute_details(
    flight_date: str,
    background_tasks: BackgroundTasks
):
    try:
        datetime.strptime(flight_date, "%Y-%m-%d")
    except ValueError:
        return {
            "status": "error",
            "message": "flight_date must be YYYY-MM-DD",
        }

    background_tasks.add_task(run_dlt_pipeline, flight_date)

    return {
        "status": "success",
        "message": f"Lufthansa data ingestion started in background for date {flight_date}"
    }
