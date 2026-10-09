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
#
# ============================================================

import os
import json
import httpx
import asyncio
import requests
import dlt

from datetime import datetime, timezone

from fastapi import APIRouter
from dlt.pipeline.exceptions import PipelineStepFailed


router = APIRouter()


# ============================================================
# CONFIGURATION
# ============================================================

DB_CONFIG = {
    "host": "postgresql",
    "port": 5432,
    "database": "n8n",
    "username": "sql_admin",
    "password": "sql_pass",
    "connect_timeout": 15,
}

LH_BASE_URL = "https://api.lufthansa.com/v1"

MAX_CONCURRENT_REQUESTS = 4
REQUEST_DELAY = 0.5


# ============================================================
# ROUTES
# ============================================================

ROUTES_FULL = [

    # --------------------------------------------------------
    # FRA - Long Haul
    # --------------------------------------------------------

    ("FRA", "SIN"),
    ("FRA", "HND"),
    ("FRA", "LAX"),
    ("FRA", "JFK"),
    ("FRA", "EWR"),
    ("FRA", "ORD"),
    ("FRA", "IAD"),
    ("FRA", "BOS"),
    ("FRA", "DEN"),
    ("FRA", "SFO"),
    ("FRA", "MIA"),
    ("FRA", "YYZ"),
    ("FRA", "MEX"),
    ("FRA", "DEL"),
    ("FRA", "BOM"),
    ("FRA", "BLR"),
    ("FRA", "HYD"),
    ("FRA", "ICN"),
    ("FRA", "GRU"),
    ("FRA", "DXB"),
    ("FRA", "CAI"),
    ("FRA", "TLV"),
    ("FRA", "BEY"),

    # --------------------------------------------------------
    # FRA - Europe
    # --------------------------------------------------------

    ("FRA", "LHR"),
    ("FRA", "LCY"),
    ("FRA", "CDG"),
    ("FRA", "AMS"),
    ("FRA", "MAD"),
    ("FRA", "BCN"),
    ("FRA", "LIS"),
    ("FRA", "ATH"),
    ("FRA", "IST"),
    ("FRA", "BER"),
    ("FRA", "HAM"),
    ("FRA", "DUS"),
    ("FRA", "MUC"),
    ("FRA", "VIE"),
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

    # --------------------------------------------------------
    # MUC - Long Haul
    # --------------------------------------------------------

    ("MUC", "LAX"),
    ("MUC", "SFO"),
    ("MUC", "DEN"),
    ("MUC", "ORD"),
    ("MUC", "EWR"),
    ("MUC", "JFK"),
    ("MUC", "BOS"),
    ("MUC", "DEL"),
    ("MUC", "BOM"),
    ("MUC", "BLR"),
    ("MUC", "BKK"),
    ("MUC", "JNB"),
    ("MUC", "CPT"),
    ("MUC", "DXB"),

    # --------------------------------------------------------
    # MUC - Europe
    # --------------------------------------------------------

    ("MUC", "LHR"),
    ("MUC", "CDG"),
    ("MUC", "AMS"),
    ("MUC", "MAD"),
    ("MUC", "BCN"),
    ("MUC", "LIS"),
    ("MUC", "ATH"),
    ("MUC", "BER"),
    ("MUC", "HAM"),
    ("MUC", "DUS"),
    ("MUC", "FRA"),
    ("MUC", "VIE"),
    ("MUC", "ZRH"),
    ("MUC", "CPH"),
    ("MUC", "OSL"),
    ("MUC", "WAW"),
    ("MUC", "PRG"),
    ("MUC", "BUD"),
    ("MUC", "FCO"),
    ("MUC", "MXP"),
    ("MUC", "MAN"),
    ("MUC", "DUB"),
    ("MUC", "TLV"),
]


# ============================================================
# AUTHENTICATION
# ============================================================

def get_lufthansa_token():

    client_id = os.getenv("LH_CLIENT_ID")
    client_secret = os.getenv("LH_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError(
            "LH_CLIENT_ID / LH_CLIENT_SECRET environment variables are missing."
        )

    token_url = f"{LH_BASE_URL}/oauth/token"

    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(
        token_url,
        data=payload,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()["access_token"]


# ============================================================
# HELPERS
# ============================================================

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize_date_lh(value):
    """
    Lufthansa schedule date format:

        29SEP26

    ->

        2026-09-29
    """

    if not value:
        return None

    value = value.strip()

    try:
        return datetime.strptime(
            value,
            "%d%b%y"
        ).date().isoformat()

    except ValueError:
        return value


def minutes_to_hhmm(minutes):
    """
    Lufthansa schedule API stores times as minutes
    since midnight.

    Example:

        435 -> 07:15
        530 -> 08:50
    """

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
    """
    Convert Lufthansa dataElements list into useful fields.

    Known IDs from the sample:

        10  = marketing/codeshare flights
        98  = arrival terminal
        99  = departure terminal

    Unknown elements are kept separately.
    """

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
                print(
                    f"Flight API error "
                    f"{origin}-{dest}: "
                    f"{response.status_code}"
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

            print(
                f"Flight API exception "
                f"{origin}-{dest}: {e}"
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

    """
    Lufthansa Schedule API.

    Example:

    /flight-schedules/flightschedules/passenger
        ?airlines=LH
        &startDate=29SEP26
        &endDate=30SEP26
        &daysOfOperation=1234567
        &timeMode=UTC
        &origin=FRA
        &destination=BUD
    """

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

                print(
                    f"Schedule API error "
                    f"{origin}-{dest}: "
                    f"{response.status_code}"
                )

                return []

            data = response.json()

            # ------------------------------------------------
            # The schedule API response can vary.
            # Your supplied response is already a list.
            # ------------------------------------------------

            if isinstance(data, list):
                schedules = data

            elif isinstance(data, dict):

                # Defensive extraction for possible wrappers
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

            print(
                f"Schedule API exception "
                f"{origin}-{dest}: {e}"
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

            # ------------------------------------------------
            # Stable business key
            # ------------------------------------------------

            flight_key = (
                f"{airline}_"
                f"{flight_number}"
                f"{suffix}_"
                f"{period_start}_"
                f"{leg_origin}_"
                f"{leg_destination}_"
                f"{sequence_number}"
            )

            # ------------------------------------------------
            # Codeshare
            # ------------------------------------------------

            marketing_flights = (
                element_info[
                    "marketing_flights"
                ]
            )

            # ------------------------------------------------
            # Create normalized row
            # ------------------------------------------------

            row = {

                # ------------------------------
                # Business identity
                # ------------------------------

                "flight_key": flight_key,

                "airline": airline,

                "flight_number": flight_number,

                "flight_suffix": suffix,

                "operating_carrier": (
                    leg.get("aircraftOwner")
                ),

                "origin": leg_origin,

                "destination": leg_destination,

                "route_key": (
                    f"{leg_origin}-{leg_destination}"
                ),

                "sequence_number": (
                    sequence_number
                ),

                # ------------------------------
                # Schedule validity
                # ------------------------------

                "schedule_start_date": (
                    period_start
                ),

                "schedule_end_date": (
                    period_end
                ),

                "days_of_operation": (
                    days_of_operation
                ),

                "schedule_start_date_lt": (
                    normalize_date_lh(
                        period_lt.get("startDate")
                    )
                ),

                "schedule_end_date_lt": (
                    normalize_date_lh(
                        period_lt.get("endDate")
                    )
                ),

                # ------------------------------
                # Aircraft
                # ------------------------------

                "aircraft_type": (
                    leg.get("aircraftType")
                ),

                "aircraft_owner": (
                    leg.get("aircraftOwner")
                ),

                "aircraft_configuration": (
                    leg.get(
                        "aircraftConfigurationVersion"
                    )
                ),

                "registration": (
                    leg.get("registration")
                ),

                # ------------------------------
                # Flight properties
                # ------------------------------

                "service_type": (
                    leg.get("serviceType")
                ),

                "operating": (
                    leg.get("op")
                ),

                # ------------------------------
                # Departure UTC
                # ------------------------------

                "departure_time_utc": (
                    minutes_to_hhmm(
                        leg.get(
                            "aircraftDepartureTimeUTC"
                        )
                    )
                ),

                "departure_date_diff_utc": (
                    leg.get(
                        "aircraftDepartureTimeDateDiffUTC"
                    )
                ),

                # ------------------------------
                # Departure Local Time
                # ------------------------------

                "departure_time_lt": (
                    minutes_to_hhmm(
                        leg.get(
                            "aircraftDepartureTimeLT"
                        )
                    )
                ),

                "departure_date_diff_lt": (
                    leg.get(
                        "aircraftDepartureTimeDateDiffLT"
                    )
                ),

                # ------------------------------
                # Arrival UTC
                # ------------------------------

                "arrival_time_utc": (
                    minutes_to_hhmm(
                        leg.get(
                            "aircraftArrivalTimeUTC"
                        )
                    )
                ),

                "arrival_date_diff_utc": (
                    leg.get(
                        "aircraftArrivalTimeDateDiffUTC"
                    )
                ),

                # ------------------------------
                # Arrival Local Time
                # ------------------------------

                "arrival_time_lt": (
                    minutes_to_hhmm(
                        leg.get(
                            "aircraftArrivalTimeLT"
                        )
                    )
                ),

                "arrival_date_diff_lt": (
                    leg.get(
                        "aircraftArrivalTimeDateDiffLT"
                    )
                ),

                # ------------------------------
                # Schedule variation
                # ------------------------------

                "departure_variation_min": (
                    leg.get(
                        "aircraftDepartureTimeVariation"
                    )
                ),

                "arrival_variation_min": (
                    leg.get(
                        "aircraftArrivalTimeVariation"
                    )
                ),

                # ------------------------------
                # Data elements
                # ------------------------------

                "marketing_flights": (
                    marketing_flights
                ),

                "departure_terminal": (
                    element_info[
                        "departure_terminal"
                    ]
                ),

                "arrival_terminal": (
                    element_info[
                        "arrival_terminal"
                    ]
                ),

                # ------------------------------
                # Raw API information
                # ------------------------------

                "data_elements_raw": json.dumps(
                    element_info[
                        "data_elements_raw"
                    ]
                ),

                # ------------------------------
                # Request metadata
                # ------------------------------

                "requested_start_date": (
                    normalize_date_lh(
                        requested_start
                    )
                ),

                "requested_end_date": (
                    normalize_date_lh(
                        requested_end
                    )
                ),

                "_ingested_at": ingestion_time,
            }

            rows.append(row)

    return rows


# ============================================================
# DLT RESOURCES
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


# ============================================================
# DLT PIPELINE
# ============================================================

def get_pipeline():

    return dlt.pipeline(
        pipeline_name="lufthansa_ingest",
        destination=dlt.destinations.postgres(
            credentials=DB_CONFIG
        ),
        dataset_name="bronze",
    )


# ============================================================
# LOAD FLIGHTS
# ============================================================

def load_flights(rows):

    if not rows:
        return None

    pipeline = get_pipeline()

    return pipeline.run(
        flights_resource(rows),
        write_disposition="merge",
        primary_key=[
            "route_key",
            "departure__scheduled__date",
            "departure__scheduled__time",
            "operatingcarrier__airlineid",
            "operatingcarrier__flightnumber",
        ],
    )


# ============================================================
# LOAD SCHEDULE
# ============================================================

def load_schedule(rows):

    if not rows:
        return None

    pipeline = get_pipeline()

    return pipeline.run(
        schedule_resource(rows),
        write_disposition="merge",
        primary_key=[
            "flight_key"
        ],
    )


# ============================================================
# FASTAPI ENDPOINT
# ============================================================

@router.get("/flights/{flight_date}")
async def get_flightroute_details(
    flight_date: str
):

    # --------------------------------------------------------
    # Authentication
    # --------------------------------------------------------

    token = get_lufthansa_token()

    sem = asyncio.Semaphore(
        MAX_CONCURRENT_REQUESTS
    )

    # --------------------------------------------------------
    # Date for schedule API
    #
    # Input:
    #
    #   2026-09-29
    #
    # Schedule API expects:
    #
    #   29SEP26
    # --------------------------------------------------------

    try:

        parsed_date = datetime.strptime(
            flight_date,
            "%Y-%m-%d"
        )

        schedule_start = (
            parsed_date.strftime("%d%b%y")
            .upper()
        )

    except ValueError:

        return {
            "status": "error",
            "message": (
                "flight_date must be YYYY-MM-DD"
            ),
        }

    # For the moment we query one day.
    # Later this can easily become a date range.

    schedule_end = schedule_start

    # --------------------------------------------------------
    # HTTP client
    # --------------------------------------------------------

    async with httpx.AsyncClient(
        timeout=90
    ) as client:

        # ====================================================
        # 1. CUSTOMER FLIGHT INFORMATION
        # ====================================================

        flight_tasks = [

            fetch_route(
                client,
                token,
                origin,
                destination,
                flight_date,
                sem,
            )

            for origin, destination
            in ROUTES_FULL
        ]

        flight_results = await asyncio.gather(
            *flight_tasks
        )

        # ====================================================
        # 2. FLIGHT SCHEDULES
        # ====================================================

        schedule_tasks = [

            fetch_schedule(
                client,
                token,
                origin,
                destination,
                schedule_start,
                schedule_end,
                sem,
            )

            for origin, destination
            in ROUTES_FULL
        ]

        schedule_results = await asyncio.gather(
            *schedule_tasks
        )

    # ========================================================
    # FLIGHT DATA
    # ========================================================

    flight_data = []

    for result in flight_results:

        if result:
            flight_data.extend(result)

    # ========================================================
    # SCHEDULE DATA
    # ========================================================

    schedule_data = []

    for result in schedule_results:

        if result:
            schedule_data.extend(result)

    # ========================================================
    # FLIGHT INGESTION METADATA
    # ========================================================

    for row in flight_data:

        row["_ingested_at"] = utc_now_iso()

    # ========================================================
    # KEEP IMPORTANT FLIGHT FIELDS
    # ========================================================

    ALLOWED_FIELDS = {

        "Status",
        "Equipment",
        "Departure",
        "Arrival",

        "AircraftDetails",

        "MarketingCarrierList",
        "OperatingCarrier",

        "route_key",

        "_ingested_at",
    }

    clean_flights = []

    for row in flight_data:

        filtered = {
            key: value
            for key, value
            in row.items()
            if key in ALLOWED_FIELDS
        }

        # ----------------------------------------------------
        # Create stable flight key
        # ----------------------------------------------------

        operating = row.get(
            "OperatingCarrier",
            {}
        )

        departure = row.get(
            "Departure",
            {}
        )

        scheduled = departure.get(
            "Scheduled",
            {}
        )

        airline = operating.get(
            "AirlineID"
        )

        flight_number = operating.get(
            "FlightNumber"
        )

        flight_date_value = scheduled.get(
            "Date"
        )

        if (
            airline
            and flight_number
            and flight_date_value
        ):

            filtered["flight_key"] = (
                f"{airline}_"
                f"{flight_number}_"
                f"{flight_date_value}"
            )

        clean_flights.append(
            filtered
        )

    # ========================================================
    # NO DATA
    # ========================================================

    if not flight_data and not schedule_data:

        return {
            "status": "no_data",
            "flight_rows": 0,
            "schedule_rows": 0,
        }

    # ========================================================
    # DLT PIPELINE
    # ========================================================

    load_results = {}

    pipeline = get_pipeline()

    # ========================================================
    # LOAD FLIGHTS
    # ========================================================

    if clean_flights:

        try:

            load_info = pipeline.run(
                flights_resource(
                    clean_flights
                ),

                write_disposition="merge",

                primary_key=[
                    "flight_key"
                ],
            )

            load_results[
                "flights"
            ] = str(load_info)

        except PipelineStepFailed as e:

            print(
                f"Flight DLT pipeline error: {e}"
            )

            pipeline.drop_pending_packages()

            load_info = pipeline.run(
                flights_resource(
                    clean_flights
                ),
                write_disposition="append",
            )

            load_results[
                "flights"
            ] = str(load_info)

    # ========================================================
    # LOAD SCHEDULE
    # ========================================================

    if schedule_data:

        try:

            load_info = pipeline.run(
                schedule_resource(
                    schedule_data
                ),

                write_disposition="merge",

                primary_key=[
                    "flight_key"
                ],
            )

            load_results[
                "schedule"
            ] = str(load_info)

        except PipelineStepFailed as e:

            print(
                f"Schedule DLT pipeline error: {e}"
            )

            pipeline.drop_pending_packages()

            load_info = pipeline.run(
                schedule_resource(
                    schedule_data
                ),
                write_disposition="append",
            )

            load_results[
                "schedule"
            ] = str(load_info)

    # ========================================================
    # RESPONSE
    # ========================================================

    return {

        "status": "loaded",

        "flight_rows": len(
            clean_flights
        ),

        "schedule_rows": len(
            schedule_data
        ),

        "tables": [
            "bronze.lh_flights",
            "bronze.lh_schedule",
        ],

        "load_results": load_results,

        "flight_sample": (
            clean_flights[:2]
        ),

        "schedule_sample": (
            schedule_data[:2]
        ),
    }
