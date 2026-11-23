# 2026.11.23  18.00
import random
from datetime import datetime, timedelta, timezone
from typing import Any

COMPANIES = [
    {"name": "Alfa Logisztika Kft.", "contact": "Kovács Gábor", "email": "gabor.kovacs@alfalog.hu", "phone": "+36304567890"},
    {"name": "Beta Építőipari Zrt.", "contact": "Szabó Péter", "email": "peter.szabo@betazrt.hu", "phone": "+36209876543"},
    {"name": "Sigma Gépészeti Kft.", "contact": "Varga László", "email": "laszlo.varga@sigmagep.hu", "phone": "+36703334455"},
    {"name": "Omega Kereskedőház Kft.", "contact": "Nagy Eszter", "email": "eszter.nagy@omegaker.hu", "phone": "+36301112233"},
    {"name": "Delta Energetika Kft.", "contact": "Tóth Dóra", "email": "dora.toth@deltaen.hu", "phone": "+36205556677"},
]

OWNERS = [
    {"id": "101", "name": "Horváth Tamás", "email": "tamas.horvath@sajatkkv.hu"},
    {"id": "102", "name": "Kiss Katalin", "email": "katalin.kiss@sajatkkv.hu"},
    {"id": "103", "name": "Molnár Bence", "email": "bence.molnar@sajatkkv.hu"},
]

LEAD_SOURCES = ["PAID_SOCIAL", "PAID_SEARCH", "ORGANIC_SEARCH", "DIRECT_TRAFFIC", "REFERRAL"]


def generate_mock_hubspot_deals(count: int = 50) -> list[dict[str, Any]]:
    """HubSpot CRM API v3 (/crm/v3/objects/deals) valós JSON struktúrája."""
    stages = [
        ("appointmentscheduled", 0.20),
        ("qualifiedtobuy", 0.40),
        ("presentationscheduled", 0.60),
        ("decisionmakerboughtin", 0.80),
        ("closedwon", 1.00),
        ("closedlost", 0.00),
    ]
    deals: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        deal_id = str(9100001 + idx)
        comp = random.choice(COMPANIES)
        owner = random.choice(OWNERS)
        stage, prob = random.choices(stages, weights=[15, 20, 15, 15, 22, 13])[0]

        created_dt = now - timedelta(days=random.randint(1, 60))
        updated_dt = created_dt + timedelta(days=random.randint(0, 15))
        close_dt = updated_dt if stage in ("closedwon", "closedlost") else (now + timedelta(days=random.randint(3, 30)))
        amount = round(random.choice([180000, 350000, 690000, 1250000, 2400000]) * random.uniform(0.9, 1.15), 2)

        deals.append({
            "id": deal_id,
            "createdAt": created_dt.isoformat(),
            "updatedAt": updated_dt.isoformat(),
            "archived": False,
            "properties": {
                "dealname": f"{comp['name']} - Éves BI & Automatizáció",
                "amount": f"{amount:.2f}",
                "deal_currency_code": "HUF",
                "pipeline": "default",
                "dealstage": stage,
                "hs_deal_stage_probability": str(prob),
                "closedate": close_dt.isoformat(),
                "createdate": created_dt.isoformat(),
                "hs_lastmodifieddate": updated_dt.isoformat(),
                "hubspot_owner_id": owner["id"],
                "hs_analytics_source": random.choice(LEAD_SOURCES),
                "closed_lost_reason": "Ár / költségvetés" if stage == "closedlost" else None,
            },
        })
    return deals


def generate_mock_pipedrive_deals(count: int = 50) -> list[dict[str, Any]]:
    """Pipedrive REST API v1 (/v1/deals) valós JSON struktúrája."""
    stages = [
        {"stage_id": 1, "name": "Új érdeklődő", "status": "open", "prob": 15},
        {"stage_id": 2, "name": "Igényfelmérés", "status": "open", "prob": 35},
        {"stage_id": 3, "name": "Ajánlat kiküldve", "status": "open", "prob": 60},
        {"stage_id": 4, "name": "Szerződéskötés", "status": "open", "prob": 85},
        {"stage_id": 5, "name": "Megnyert", "status": "won", "prob": 100},
        {"stage_id": 6, "name": "Elvesztett", "status": "lost", "prob": 0},
    ]
    deals: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        deal_id = 4001 + idx
        comp = random.choice(COMPANIES)
        owner = random.choice(OWNERS)
        st = random.choices(stages, weights=[14, 18, 20, 12, 24, 12])[0]

        add_dt = now - timedelta(days=random.randint(1, 60))
        upd_dt = add_dt + timedelta(days=random.randint(0, 14))
        won_str = upd_dt.strftime("%Y-%m-%d %H:%M:%S") if st["status"] == "won" else None
        lost_str = upd_dt.strftime("%Y-%m-%d %H:%M:%S") if st["status"] == "lost" else None
        exp_close = (now + timedelta(days=random.randint(2, 25))).date().isoformat()
        value = float(random.choice([220000, 480000, 850000, 1490000, 2900000]))

        deals.append({
            "id": deal_id,
            "title": f"{comp['name']} - Rendszerintegráció",
            "value": value,
            "currency": "HUF",
            "status": st["status"],
            "stage_id": st["stage_id"],
            "pipeline_id": 1,
            "probability": st["prob"],
            "add_time": add_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "update_time": upd_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "won_time": won_str,
            "lost_time": lost_str,
            "lost_reason": "Versenytársat választott" if st["status"] == "lost" else None,
            "expected_close_date": exp_close,
            "user_id": {
                "id": int(owner["id"]),
                "name": owner["name"],
                "email": owner["email"],
            },
            "person_id": {
                "value": 700 + idx,
                "name": comp["contact"],
                "email": [{"value": comp["email"], "primary": True}],
                "phone": [{"value": comp["phone"], "primary": True}],
            },
            "org_id": {
                "value": 300 + (idx % len(COMPANIES)),
                "name": comp["name"],
            },
        })
    return deals


def generate_mock_minicrm_projects(count: int = 50) -> list[dict[str, Any]]:
    """MiniCRM REST API v3 (/Api/R3/Project/{Id}) valós magyar KKV mezőstruktúrája."""
    statuses = [
        {"StatusId": 2801, "StatuszNev": "Új lead"},
        {"StatusId": 2802, "StatuszNev": "Kapcsolatfelvétel"},
        {"StatusId": 2803, "StatuszNev": "Ajánlatadás"},
        {"StatusId": 2804, "StatuszNev": "Megrendelve / Sikeres"},
        {"StatusId": 2805, "StatuszNev": "Elutasítva / Sikertelen"},
    ]
    utm_sources = ["facebook", "google", "instagram", "tiktok", "ajanlas", "direkt"]
    projects: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        proj_id = 80001 + idx
        comp = random.choice(COMPANIES)
        owner = random.choice(OWNERS)
        st = random.choices(statuses, weights=[15, 20, 22, 28, 15])[0]

        created_dt = now - timedelta(days=random.randint(1, 60))
        updated_dt = created_dt + timedelta(days=random.randint(0, 12))
        amount = float(random.choice([150000, 290000, 540000, 980000, 1750000]))

        projects.append({
            "Id": proj_id,
            "CategoryId": 12,
            "ContactId": 5000 + idx,
            "BusinessId": 2000 + (idx % len(COMPANIES)),
            "UserId": int(owner["id"]),
            "FelelosNev": owner["name"],
            "Name": f"{comp['name']} - Értékesítési projekt #{idx + 1}",
            "Cegnev": comp["name"],
            "Kapcsolattarto": comp["contact"],
            "Email": comp["email"],
            "Telefon": comp["phone"],
            "StatusId": st["StatusId"],
            "Statusz": st["StatuszNev"],
            "Ertek": amount,
            "Penznem": "HUF",
            "Forras": random.choice(utm_sources),
            "VarhatoZaras": (now + timedelta(days=random.randint(1, 30))).date().isoformat(),
            "Deleted": "0",
            "CreatedAt": created_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "UpdatedAt": updated_dt.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return projects
