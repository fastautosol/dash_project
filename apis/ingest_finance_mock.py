# 2026.11.23  18.00
import random
from datetime import datetime, timedelta, timezone
from typing import Any

PARTNERS = [
    {"id": 104501, "name": "Alfa Logisztika Kft.", "taxcode": "12345678-2-41", "zip": "1134", "city": "Budapest", "address": "Váci út 45.", "email": "penzugy@alfalog.hu"},
    {"id": 104502, "name": "Beta Építőipari Zrt.", "taxcode": "23456789-2-42", "zip": "9021", "city": "Győr", "address": "Baross út 21.", "email": "szamla@betazrt.hu"},
    {"id": 104503, "name": "Sigma Gépészeti Kft.", "taxcode": "67890123-2-05", "zip": "7621", "city": "Pécs", "address": "Király utca 19.", "email": "iroda@sigmagep.hu"},
    {"id": 104504, "name": "Kovács Gábor (Magánszemély)", "taxcode": "", "zip": "4025", "city": "Debrecen", "address": "Piac utca 12.", "email": "gabor.kovacs@ceg.hu"},
    {"id": 104505, "name": "Omega Kereskedőház Kft.", "taxcode": "34567890-2-13", "zip": "2040", "city": "Budaörs", "address": "Szabadság út 88.", "email": "beszerzes@omegaker.hu"},
]

INVOICE_ITEMS = [
    {"sku": "SKU-1001", "name": "Ergonomikus Irodai Szék Pro", "net_unit": 69900.0, "cogs_net": 38500.0, "vat": 27},
    {"sku": "SKU-1002", "name": "Állítható Magasságú Íróasztal", "net_unit": 115000.0, "cogs_net": 64000.0, "vat": 27},
    {"sku": "SKU-1003", "name": "USB-C Dokkoló Állomás 11-in-1", "net_unit": 24900.0, "cogs_net": 11200.0, "vat": 27},
    {"sku": "SKU-1004", "name": "Vezeték Nélküli Mechanikus Billentyűzet", "net_unit": 32900.0, "cogs_net": 15800.0, "vat": 27},
    {"sku": "SKU-1005", "name": "4K Webkamera Autofókusszal", "net_unit": 28500.0, "cogs_net": 13400.0, "vat": 27},
    {"sku": "SKU-1006", "name": "Zajszűrős Bluetooth Fejhallgató", "net_unit": 44900.0, "cogs_net": 21900.0, "vat": 27},
]


def generate_mock_billingo_documents(count: int = 50) -> list[dict[str, Any]]:
    """Billingo API v3 (/v3/documents) valós JSON struktúrája."""
    docs: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        doc_id = 48900001 + idx
        partner = random.choice(PARTNERS)
        inv_date = (now - timedelta(days=random.randint(0, 45))).date()
        fulfill_date = inv_date
        due_date = inv_date + timedelta(days=random.choice([0, 8, 15]))

        doc_type = random.choices(["invoice", "invoice", "proforma", "cancellation"], weights=[75, 10, 10, 5])[0]
        if doc_type == "cancellation":
            payment_status = "paid"
            paid_date = inv_date.isoformat()
        else:
            payment_status = random.choices(["paid", "outstanding", "expired"], weights=[72, 18, 10])[0]
            paid_date = (inv_date + timedelta(days=random.randint(0, 5))).isoformat() if payment_status == "paid" else None

        sign = -1 if doc_type == "cancellation" else 1
        selected = random.sample(INVOICE_ITEMS, k=random.randint(1, 3))
        items = []
        net_total = 0.0
        gross_total = 0.0

        for item in selected:
            qty = random.randint(1, 4) * sign
            line_net = round(item["net_unit"] * qty, 2)
            line_gross = round(line_net * (1 + item["vat"] / 100.0), 2)
            net_total += line_net
            gross_total += line_gross

            items.append({
                "product_id": 9900 + int(item["sku"].split("-")[1]),
                "name": f"[{item['sku']}] {item['name']}",
                "net_unit_amount": item["net_unit"],
                "quantity": qty,
                "unit": "db",
                "net_amount": line_net,
                "gross_amount": line_gross,
                "vat": f"{item['vat']}%",
            })

        docs.append({
            "id": doc_id,
            "invoice_number": f"E-BIL-2026-{1000 + idx}",
            "type": doc_type,
            "cancelled": doc_type == "cancellation",
            "block_id": 12450,
            "payment_status": payment_status,
            "payment_method": random.choice(["wire_transfer", "barion", "cod", "bankcard"]),
            "gross_total": round(gross_total, 2),
            "net_total": round(net_total, 2),
            "currency": "HUF",
            "conversion_rate": 1.0,
            "invoice_date": inv_date.isoformat(),
            "fulfillment_date": fulfill_date.isoformat(),
            "due_date": due_date.isoformat(),
            "paid_date": paid_date,
            "organization": {
                "name": "Saját KKV Kft.",
                "tax_number": "11223344-2-42",
            },
            "partner": {
                "id": partner["id"],
                "name": partner["name"],
                "taxcode": partner["taxcode"],
                "emails": [partner["email"]],
                "address": {
                    "country_code": "HU",
                    "post_code": partner["zip"],
                    "city": partner["city"],
                    "address": partner["address"],
                },
            },
            "items": items,
        })
    return docs


def generate_mock_szamlazzhu_invoices(count: int = 50) -> list[dict[str, Any]]:
    """Számlázz.hu Számla Agent XML -> dict valós struktúrája (szamlaszam PK-val)."""
    invoices: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        szamlaszam = f"E-SZH-2026-{3000 + idx}"
        partner = random.choice(PARTNERS)
        kelt = (now - timedelta(days=random.randint(0, 45))).date()
        teljesites = kelt
        fizhat = kelt + timedelta(days=random.choice([0, 8, 15]))
        is_storno = random.random() < 0.05
        sign = -1 if is_storno else 1

        selected = random.sample(INVOICE_ITEMS, k=random.randint(1, 3))
        tetelek = []
        osszes_netto = 0.0
        osszes_afa = 0.0
        osszes_brutto = 0.0

        for item in selected:
            mennyiseg = random.randint(1, 3) * sign
            netto = round(item["net_unit"] * mennyiseg, 2)
            afa = round(netto * (item["vat"] / 100.0), 2)
            brutto = round(netto + afa, 2)
            osszes_netto += netto
            osszes_afa += afa
            osszes_brutto += brutto

            tetelek.append({
                "azonosito": item["sku"],
                "megnevezes": item["name"],
                "mennyiseg": mennyiseg,
                "mennyisegiEgyseg": "db",
                "nettoEgysegar": item["net_unit"],
                "afakulcs": str(item["vat"]),
                "netto": netto,
                "afa": afa,
                "brutto": brutto,
            })

        fizetve = is_storno or (random.random() < 0.78)
        invoices.append({
            "szamlaszam": szamlaszam,
            "tipus": "SS" if is_storno else "SZ",
            "kelt": kelt.isoformat(),
            "teljesites": teljesites.isoformat(),
            "fizhat": fizhat.isoformat(),
            "fizmod": random.choice(["Átutalás", "Bankkártya", "Utánvét"]),
            "devizanem": "HUF",
            "arfolyam": 1.0,
            "fizetve": fizetve,
            "kifizetesDatum": (kelt + timedelta(days=random.randint(0, 4))).isoformat() if fizetve else None,
            "netto": round(osszes_netto, 2),
            "afa": round(osszes_afa, 2),
            "brutto": round(osszes_brutto, 2),
            "megjegyzes": f"Rendelésszám: #10{idx:02d}",
            "vevo": {
                "nev": partner["name"],
                "adoszam": partner["taxcode"],
                "irsz": partner["zip"],
                "telepules": partner["city"],
                "cim": partner["address"],
                "email": partner["email"],
            },
            "tetelek": tetelek,
        })
    return invoices


def generate_mock_product_cogs() -> list[dict[str, Any]]:
    """Beszerzési árak (COGS) mock rekordok a Gold rétegbeli árrésszámításhoz."""
    today = datetime.now(timezone.utc).date().isoformat()
    return [
        {
            "sku": item["sku"],
            "termek_nev": item["name"],
            "beszerzesi_netto_ar": item["cogs_net"],
            "devizanem": "HUF",
            "beszallito": random.choice(["EuroImport Kft.", "AsiaDirect Trading", "TechDistri HU"]),
            "ervenyes_ettol": today,
        }
        for item in INVOICE_ITEMS
    ]
