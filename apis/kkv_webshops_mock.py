# 2026.11.23  15.00
import base64
import json
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Any

# --- Közös törzsadatok (UNAS, Shoprenter, WooCommerce) ---
WEBSHOP_CATALOG = [
    {"sku": "SKU-1001", "name": "Ergonomikus Irodai Szék Pro", "net_price": 69900.0, "gross_price": 88773.0, "vat": 27},
    {"sku": "SKU-1002", "name": "Állítható Magasságú Íróasztal", "net_price": 115000.0, "gross_price": 146050.0, "vat": 27},
    {"sku": "SKU-1003", "name": "USB-C Dokkoló Állomás 11-in-1", "net_price": 24900.0, "gross_price": 31623.0, "vat": 27},
    {"sku": "SKU-1004", "name": "Vezeték Nélküli Mechanikus Billentyűzet", "net_price": 32900.0, "gross_price": 41783.0, "vat": 27},
    {"sku": "SKU-1005", "name": "4K Webkamera Autofókusszal", "net_price": 28500.0, "gross_price": 36195.0, "vat": 27},
    {"sku": "SKU-1006", "name": "Zajszűrős Bluetooth Fejhallgató", "net_price": 44900.0, "gross_price": 57023.0, "vat": 27},
]

WEBSHOP_CUSTOMERS = [
    {"first": "Gábor", "last": "Kovács", "company": "Alfa Logisztika Kft.", "tax": "12345678-2-41", "city": "Budapest", "zip": "1134", "street": "Váci út 45."},
    {"first": "Eszter", "last": "Nagy", "company": None, "tax": None, "city": "Debrecen", "zip": "4025", "street": "Piac utca 12."},
    {"first": "Péter", "last": "Szabó", "company": "Beta Építőipari Zrt.", "tax": "23456789-2-42", "city": "Győr", "zip": "9021", "street": "Baross út 21."},
    {"first": "Dóra", "last": "Tóth", "company": None, "tax": None, "city": "Szeged", "zip": "6720", "street": "Kárász utca 8."},
    {"first": "László", "last": "Varga", "company": "Sigma Gépészeti Kft.", "tax": "67890123-2-05", "city": "Pécs", "zip": "7621", "street": "Király utca 19."},
]

UTM_SOURCES = [
    {"utm_source": "facebook", "utm_medium": "cpc", "utm_campaign": "META_CONV_PROSPECTING_HU"},
    {"utm_source": "instagram", "utm_medium": "cpc", "utm_campaign": "META_RETARGETING_DPA"},
    {"utm_source": "google", "utm_medium": "cpc", "utm_campaign": "GADS_PMAX_TOP_MARGIN"},
    {"utm_source": "youtube", "utm_medium": "cpc", "utm_campaign": "YT_VIDEO_ACTION_REMARKETING"},
    {"utm_source": "tiktok", "utm_medium": "cpc", "utm_campaign": "TT_CONV_UGC_SCALE_HU"},
    {"utm_source": "direct", "utm_medium": "none", "utm_campaign": ""},
]


# --- Shopify segédfüggvények és generátor ---
def _load_optional_tickets() -> list[dict[str, str]]:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base_dir, "generated_tickets_hu.json")
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return [
        {"message": "Hol tart a rendelésem?", "intent": "order_status"},
        {"message": "Áfás számlát szeretnék kérni.", "intent": "invoice_request"},
        {"message": "Digitális letöltési link nem érkezett meg.", "intent": "digital_delivery"},
    ]


def _money_set(amount: float, currency: str = "HUF") -> dict[str, dict[str, str]]:
    val = f"{amount:.2f}"
    return {
        "shop_money": {"amount": val, "currency_code": currency},
        "presentment_money": {"amount": val, "currency_code": currency},
    }


def _build_shopify_address(first_name: str, last_name: str, phone: str) -> dict[str, Any]:
    city, zip_code = random.choice([
        ("Budapest", "1134"),
        ("Debrecen", "4025"),
        ("Szeged", "6720"),
        ("Győr", "9021"),
        ("Pécs", "7621"),
        ("Miskolc", "3525"),
        ("Nyíregyháza", "4400"),
        ("Kecskemét", "6000"),
        ("Székesfehérvár", "8000"),
        ("Szombathely", "9700"),
    ])

    street = (
        f"{random.choice(['Váci út', 'Kossuth Lajos utca', 'Petőfi Sándor utca', 'Rákóczi út', 'Bajcsy-Zsilinszky út', 'Andrássy út', 'Üllői út', 'Bartók Béla út', 'Széchenyi István tér', 'Dózsa György út'])} "
        f"{random.randint(1, 120)}."
    )
    company = random.choice([
        None, None, "Példa Kft.", "Alfa Logisztika Zrt.", "NextGen Trade Bt.", "Sigma Gépészeti Kft.",
        "Delta Energetika Kft.", "Omega Kereskedőház Kft.", "Zéta Bútor és Design Kft.", "Epsilon MedTech Kft.",
    ])

    return {
        "first_name": first_name,
        "last_name": last_name,
        "name": f"{last_name} {first_name}",
        "company": company,
        "address1": street,
        "address2": random.choice([None, "2. em. 4.", "B épület"]),
        "city": city,
        "province": city,
        "province_code": "HU",
        "country": "Hungary",
        "country_code": "HU",
        "zip": zip_code,
        "phone": phone,
        "latitude": round(random.uniform(46.0, 48.2), 6),
        "longitude": round(random.uniform(16.5, 21.8), 6),
    }


def generate_mock_shopify_orders(count: int = 50) -> list[dict[str, Any]]:
    """Shopify Admin REST API valós rendelésstruktúrája."""
    tickets = _load_optional_tickets()
    catalog = [
        {"product_id": 881001, "variant_id": 44100101, "sku": "DIG-BP-001", "title": "KKV Automatizációs Blueprint (PDF)", "variant_title": "Alap licenc", "vendor": "DataFlow Kft.", "price": 29900.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881002, "variant_id": 44100201, "sku": "DIG-N8N-002", "title": "n8n + PostgreSQL Sabloncsomag", "variant_title": "Pro csomag", "vendor": "DataFlow Kft.", "price": 59900.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881003, "variant_id": 44100301, "sku": "SRV-AUD-003", "title": "AI Adat-Audit Konzultáció", "variant_title": "60 perces online", "vendor": "DataFlow Kft.", "price": 89000.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881004, "variant_id": 44100401, "sku": "HW-SCN-004", "title": "Okos Raktár Vonalkód Olvasó", "variant_title": "USB + Bluetooth", "vendor": "ZebraTech", "price": 44900.0, "grams": 380, "requires_shipping": True},
        {"product_id": 881005, "variant_id": 44100501, "sku": "DIG-DASH-005", "title": "Vezetői KPI Dashboard Sablon (Python Dash)", "variant_title": "Forráskód + Docker", "vendor": "DataFlow Kft.", "price": 74900.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881006, "variant_id": 44100601, "sku": "DIG-SQL-006", "title": "E-commerce Adattárház Séma Csomag", "variant_title": "PostgreSQL + dlt", "vendor": "DataFlow Kft.", "price": 49900.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881007, "variant_id": 44100701, "sku": "DIG-RAG-007", "title": "Céges Tudásbázis Prompt & RAG Kézikönyv", "variant_title": "E-book + JSON", "vendor": "DataFlow Kft.", "price": 24900.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881008, "variant_id": 44100801, "sku": "SRV-SETUP-008", "title": "VPS + Coolify Infrastruktúra Telepítés", "variant_title": "Kulcsrakész csomag", "vendor": "DataFlow Kft.", "price": 129000.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881009, "variant_id": 44100901, "sku": "SRV-TRN-009", "title": "n8n Csapat Workshop (3 óra)", "variant_title": "Online élő képzés", "vendor": "DataFlow Kft.", "price": 149000.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881010, "variant_id": 44101001, "sku": "HW-LBL-010", "title": "Hőérzékeny Címkenyomtató (GLS/Foxpost)", "variant_title": "100x150 mm", "vendor": "ZebraTech", "price": 62900.0, "grams": 1450, "requires_shipping": True},
        {"product_id": 881011, "variant_id": 44101101, "sku": "HW-RLL-011", "title": "Termo Címketekercs (500 db/tekercs)", "variant_title": "6 tekercses csomag", "vendor": "PackPro", "price": 11900.0, "grams": 2100, "requires_shipping": True},
        {"product_id": 881012, "variant_id": 44101201, "sku": "HW-NFC-012", "title": "Ipari NFC Munkaidő-nyilvántartó Terminál", "variant_title": "WiFi + PoE", "vendor": "IoT Hungary", "price": 84900.0, "grams": 520, "requires_shipping": True},
        {"product_id": 881013, "variant_id": 44101301, "sku": "HW-CRD-013", "title": "Programozható NFC Belépőkártya (50 db)", "variant_title": "Fehér PVC", "vendor": "IoT Hungary", "price": 18900.0, "grams": 300, "requires_shipping": True},
        {"product_id": 881014, "variant_id": 44101401, "sku": "HW-PDA-014", "title": "Androidos Raktári Adatgyűjtő Terminál", "variant_title": "4GB / 64GB", "vendor": "ZebraTech", "price": 179000.0, "grams": 460, "requires_shipping": True},
        {"product_id": 881015, "variant_id": 44101501, "sku": "DIG-BIL-015", "title": "Billingo + Shopify Szinkronizáló Modul", "variant_title": "Éves licenc", "vendor": "DataFlow Kft.", "price": 69900.0, "grams": 0, "requires_shipping": False},
    ]

    landing_options = [
        ("/products/automatizacios-blueprint?utm_source=facebook&utm_medium=cpc", "https://l.facebook.com/"),
        ("/products/n8n-sabloncsomag?utm_source=instagram&utm_medium=cpc", "https://l.instagram.com/"),
        ("/products/n8n-sabloncsomag?utm_source=google&utm_medium=cpc", "https://www.google.com/"),
        ("/products/kpi-dashboard?utm_source=youtube&utm_medium=cpc", "https://www.youtube.com/"),
        ("/products/rag-kezikonyv?utm_source=tiktok&utm_medium=cpc", "https://www.tiktok.com/"),
        ("/", "https://www.linkedin.com/"),
        ("/collections/all", None),
    ]

    orders: list[dict[str, Any]] = []
    for idx in range(count):
        order_id = random.randint(5000000000000, 5999999999999)
        order_num = 1001 + idx
        currency = "HUF"

        first_name = random.choice(["Anna", "Bence", "Csaba", "Dóra", "Eszter", "Gábor", "Katalin", "László", "Péter", "Zsófia"])
        last_name = random.choice(["Nagy", "Kovács", "Tóth", "Szabó", "Horváth", "Varga", "Kiss", "Molnár", "Németh", "Farkas"])
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@{random.choice(['ceg.hu', 'gmail.com', 'vallalkozas.hu', 'kft.hu'])}"
        phone = f"+36{random.choice(['20', '30', '70'])}{random.randint(1000000, 9999999)}"
        address = _build_shopify_address(first_name, last_name, phone)

        created_dt = datetime.now(timezone.utc) - timedelta(hours=random.randint(1, 720))
        updated_dt = created_dt + timedelta(minutes=random.randint(1, 180))

        is_cancelled = random.random() < 0.06
        cancelled_at = (created_dt + timedelta(hours=2)).isoformat() if is_cancelled else None
        cancel_reason = random.choice(["customer", "inventory", "declined"]) if is_cancelled else None

        financial_status = "voided" if is_cancelled else random.choice(["paid", "paid", "paid", "pending", "refunded", "partially_paid"])
        fulfillment_status = None if is_cancelled else random.choice(["fulfilled", "fulfilled", "unfulfilled", "partial", None])
        closed_at = updated_dt.isoformat() if (fulfillment_status == "fulfilled" and financial_status == "paid") else None

        discount_rate = random.choice([0.0, 0.0, 0.0, 0.10, 0.15])
        discount_codes = [{"code": "WELCOME10" if discount_rate == 0.10 else "VIP15", "amount": "0.00", "type": "percentage"}] if discount_rate > 0 else []

        selected_items = random.sample(catalog, k=random.randint(1, 3))
        line_items = []
        total_line_items_price = 0.0
        total_discounts = 0.0
        total_weight = 0
        has_physical_item = False

        for item in selected_items:
            qty = random.randint(1, 3)
            gross_line = round(item["price"] * qty, 2)
            line_discount = round(gross_line * discount_rate, 2)
            net_line = round(gross_line - line_discount, 2)
            line_tax = round(net_line - (net_line / 1.27), 2)

            total_line_items_price += gross_line
            total_discounts += line_discount
            total_weight += item["grams"] * qty
            if item["requires_shipping"]:
                has_physical_item = True

            line_items.append({
                "id": random.randint(9000000000000, 9999999999999),
                "admin_graphql_api_id": f"gid://shopify/LineItem/{random.randint(9000000000000, 9999999999999)}",
                "product_id": item["product_id"],
                "variant_id": item["variant_id"],
                "sku": item["sku"],
                "title": item["title"],
                "variant_title": item["variant_title"],
                "name": f"{item['title']} - {item['variant_title']}",
                "vendor": item["vendor"],
                "quantity": qty,
                "fulfillable_quantity": 0 if fulfillment_status == "fulfilled" else qty,
                "fulfillment_service": "manual",
                "fulfillment_status": fulfillment_status,
                "gift_card": False,
                "grams": item["grams"],
                "price": f"{item['price']:.2f}",
                "price_set": _money_set(item["price"], currency),
                "total_discount": f"{line_discount:.2f}",
                "total_discount_set": _money_set(line_discount, currency),
                "product_exists": True,
                "requires_shipping": item["requires_shipping"],
                "taxable": True,
                "properties": [],
                "duties": [],
                "discount_allocations": (
                    [{"amount": f"{line_discount:.2f}", "amount_set": _money_set(line_discount, currency), "discount_application_index": 0}]
                    if line_discount > 0 else []
                ),
                "tax_lines": [{"channel_liable": False, "price": f"{line_tax:.2f}", "price_set": _money_set(line_tax, currency), "rate": 0.27, "title": "ÁFA"}],
            })

        if discount_codes:
            discount_codes[0]["amount"] = f"{total_discounts:.2f}"

        shipping_cost = 1990.0 if has_physical_item else 0.0
        shipping_lines = (
            [{
                "id": random.randint(4000000000000, 4999999999999),
                "carrier_identifier": "gls_hu",
                "code": "GLS Házhozszállítás",
                "title": "GLS Futárszolgálat",
                "source": "shopify",
                "phone": None,
                "price": f"{shipping_cost:.2f}",
                "price_set": _money_set(shipping_cost, currency),
                "discounted_price": f"{shipping_cost:.2f}",
                "discounted_price_set": _money_set(shipping_cost, currency),
                "is_removed": False,
                "requested_fulfillment_service_id": None,
                "tax_lines": [],
                "discount_allocations": [],
            }]
            if has_physical_item else []
        )

        subtotal_price = round(total_line_items_price - total_discounts, 2)
        total_price = round(subtotal_price + shipping_cost, 2)
        total_tax = round(total_price - (total_price / 1.27), 2)
        total_outstanding = total_price if financial_status == "pending" else 0.0

        landing_site, referring_site = random.choice(landing_options)
        ticket_sample = random.choice(tickets)
        cust_id = random.randint(7000000000000, 7999999999999)

        orders.append({
            "id": order_id,
            "admin_graphql_api_id": f"gid://shopify/Order/{order_id}",
            "app_id": 580111,
            "browser_ip": f"84.2.{random.randint(1, 254)}.{random.randint(1, 254)}",
            "buyer_accepts_marketing": random.choice([True, False]),
            "cancel_reason": cancel_reason,
            "cancelled_at": cancelled_at,
            "cart_token": f"c1-{random.randint(10**15, 10**16 - 1)}",
            "checkout_id": random.randint(30000000000000, 39999999999999),
            "checkout_token": f"chk_{random.randint(10**15, 10**16 - 1)}",
            "client_details": {
                "accept_language": "hu-HU,hu;q=0.9,en-US;q=0.8",
                "browser_height": 900,
                "browser_ip": f"84.2.{random.randint(1, 254)}.{random.randint(1, 254)}",
                "browser_width": 1440,
                "session_hash": None,
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0",
            },
            "closed_at": closed_at,
            "confirmation_number": f"CONF{random.randint(100000, 999999)}",
            "confirmed": True,
            "contact_email": email,
            "created_at": created_dt.isoformat(),
            "currency": currency,
            "current_subtotal_price": f"{subtotal_price:.2f}",
            "current_subtotal_price_set": _money_set(subtotal_price, currency),
            "current_total_discounts": f"{total_discounts:.2f}",
            "current_total_discounts_set": _money_set(total_discounts, currency),
            "current_total_duties_set": None,
            "current_total_price": f"{total_price:.2f}",
            "current_total_price_set": _money_set(total_price, currency),
            "current_total_tax": f"{total_tax:.2f}",
            "current_total_tax_set": _money_set(total_tax, currency),
            "customer_locale": "hu",
            "device_id": None,
            "discount_codes": discount_codes,
            "discount_applications": (
                [{"target_type": "line_item", "type": "discount_code", "value": f"{discount_rate * 100:.1f}", "value_type": "percentage", "allocation_method": "across", "target_selection": "all", "code": discount_codes[0]["code"]}]
                if discount_codes else []
            ),
            "email": email,
            "estimated_taxes": False,
            "financial_status": financial_status,
            "fulfillment_status": fulfillment_status,
            "landing_site": landing_site,
            "landing_site_ref": None,
            "location_id": None,
            "merchant_of_record_app_id": None,
            "name": f"#{order_num}",
            "note": ticket_sample.get("message"),
            "note_attributes": [],
            "number": idx + 1,
            "order_number": order_num,
            "order_status_url": f"https://teszt-bolt.myshopify.com/123456/orders/{order_id}/authenticate?key=mock",
            "original_total_duties_set": None,
            "payment_gateway_names": [random.choice(["stripe", "barion", "bank_transfer", "cod"])],
            "phone": phone,
            "po_number": None,
            "presentment_currency": currency,
            "processed_at": created_dt.isoformat(),
            "reference": None,
            "referring_site": referring_site,
            "source_identifier": None,
            "source_name": "web",
            "source_url": None,
            "subtotal_price": f"{subtotal_price:.2f}",
            "subtotal_price_set": _money_set(subtotal_price, currency),
            "tags": ticket_sample.get("intent") or "",
            "tax_exempt": False,
            "tax_lines": [{"channel_liable": False, "price": f"{total_tax:.2f}", "price_set": _money_set(total_tax, currency), "rate": 0.27, "title": "ÁFA"}],
            "taxes_included": True,
            "test": True,
            "token": f"tok_{random.randint(10**15, 10**16 - 1)}",
            "total_discounts": f"{total_discounts:.2f}",
            "total_discounts_set": _money_set(total_discounts, currency),
            "total_line_items_price": f"{total_line_items_price:.2f}",
            "total_line_items_price_set": _money_set(total_line_items_price, currency),
            "total_outstanding": f"{total_outstanding:.2f}",
            "total_price": f"{total_price:.2f}",
            "total_price_set": _money_set(total_price, currency),
            "total_price_usd": f"{round(total_price / 385.0, 2):.2f}",
            "total_shipping_price_set": _money_set(shipping_cost, currency),
            "total_tax": f"{total_tax:.2f}",
            "total_tax_set": _money_set(total_tax, currency),
            "total_tip_received": "0.00",
            "total_weight": total_weight,
            "updated_at": updated_dt.isoformat(),
            "user_id": None,
            "billing_address": address,
            "shipping_address": address if has_physical_item else None,
            "customer": {
                "id": cust_id,
                "admin_graphql_api_id": f"gid://shopify/Customer/{cust_id}",
                "email": email,
                "created_at": (created_dt - timedelta(days=random.randint(1, 180))).isoformat(),
                "updated_at": updated_dt.isoformat(),
                "first_name": first_name,
                "last_name": last_name,
                "state": "enabled",
                "note": None,
                "verified_email": True,
                "multipass_identifier": None,
                "tax_exempt": False,
                "phone": phone,
                "tags": "",
                "currency": currency,
                "tax_exemptions": [],
                "email_marketing_consent": {
                    "state": random.choice(["subscribed", "not_subscribed"]),
                    "opt_in_level": "single_opt_in",
                    "consent_updated_at": created_dt.isoformat(),
                },
                "default_address": address,
            },
            "line_items": line_items,
            "shipping_lines": shipping_lines,
            "fulfillments": [],
            "refunds": [],
        })
    return orders


# --- UNAS generátor ---
def generate_mock_unas_orders(count: int = 50) -> list[dict[str, Any]]:
    """UNAS getOrder XML->dict valós struktúrája (Key elsődleges kulccsal)."""
    orders: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        order_key = str(20260001 + idx)
        cust = random.choice(WEBSHOP_CUSTOMERS)
        utm = random.choice(UTM_SOURCES)
        created_dt = now - timedelta(hours=random.randint(1, 720))
        mod_dt = created_dt + timedelta(hours=random.randint(1, 24))
        status = random.choice(["Teljesítve", "Teljesítve", "Feldolgozás alatt", "Új megrendelés", "Törölve"])

        selected = random.sample(WEBSHOP_CATALOG, k=random.randint(1, 3))
        items = []
        sum_net = 0.0
        sum_gross = 0.0

        for line_idx, prod in enumerate(selected, start=1):
            qty = random.randint(1, 3)
            line_net = round(prod["net_price"] * qty, 2)
            line_gross = round(prod["gross_price"] * qty, 2)
            sum_net += line_net
            sum_gross += line_gross

            items.append({
                "Id": str(900000 + idx * 10 + line_idx),
                "Sku": prod["sku"],
                "Name": prod["name"],
                "Unit": "db",
                "Quantity": qty,
                "PriceNet": prod["net_price"],
                "PriceGross": prod["gross_price"],
                "Vat": f"{prod['vat']}%",
            })

        ship_net = 1567.0
        ship_gross = 1990.0
        sum_net = round(sum_net + ship_net, 2)
        sum_gross = round(sum_gross + ship_gross, 2)

        orders.append({
            "Key": order_key,
            "Date": created_dt.strftime("%Y.%m.%d %H:%M:%S"),
            "DateMod": mod_dt.strftime("%Y.%m.%d %H:%M:%S"),
            "Status": status,
            "Currency": "HUF",
            "SumPriceNet": sum_net,
            "SumPriceGross": sum_gross,
            "Payment": {
                "Name": random.choice(["Barion bankkártyás fizetés", "Utánvét", "Banki átutalás"]),
                "Status": "paid" if status == "Teljesítve" else "unpaid",
            },
            "Shipping": {
                "Name": random.choice(["GLS Házhozszállítás", "Foxpost Csomagautomata", "Packeta Z-BOX"]),
                "CostNet": ship_net,
                "CostGross": ship_gross,
            },
            "Customer": {
                "Email": f"{cust['first'].lower()}.{cust['last'].lower()}{idx}@ceg.hu",
                "Name": cust["company"] or f"{cust['last']} {cust['first']}",
                "TaxNumber": cust["tax"] or "",
                "Phone": f"+3630{random.randint(1000000, 9999999)}",
                "Zip": cust["zip"],
                "City": cust["city"],
                "Street": cust["street"],
                "Country": "Magyarország",
            },
            "Utm": utm,
            "Items": items,
        })
    return orders


# --- Shoprenter generátor ---
def generate_mock_shoprenter_orders(count: int = 50) -> list[dict[str, Any]]:
    """Shoprenter REST API (/orders?full=1) valós struktúrája."""
    orders: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        inner_id = 5001 + idx
        sr_id = base64.b64encode(f"order-order_id={inner_id}".encode()).decode()
        cust = random.choice(WEBSHOP_CUSTOMERS)
        utm = random.choice(UTM_SOURCES)
        created_dt = now - timedelta(hours=random.randint(1, 720))
        updated_dt = created_dt + timedelta(hours=random.randint(1, 12))

        selected = random.sample(WEBSHOP_CATALOG, k=random.randint(1, 3))
        order_products = []
        total_net = 0.0
        total_gross = 0.0

        for line_idx, prod in enumerate(selected, start=1):
            qty = random.randint(1, 3)
            line_net = round(prod["net_price"] * qty, 2)
            line_gross = round(prod["gross_price"] * qty, 2)
            total_net += line_net
            total_gross += line_gross

            order_products.append({
                "id": base64.b64encode(f"orderProduct-order_product_id={inner_id}{line_idx}".encode()).decode(),
                "sku": prod["sku"],
                "name": prod["name"],
                "quantity": str(qty),
                "price": f"{prod['net_price']:.2f}",
                "grossPrice": f"{prod['gross_price']:.2f}",
                "taxRate": "27.0000",
            })

        ship_gross = 1890.0
        ship_net = round(ship_gross / 1.27, 2)
        total_net = round(total_net + ship_net, 2)
        total_gross = round(total_gross + ship_gross, 2)

        orders.append({
            "id": sr_id,
            "innerId": str(inner_id),
            "invoiceId": f"SR-2026-{inner_id}",
            "firstname": cust["first"],
            "lastname": cust["last"],
            "email": f"{cust['first'].lower()}.{cust['last'].lower()}{idx}@vallalkozas.hu",
            "phone": f"+3620{random.randint(1000000, 9999999)}",
            "paymentCompany": cust["company"] or "",
            "paymentTaxNumber": cust["tax"] or "",
            "paymentPostcode": cust["zip"],
            "paymentCity": cust["city"],
            "paymentAddress1": cust["street"],
            "paymentCountryName": "Magyarország",
            "shippingMethodName": random.choice(["GLS Futárszolgálat", "MPL Csomagautomata"]),
            "paymentMethodName": random.choice(["SimplePay online fizetés", "Utánvétes fizetés", "Átutalás"]),
            "currency": {"code": "HUF"},
            "totalNet": f"{total_net:.2f}",
            "totalTax": f"{round(total_gross - total_net, 2):.2f}",
            "total": f"{total_gross:.2f}",
            "shippingTotal": f"{ship_gross:.2f}",
            "dateCreated": created_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "dateUpdated": updated_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "orderStatus": {"name": random.choice(["Teljesítve", "Teljesítve", "Feldolgozás alatt", "Sztornózva"])},
            "utmSource": utm["utm_source"],
            "utmMedium": utm["utm_medium"],
            "utmCampaign": utm["utm_campaign"],
            "orderProducts": order_products,
        })
    return orders


# --- WooCommerce generátor ---
def generate_mock_woocommerce_orders(count: int = 50) -> list[dict[str, Any]]:
    """WooCommerce REST API v3 (/wp-json/wc/v3/orders) valós struktúrája."""
    orders: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        order_id = 70001 + idx
        cust = random.choice(WEBSHOP_CUSTOMERS)
        utm = random.choice(UTM_SOURCES)
        created_dt = now - timedelta(hours=random.randint(1, 720))
        modified_dt = created_dt + timedelta(minutes=random.randint(15, 360))
        status = random.choice(["completed", "completed", "processing", "on-hold", "cancelled", "refunded"])

        selected = random.sample(WEBSHOP_CATALOG, k=random.randint(1, 3))
        line_items = []
        total_net = 0.0
        total_tax = 0.0

        for line_idx, prod in enumerate(selected, start=1):
            qty = random.randint(1, 3)
            line_net = round(prod["net_price"] * qty, 2)
            line_tax = round((prod["gross_price"] - prod["net_price"]) * qty, 2)
            total_net += line_net
            total_tax += line_tax

            line_items.append({
                "id": order_id * 10 + line_idx,
                "name": prod["name"],
                "product_id": 300 + line_idx,
                "variation_id": 0,
                "quantity": qty,
                "sku": prod["sku"],
                "price": prod["net_price"],
                "subtotal": f"{line_net:.2f}",
                "subtotal_tax": f"{line_tax:.2f}",
                "total": f"{line_net:.2f}",
                "total_tax": f"{line_tax:.2f}",
            })

        ship_net = 1567.0
        ship_tax = 423.0
        grand_total = round(total_net + total_tax + ship_net + ship_tax, 2)

        billing = {
            "first_name": cust["first"],
            "last_name": cust["last"],
            "company": cust["company"] or "",
            "address_1": cust["street"],
            "city": cust["city"],
            "postcode": cust["zip"],
            "country": "HU",
            "email": f"{cust['first'].lower()}.{cust['last'].lower()}{idx}@gmail.com",
            "phone": f"+3670{random.randint(1000000, 9999999)}",
        }

        orders.append({
            "id": order_id,
            "number": str(order_id),
            "status": status,
            "currency": "HUF",
            "date_created": created_dt.isoformat(),
            "date_modified": modified_dt.isoformat(),
            "discount_total": "0.00",
            "shipping_total": f"{ship_net:.2f}",
            "shipping_tax": f"{ship_tax:.2f}",
            "total": f"{grand_total:.2f}",
            "total_tax": f"{round(total_tax + ship_tax, 2):.2f}",
            "payment_method": random.choice(["barion", "simplepay", "cod", "bacs"]),
            "payment_method_title": random.choice(["Barion", "SimplePay", "Utánvét", "Közvetlen banki átutalás"]),
            "billing": billing,
            "shipping": billing,
            "line_items": line_items,
            "shipping_lines": [
                {"id": order_id * 100 + 1, "method_title": "GLS Futárszolgálat", "method_id": "gls", "total": f"{ship_net:.2f}", "total_tax": f"{ship_tax:.2f}"}
            ],
            "meta_data": [
                {"id": 1, "key": "_billing_tax_number", "value": cust["tax"] or ""},
                {"id": 2, "key": "_wc_order_attribution_utm_source", "value": utm["utm_source"]},
                {"id": 3, "key": "_wc_order_attribution_utm_medium", "value": utm["utm_medium"]},
                {"id": 4, "key": "_wc_order_attribution_utm_campaign", "value": utm["utm_campaign"]},
            ],
        })
    return orders
