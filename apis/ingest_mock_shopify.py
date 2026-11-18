import json
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Any


def _load_optional_tickets() -> list[dict[str, str]]:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base_dir, "generated_tickets.json")
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


def _build_address(first_name: str, last_name: str, phone: str) -> dict[str, Any]:
    city, zip_code = random.choice([
        ("Budapest", "1134"),
        ("Debrecen", "4025"),
        ("Szeged", "6720"),
        ("Győr", "9021"),
        ("Pécs", "7621"),
        ("Miskolc", "3525"),
    ])
    street = f"{random.choice(['Váci út', 'Kossuth Lajos utca', 'Petőfi Sándor utca', 'Rákóczi út', 'Bajcsy-Zsilinszky út'])} {random.randint(1, 120)}."
    company = random.choice([None, None, "Példa Kft.", "Alfa Logisztika Zrt.", "NextGen Trade Bt."])
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


def generate_mock_shopify_orders(count: int) -> list[dict[str, Any]]:
    tickets = _load_optional_tickets()
    catalog = [
        {"product_id": 881001, "variant_id": 44100101, "sku": "DIG-BP-001", "title": "KKV Automatizációs Blueprint (PDF)", "variant_title": "Alap licenc", "vendor": "DataFlow Kft.", "price": 29900.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881002, "variant_id": 44100201, "sku": "DIG-N8N-002", "title": "n8n + PostgreSQL Sabloncsomag", "variant_title": "Pro csomag", "vendor": "DataFlow Kft.", "price": 59900.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881003, "variant_id": 44100301, "sku": "SRV-AUD-003", "title": "AI Adat-Audit Konzultáció", "variant_title": "60 perces online", "vendor": "DataFlow Kft.", "price": 89000.0, "grams": 0, "requires_shipping": False},
        {"product_id": 881004, "variant_id": 44100401, "sku": "HW-SCN-004", "title": "Okos Raktár Vonalkód Olvasó", "variant_title": "USB + Bluetooth", "vendor": "ZebraTech", "price": 44900.0, "grams": 380, "requires_shipping": True},
    ]

    landing_options = [
        ("/products/automatizacios-blueprint?utm_source=facebook&utm_medium=cpc", "https://l.facebook.com/"),
        ("/products/n8n-sabloncsomag?utm_source=google&utm_medium=cpc", "https://www.google.com/"),
        ("/", "https://www.linkedin.com/"),
        ("/collections/all", None),
    ]

    orders = []
    for idx in range(count):
        order_id = random.randint(5000000000000, 5999999999999)
        order_num = 1001 + idx
        currency = "HUF"

        first_name = random.choice(["Anna", "Bence", "Csaba", "Dóra", "Eszter", "Gábor", "Katalin", "László", "Péter", "Zsófia"])
        last_name = random.choice(["Nagy", "Kovács", "Tóth", "Szabó", "Horváth", "Varga", "Kiss", "Molnár", "Németh", "Farkas"])
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@{random.choice(['ceg.hu', 'gmail.com', 'vallalkozas.hu', 'kft.hu'])}"
        phone = f"+36{random.choice(['20', '30', '70'])}{random.randint(1000000, 9999999)}"
        address = _build_address(first_name, last_name, phone)

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
