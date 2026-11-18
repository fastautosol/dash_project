import random
from datetime import datetime, timedelta, timezone
from typing import Any


def generate_mock_billingo_documents(count: int) -> list[dict[str, Any]]:
    partners = [
        {"id": 10201, "name": "Alfa Logisztika Kft.", "taxcode": "12345678-2-41", "email": "penzugy@alfalog.hu", "city": "Budapest", "post_code": "1134", "address": "Váci út 45."},
        {"id": 10202, "name": "Beta Építőipari Zrt.", "taxcode": "23456789-2-42", "email": "szamla@betaepito.hu", "city": "Debrecen", "post_code": "4025", "address": "Piac utca 12."},
        {"id": 10203, "name": "Gamma Webkereskedelmi Bt.", "taxcode": "34567890-2-13", "email": "info@gammashop.hu", "city": "Szeged", "post_code": "6720", "address": "Kárász utca 8."},
        {"id": 10204, "name": "Delta Energetika Kft.", "taxcode": "45678901-2-08", "email": "iroda@deltaenergy.hu", "city": "Győr", "post_code": "9021", "address": "Baross Gábor út 21."},
        {"id": 10205, "name": "Omega Kereskedőház Kft.", "taxcode": "56789012-2-43", "email": "szamlaszolg@omegatrade.hu", "city": "Pécs", "post_code": "7621", "address": "Király utca 19."},
    ]

    services = [
        {"product_id": 501, "name": "Havi üzemeltetési és SLA átalánydíj", "unit": "hó", "net_unit_price": 85000.0, "vat": "27%"},
        {"product_id": 502, "name": "Egyedi adatpipeline fejlesztés", "unit": "projekt", "net_unit_price": 450000.0, "vat": "27%"},
        {"product_id": 503, "name": "Rendszerintegrációs tanácsadás", "unit": "óra", "net_unit_price": 25000.0, "vat": "27%"},
        {"product_id": 504, "name": "n8n workflow karbantartás", "unit": "db", "net_unit_price": 45000.0, "vat": "27%"},
    ]

    documents = []
    today = datetime.now(timezone.utc).date()

    for idx in range(count):
        doc_id = 30000000 + idx
        doc_type = random.choice(["invoice", "invoice", "invoice", "proforma", "advance"])
        prefix = "PRO" if doc_type == "proforma" else "E-KCS"
        invoice_number = f"{prefix}-2026-{1001 + idx}"

        partner = random.choice(partners)
        days_ago = random.randint(0, 60)
        invoice_date = today - timedelta(days=days_ago)
        payment_term = random.choice([8, 15, 30])
        due_date = invoice_date + timedelta(days=payment_term)

        if days_ago > payment_term + 5:
            payment_status = random.choice(["paid", "paid", "paid", "expired"])
        elif days_ago > 3:
            payment_status = random.choice(["paid", "paid", "outstanding"])
        else:
            payment_status = random.choice(["paid", "outstanding"])

        paid_date = (
            (invoice_date + timedelta(days=random.randint(0, payment_term))).isoformat()
            if payment_status == "paid"
            else None
        )

        selected_items = random.sample(services, k=random.randint(1, 2))
        items = []
        net_total = 0.0
        gross_total = 0.0

        for srv in selected_items:
            qty = random.randint(1, 4)
            item_net = round(srv["net_unit_price"] * qty, 2)
            item_vat = round(item_net * 0.27, 2)
            item_gross = round(item_net + item_vat, 2)

            net_total += item_net
            gross_total += item_gross

            items.append({
                "product_id": srv["product_id"],
                "name": srv["name"],
                "quantity": float(qty),
                "unit": srv["unit"],
                "net_unit_price": srv["net_unit_price"],
                "net_amount": item_net,
                "gross_amount": item_gross,
                "vat": srv["vat"],
                "entitlement": None,
                "comment": "",
            })

        documents.append({
            "id": doc_id,
            "invoice_number": invoice_number,
            "type": doc_type,
            "cancelled": False,
            "block_id": 14820,
            "payment_status": payment_status,
            "payment_method": random.choice(["wire_transfer", "barion", "bankcard", "cash"]),
            "gross_total": round(gross_total, 2),
            "net_total": round(net_total, 2),
            "currency": "HUF",
            "conversion_rate": 1.0,
            "invoice_date": invoice_date.isoformat(),
            "fulfillment_date": invoice_date.isoformat(),
            "due_date": due_date.isoformat(),
            "paid_date": paid_date,
            "organization": {
                "name": "Saját KKV Vállalkozás EV",
                "tax_number": "99887766-1-41",
                "bank_account_number": "11700000-20000000-00000000",
                "bank_account_iban": "HU42117000002000000000000000",
                "swift": "OTPVHUHB",
                "small_taxpayer": True,
                "ev_number": "58123456",
                "eu_tax_number": None,
                "cash_settled": False,
            },
            "partner": {
                "id": partner["id"],
                "name": partner["name"],
                "address": {"country_code": "HU", "post_code": partner["post_code"], "city": partner["city"], "address": partner["address"]},
                "emails": [partner["email"]],
                "taxcode": partner["taxcode"],
                "iban": "",
                "swift": "",
                "account_number": "",
                "phone": "+36301234567",
                "general_ledger_number": "",
                "tax_type": "HAS_TAX_NUMBER",
                "custom_billing_settings": {
                    "payment_method": "wire_transfer",
                    "document_form": "electronic",
                    "due_days": payment_term,
                    "document_currency": "HUF",
                    "template_language_code": "hu",
                    "discount": None,
                },
                "group_member_tax_number": None,
            },
            "document_partner": {
                "id": partner["id"],
                "name": partner["name"],
                "address": {"country_code": "HU", "post_code": partner["post_code"], "city": partner["city"], "address": partner["address"]},
                "emails": [partner["email"]],
                "taxcode": partner["taxcode"],
            },
            "electronic": True,
            "comment": "",
            "tags": ["kkv-auto"],
            "notification_status": "opened",
            "language": "hu",
            "items": items,
            "summary": {
                "net_amount_local": round(net_total, 2),
                "gross_amount_local": round(gross_total, 2),
                "vat_amount_local": round(gross_total - net_total, 2),
            },
            "settings": {
                "mediated_service": False,
                "without_financial_fulfillment": False,
                "online_payment": "Barion",
                "round": "five",
                "no_send_onlineszamla_by_user": False,
                "order_number": f"ORD-{1001 + idx}",
                "place_id": None,
                "instant_payment": False,
            },
            "online_szamla_status": "done",
            "related_documents": [],
            "discount": None,
            "correction_type": "none",
        })
    return documents
