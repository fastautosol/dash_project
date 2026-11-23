# 2026.11.23
import base64
import random
from datetime import datetime, timedelta, timezone
from typing import Any

CATALOG = [
    {"sku": "SKU-1001", "name": "Ergonomikus Irodai Szék Pro", "net_price": 69900.0, "gross_price": 88773.0, "vat": 27},
    {"sku": "SKU-1002", "name": "Állítható Magasságú Íróasztal", "net_price": 115000.0, "gross_price": 146050.0, "vat": 27},
    {"sku": "SKU-1003", "name": "USB-C Dokkoló Állomás 11-in-1", "net_price": 24900.0, "gross_price": 31623.0, "vat": 27},
    {"sku": "SKU-1004", "name": "Vezeték Nélküli Mechanikus Billentyűzet", "net_price": 32900.0, "gross_price": 41783.0, "vat": 27},
    {"sku": "SKU-1005", "name": "4K Webkamera Autofókusszal", "net_price": 28500.0, "gross_price": 36195.0, "vat": 27},
    {"sku": "SKU-1006", "name": "Zajszűrős Bluetooth Fejhallgató", "net_price": 44900.0, "gross_price": 57023.0, "vat": 27},
]

CUSTOMERS = [
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


def generate_mock_unas_orders(count: int = 50) -> list[dict[str, Any]]:
    """UNAS getOrder XML->dict valós struktúrája (Key elsődleges kulccsal)."""
    orders: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        order_key = str(20260001 + idx)
        cust = random.choice(CUSTOMERS)
        utm = random.choice(UTM_SOURCES)
        created_dt = now - timedelta(hours=random.randint(1, 720))
        mod_dt = created_dt + timedelta(hours=random.randint(1, 24))
        status = random.choice(["Teljesítve", "Teljesítve", "Feldolgozás alatt", "Új megrendelés", "Törölve"])

        selected = random.sample(CATALOG, k=random.randint(1, 3))
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


def generate_mock_shoprenter_orders(count: int = 50) -> list[dict[str, Any]]:
    """Shoprenter REST API (/orders?full=1) valós struktúrája."""
    orders: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        inner_id = 5001 + idx
        sr_id = base64.b64encode(f"order-order_id={inner_id}".encode()).decode()
        cust = random.choice(CUSTOMERS)
        utm = random.choice(UTM_SOURCES)
        created_dt = now - timedelta(hours=random.randint(1, 720))
        updated_dt = created_dt + timedelta(hours=random.randint(1, 12))

        selected = random.sample(CATALOG, k=random.randint(1, 3))
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


def generate_mock_woocommerce_orders(count: int = 50) -> list[dict[str, Any]]:
    """WooCommerce REST API v3 (/wp-json/wc/v3/orders) valós struktúrája."""
    orders: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for idx in range(count):
        order_id = 70001 + idx
        cust = random.choice(CUSTOMERS)
        utm = random.choice(UTM_SOURCES)
        created_dt = now - timedelta(hours=random.randint(1, 720))
        modified_dt = created_dt + timedelta(minutes=random.randint(15, 360))
        status = random.choice(["completed", "completed", "processing", "on-hold", "cancelled", "refunded"])

        selected = random.sample(CATALOG, k=random.randint(1, 3))
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
