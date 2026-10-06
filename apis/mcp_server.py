# server.py
from fastmcp import FastMCP
import requests
import psycopg2

# 1. Létrehozzuk a központi szervert
mcp = FastMCP("KKV-Intelligens-Központ")

DB_PARAMS = "dbname=sales_data user=postgres password=secret host=localhost"
N8N_WEBHOOK_URL = "https://cegnev.hu"

# --- 1. FUNKCIÓ: Adatbázis lekérdezés (Olvasás) ---
@mcp.tool()
def get_top_customers(limit: int = 5) -> str:
    """Lekérdezi a legnagyobb értékben vásárló ügyfeleket a PostgreSQL-ből."""
    conn = psycopg2.connect(DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT customer, SUM(total_huf) FROM sales_data.orders GROUP BY customer ORDER BY 2 DESC LIMIT %s;", (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return "Top vásárlók:\n" + "".join([f"- {r[0]}: {r[1]} HUF\n" for r in rows])

# --- 2. FUNKCIÓ: n8n workflow indítás (Írás / Akció) ---
@mcp.tool()
def trigger_customer_compensation(customer_name: str, email: str, discount_percent: int = 10) -> str:
    """Elindítja a panaszos ügyfél kárpótlását n8n-en át: kupon generálás, email, slack."""
    payload = {"ugyfel_neve": customer_name, "email": email, "kupon_szazalek": discount_percent}
    try:
        response = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=10)
        return f"Sikeres! Az n8n folyamat elindult {customer_name} részére." if response.status_code == 200 else "Hiba az n8n hívásakor."
    except Exception as e:
        return f"Kapcsolódási hiba: {str(e)}"

# --- Futtatás SSE (Server-Sent Events) protokollal, hogy az n8n elérje hálózaton ---
if __name__ == "__main__":
    mcp.run(transport="sse", host="0.0.0.0", port=8000)
