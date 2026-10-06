# 2026.10.06 15.00 (MCP server)
from fastmcp import FastMCP
import requests
import psycopg2

# 1. Létrehozzuk a központi szervert
mcp = FastMCP("KKV-Intelligens-Központ")

DB_PARAMS = "dbname=sales_data user=postgres password=secret host=localhost"
N8N_WEBHOOK_URL = "https://cegnev.hu"

# ==================== ÚJ FUNKCIÓK (MOVIES DLT TÁBLA) ====================

@mcp.tool()
def get_movie_stats(title_keyword: str) -> str:
    """Lekérdezi egy film alapvető számszerű adatait (büdzsé, bevétel, értékelés) a dlt user_reviews táblából."""
    conn = psycopg2.connect(DB_PARAMS)
    cur = conn.cursor()
    
    # Biztonságos SQL lekérdezés kulcsszó alapján
    query = """
        SELECT title, budget, revenue, vote_average, release_date 
        FROM sales_data.user_reviews 
        WHERE title ILIKE %s 
        LIMIT 1;
    """
    cur.execute(query, (f"%{title_keyword}%",))
    row = cur.fetchone()
    cur.close()
    conn.close()
    
    if not row:
        return f"Nem találtam '{title_keyword}' nevű filmet az adatbázisban."
        
    title, budget, revenue, vote_avg, release_date = row
    return (
        f"🎬 **Film:** {title}\n"
        f"📅 **Megjelenés:** {release_date}\n"
        f"💰 **Költségvetés:** ${budget:,.0f}\n"
        f"💵 **Bevétel:** ${revenue:,.0f}\n"
        f"⭐️ **Értékelés:** {vote_avg}/10"
    )

@mcp.tool()
def search_movie_context(query: str, limit: int = 3) -> str:
    """RAG-szerű kontextus keresés a filmek leírása (overview) és kritikái között kulcsszavak alapján."""
    conn = psycopg2.connect(DB_PARAMS)
    cur = conn.cursor()
    
    # Keresés a film leírásában (overview)
    sql = """
        SELECT title, overview, vote_average 
        FROM sales_data.user_reviews 
        WHERE overview ILIKE %s OR title ILIKE %s
        ORDER BY vote_average DESC 
        LIMIT %s;
    """
    like_param = f"%{query}%"
    cur.execute(sql, (like_param, like_param, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    if not rows:
        return f"Nincs releváns találat a(z) '{query}' témában."
        
    result = f"Találatok a(z) '{query}' témára a belső adatbázisból:\n\n"
    for r in rows:
        result += f"🎥 **{r[0]}** (Értékelés: {r[2]})\n📝 Leírás: {r[1][:400]}...\n\n"
    
    return result


# ==================== Régi FUNKCIÓK (MOVIES DLT TÁBLA) ====================

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


# --- 3. FUNKCIÓ: Automatikus "Feature Request" (Fejlesztési igény) ---
@mcp.tool()
def log_missing_feature(feature_description: str) -> str:
    """Akkor hívd meg, ha a felhasználó olyat kér, amit az adatbázisból nem tudsz kiszolgálni."""
    # Ez a funkció küld egy n8n webhookot, ami neked dob egy e-mailt vagy Slack üzenetet: 
    # "Az ügyfél az X funkciót kereste, írd meg Pythonban!"
    requests.post("https://cegnev.hu", json={"req": feature_description})
    return "Igény regisztrálva a fejlesztő felé."


# --- Futtatás SSE (Server-Sent Events) protokollal, hogy az n8n elérje hálózaton ---
if __name__ == "__main__":
    mcp.run(transport="sse", host="0.0.0.0", port=8000)
