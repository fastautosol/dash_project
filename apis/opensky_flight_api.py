# apis/flight_tracker_api.py
import httpx
import psycopg2
from fastapi import APIRouter
from mcp.server.fastmcp import FastMCP

router = APIRouter()

# Központi FastMCP regisztráció (az app_mcp.py-ban lévő tmdb mintájára külön is futhat, 
# vagy importálható a központi szerverre)
mcp = FastMCP("Emirates-Flight-Radar")

DB_PARAMS = "dbname=sales_data user=postgres password=secret host=localhost"
OPENSKY_STATES_URL = "https://opensky-network.org"

def get_tracked_icao_codes() -> list[str]:
    """Kihozza a PostgreSQL-ből a dlt által korábban összegyűjtött egyedi icao24 kódokat."""
    try:
        conn = psycopg2.connect(DB_PARAMS)
        cur = conn.cursor()
        # Kiválasztjuk az egyedi UAE gépeket, amiket a reptéri dlt pipeline mentett le
        cur.execute("SELECT DISTINCT icao24 FROM sky_monitor.scheduled_a380_flights WHERE icao24 IS NOT NULL;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [r[0].strip() for r in rows if r[0]]
    except Exception as e:
        print(f"Adatbázis hiba az ICAO kódok lekérésekor: {e}")
        return []

async def fetch_live_states_from_opensky(icao_list: list[str]) -> list[dict]:
    """Célzottan lekéri az OpenSky-tól az adott ICAO24 kódok élő pozícióit."""
    if not icao_list:
        return []

    # Az OpenSky többszörös 'icao24' query paramétert vár: ?icao24=c81ca2&icao24=c81ca3
    params = [("icao24", icao_3) for icao_3 in icao_list]
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            # Itt opcionálisan hozzáadhatod az OAuth2 Bearer tokent is a headers-höz, 
            # de a célzott lekérdezés token nélkül is magasabb rate-limittel fut, mint a globális
            response = await client.get(OPENSKY_STATES_URL, params=params)
            if response.status_code != 200:
                return []
                
            data = response.json()
            states = data.get("states", [])
            
            live_flights = []
            if states:
                for s in states:
                    # Szűrés és validálás: csak a valós koordinátákkal rendelkező, levegőben lévő gépek
                    if s[5] is not None and s[6] is not None:
                        live_flights.append({
                            "icao24": s[0].strip() if s[0] else "N/A",
                            "callsign": s[1].strip() if s[1] else "UNKNOWN",
                            "origin_country": s[2],
                            "longitude": float(s[5]),
                            "latitude": float(s[6]),
                            "altitude_m": float(s[7]) if s[7] else 0.0,
                            "on_ground": bool(s[8]),
                            "velocity_mps": float(s[9]) if s[9] else 0.0,
                            "heading_deg": float(s[10]) if s[10] else 0.0,
                            "vertical_rate_mps": float(s[11]) if s[11] else 0.0,
                        })
            return live_flights
        except Exception as e:
            print(f"Hiba az OpenSky API hívásakor: {e}")
            return []

# ----- 1. FASTAPI ENDPOINT (A Dash térkép aszinkron AJAX hívásaihoz) -----
@router.get("/live-positions")
async def get_live_positions():
    """Visszaadja a Dash frontend számára az éppen levegőben lévő Emirates A380-asok koordinátáit."""
    icao_codes = get_tracked_icao_codes()
    if not icao_codes:
        return {"flights": []}
    
    # Korlátozzuk a paraméterek számát (az OpenSky egyszerre max 50-100 kódot szeret), ha a dlt túl sokat gyűjtött
    live_data = await fetch_live_states_from_opensky(icao_codes[:50])
    return {"flights": live_data}

# ----- 2. FASTMCP TOOL (Az n8n és Mistral AI Agent számára) -----
@mcp.tool()
async def track_emirates_fleet() -> str:
    """Megmutatja az n8n AI Agent számára a megfigyelt Emirates flotta aktuális helyzetét, magasságát és sebességét."""
    icao_codes = get_tracked_icao_codes()
    if not icao_codes:
        return "Jelenleg nincsenek követett ICAO24 kódok az adatbázisban. Futtasd a dlt pipeline-t!"
        
    live_data = await fetch_live_states_from_opensky(icao_codes[:50])
    if not live_data:
        return "Jelenleg egyetlen követett Emirates óriásgép sem tartózkodik az OpenSky által látott légtérben."
        
    report = f"✈️ **Aktív Emirates Óriásgép Jelentés ({len(live_data)} gép a levegőben):**\n\n"
    for f in live_data:
        report += (
            f"- **Járat:** {f['callsign']} (ICAO: {f['icao24']})\n"
            f"  📍 Pozíció: Lon {f['longitude']:.4f}, Lat {f['latitude']:.4f}\n"
            f"  🧭 Irány: {f['heading_deg']}°, Sebesség: {f['velocity_mps'] * 3.6:.0f} km/h\n"
            f"  📈 Magasság: {f['altitude_m']:.0f} méter\n\n"
        )
    return report
