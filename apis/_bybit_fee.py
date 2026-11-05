import asyncio
import logging
import ccxt

# ==========================================
# 1. KONFIGURÁCIÓ & PARAMÉTEREK (A te értékeid)
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger(__name__)

POLL_INTERVAL = 900          # 15 perces ciklus (900 másodperc)
MAX_OPEN_POSITIONS = 3       # Egyszerre max 3 nyitott pozíció
LEVERAGE = 5                 # Fix 5x tőkeáttétel a bot megbízásaihoz
POSITION_SIZE_USD = 100.0    # Teljes pozícióméret dollárban (Margin = ~20 USDT)

# A te szűrési küszöbértékeid
MIN_LEV       = 50
MIN_TURNOVER  = 1_000_000   # USDT / 24h
MIN_R24       = 3.0         # % (24 órás momentum)
MIN_R1        = 0.25        # % (1 órás friss momentum)

FEE_RATE_TAKER = 0.00055     # Bybit VIP 0 Futures Taker díj (0.055%)

# Bybit kliens inicializálása (Írási jogú API kulcsokkal az autotradinghez)
ex = ccxt.bybit({
    "enableRateLimit": True, 
    "apiKey": "YOUR_API_KEY", 
    "secret": "YOUR_SECRET",
    "options": {"defaultType": "linear"}
})

# ==========================================
# 2. DÍJ-KALKULÁTOR MODUL
# ==========================================
def calculate_and_log_fees(symbol: str, price: float, amount: float, current_leverage: int):
    """Kiszámolja és transzparensen kiírja a logba a várható Taker díjakat."""
    notional_value = amount * price
    margin_used = notional_value / current_leverage
    
    # Nyitási és becsült zárási díj (Taker: 0.055%)
    open_fee = notional_value * FEE_RATE_TAKER
    estimated_close_fee = notional_value * FEE_RATE_TAKER # Elméleti (ha az ár nem változik)
    total_roundtrip_fee = open_fee + estimated_close_fee
    
    log.info(f"--- [DÍJ KALKULÁTOR] {symbol} ---")
    log.info(f"  | Teljes méret (Notional): {notional_value:.2f} USDT")
    log.info(f"  | Felhasznált fedezet (Margin): {margin_used:.2f} USDT ({current_leverage}x leverage)")
    log.info(f"  | Nyitási díj (Taker 0.055%): {open_fee:.4f} USDT")
    log.info(f"  | Becsült zárási díj (Taker 0.055%): {estimated_close_fee:.4f} USDT")
    log.info(f"  | Összes oda-vissza költség: {total_roundtrip_fee:.4f} USDT (A letett tőke {((total_roundtrip_fee/margin_used)*100):.2f}%-a!)")
    log.info(f"---------------------------------")

# ==========================================
# 3. POZÍCIÓ KEZELŐK (Taker / Market)
# ==========================================
def get_active_positions():
    """Lekéri a Bybitről a ténylegesen nyitott határidős pozícióidat."""
    try:
        ex.load_markets()
        positions = ex.fetch_positions()
        # Csak azokat a pozíciókat gyűjtjük be, amik ténylegesen nyitva vannak (size > 0)
        return {p['symbol']: float(p['size']) for p in positions if float(p.get('size', 0)) > 0}
    except Exception as e:
        log.error(f"[API HIBA] Pozíciók lekérése sikertelen: {e}")
        return {}

def execute_market_order(symbol: str, side: str, amount: float):
    """Végrehajtja a piaci megbízást."""
    try:
        order = ex.create_order(
            symbol=symbol,
            type='market',
            side=side,
            amount=amount,
            params={'positionIdx': 0}
        )
        log.info(f"[SIKER] {side.upper()} order elküldve! ID: {order['id']}")
        return order
    except Exception as e:
        log.error(f"[MEGBÍZÁS HIBA] {symbol} {side} sikertelen: {e}")
        return None

# ==========================================
# 4. FORGÓAJTÓ CIKLUS (A te kódodra építve)
# ==========================================
async def trading_cycle():
    log.info("[FORGÓAJTÓ] Új ciklus indul, piac szkennelése...")
    
    # A) Lekérjük a Bybitről, mi van MOST nyitva (Valós állapot szinkron)
    current_portfolio = await asyncio.to_thread(get_active_positions)
    log.info(f"[PORTFÓLIÓ] Aktív pozíciók a tőzsdén: {list(current_portfolio.keys())}")
    
    # B) A TE EREDETI KÓDRÉSZLETED ( fetch_tickers + szűrés )
    markets = await asyncio.to_thread(ex.load_markets)
    tickers = await asyncio.to_thread(ex.fetch_tickers, {"category": "linear"})

    rows = []
    for sym, t in tickers.items():
        m = markets.get(sym)
        if not m or not m.get("swap") or m.get("quote") != "USDT" or m.get("settle") != "USDT":
            continue

        lev = float((m["info"].get("leverageFilter") or {}).get("maxLeverage") or 0)
        if lev < MIN_LEV:
            continue

        info = t["info"]
        last = float(info["lastPrice"])
        p1   = float(info["prevPrice1h"])
        p24  = float(info["prevPrice24h"])
        turnover = float(info["turnover24h"])
        if not (p1 and p24):
            continue

        r1  = (last / p1  - 1) * 100
        r24 = (last / p24 - 1) * 100

        # Itt alkalmazzuk a te szűrési feltételeidet (aktiválva az r1-et is!)
        if turnover >= MIN_TURNOVER and r24 >= MIN_R24 and r1 >= MIN_R1:
            rows.append({
                "symbol": sym, "price": last, "r1": r1, "r24": r24, "turnover": turnover
            })

    # Rangsoroljuk a talált momentum coinokat r24 alapján (a te eredeti sorted-ed szerint)
    sorted_signals = sorted(rows, key=lambda x: x["r24"], reverse=True)
    signal_symbols = [s["symbol"] for s in sorted_signals]
    log.info(f"[SZŰRŐ] Szűrésnek megfelelő coinok ({len(signal_symbols)} db): {signal_symbols[:10]}...")

    # C) FORGÓAJTÓ DÖNTÉSI MECHANIZMUS
    
    # 1. EXIT szabály: Ha egy coinunk nyitva van, de KIESETT a szűrt listáról -> Azonnal zárjuk (Töröljük)
    for open_sym in list(current_portfolio.keys()):
        if open_sym not in signal_symbols:
            log.info(f"[FORGÓAJTÓ - EXIT] {open_sym} elvesztette a momentumot, pozíció lezárása.")
            # Lekérjük a pontos nyitott darabszámot, hogy mindent lezárjunk
            exact_size = current_portfolio[open_sym]
            await asyncio.to_thread(execute_market_order, open_sym, "sell", exact_size)
            del current_portfolio[open_sym]

    # 2. ENTRY szabály: Ha van szabad helyünk, az új (rangsorolt) toplistásokat megvesszük
    for signal in sorted_signals:
        sym = signal["symbol"]
        price = signal["price"]
        
        # Ha elértük a max pozíció korlátot, megállunk
        if len(current_portfolio) >= MAX_OPEN_POSITIONS:
            log.info(f"[RISK] Portfólió megtelt ({MAX_OPEN_POSITIONS}/{MAX_OPEN_POSITIONS}). Nincs több nyitás.")
            break
            
        # Ha a toplistás coin még nincs nyitva, megnyitjuk
        if sym not in current_portfolio:
            log.info(f"[FORGÓAJTÓ - ENTRY] Új jelölt észlelése: {sym} (r24: {signal['r24']:.2f}%)")
            
            # Tőkeáttétel kényszerítése
            await asyncio.to_thread(ex.set_leverage, LEVERAGE, sym)
            
            # Darabszám kiszámítása és kerekítése Bybit szabályok szerint
            raw_amount = POSITION_SIZE_USD / price
            amount = float(ex.amount_to_precision(sym, raw_amount))
            
            # DÍJ-KALKULÁCIÓ MEGJELENÍTÉSE NYITÁS ELŐTT
            calculate_and_log_fees(sym, price, amount, LEVERAGE)
            
            # Pozíció megnyitása (Market Buy)
            order = await asyncio.to_thread(execute_market_order, sym, "buy", amount)
            if order:
                current_portfolio[sym] = amount

# ==========================================
# 5. FŐPROGRAM
# ==========================================
async def main():
    log.info(f"Bybit Automata Forgóajtó Bot elindítva. Frissítés {POLL_INTERVAL} másodpercenként.")
    while True:
        try:
            await trading_cycle()
        except Exception as e:
            log.error(f"[CIKLUS HIBA] Vratlan hiba történt: {e}")
        
        await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
