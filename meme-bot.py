# 2026.08.19  12.00
import asyncio
import aiohttp
import time
from datetime import datetime, UTC, timedelta
import pandas as pd
import dlt

# =========================
# CONFIGURATION
# =========================
N8N_WEBHOOK_URL = "https://n8n.fastautosol.com/webhook/meme-alert"
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
POLL_INTERVAL = 60          # Mémeknél 60 mp-enként frissítünk (DexScreener engedi)
WATCHLIST_EXPIRY_MINS = 30  # Ha 30 percig nem indul meg a mém, eldobjuk, mert "halott"
CLEANUP_HOURS = 12          # A mém-adatok gyorsan avulnak, 12 óra elég

class MemeState:
    def __init__(self):
        # Strukturált tárolás: { token_address: { "base_price": float, "base_volume": float, "added_at": float } }
        self.watchlist = {}
        self.last_cleanup_time = 0
        self.last_new_tokens_fetch = 0

state = MemeState()

# =========================
# RUGCHECK & DISCOVERY ENGINE (Biztonsági szűrés)
# =========================
async def discover_and_filter_tokens(session):
    "Ez a függvény felelős azért, hogy új/trendi tokeneket keressen és átengedje őket a biztonsági szűrőn"
    try:
        # Ingyenes DexScreener token profilok (legutóbbiak)
        async with session.get("https://dexscreener.com") as resp:
            if resp.status != 200: return
            profiles = await resp.json()
            
        for p in profiles[:25]:
            token_address = p.get("tokenAddress")
            chain_id = p.get("chainId")
            
            if chain_id != "solana" or token_address in state.watchlist:
                continue # Csak solana mémekkel játszunk, és amit már figyelünk, azt kihagyjuk
                
            # RUGCHECK FUTTATÁSA - Kis szünettel a kérések között
            rug_url = f"https://rugcheck.xyz{token_address}/report"
            await asyncio.sleep(0.25)
            async with session.get(rug_url) as rug_resp:
                if rug_resp.status != 200: continue
                rug_data = await rug_resp.json()
                
                # Gyors kiszűrés: Ha veszélyes, átugorjuk
                risks = rug_data.get("risks", [])
                is_dangerous = any("Large Shareholder" in r.get("name", "") or "Freeze" in r.get("name", "") for r in risks)
                
                if not is_dangerous:
                    # HA TISZTA BETESSZÜK A VÁRÓLISTÁRA
                    state.watchlist[token_address] = {
                        "base_price": None,
                        "base_volume": None,
                        "added_at": time.time(),
                        "symbol": p.get("symbol", "MEME")
                    }
                    print(f"[DISCOVERY] Új tiszta token várólistára véve: {p.get('symbol')} ({token_address})")
    except Exception as e:
        print(f"Discovery Error: {e}")

# =========================
# PRICE & BREAKOUT ANALYTICS
# =========================
async def analyze_watchlist(session, pipeline):
    if not state.watchlist:
        return

    # Kitöröljük a lejárt (30 percnél régebbi) tokeneket a memóriából
    now_ts = time.time()
    expired = [addr for addr, data in state.watchlist.items() if now_ts - data["added_at"] > (WATCHLIST_EXPIRY_MINS * 60)]
    for addr in expired:
        del state.watchlist[addr]
        print(f"[WATCHLIST] Token lejárt, eltávolítva: {addr}")

    if not state.watchlist: return

    try:
        # DexScreener csoportos lekérdezés (vesszővel elválasztva az összes figyelt cím)
        addresses = ",".join(state.watchlist.keys())
        url = f"https://dexscreener.com{addresses}"
        
        async with session.get(url) as resp:
            if resp.status != 200: return
            data = await resp.json()
            pairs = data.get("pairs", [])

        records_to_db = []
        alerts_to_send = []

        for pair in pairs:
            addr = pair["baseToken"]["address"]
            symbol = pair["baseToken"]["symbol"]
            
            if addr not in state.watchlist: continue
            
            current_price = float(pair.get("priceUsd", 0))
            current_vol_5m = float(pair.get("volume", {}).get("m5", 0)) # 5 perces volumen!
            
            token_state = state.watchlist[addr]

            # Ha ez az első lekérdezés a tokenre, beállítjuk az alapárat/volument baseline-nak
            if token_state["base_price"] is None:
                token_state["base_price"] = current_price
                token_state["base_volume"] = current_vol_5m
                continue

            # SZÁMOLÁS (Ár és Volumen változás az alapértékhez képest)
            base_price = token_state["base_price"]
            base_vol = token_state["base_volume"] if token_state["base_volume"] > 0 else 1
            
            price_change_pcnt = ((current_price - base_price) / base_price) * 100
            vol_growth_factor = current_vol_5m / base_vol

            # SQL adatbázis rekord előkészítése (DLT-nek)
            record = {
                "token_address": addr,
                "symbol": symbol,
                "timestamp": datetime.now(UTC),
                "price": current_price,
                "volume_5m": current_vol_5m,
                "price_change_from_base": round(price_change_pcnt, 2),
                "vol_growth_factor": round(vol_growth_factor, 2),
                "liquidity_usd": round(float(pair.get("liquidity", {}).get("usd", 0)), 2)
            }
            records_to_db.append(record)

            # KITÖRÉSI TRIGGER FELTÉTELEK:
            if 10.0 <= price_change_pcnt <= 50.0 and vol_growth_factor >= 2.0:
                
                # Azonnal kivesszük a watchlistből, hogy ne triggereljen újra 60 mp múlva
                del state.watchlist[addr] 
                
                # Payload az N8N-nek + közvetlen OKX Web3 tárcás swap link generálása a kényelemért!
                payload = record | {
                    "signal": "MEME_PUMP_BREAKOUT",
                    "okx_swap_url": f"https://okx.com_{addr}",
                    "timestamp": record["timestamp"].isoformat()
                }
                alerts_to_send.append(payload)

        # ----- Webhook riasztások kiküldése az N8N-nek -----
        for alert in alerts_to_send:
            await session.post(N8N_WEBHOOK_URL, json=alert)
            print(f"[{datetime.now(UTC).isoformat(timespec='seconds')}] ALERT {alert['symbol']} TRIGGERED! Küldve az N8N-nek.")

        # ----- SQL DB mentés DLT-vel -----
        if records_to_db:
            await asyncio.to_thread(
                pipeline.run, 
                records_to_db, 
                table_name="meme_watchlist", 
                write_disposition="append"
            )

    except Exception as e:
        print(f"Analysis Loop Error: {e}")

# =========================
# MAIN ASYNC LOOP
# =========================
async def main():
    pipeline = dlt.pipeline(
        pipeline_name="meme_pump_strategy",
        destination=dlt.destinations.postgres(credentials=DB_URL),
        dataset_name="meme_data"
    )

    async with aiohttp.ClientSession() as session:
        while True:
            now = time.time()
            try:
                # 1. Új tokenek felfedezése és RugCheck szűrése (3 percenként)
                if now - state.last_new_tokens_fetch > 180:
                    await discover_and_filter_tokens(session)
                    state.last_new_tokens_fetch = now

                # 2. Várólista elemzése és Kitörés csekkolás (60 másodpercenként)
                await analyze_watchlist(session, pipeline)

                # 3. Adatbázis takarítás (Óránként, a CLEANUP_HOURS-nál régebbi adatok törlése)
                if (now - state.last_cleanup_time) > 3600:
                    try:
                        with pipeline.sql_client() as client:    
                            table_name = client.make_qualified_table_name("meme_watchlist")                        
                            threshold = datetime.now(UTC) - timedelta(hours=CLEANUP_HOURS)
                            client.execute_sql(f"DELETE FROM {table_name} WHERE timestamp < %s", threshold)
                        state.last_cleanup_time = now
                        print(f"[{datetime.now(UTC).isoformat(timespec='seconds')}] Adatbázis takarítás kész.")
                    except Exception as db_e:
                        print(f"Cleanup error: {db_e}")

            except Exception as e:
                print(f"Main Loop Error: {e}")

            await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
