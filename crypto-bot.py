# 2026.06.21  17.00
import asyncio
import ccxt.async_support as ccxt
import httpx
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger(__name__)

WEBHOOK_URL = "https://n8n.fastautosol.com/webhook/crypto-alerts"
POLL_INTERVAL = 300  # 5 minutes

# Custom Strategy Thresholds
PRICE_CHANGE_1H_THRESHOLD = 2.0   # Trigger if 1h price change exceeds +2% (or drops below -2%)
VOLUME_SPIKE_THRESHOLD = 2.0      # Trigger if current 24h rolling volume is 2x greater than 5m ago
OI_INCREASE_THRESHOLD = 3.0       # Trigger if Open Interest increases by 3% inside 5 minutes

exchange = None
http_client = None
symbols = []
previous_state = {}  # Format: {symbol: {"price": float, "volume": float, "oi": float}}

# =========================
# FUNCTIONS
# =========================

async def initialize_markets():
    global symbols, exchange
    log.info("Loading Bybit markets and filtering for >= 20x leverage...") 
    markets = await exchange.load_markets()
    
    for symbol, market in markets.items():
        if market.get('linear') and market.get('swap'):
            lev_filter = market.get('info', {}).get('leverageFilter', {})
            max_leverage = float(lev_filter.get('maxLeverage', 0))          
            if max_leverage >= 20 and symbol not in symbols:
                symbols.append(symbol)
                
    log.info(f"Filtered {len(symbols)} linear contracts supporting >= 20x leverage.") 
            

async def send_webhook(payload: dict):
    global http_client
    try:
        response = await http_client.post(WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            log.info(f"[ALERT SENT] Webhook successful for {payload['symbol']}")
        else:
            log.error(f"[WEBHOOK ERROR] Status code: {response.status_code}")
    except Exception as e:
        log.error(f"[WEBHOOK FAILED] Could not connect to webhook server: {e}")


async def check_metrics():
    global symbols, exchange, previous_state   
    try:
        log.info("Fetching latest market tickers via REST...")
        tickers = await exchange.fetch_tickers(symbols=symbols)
    except Exception as e:
        log.error(f"Failed to fetch tickers: {e}")
        return

    for symbol in symbols:

        ticker_data = tickers.get(symbol)
        if not ticker_data or 'info' not in ticker_data:
            continue

        info = ticker_data['info']
        
        try:
            last_price = float(info.get("lastPrice") or ticker_data.get("last") or 0)
            prev_1h = float(info.get("prevPrice1h") or last_price or 0)
            prev_24h = float(info.get("prevPrice24h") or last_price or 0)
            change_24ho = float(info.get('price24hPcnt') or 0) * 100

            funding = float(info.get("fundingRate") or 0)
            open_interest = float(info.get("openInterest") or 0)
            turnover = float(info.get("turnover24h") or 0)
            volume_24h = float(info.get("volume24h") or ticker_data.get("baseVolume") or 0)

            change_1h = ((last_price - prev_1h) / prev_1h) * 100 if prev_1h > 0 else 0.0
            change_24h = ((last_price - prev_24h) / prev_24h) * 100 if prev_24h > 0 else 0.0

            # ----- Signal Evaluation Layer (Comparing current data vs last 5-min state) -----
            if symbol in previous_state:
                prev = previous_state[symbol]         
                volume_ratio = volume_24h / prev['volume'] if prev['volume'] > 0 else 1.0
                oi_change_pct = ((open_interest - prev['oi']) / prev['oi']) * 100 if prev['oi'] > 0 else 0.0        
                alerts_triggered = []

                # ----- Signal #1: Volume explosion along with 1h breakout momentum -----
                if change_1h >= PRICE_CHANGE_1H_THRESHOLD and volume_ratio >= VOLUME_SPIKE_THRESHOLD:
                    alerts_triggered.append({
                        "type": "VOLUME_EXPLOSION",
                        "details": f"1h Price +{change_1h:.2f}% (Threshold: >={PRICE_CHANGE_1H_THRESHOLD}%) with unusual 5m volume spike of {volume_ratio:.2f}x (Threshold: >={VOLUME_SPIKE_THRESHOLD}x)"
                    })

                # ----- Signal #2: Open Interest Rising (Fresh Aggressive Money Entering Longs) -----
                if change_1h > 0 and oi_change_pct >= OI_INCREASE_THRESHOLD and volume_ratio >= VOLUME_SPIKE_THRESHOLD:
                    alerts_triggered.append({
                        "type": "OPEN_INTEREST_RISING",
                        "details": f"Aggressive positioning detected. Price rising (+{change_1h:.2f}%) accompanied by rapid +{oi_change_pct:.2f}% OI growth (Threshold: >={OI_INCREASE_THRESHOLD}%)"
                    })

                # ----- Signal #3 (Alternate): Short Covering (Unstable pump) -----
                if change_1h >= PRICE_CHANGE_1H_THRESHOLD and oi_change_pct <= -OI_INCREASE_THRESHOLD:
                    alerts_triggered.append({
                        "type": "SHORT_COVERING_PUMP",
                        "details": f"Price is rising (+{change_1h:.2f}%), but Open Interest is falling rapidly (-{abs(oi_change_pct):.2f}%). This upward movement might be unsustainable short liquidations."
                    })

                # ----- Fire the webhook background task if any logic rule triggered -----
                if alerts_triggered:
                    payload = {
                        "symbol": symbol,
                        "last_price": last_price,
                        "change_1h": change_1h,
                        "change_24h": change_24h,
                        "change_24ho": change_24ho,
                        "funding_rate": funding,
                        "open_interest": open_interest,
                        "turnover_24h": turnover,
                        "volume_24h": volume_24h,
                        "alerts": alerts_triggered
                    }
                    asyncio.create_task(send_webhook(payload))

            # Update snapshots for the next 5-minute interval cycle check
            previous_state[symbol] = {"price": last_price, "volume": volume_24h, "oi": open_interest}

        except Exception as parse_err:
            log.debug(f"Skipping calculations on {symbol}: {parse_err}")


async def main():
    global exchange, http_client    
    exchange = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})
    http_client = httpx.AsyncClient(timeout=10.0)
    
    try:
        await initialize_markets()
        await check_metrics()
        log.info(f"Initial configurations seeded. Starting alert loop every {POLL_INTERVAL}s.")

        while True:
            await asyncio.sleep(POLL_INTERVAL)
            start_time = time.time()        
            await check_metrics()         
            elapsed = time.time() - start_time
            log.info(f"Completed metric screening analysis cycle in {elapsed:.2f} seconds.")
            
    except asyncio.CancelledError:
        log.info("Shutdown requested.")
    finally:
        await exchange.close()
        await http_client.aclose()
        log.info("Connections cleanly severed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
