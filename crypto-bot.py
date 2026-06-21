# 2026.06.21
import asyncio
import ccxt.async_support as ccxt
import httpx
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger(__name__)

# =========================
# CONFIGURATION & GLOBAL STATE
# =========================
WEBHOOK_URL = "http://your-n8n-or-fastapi-server:5678/webhook/crypto-alerts"
POLL_INTERVAL = 300  # 5 minutes

PRICE_CHANGE_THRESHOLD = 3.5  # % change in 5 mins
VOLUME_SPIKE_THRESHOLD = 2.0  # 2x volume increase in 5 mins

# Global state variables (replacing the class attributes)
exchange = None
http_client = None
symbols = []
previous_state = {}  # Format: {symbol: {"price": float, "volume": float}}

# =========================
# FUNCTIONS
# =========================

async def initialize_markets():
    """Loads markets from Bybit and filters for linear pairs with >= 25x leverage."""
    global symbols, exchange
    log.info("Loading Bybit markets...")
    
    markets = await exchange.load_markets()
    
    for symbol, market in markets.items():
        # Ensure it's a linear perpetual/swap contract
        if market.get('linear') and market.get('swap'):
            lev_filter = market.get('info', {}).get('leverageFilter', {})
            max_leverage = float(lev_filter.get('maxLeverage', 0))
            
            if max_leverage >= 25:
                symbols.append(symbol)
                
    log.info(f"Filtered {len(symbols)} linear contracts supporting >= 25x leverage.")


async def send_webhook(payload: dict):
    """Sends an alert JSON payload to your webhook endpoint natively."""
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
    """Fetches all tickers at once, compares against the last snapshot, and triggers alerts."""
    global symbols, exchange, previous_state
    
    try:
        log.info("Fetching latest market tickers...")
        # Fetch all filtered symbols at once to stay safe from rate limits
        tickers = await exchange.fetch_tickers(symbols=symbols)
    except Exception as e:
        log.error(f"Failed to fetch tickers: {e}")
        return

    for symbol in symbols:
        ticker = tickers.get(symbol)
        if not ticker or 'last' not in ticker or 'baseVolume' not in ticker:
            continue

        current_price = ticker['last']
        current_volume = ticker['baseVolume']  # Rolling 24h volume accumulated

        # If we have a past 5-minute snapshot for this asset, run calculations
        if symbol in previous_state:
            prev = previous_state[symbol]
            
            # 1. Calculate Price Change %
            price_pct_change = ((current_price - prev['price']) / prev['price']) * 100
            
            # 2. Calculate Volume Spike Ratio
            volume_ratio = current_volume / prev['volume'] if prev['volume'] > 0 else 1.0

            alerts_triggered = []

            if abs(price_pct_change) >= PRICE_CHANGE_THRESHOLD:
                alerts_triggered.append({
                    "type": "PRICE_SUDDEN_CHANGE",
                    "details": f"Price shifted {price_pct_change:.2f}% inside 5 minutes."
                })

            if volume_ratio >= VOLUME_SPIKE_THRESHOLD:
                alerts_triggered.append({
                    "type": "VOLUME_SPIKE",
                    "details": f"24h rolling volume expanded by {volume_ratio:.2f}x over last 5 minutes."
                })

            # Fire the webhook background task safely without blocking the rest of the loop
            if alerts_triggered:
                payload = {
                    "symbol": symbol,
                    "timestamp": ticker.get('datetime'),
                    "current_price": current_price,
                    "alerts": alerts_triggered,
                    "leverage_tier_eligible": "25x+"
                }
                asyncio.create_task(send_webhook(payload))

        # Save current state as historical context for the next 5-minute iteration
        previous_state[symbol] = {
            "price": current_price,
            "volume": current_volume
        }


async def main():
    global exchange, http_client
    
    # Initialize connection instances globally
    exchange = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})
    http_client = httpx.AsyncClient(timeout=10.0)
    
    try:
        # Load markets & filter pairs
        await initialize_markets()
        
        # Run a baseline check to seed the `previous_state` snapshot dictionary
        await check_metrics()
        log.info(f"Initial cache seeded. Polling every {POLL_INTERVAL}s.")

        # Main alert loop
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            start_time = time.time()
            
            await check_metrics()
            
            elapsed = time.time() - start_time
            log.info(f"Completed analysis cycle in {elapsed:.2f} seconds.")
            
    except asyncio.CancelledError:
        log.info("Shutdown requested.")
    finally:
        # Clean up HTTP and exchange sessions safely on exit
        await exchange.close()
        await http_client.aclose()
        log.info("Connections cleanly severed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
