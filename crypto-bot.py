# 2026.06.21  11.00
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

# Thresholds for custom rules/alerts
PRICE_CHANGE_1H_THRESHOLD = 5.0   # Trigger if 1h change exceeds 5%
VOLUME_SPIKE_THRESHOLD = 2.0      # Trigger if current 24h volume is 2x the previous check's volume

# Global state variables (no classes)
exchange = None
http_client = None
symbols = []
previous_state = {}  # Format: {symbol: {"price": float, "volume": float, "oi": float}}

# =========================
# FUNCTIONS
# =========================

async def initialize_markets():
    """Loads markets from Bybit and filters for linear perpetuals with >= 25x leverage."""
    global symbols, exchange
    log.info("Loading Bybit markets and filtering for >= 25x leverage...")
    
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
    """Fetches all tickers, extracts the required dictionary attributes, and checks signals."""
    global symbols, exchange, previous_state
    
    try:
        log.info("Fetching latest market tickers via REST...")
        # Using fetch_derivatives_tickers ensures info block has funding rate, open interest, and historical price baselines
        tickers = await exchange.fetch_derivatives_tickers(symbols=symbols)
    except Exception as e:
        log.error(f"Failed to fetch tickers: {e}")
        return

    for symbol in symbols:
        ticker_data = tickers.get(symbol)
        if not ticker_data or 'info' not in ticker_data:
            continue

        info = ticker_data['info']
        
        try:
            # Extract requested metric items safely
            last_price = float(info.get("lastPrice") or ticker_data.get("last") or 0)
            prev_1h = float(info.get("prevPrice1h") or last_price or 0)
            prev_24h = float(info.get("prevPrice24h") or last_price or 0)
            change_24ho = float(info.get('price24hPcnt') or 0) * 100

            funding = float(info.get("fundingRate") or 0)
            open_interest = float(info.get("openInterest") or 0)
            turnover = float(info.get("turnover24h") or 0)
            volume_24h = float(info.get("volume24h") or ticker_data.get("baseVolume") or 0)

            # Performance variations calculations requested
            change_1h = ((last_price - prev_1h) / prev_1h) * 100 if prev_1h > 0 else 0.0
            change_24h = ((last_price - prev_24h) / prev_24h) * 100 if prev_24h > 0 else 0.0

            # Signal Evaluation Layer (Comparing current ticker variables to last 5-min state)
            if symbol in previous_state:
                prev = previous_state[symbol]
                
                # Check for 5m Volume spike velocity
                volume_ratio = volume_24h / prev['volume'] if prev['volume'] > 0 else 1.0
                oi_change_pct = ((open_interest - prev['oi']) / prev['oi']) * 100 if prev['oi'] > 0 else 0.0

                alerts_triggered = []

                # Signal Rule Example #1: Volume explosion along with strong 1h momentum
                if change_1h > 3.0 and volume_ratio > 1.8:
                    alerts_triggered.append({
                        "type": "VOLUME_EXPLOSION_BEFORE_PRICE",
                        "details": f"1h Return: {change_1h:.2f}%, 5m Volume expanded by {volume_ratio:.2f}x"
                    })

                # Signal Rule Example #2: OI + Price entering momentum phase
                if change_1h > 2.0 and oi_change_pct > 4.0:
                    alerts_triggered.append({
                        "type": "OPEN_INTEREST_RISING",
                        "details": f"New money entering! 1h Price +{change_1h:.2f}%, 5m OI increased by {oi_change_pct:.2f}%"
                    })

                # Fire the non-blocking background webhook if any signals matched
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

            # Store history state context for next iteration calculations
            previous_state[symbol] = {
                "price": last_price,
                "volume": volume_24h,
                "oi": open_interest
            }

        except Exception as parse_err:
            log.debug(f"Skipping processing calculations on {symbol}: {parse_err}")


async def main():
    global exchange, http_client
    
    # Initialize connection instances globally
    exchange = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})
    http_client = httpx.AsyncClient(timeout=10.0)
    
    try:
        # Load markets & filter pairs supporting high leverage tiers
        await initialize_markets()
        
        # Run a baseline check to seed the `previous_state` dictionary snapshot mapping
        await check_metrics()
        log.info(f"Initial snapshot seeds completed. Active monitoring polling loop every {POLL_INTERVAL}s.")

        # Main loop framework
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            start_time = time.time()
            
            await check_metrics()
            
            elapsed = time.time() - start_time
            log.info(f"Completed metric screening analysis cycle in {elapsed:.2f} seconds.")
            
    except asyncio.CancelledError:
        log.info("Shutdown requested.")
    finally:
        # Clean up HTTP client and exchange connections safely on script exit
        await exchange.close()
        await http_client.aclose()
        log.info("Connections cleanly severed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
