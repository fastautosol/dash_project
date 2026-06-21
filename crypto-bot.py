# 2026.06.21  18.00
import asyncio
import ccxt.async_support as ccxt
import dlt
import httpx
import logging
import time
from datetime import datetime, UTC

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)  # Mute HTTPX routing chatter

# =========================
# CONFIGURATION & GLOBAL STATE
# =========================
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
WEBHOOK_URL = "https://n8n.fastautosol.com/webhook/crypto-alerts"
POLL_INTERVAL = 300  # 5 minutes

# Advanced Strategy Thresholds
PRICE_CHANGE_1H_THRESHOLD = 5.0   # Trigger if 1h price change exceeds +5% (or drops below -5%)
VOLUME_SPIKE_THRESHOLD = 2.0      # Trigger if current 24h rolling volume is 2x greater than 5m ago
OI_INCREASE_THRESHOLD = 4.0       # Trigger if Open Interest increases by 4% inside 5 minutes
FUNDING_DIVERGENCE_LIMIT = -0.05  # -0.05% or worse implies extreme short-selling friction / squeeze vulnerability
BTC_SYMBOL = "BTC/USDT"           

# Global functional state variables
exchange = None
http_client = None
pipeline = None
symbols = []
leverage_cache = {}  # Format: {symbol: max_leverage}
previous_state = {}  # Format: {symbol: {"price": float, "volume": float, "oi": float}}

# =========================
# FUNCTIONS
# =========================

async def initialize_markets():
    global symbols, exchange, leverage_cache   
    markets = await exchange.load_markets()
    for symbol, market in markets.items():
        if market.get('linear') and market.get('swap'):
            lev_filter = market.get('info', {}).get('leverageFilter', {})
            max_leverage = float(lev_filter.get('maxLeverage', 0))
            leverage_cache[symbol] = max_leverage
            if max_leverage >= 25 and symbol not in symbols:
                symbols.append(symbol)
                
    log.info(f"Initialized {len(symbols)} high-leverage assets.")


async def send_webhook(payload: dict):
    global http_client
    try:
        response = await http_client.post(WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            log.info(f"[ALERT WEBHOOK] Dispatched for {payload['symbol']}")
        else:
            log.error(f"[WEBHOOK ERROR] Status code: {response.status_code}")
    except Exception as e:
        log.error(f"[WEBHOOK FAILED] Could not connect to webhook server: {e}")


async def check_metrics():
    global symbols, exchange, previous_state, pipeline, leverage_cache    
    try:
        tickers = await exchange.fetch_tickers(symbols=symbols)
    except Exception as e:
        log.error(f"Failed to fetch tickers: {e}")
        return

    now_utc = datetime.now(UTC)
    db_alert_records = []

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
            
            # Extract Max Leverage tier assigned to this asset
            max_lev_tier = leverage_cache.get(symbol, 0.0)

            if symbol in previous_state:
                prev = previous_state[symbol]
                
                volume_ratio = volume_24h / prev['volume'] if prev['volume'] > 0 else 1.0
                oi_change_pct = ((open_interest - prev['oi']) / prev['oi']) * 100 if prev['oi'] > 0 else 0.0
                toi_ratio = turnover / open_interest if open_interest > 0 else 0.0
                alerts_triggered = []

                # Signal #1: Volume explosion
                if change_1h >= PRICE_CHANGE_1H_THRESHOLD and volume_ratio >= VOLUME_SPIKE_THRESHOLD:
                    alerts_triggered.append({
                        "alert_type": "VOLUME_EXPLOSION",
                        "details": f"Price +{change_1h:.2f}% with 5m volume spike of {volume_ratio:.2f}x"})

                # Signal #2: Open Interest Rising (Organic Long expansion)
                if change_1h > 0 and oi_change_pct >= OI_INCREASE_THRESHOLD and volume_ratio >= VOLUME_SPIKE_THRESHOLD:
                    alerts_triggered.append({
                        "alert_type": "OPEN_INTEREST_RISING",
                        "details": f"Price rising under strong structural accumulation (+{oi_change_pct:.2f}% OI growth)"})
                
                # Signal #3: Short Squeeze Breakout (Funding Divergence Confirmation)
                if change_1h >= 3.0 and funding <= FUNDING_DIVERGENCE_LIMIT:
                    alerts_triggered.append({
                        "alert_type": "SHORT_SQUEEZE_CONFIRMED",
                        "details": f"Aggressive upward break (+{change_1h:.2f}%) meeting extreme short friction. Funding: {funding*100:.4f}%"})

                # Signal #4: Turnover Velocity Spike (High Capital Density Acceleration)
                if volume_ratio >= VOLUME_SPIKE_THRESHOLD and toi_ratio > 1.5:
                    alerts_triggered.append({
                        "alert_type": "HIGH_TURNOVER_DENSITY",
                        "details": f"Heavy capital rotation detected. 24h Turnover is {toi_ratio:.2f}x greater than open interest."})

                # If signals fired, calculate momentum score and process distribution pipeline
                if alerts_triggered:
                    # Extended Comprehensive Scoring Algorithm
                    v_score = min(20.0, (volume_ratio / VOLUME_SPIKE_THRESHOLD) * 20.0) if VOLUME_SPIKE_THRESHOLD > 0 else 0
                    oi_score = min(20.0, (max(0, oi_change_pct) / OI_INCREASE_THRESHOLD) * 20.0) if OI_INCREASE_THRESHOLD > 0 else 0
                    p_score = min(20.0, (max(0, change_1h) / PRICE_CHANGE_1H_THRESHOLD) * 20.0) if PRICE_CHANGE_1H_THRESHOLD > 0 else 0
                    
                    # Leverage tier extra booster: assets with higher systemic capacity (50x-75x+) receive scaling weight
                    lev_booster = min(10.0, (max_lev_tier / 50.0) * 10.0) if max_lev_tier > 0 else 0
                    
                    # Funding Premium booster: rewards situations where shorts are aggressively trapped
                    sq_booster = 20.0 if funding <= FUNDING_DIVERGENCE_LIMIT else 0.0

                    momentum_score = round(p_score + v_score + oi_score + lev_booster + sq_booster, 2)

                    # 1. Fire Webhook out to notification routing engines
                    payload = {
                        "symbol": symbol,
                        "last_price": last_price,
                        "change_1h": change_1h,
                        "volume_24h": volume_24h,
                        "funding_rate": funding,
                        "max_leverage_tier": max_lev_tier,
                        "turnover_to_oi_ratio": toi_ratio,
                        "calculated_momentum_score": momentum_score,
                        "alerts": alerts_triggered
                    }
                    asyncio.create_task(send_webhook(payload))

                    # 2. Append directly to structural database arrays
                    for alert in alerts_triggered:
                        db_alert_records.append({
                            "timestamp": now_utc,
                            "symbol": symbol,
                            "alert_type": alert["alert_type"],
                            "details": alert["details"],
                            "last_price": last_price,
                            "change_1h_pct": change_1h,
                            "change_24h_pct": change_24h,
                            "change_24ho_pct": change_24ho,
                            "funding_rate": funding,
                            "open_interest": open_interest,
                            "turnover_24h": turnover,
                            "volume_24h": volume_24h,
                            "max_leverage_tier": max_lev_tier,
                            "turnover_to_oi_ratio": toi_ratio,
                            "momentum_score": momentum_score
                        })

            # Commit current context data state parameters
            previous_state[symbol] = {
                "price": last_price,
                "volume": volume_24h,
                "oi": open_interest
            }

        except Exception as parse_err:
            log.debug(f"Skipping row iteration for {symbol}: {parse_err}")

    # Stream everything directly to PostgreSQL in a single concurrent thread execution
    if db_alert_records:
        try:
            await asyncio.to_thread(
                pipeline.run,
                db_alert_records,
                table_name="bybit_crypto_signals",
                write_disposition="append"
            )
            log.info(f"[POSTGRES] Streamed {len(db_alert_records)} signal records into tracking tables.")
        except Exception as db_err:
            log.error(f"[POSTGRES ERROR] Pipeline writing routine failed: {db_err}")


async def main():
    global exchange, http_client, pipeline
    
    exchange = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})
    http_client = httpx.AsyncClient(timeout=10.0)
    
    # Initialize your native dlt pipeline configuration
    pipeline = dlt.pipeline(
        pipeline_name="crypto_alert_bot",
        destination=dlt.destinations.postgres(credentials=DB_URL),
        dataset_name="bybit_data"
    )
    pipeline.drop_pending_packages()
    
    try:
        await initialize_markets()
        
        # Seed state definitions
        await check_metrics()
        log.info(f"Bot activated. Core strategy monitoring loop operational on {POLL_INTERVAL}s frequency.")

        while True:
            await asyncio.sleep(POLL_INTERVAL)
            await check_metrics()
            
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
