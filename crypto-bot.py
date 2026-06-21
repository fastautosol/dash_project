# 2026.06.21  17.00
import asyncio
import ccxt.async_support as ccxt
import dlt
import httpx
import logging
import time
from datetime import datetime, UTC

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# =========================
# CONFIGURATION & GLOBAL STATE
# =========================
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
WEBHOOK_URL = "https://n8n.fastautosol.com/webhook/crypto-alerts"
POLL_INTERVAL = 300  # 5 minutes

# Custom Strategy Thresholds
PRICE_CHANGE_1H_THRESHOLD = 2.0   
VOLUME_SPIKE_THRESHOLD = 1.5      
OI_INCREASE_THRESHOLD = 1.5               

exchange = None
http_client = None
pipeline = None
symbols = []
previous_state = {}  # Format: {symbol: {"price": float, "volume": float, "oi": float}}

# =========================
# FUNCTIONS
# =========================

async def initialize_markets():
    global symbols, exchange    
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
            log.info(f"[ALERT WEBHOOK] Signal for {payload['symbol']}")
        else:
            log.error(f"[WEBHOOK ERROR] Status code: {response.status_code}")
    except Exception as e:
        log.error(f"[WEBHOOK FAILED] Could not connect to webhook server: {e}")


async def check_metrics():
    global symbols, exchange, previous_state, pipeline    
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

            if symbol in previous_state:
                prev = previous_state[symbol]       
                volume_ratio = volume_24h / prev['volume'] if prev['volume'] > 0 else 1.0
                oi_change_pct = ((open_interest - prev['oi']) / prev['oi']) * 100 if prev['oi'] > 0 else 0.0
                alerts_triggered = []

                # ----- Signal #1: Volume explosion -----
                if change_1h >= PRICE_CHANGE_1H_THRESHOLD and volume_ratio >= VOLUME_SPIKE_THRESHOLD:
                    alerts_triggered.append({
                        "alert_type": "VOLUME_EXPLOSION",
                        "details": f"Price +{change_1h:.2f}% with 5m volume spike of {volume_ratio:.2f}x"})

                # ----- Signal #2: Open Interest Rising -----
                if change_1h > 0 and oi_change_pct >= OI_INCREASE_THRESHOLD and volume_ratio >= VOLUME_SPIKE_THRESHOLD:
                    alerts_triggered.append({
                        "alert_type": "OPEN_INTEREST_RISING",
                        "details": f"Price rising under heavy buying pressure with +{oi_change_pct:.2f}% OI growth"})

                # ----- Signal #3 (Alternate): Short Covering -----
                if change_1h >= PRICE_CHANGE_1H_THRESHOLD and oi_change_pct <= -OI_INCREASE_THRESHOLD:
                    alerts_triggered.append({
                        "alert_type": "SHORT_COVERING_PUMP",
                        "details": f"Price rising (+{change_1h:.2f}%), but open interest shedding (-{abs(oi_change_pct):.2f}%)"})

                # If signals fired, log them and push to arrays
                if alerts_triggered:
                    # Calculate Momentum Score
                    v_score = min(20.0, (volume_ratio / VOLUME_SPIKE_THRESHOLD) * 20.0) if VOLUME_SPIKE_THRESHOLD > 0 else 0
                    oi_score = min(20.0, (max(0, oi_change_pct) / OI_INCREASE_THRESHOLD) * 20.0) if OI_INCREASE_THRESHOLD > 0 else 0
                    p_score = min(20.0, (max(0, change_1h) / PRICE_CHANGE_1H_THRESHOLD) * 20.0) if PRICE_CHANGE_1H_THRESHOLD > 0 else 0
                    momentum_score = round(p_score + v_score + oi_score, 2)

                    # 1. Fire non-blocking Webhook to N8N/FastAPI
                    payload = {
                        "symbol": symbol,
                        "last_price": last_price,
                        "change_1h": change_1h,
                        "volume_24h": volume_24h,
                        "calculated_momentum_score": momentum_score,
                        "alerts": alerts_triggered
                    }
                    asyncio.create_task(send_webhook(payload))

                    # 2. Flatten for clean relational Database rows
                    for alert in alerts_triggered:
                        db_alert_records.append({
                            "timestamp": now_utc,
                            "symbol": symbol,
                            "alert_type": alert["alert_type"],
                            "details": alert["details"],
                            "last_price": last_price,
                            "change_1h_pct": round(change_1h, 1) if change_1h is not None else None,
                            "change_24h_pct": round(change_24h, 1) if change_24h is not None else None,
                            "change_24ho_pct": round(change_24ho, 1) if change_24ho is not None else None,
                            "funding_rate": round(funding, 5) if funding is not None else None,
                            "open_interest": round(open_interest, 1) if open_interest is not None else None,
                            "turnover_24h": round(turnover, 0) if turnover is not None else None,
                            "volume_24h": round(volume_24h, 0) if volume_24h is not None else None,
                            "momentum_score": momentum_score
                        })

            # Save state snap
            previous_state[symbol] = {"price": last_price, "volume": volume_24h, "oi": open_interest}

        except Exception as parse_err:
            log.debug(f"Skipping row error for {symbol}: {parse_err}")

    # ----- Write metrics cleanly to Postgres via DLT -----
    if db_alert_records:
        try:
            # We run this in a threadpool executor so it doesn't halt async event tickers
            await asyncio.to_thread(
                pipeline.run,
                db_alert_records,
                table_name="bybit_crypto_signals",
                write_disposition="append"  # We want a historical continuous log table of warnings
            )
            log.info(f"[POSTGRES] Streamed {len(db_alert_records)} signal records into database.")
        except Exception as db_err:
            log.error(f"[POSTGRES ERROR] Pipeline failed to ingest logs: {db_err}")


async def main():
    global exchange, http_client, pipeline    
    exchange = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})
    http_client = httpx.AsyncClient(timeout=10.0)
    
    pipeline = dlt.pipeline(
        pipeline_name="crypto_alert_bot",
        destination=dlt.destinations.postgres(credentials=DB_URL),
        dataset_name="bybit_data"
    )
    pipeline.drop_pending_packages()
    
    try:
        await initialize_markets()
        await check_metrics()
        log.info(f"Bot activated. Monitoring and logging to Postgres every {POLL_INTERVAL}s.")

        while True:
            await asyncio.sleep(POLL_INTERVAL)
            start_time = time.time()          
            await check_metrics()          
            elapsed = time.time() - start_time
            log.info(f"{datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} Loop completed in {elapsed:.2f} seconds.")
            
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
