# 2026.06.14  18.00
import asyncio
import ccxt.pro as ccxtpro
import dlt
import aiohttp
from datetime import datetime, UTC, timedelta
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger(__name__)

# =========================
# CONFIGURATION
# =========================
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"

CRYPTO_SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "SUI/USDT", "HYPE/USDT", "LTC/USDT", "ETC/USDT", "COMP/USDT",
    "AVAX/USDT", "AXS/USDT", "LINK/USDT", "BCH/USDT", "TIA/USDT", "ZEN/USDT", "NEAR/USDT", "RIVER/USDT", "IP/USDT", ICP/USDT"
]

XSTOCK_SYMBOLS = ["AAPLX/USDT", "TSLAX/USDT", "NVDAX/USDT", "AMZNX/USDT", "COINX/USDT", "CRCLX/USDT", "METAX/USDT", "HOODX/USDT", "GOOGLX/USDT"]

ALL_SYMBOLS = CRYPTO_SYMBOLS + XSTOCK_SYMBOLS

POLL_INTERVAL = 75       # Seconds between DB upserts (fast, no artificial delay)
TICKER_INTERVAL = 300    # Seconds between ticker cache refreshes (5 mins)
CLEANUP_HOURS = 60       # Hours of data to retain

# =========================
# ALERT CONFIG
# =========================
ALERT_THRESHOLDS = {
    "change_1h_pct": 2.0,            # 1h % move that counts as "starting to move"
    "change_24h_pct": 0.0,           # require non-negative 24h trend (avoid dead-cat bounces)
    "min_turnover_24h": 1_000_000,   # liquidity filter in USDT
    "max_funding_rate": 0.0005,      # skip if already overheated (longs crowded/paying a lot)
    "cooldown_minutes": 60,          # don't re-alert the same symbol within this window
}

N8N_ALERT_WEBHOOK = "http://n8n:5678/webhook/crypto-alert"  # internal docker network URL

# =========================
# SHARED STATE
# =========================
class MarketState:
    def __init__(self) -> None:
        self.ohlcv: dict[str, list] = {}
        self.ticker: dict[str, dict] = {}  # Cache for 24h stats
        self.last_cleanup: float = 0.0
        self.pipeline_lock = asyncio.Lock()  # Serialize dlt pipeline calls

state = MarketState()

# =========================
# WEBSOCKET — OHLCV WATCHER
# =========================
async def watch_ohlcv_symbol(exchange: ccxtpro.bybit, symbol: str) -> None:
    """Continuously watch 5m candles and update shared state in real-time."""
    while True:
        try:
            ohlcv = await exchange.watch_ohlcv(symbol, timeframe="5m", limit=1)
            if ohlcv:
                state.ohlcv[symbol] = ohlcv[-1]
        except Exception as e:
            log.warning(f"[WS] {symbol} error: {e} — reconnecting in 3s")
            await asyncio.sleep(3)

# =========================
# REST — TICKER CACHE REFRESH
# =========================
async def ticker_refresh_loop(ex_linear: ccxtpro.bybit, ex_spot: ccxtpro.bybit) -> None:
    """Fetches 24h stats every 5 minutes and stores them in state.ticker."""
    while True:
        await asyncio.sleep(TICKER_INTERVAL)

        for exchange, symbols in [(ex_linear, CRYPTO_SYMBOLS), (ex_spot, XSTOCK_SYMBOLS)]:
            if not symbols:
                continue
            try:
                tickers = await exchange.fetch_tickers(symbols=symbols)
                # Update the shared cache
                state.ticker.update(tickers)
                log.info(f"[TICKER] Cached {len(tickers)} {exchange.options['defaultType']} symbols")
            except Exception as e:
                log.error(f"[TICKER] {exchange.options['defaultType']} refresh error: {e}")

# =========================
# N8N WEBHOOK NOTIFIER
# =========================
async def notify_n8n(alerts: list[dict]) -> None:
    """Best-effort push to N8N. Failure here is non-fatal — the DB row remains
    for a slow fallback poll on the N8N side to pick up later."""
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            for alert in alerts:
                payload = {**alert, "timestamp": alert["timestamp"].isoformat()}
                async with session.post(N8N_ALERT_WEBHOOK, json=payload) as resp:
                    if resp.status >= 300:
                        log.warning(f"[ALERT] webhook returned {resp.status} for {alert['symbol']}")
    except Exception as e:
        log.warning(f"[ALERT] webhook push failed (will rely on poll fallback): {e}")

# =========================
# ALERT SCAN LOOP
# =========================
async def alert_scan_loop(ex_linear: ccxtpro.bybit, pipeline) -> None:
    """Periodically scans tickers for early-momentum conditions and logs alerts to DB."""
    last_alert: dict[str, datetime] = {}

    while True:
        await asyncio.sleep(POLL_INTERVAL)

        try:
            tickers = await ex_linear.fetch_tickers(symbols=CRYPTO_SYMBOLS)
        except Exception as e:
            log.error(f"[ALERT] ticker fetch failed: {e}")
            continue

        now = datetime.now(UTC)
        alerts = []

        for sym, t in tickers.items():
            info = t.get("info", {})
            try:
                last_price = float(info["lastPrice"])
                prev_1h = float(info["prevPrice1h"])
                prev_24h = float(info["prevPrice24h"])
                funding = float(info.get("fundingRate") or 0)
                open_interest = float(info.get("openInterest") or 0)
                turnover = float(info.get("turnover24h") or 0)
            except (KeyError, TypeError, ValueError):
                continue

            if not prev_1h or not prev_24h:
                continue

            change_1h = (last_price - prev_1h) / prev_1h * 100
            change_24h = (last_price - prev_24h) / prev_24h * 100

            # Cooldown — skip if we already fired for this symbol recently
            fired = last_alert.get(sym)
            if fired and (now - fired) < timedelta(minutes=ALERT_THRESHOLDS["cooldown_minutes"]):
                continue

            if (change_1h >= ALERT_THRESHOLDS["change_1h_pct"]
                    and change_24h >= ALERT_THRESHOLDS["change_24h_pct"]
                    and turnover >= ALERT_THRESHOLDS["min_turnover_24h"]
                    and funding <= ALERT_THRESHOLDS["max_funding_rate"]):

                alerts.append({
                    "symbol": sym,
                    "timestamp": now,
                    "alert_type": "momentum_1h",
                    "last_price": last_price,
                    "change_1h_pct": round(change_1h, 3),
                    "change_24h_pct": round(change_24h, 3),
                    "funding_rate": funding,
                    "open_interest": open_interest,
                    "turnover_24h": turnover,
                    "notified": False,
                })
                last_alert[sym] = now

        if alerts:
            try:
                async with state.pipeline_lock:
                    await asyncio.to_thread(
                        pipeline.run, alerts,
                        table_name="bybit_alerts",
                        write_disposition="append",
                    )
                log.info(f"[ALERT] {len(alerts)} signal(s): {[a['symbol'] for a in alerts]}")
                await notify_n8n(alerts)
            except Exception as e:
                log.error(f"[ALERT] write failed: {e}")

# =========================
# DB WRITER & CLEANUP LOOP
# =========================
async def db_writer_loop(pipeline) -> None:
    """Upserts the latest state + ticker stats to DB every POLL_INTERVAL seconds."""
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        now = time.time()
        now_utc = datetime.now(UTC)
        records = []

        # 1. Build records from WebSocket state + Ticker cache
        for sym in ALL_SYMBOLS:
            bar = state.ohlcv.get(sym)
            if not bar:
                continue

            # Fetch latest ticker info for this symbol (fallback to empty dict if not yet cached)
            ticker_data = state.ticker.get(sym, {})
            info = ticker_data.get("info", {})

            records.append({
                "symbol":        sym,
                "timestamp":     datetime.fromtimestamp(bar[0] / 1000, tz=UTC),
                "open":          float(bar[1] or 0),
                "high":          float(bar[2] or 0),
                "low":           float(bar[3] or 0),
                "close":         float(bar[4] or 0),
                "volume":        float(bar[5] or 0),
                "vwap":          float(info.get("vwap24h") or ticker_data.get("vwap") or 0),
                "turnover_24h":  float(info.get("turnover24h") or ticker_data.get("quoteVolume") or 0),
                "price_24h_pct": float(info.get("price24hPcnt") or ticker_data.get("percentage") or 0),
            })

        # 2. Upsert to database (Single unified table, just like the old bot)
        if records:
            try:
                async with state.pipeline_lock:
                    await asyncio.to_thread(pipeline.run, records, table_name="bybit_candles", write_disposition="merge", primary_key=["symbol", "timestamp"])
                log.info(f"[DB] Upserted {len(records)} enriched candles")
            except Exception as e:
                log.error(f"[DB] Upsert failed: {e}")

        # 3. Hourly Cleanup
        if now - state.last_cleanup >= 3600:
            threshold = now_utc - timedelta(hours=CLEANUP_HOURS)
            try:
                async with state.pipeline_lock:
                    def _cleanup():
                        with pipeline.sql_client() as client:
                            tname = client.make_qualified_table_name("bybit_candles")
                            client.execute_sql(f"DELETE FROM {tname} WHERE timestamp < %s", (threshold,))

                    await asyncio.to_thread(_cleanup)
                log.info(f"[CLEANUP] Removed data older than {CLEANUP_HOURS}h")
            except Exception as e:
                log.error(f"[CLEANUP] Failed: {e}")
            state.last_cleanup = now

# =========================
# MAIN
# =========================
async def main() -> None:
    # Initialize TWO exchange instances: linear (crypto) and spot (x-stocks)
    base_cfg = {"enableRateLimit": True}
    ex_linear = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "linear"}})
    ex_spot   = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "spot"}})

    # Initialize dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="crypto_strategy_bybit",
        destination=dlt.destinations.postgres(credentials=DB_URL),
        dataset_name="bybit_data")
    pipeline.drop_pending_packages()

    tasks = []

    # Start WebSocket watchers for Crypto
    for sym in CRYPTO_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_linear, sym), name=f"ws-linear-{sym}"))

    # Start WebSocket watchers for X-Stocks
    for sym in XSTOCK_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_spot, sym), name=f"ws-spot-{sym}"))

    # Start unified DB writer
    tasks.append(asyncio.create_task(db_writer_loop(pipeline), name="db-writer"))

    # Start background ticker cache refresh
    tasks.append(asyncio.create_task(ticker_refresh_loop(ex_linear, ex_spot), name="ticker-refresh"))

    # Start momentum alert scanner
    tasks.append(asyncio.create_task(alert_scan_loop(ex_linear, pipeline), name="alert-scan"))

    #log.info(f"Bot started — Watching {len(CRYPTO_SYMBOLS)} Crypto + {len(XSTOCK_SYMBOLS)} X-Stocks")
    log.info(f"DB Upsert every {POLL_INTERVAL}s (with VWAP/Turnover/Pct) | Ticker cache every {TICKER_INTERVAL}s | Alert scan every {POLL_INTERVAL}s")

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log.info("Shutdown signal received.")
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(ex_linear.close(), ex_spot.close(), return_exceptions=True)
        log.info("All connections closed. Bye.")

if __name__ == "__main__":
    asyncio.run(main())
