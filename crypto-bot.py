# 2026.06.06  11.00
import asyncio
import ccxt.pro as ccxtpro
import dlt
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
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT",
    "SUI/USDT", "HYPE/USDT", "LTC/USDT", "ETC/USDT", "COMP/USDT",
    "AVAX/USDT", "AXS/USDT", "LINK/USDT", "BCH/USDT", "TIA/USDT", "ZEN/USDT"
]

XSTOCK_SYMBOLS = [
    "AAPLX/USDT", "TSLAX/USDT", "NVDAX/USDT", "AMZNX/USDT", 
    "COINX/USDT", "CRCLX/USDT", "METAX/USDT", "HOODX/USDT", "GOOGLX/USDT"
]

ALL_SYMBOLS = CRYPTO_SYMBOLS + XSTOCK_SYMBOLS

POLL_INTERVAL = 60       # Seconds between DB upserts (fast, no artificial delay)
TICKER_INTERVAL = 300    # Seconds between ticker refreshes (5 mins)
CLEANUP_HOURS = 72       # Hours of data to retain

# =========================
# SHARED STATE
# =========================
class MarketState:
    def __init__(self) -> None:
        self.ohlcv: dict[str, list] = {}
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
            # watch_ohlcv returns a list of candles. We only care about the latest (forming) one.
            ohlcv = await exchange.watch_ohlcv(symbol, timeframe="5m", limit=1)
            if ohlcv:
                state.ohlcv[symbol] = ohlcv[-1]
        except Exception as e:
            log.warning(f"[WS] {symbol} error: {e} — reconnecting in 3s")
            await asyncio.sleep(3)

# =========================
# REST — TICKER REFRESH (Optional but recommended)
# =========================
async def ticker_refresh_loop(ex_linear: ccxtpro.bybit, ex_spot: ccxtpro.bybit, pipeline) -> None:
    """Fetches 24h stats every 5 minutes for both markets."""
    while True:
        await asyncio.sleep(TICKER_INTERVAL)
        records = []
        now_utc = datetime.now(UTC)

        for exchange, symbols in [(ex_linear, CRYPTO_SYMBOLS), (ex_spot, XSTOCK_SYMBOLS)]:
            if not symbols:
                continue
            try:
                tickers = await exchange.fetch_tickers(symbols=symbols)
                for sym, t in tickers.items():
                    info = t.get("info", {})
                    records.append({
                        "symbol":        sym,
                        "timestamp":     now_utc,
                        "vwap":          float(info.get("vwap24h") or t.get("vwap") or 0),
                        "turnover_24h":  float(info.get("turnover24h") or t.get("quoteVolume") or 0),
                        "price_24h_pct": float(info.get("price24hPcnt") or t.get("percentage") or 0),
                    })
                log.info(f"[TICKER] Refreshed {len(tickers)} {exchange.options['defaultType']} symbols")
            except Exception as e:
                log.error(f"[TICKER] {exchange.options['defaultType']} refresh error: {e}")

        if records:
            try:
                async with state.pipeline_lock:
                    await asyncio.to_thread(
                        pipeline.run,
                        records,
                        table_name="bybit_tickers",
                        write_disposition="merge",
                        primary_key=["symbol", "timestamp"]
                    )
            except Exception as e:
                log.error(f"[TICKER] DB write failed: {e}")

# =========================
# DB WRITER & CLEANUP LOOP
# =========================
async def db_writer_loop(pipeline) -> None:
    """Upserts the latest state to DB every POLL_INTERVAL seconds."""
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        now = time.time()
        now_utc = datetime.now(UTC)
        records = []

        # 1. Build records from current WebSocket state (both Crypto and X-Stocks)
        for sym in ALL_SYMBOLS:
            bar = state.ohlcv.get(sym)
            if not bar:
                continue
            
            # A 5m candle is "complete" if current time is >= candle start time + 5 minutes (300,000 ms)
            is_complete = (now * 1000 - bar[0]) >= 300000

            records.append({
                "symbol":    sym,
                "timestamp": datetime.fromtimestamp(bar[0] / 1000, tz=UTC),
                "open":      float(bar[1] or 0),
                "high":      float(bar[2] or 0),
                "low":       float(bar[3] or 0),
                "close":     float(bar[4] or 0),
                "volume":    float(bar[5] or 0),
                "complete":  is_complete
            })

        # 2. Upsert to database
        if records:
            try:
                async with state.pipeline_lock:
                    await asyncio.to_thread(
                        pipeline.run,
                        records,
                        table_name="bybit_candles",
                        write_disposition="merge",
                        primary_key=["symbol", "timestamp"]
                    )
                log.info(f"[DB] Upserted {len(records)} candles (Crypto + X-Stocks)")
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
    # Initialize TWO exchange instances: one for linear (crypto), one for spot (x-stocks)
    base_cfg = {"enableRateLimit": True}
    ex_linear = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "linear"}})
    ex_spot   = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "spot"}})

    # Initialize dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="crypto_strategy_ws_unified",
        destination=dlt.destinations.postgres(credentials=DB_URL),
        dataset_name="bybit_data"
    )

    tasks = []

    # Start WebSocket watchers for Crypto (Linear)
    for sym in CRYPTO_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_linear, sym), name=f"ws-linear-{sym}"))

    # Start WebSocket watchers for X-Stocks (Spot)
    for sym in XSTOCK_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_spot, sym), name=f"ws-spot-{sym}"))

    # Start unified DB writer
    tasks.append(asyncio.create_task(db_writer_loop(pipeline), name="db-writer"))

    # Start unified ticker refresh
    tasks.append(asyncio.create_task(ticker_refresh_loop(ex_linear, ex_spot, pipeline), name="ticker-refresh"))

    log.info(f"Bot started — Watching {len(CRYPTO_SYMBOLS)} Crypto + {len(XSTOCK_SYMBOLS)} X-Stocks via WebSocket")
    log.info(f"DB Upsert every {POLL_INTERVAL}s | Ticker every {TICKER_INTERVAL}s")

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log.info("Shutdown signal received.")
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(ex_linear.close(), ex_spot.close(), return_exceptions=True)
        log.info("All connections closed. Bye.")

if __name__ == "__ZYTHON__": # Fixed typo from standard template
    pass

if __name__ == "__main__":
    asyncio.run(main())
