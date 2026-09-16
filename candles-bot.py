# 2026.09.16  10.00
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
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "SUI/USDT", "HYPE/USDT", "LTC/USDT", "ETC/USDT", "COMP/USDT",
    "AVAX/USDT", "AXS/USDT", "LINK/USDT", "BCH/USDT", "TIA/USDT", "ZEN/USDT",  "NEAR/USDT", "AAVE/USDT", "LTC/USDT", "ICP/USDT"
]

XSTOCK_SYMBOLS = ["AAPLX/USDT", "TSLAX/USDT", "NVDAX/USDT", "AMZNX/USDT",  "COINX/USDT", "CRCLX/USDT", "METAX/USDT", "HOODX/USDT", "GOOGLX/USDT"]

ALL_SYMBOLS = CRYPTO_SYMBOLS + XSTOCK_SYMBOLS

POLL_INTERVAL = 75        # Seconds between DB upserts
TICKER_INTERVAL = 300     # Seconds between ticker cache refreshes (funding/OI/turnover)
CLEANUP_HOURS = 60        # Hours of data to retain

def to_linear(symbol: str) -> str:
    return f"{symbol}:USDT"

# =========================
# SHARED STATE
# =========================
class MarketState:
    def __init__(self) -> None:
        self.ohlcv: dict[str, list] = {}
        self.ticker_cache: dict[str, dict] = {}
        self.last_ticker_fetch: float = 0.0
        self.last_cleanup: float = 0.0
        self.pipeline_lock = asyncio.Lock()  # Serialize dlt pipeline calls

state = MarketState()

# =========================
# WEBSOCKET — OHLCV WATCHER
# =========================
async def watch_ohlcv_symbol(exchange: ccxtpro.bybit, symbol: str, is_linear: bool) -> None:
    ws_symbol = to_linear(symbol) if is_linear else symbol
    while True:
        try:
            ohlcv = await exchange.watch_ohlcv(ws_symbol, timeframe="5m", limit=1)
            if ohlcv:
                state.ohlcv[symbol] = ohlcv[-1]
        except Exception as e:
            log.warning(f"[WS] {symbol} error: {e} — reconnecting in 3s")
            await asyncio.sleep(3)

# =========================
# TICKER CACHE REFRESH (funding / OI / turnover / 24h change)
# =========================
async def refresh_ticker_cache(ex_linear: ccxtpro.bybit, ex_spot: ccxtpro.bybit) -> None:
    """Refreshes funding/OI/turnover/vwap data every TICKER_INTERVAL seconds.
    Uses REST fetch_tickers (cheap, batched) rather than per-candle REST calls."""
    while True:
        try:
            linear_symbols = [to_linear(s) for s in CRYPTO_SYMBOLS]
            linear_tickers_raw = await ex_linear.fetch_tickers(symbols=linear_symbols)
            spot_tickers = await ex_spot.fetch_tickers(symbols=XSTOCK_SYMBOLS)
            linear_tickers = {sym: linear_tickers_raw[to_linear(sym)] for sym in CRYPTO_SYMBOLS if to_linear(sym) in linear_tickers_raw}
            state.ticker_cache = {**linear_tickers, **spot_tickers}
            state.last_ticker_fetch = time.time()
        except Exception as e:
            log.warning(f"[TICKER] refresh failed: {e}")
        await asyncio.sleep(TICKER_INTERVAL)

# =========================
# DB WRITER & CLEANUP LOOP
# =========================
def _safe_float(value, default=0.0) -> float:
    """Never lets a None/missing field crash record building — no silent bad records."""
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

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

            ticker = state.ticker_cache.get(sym, {})
            info = ticker.get("info", {}) if ticker else {}

            records.append({
                "symbol":         sym,
                "timestamp":      datetime.fromtimestamp(bar[0] / 1000, tz=UTC),
                # Raw floats — do NOT round to 2 decimals here, sub-$1 symbols
                # (SUI, ZEN, etc.) would collapse to 0.00 and lose precision.
                "open":           _safe_float(bar[1]),
                "high":           _safe_float(bar[2]),
                "low":            _safe_float(bar[3]),
                "close":          _safe_float(bar[4]),
                "volume":         _safe_float(bar[5]),
                "vwap":           _safe_float(ticker.get("vwap")) if ticker else None,
                "turnover24h":    _safe_float(info.get("turnover24h")),
                "price24hpcnt":   _safe_float(info.get("price24hPcnt")),
                "funding":        _safe_float(info.get("fundingRate") or info.get("lastFundingRate")),
                "oi":             _safe_float(info.get("openInterest")),
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
    pipeline = dlt.pipeline(pipeline_name="crypto_candles_bybit", destination=dlt.destinations.postgres(credentials=DB_URL), dataset_name="bybit_data")
    pipeline.abort_packages()

    tasks = []

    for sym in CRYPTO_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_linear, sym, is_linear=True), name=f"ws-linear-{sym}"))

    for sym in XSTOCK_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_spot, sym, is_linear=False), name=f"ws-spot-{sym}"))

    tasks.append(asyncio.create_task(refresh_ticker_cache(ex_linear, ex_spot), name="ticker-cache"))

    # Start unified DB writer
    tasks.append(asyncio.create_task(db_writer_loop(pipeline), name="db-writer"))

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
