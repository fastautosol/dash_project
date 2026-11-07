# 2026.11.07  18.00
import asyncio
import ccxt.pro as ccxtpro
import dlt
from datetime import datetime, UTC, timedelta
import time
import logging
import os

os.environ.setdefault("LOAD__DELETE_COMPLETED_JOBS", "true")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger(__name__)

# =========================
# CONFIGURATION
# =========================
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"

CRYPTO_SYMBOLS = ["AAVE/USDT","ALGO/USDT","APT/USDT","ARB/USDT","AVAX/USDT","AXS/USDT","BCH/USDT","BNB/USDT","BTC/USDT","COMP/USDT","DASH/USDT",
        "ENA/USDT","ETC/USDT","ETH/USDT","HBAR/USDT","HYPE/USDT","ICP/USDT","INJ/USDT","LINK/USDT","LIT/USDT","LTC/USDT","NEAR/USDT","ONDO/USDT",
        "OP/USDT","POL/USDT","SAND/USDT","SOL/USDT","SUI/USDT","TIA/USDT","TAO/USDT","ZEC/USDT","ZEN/USDT","UNI/USDT","XRP/USDT","XTZ/USDT"]

POLL_INTERVAL = 150     # Seconds between DB upserts
TICKER_INTERVAL = 300   # Seconds between ticker cache refreshes (funding/OI/turnover)
CLEANUP_HOURS = 120     # Hours of data to retain
TABLE_NAME = "bybit_marketinfo"

def to_linear(symbol: str) -> str:
    """BTC/USDT -> BTC/USDT:USDT (Bybit USDT perpetual)"""
    return f"{symbol}:USDT"

LINEAR_SYMBOLS = [to_linear(s) for s in CRYPTO_SYMBOLS]

# =========================
# SHARED STATE
# =========================
class MarketState:
    def __init__(self) -> None:
        self.ohlcv: dict[str, list] = {}          # key: spot-style symbol (BTC/USDT)
        self.ticker_cache: dict[str, dict] = {}   # key: linear symbol (BTC/USDT:USDT)
        self.last_ticker_fetch: float = 0.0
        self.last_cleanup: float = 0.0
        self.pipeline_lock = asyncio.Lock()  # Serialize dlt pipeline calls

state = MarketState()


# ============================
# HISTORICAL OHLCV
# ============================
async def load_historical_ohlcv(exchange: ccxtpro.bybit, symbol: str, limit: int = 1000) -> list:
    """Load historical candles on startup"""
    try:
        log.info(f"[HIST] Loading {limit} candles for {symbol}...")
        ohlcv = await exchange.fetch_ohlcv(to_linear(symbol), timeframe="5m", limit=limit)
        log.info(f"[HIST] {symbol}: Loaded {len(ohlcv)} historical candles")
        return ohlcv
    except Exception as e:
        log.error(f"[HIST] {symbol} failed: {e}")
        return []

# =========================
# WEBSOCKET — OHLCV WATCHER
# =========================
async def watch_ohlcv_symbol(exchange: ccxtpro.bybit, symbol: str) -> None:
    ws_symbol = to_linear(symbol)

    # 1. Load historical data FIRST (only the latest candle is kept in state)
    historical = await load_historical_ohlcv(exchange, symbol, limit=1000)
    if historical:
        state.ohlcv[symbol] = historical[-1]

    # 2. THEN start WebSocket for real-time updates
    while True:
        try:
            ohlcv = await exchange.watch_ohlcv(ws_symbol, timeframe="5m", limit=1)
            if ohlcv:
                state.ohlcv[symbol] = ohlcv[-1]
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"[WS] {symbol} error: {e} — reconnecting in 3s")
            await asyncio.sleep(3)

# =========================
# TICKER CACHE REFRESH (funding / OI / turnover / 24h change)
# =========================
async def refresh_ticker_cache(exchange: ccxtpro.bybit) -> None:
    while True:
        try:
            log.info("=" * 45)
            tickers = await exchange.fetch_tickers(symbols=LINEAR_SYMBOLS)
            state.ticker_cache = tickers
            state.last_ticker_fetch = time.time()
            log.info(f"[TICKER] Cache refreshed for {len(tickers)} symbols.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning(f"[TICKER] Refresh failed: {e}")

        delay = TICKER_INTERVAL - (time.time() % TICKER_INTERVAL)
        log.info("[WAIT] Next ticker refresh in %.1f seconds.", delay)
        await asyncio.sleep(delay)

# =========================
# DB WRITER & CLEANUP LOOP
# =========================
def _safe_float(value, default=0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

async def db_writer_loop(pipeline) -> None:
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        now = time.time()
        now_utc = datetime.now(UTC)
        records = []

        # 1. Build records from WebSocket state + Ticker cache
        for sym in CRYPTO_SYMBOLS:
            bar = state.ohlcv.get(sym)
            if not bar:
                continue

            ticker = state.ticker_cache.get(to_linear(sym), {})
            info = ticker.get("info", {}) if ticker else {}

            records.append({
                "symbol":         sym,
                "timestamp":      datetime.fromtimestamp(bar[0] / 1000, tz=UTC),
                "open":           _safe_float(bar[1]),
                "high":           _safe_float(bar[2]),
                "low":            _safe_float(bar[3]),
                "close":          _safe_float(bar[4]),
                "volume":         _safe_float(bar[5]),
                "vwap":           _safe_float(ticker.get("vwap")) if ticker else None,
                "prevPrice1h":  _safe_float(info.get("prevPrice1h")),
                "price24hpcnt":   _safe_float(info.get("price24hPcnt")),
                "turnover24h":    _safe_float(info.get("turnover24h")),
                "funding":        _safe_float(info.get("fundingRate") or info.get("lastFundingRate")),
                "oi":             _safe_float(info.get("openInterest")),
            })

        # 2. Upsert to database
        if records:
            try:
                async with state.pipeline_lock:
                    await asyncio.to_thread(
                        pipeline.run, records,
                        table_name=TABLE_NAME, write_disposition="merge", primary_key=["symbol", "timestamp"],
                    )
                log.info(f"[DB] Upserted {len(records)} records.")
            except Exception as e:
                log.error(f"[DB] Upsert failed: {e}")

        # 3. Hourly Cleanup
        if now - state.last_cleanup >= 3600:
            threshold = now_utc - timedelta(hours=CLEANUP_HOURS)
            try:
                async with state.pipeline_lock:
                    def _cleanup():
                        with pipeline.sql_client() as client:
                            tname = client.make_qualified_table_name(TABLE_NAME)
                            client.execute_sql(f"DELETE FROM {tname} WHERE timestamp < %s", threshold)

                    await asyncio.to_thread(_cleanup)
                log.info(f"[CLEANUP] Removed data older than {CLEANUP_HOURS}h")
            except Exception as e:
                log.error(f"[CLEANUP] Failed: {e}")
            state.last_cleanup = now

# =========================
# MAIN
# =========================
async def main() -> None:
    base_cfg = {"enableRateLimit": True}
    ex_linear = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "linear"}})

    # Initialize dlt pipeline
    pipeline = dlt.pipeline(pipeline_name="crypto_candles_bybit", destination=dlt.destinations.postgres(credentials=DB_URL), dataset_name="bronze")
    pipeline.abort_packages()

    tasks = []

    for sym in CRYPTO_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_linear, sym), name=f"ws-{sym}"))
    tasks.append(asyncio.create_task(refresh_ticker_cache(ex_linear), name="ticker-cache"))
    tasks.append(asyncio.create_task(db_writer_loop(pipeline), name="db-writer"))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log.info("Shutdown signal received.")
    finally:
        for t in tasks:
            t.cancel()
        await ex_linear.close()
        log.info("All connections closed. Bye.")

if __name__ == "__main__":
    asyncio.run(main())
