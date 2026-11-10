# 2026.11.10  10.00
import asyncio
import ccxt.pro as ccxtpro
import dlt
from datetime import datetime, UTC, timedelta
import time
import logging
import os
from bybit_silver import run_bybit_silver_transformation

os.environ.setdefault("LOAD__DELETE_COMPLETED_JOBS", "true")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger(__name__)

# =========================================================
# 1. CONFIGURATION
# =========================================================
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"

CRYPTO_SYMBOLS = [
    "AAVE/USDT", "ADA/USDT", "ALGO/USDT", "APT/USDT", "ARB/USDT", "ASTER/USDT", "AVAX/USDT", "AXS/USDT", "BCH/USDT", "BNB/USDT", "BTC/USDT", 
    "COMP/USDT", "CRV/USDT", "DASH/USDT", "DOGE/USDT", "DOT/USDT", "EGLD/USDT", "ENA/USDT", "ETC/USDT", "ETH/USDT", "FIL/USDT", "HBAR/USDT", "HYPE/USDT", 
    "ICP/USDT", "INJ/USDT", "KAS/USDT", "LINK/USDT", "LIT/USDT", "LTC/USDT", "NEAR/USDT", "ONDO/USDT", "OP/USDT", "QNT/USDT", "PLUME/USDT", "POL/USDT", 
    "PYTH/USDT", "RENDER/USDT", "RUNE/USDT", "SAND/USDT", "SOL/USDT", "SEI/USDT", "SUI/USDT", "TIA/USDT", "TAO/USDT", "VIRTUAL/USDT", "ZEC/USDT", 
    "ZEN/USDT", "ZRO/USDT", "UNI/USDT", "XPL/USDT", "XLM/USDT", "XMR/USDT", "XRP/USDT", "XTZ/USDT", "W/USDT"]

POLL_INTERVAL = 150     # Seconds between DB upserts
TICKER_INTERVAL = 300   # Seconds between ticker cache refreshes
SILVER_INTERVAL = 300   # Seconds between Silver transformations
CLEANUP_HOURS = 72      # Hours of data to retain
TABLE_NAME = "bybit_marketinfo"

def to_linear(symbol: str) -> str:
    return f"{symbol}:USDT"

LINEAR_SYMBOLS = [to_linear(s) for s in CRYPTO_SYMBOLS]

# =========================================================
# 2. SHARED STATE
# =========================================================
class MarketState:
    def __init__(self) -> None:
        self.ohlcv: dict[str, list] = {}
        self.ticker_cache: dict[str, dict] = {}
        self.last_ticker_fetch: float = 0.0
        self.last_cleanup: float = 0.0
        self.pipeline_lock = asyncio.Lock()
state = MarketState()

# =========================================================
# 3. SILVER LOOP (a transzformáció az apis/bybit_silver.py-ban van)
# =========================================================
async def silver_etl_loop() -> None:
    """Periodikusan futtatja a Silver transzformációt a háttérben."""
    while True:
        await asyncio.sleep(SILVER_INTERVAL)
        try:
            log.info("[SILVER] Starting Silver transformation...")
            silver_rows = await asyncio.to_thread(run_bybit_silver_transformation)
            log.info("[SILVER] Transformation completed. Rows: %s", silver_rows)
        except Exception:
            log.exception("[SILVER] Transformation failed. Bronze data is preserved; previous Silver state remains.")

# =========================================================
# 4. HISTORICAL OHLCV
# =========================================================
async def load_historical_ohlcv(exchange: ccxtpro.bybit, symbol: str, limit: int = 300) -> list:
    try:
        ohlcv = await exchange.fetch_ohlcv(to_linear(symbol), timeframe="5m", limit=limit)
        return ohlcv
    except Exception as e:
        log.error(f"[HIST] {symbol} failed: {e}")
        return []

# =========================================================
# 5. WEBSOCKET — OHLCV WATCHER
# =========================================================
async def watch_ohlcv_symbol(exchange: ccxtpro.bybit, symbol: str) -> None:
    ws_symbol = to_linear(symbol)
    historical = await load_historical_ohlcv(exchange, symbol, limit=1000)
    if historical:
        state.ohlcv[symbol] = historical[-1]
    
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

# =========================================================
# 6. TICKER CACHE REFRESH
# =========================================================
async def refresh_ticker_cache(exchange: ccxtpro.bybit) -> None:
    while True:
        try:
            log.info("\n" + "=" * 45)
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

# =========================================================
# 7. DB WRITER & CLEANUP LOOP
# =========================================================
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
                "prevPrice1h":    _safe_float(info.get("prevPrice1h")),
                "price24hpcnt":   _safe_float(info.get("price24hPcnt")),
                "turnover24h":    _safe_float(info.get("turnover24h")),
                "funding":        _safe_float(info.get("fundingRate") or info.get("lastFundingRate")),
                "oi":             _safe_float(info.get("openInterest")),
            })

        if records:
            try:
                async with state.pipeline_lock:
                    await asyncio.to_thread(pipeline.run, records, table_name=TABLE_NAME, write_disposition="merge", primary_key=["symbol", "timestamp"])
                log.info(f"[DB] Upserted {len(records)} records.")
            except Exception as e:
                log.error(f"[DB] Upsert failed: {e}")

        if now - state.last_cleanup >= 3600:
            threshold = now_utc - timedelta(hours=CLEANUP_HOURS)
            try:
                async with state.pipeline_lock:
                    def _cleanup():
                        with pipeline.sql_client() as client:
                            tname = client.make_qualified_table_name(TABLE_NAME)
                            client.execute_sql(f"DELETE FROM {tname} WHERE timestamp < %s", (threshold,))
                    await asyncio.to_thread(_cleanup)
                log.info(f"[CLEANUP] Removed data older than {CLEANUP_HOURS}h")
            except Exception as e:
                log.error(f"[CLEANUP] Failed: {e}")
            state.last_cleanup = now

# =========================================================
# 8. MAIN
# =========================================================
async def main() -> None:
    base_cfg = {"enableRateLimit": True}
    ex_linear = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "linear"}})
    
    pipeline = dlt.pipeline(pipeline_name="crypto_candles_bybit", destination=dlt.destinations.postgres(credentials=DB_URL), dataset_name="bronze")
    pipeline.abort_packages()

    tasks = []
    for sym in CRYPTO_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_linear, sym), name=f"ws-{sym}"))  
    tasks.append(asyncio.create_task(refresh_ticker_cache(ex_linear), name="ticker-cache"))
    tasks.append(asyncio.create_task(db_writer_loop(pipeline), name="db-writer"))
    tasks.append(asyncio.create_task(silver_etl_loop(), name="silver-etl"))

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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("[STOP] Bot stopped by user.")
