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
     "AAPLX/USDT", "TSLAX/USDT", "NVDAX/USDT", "AMZNX/USDT", "COINX/USDT", 
    "CRCLX/USDT", "METAX/USDT", "HOODX/USDT", "GOOGLX/USDT"
]

UPSERT_INTERVAL  = 75     # seconds
INSERT_INTERVAL  = 300    # seconds
TICKER_INTERVAL  = 300    # seconds
CLEANUP_INTERVAL = 3600   # seconds
CLEANUP_HOURS    = 60     # hours

# =========================
# SHARED STATE
# =========================
class MarketState:
    def __init__(self) -> None:
        self.ohlcv: dict[str, list] = {}
        self.ticker: dict[str, dict] = {}
        self.prev_ts: dict[str, int] = {}
        self.completed_buffer: list[dict] = []
        self.completed_lock = asyncio.Lock()
        
        self.last_upsert: float = 0.0
        self.last_insert: float = 0.0
        self.last_cleanup: float = 0.0

# =========================
# WEBSOCKET — OHLCV WATCHER
# =========================
async def watch_ohlcv_symbol(exchange: ccxtpro.bybit, symbol: str, state: MarketState) -> None:
    while True:
        try:
            ohlcv = await exchange.watch_ohlcv(symbol, "5m", limit=2)
            if not ohlcv or len(ohlcv) < 2:
                continue

            latest = ohlcv[-1]
            ts = int(latest[0])
            prev = state.prev_ts.get(symbol)

            # Detect candle close
            if prev is not None and ts != prev:
                closed = ohlcv[-2]
                # Simplified inline record creation (replacing _build_record helper)
                rec = {
                    "symbol": symbol,
                    "timestamp": datetime.fromtimestamp(closed[0] / 1000, tz=UTC),
                    "open": float(closed[1] or 0),
                    "high": float(closed[2] or 0),
                    "low": float(closed[3] or 0),
                    "close": float(closed[4] or 0),
                    "volume": float(closed[5] or 0),
                    "complete": True
                }
                async with state.completed_lock:
                    state.completed_buffer.append(rec)
                log.debug(f"[CLOSE] {symbol} @ {rec['timestamp'].isoformat()}")

            state.prev_ts[symbol] = ts
            state.ohlcv[symbol] = latest

        except Exception as e:
            log.warning(f"[WS] {symbol} error: {e} — retrying in 5s")
            await asyncio.sleep(5)

# =========================
# REST — TICKER REFRESH
# =========================
async def ticker_refresh_loop(ex_linear: ccxtpro.bybit, ex_spot: ccxtpro.bybit, state: MarketState) -> None:
    while True:
        await asyncio.sleep(TICKER_INTERVAL)
        for exchange, symbols in [(ex_linear, CRYPTO_SYMBOLS), (ex_spot, XSTOCK_SYMBOLS)]:
            if not symbols:
                continue
            try:
                tickers = await exchange.fetch_tickers(symbols=symbols)
                state.ticker.update(tickers)
                log.info(f"[TICKER] Refreshed {len(tickers)} symbols")
            except Exception as e:
                log.error(f"[TICKER] Refresh error: {e}")

# =========================
# DB WRITER
# =========================
async def db_writer_loop(pipeline, state: MarketState) -> None:
    all_symbols = CRYPTO_SYMBOLS + XSTOCK_SYMBOLS
    while True:
        await asyncio.sleep(1)
        now = time.time()

        # ── 75s UPSERT: forming candles ──────────────────────────────────────
        if now - state.last_upsert >= UPSERT_INTERVAL:
            records = []
            for sym in all_symbols:
                bar = state.ohlcv.get(sym)
                if bar:
                    # Simplified inline record creation
                    records.append({
                        "symbol": sym,
                        "timestamp": datetime.fromtimestamp(bar[0] / 1000, tz=UTC),
                        "open": float(bar[1] or 0),
                        "high": float(bar[2] or 0),
                        "low": float(bar[3] or 0),
                        "close": float(bar[4] or 0),
                        "volume": float(bar[5] or 0),
                        "complete": False
                    })
            
            if records:
                try:
                    await asyncio.to_thread(
                        pipeline.run, records, table_name="bybit_candles", 
                        write_disposition="merge", primary_key=["symbol", "timestamp"]
                    )
                    log.info(f"[UPSERT] {len(records)} forming candles")
                except Exception as e:
                    log.error(f"[UPSERT] Failed: {e}")
            state.last_upsert = now

        # ── 5min INSERT: completed candles ────────────────────────────────────
        if now - state.last_insert >= INSERT_INTERVAL:
            async with state.completed_lock:
                to_insert = state.completed_buffer[:]
                state.completed_buffer.clear()

            if to_insert:
                try:
                    await asyncio.to_thread(
                        pipeline.run, to_insert, table_name="bybit_candles_history", 
                        write_disposition="append"
                    )
                    log.info(f"[INSERT] {len(to_insert)} completed candles → history")
                except Exception as e:
                    log.error(f"[INSERT] Failed: {e}")
                    # Re-queue on failure
                    async with state.completed_lock:
                        state.completed_buffer[:0] = to_insert
            state.last_insert = now

        # ── Hourly cleanup ────────────────────────────────────────────────────
        if now - state.last_cleanup >= CLEANUP_INTERVAL:
            try:
                threshold = datetime.now(UTC) - timedelta(hours=CLEANUP_HOURS)
                with pipeline.sql_client() as client:
                    tname = client.make_qualified_table_name("bybit_candles")
                    client.execute_sql(f"DELETE FROM {tname} WHERE timestamp < %s", threshold)
                log.info(f"[CLEANUP] Removed forming candles older than {CLEANUP_HOURS}h")
            except Exception as e:
                log.error(f"[CLEANUP] Failed: {e}")
            state.last_cleanup = now

# =========================
# MAIN
# =========================
async def main() -> None:
    state = MarketState()
    
    base_cfg = {"enableRateLimit": True}
    ex_linear = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "linear"}})
    ex_spot = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "spot"}})

    pipeline = dlt.pipeline(
        pipeline_name="crypto_strategy",
        destination=dlt.destinations.postgres(credentials=DB_URL),
        dataset_name="bybit_data"
    )

    tasks: list[asyncio.Task] = []

    # Direct task creation (supervised wrapper removed)
    for sym in CRYPTO_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_linear, sym, state), name=f"ws-{sym}"))
    for sym in XSTOCK_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_spot, sym, state), name=f"ws-{sym}"))

    tasks.append(asyncio.create_task(ticker_refresh_loop(ex_linear, ex_spot, state), name="ticker-refresh"))
    tasks.append(asyncio.create_task(db_writer_loop(pipeline, state), name="db-writer"))

    log.info(f"Bot started — {len(CRYPTO_SYMBOLS)} crypto + {len(XSTOCK_SYMBOLS)} xstocks | UPSERT {UPSERT_INTERVAL}s | INSERT {INSERT_INTERVAL}s")

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
