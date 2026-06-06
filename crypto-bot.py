# 2026.06.06  10.00
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

XSTOCK_SYMBOLS = ["AAPLX/USDT", "TSLAX/USDT", "NVDAX/USDT", "AMZNX/USDT", "COINX/USDT", "CRCLX/USDT", "METAX/USDT", "HOODX/USDT", "GOOGLX/USDT"]

UPSERT_INTERVAL  = 75     # seconds
INSERT_INTERVAL  = 300    # seconds
TICKER_INTERVAL  = 300    # seconds
CLEANUP_INTERVAL = 3600   # seconds
CLEANUP_HOURS    = 72     # hours

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

        self.pipeline_lock = asyncio.Lock()  # serialise all pipeline.run() calls

# =========================
# WEBSOCKET — OHLCV WATCHER
# =========================
async def watch_ohlcv_symbol(exchange: ccxtpro.bybit, symbol: str, state: MarketState) -> None:
    while True:
        try:
            ohlcv = await exchange.watch_ohlcv(symbol, "5m", limit=2)
            if not ohlcv:
                continue

            latest = ohlcv[-1]
            ts = int(latest[0])
            prev = state.prev_ts.get(symbol)

            # Detect candle close BEFORE updating state.
            # state.ohlcv[symbol] still holds the final bar of the previous candle,
            # so no reliance on ohlcv[-2] or len >= 2 (breaks when Bybit sends
            # only the new candle in the boundary message).
            if prev is not None and ts != prev:
                closed_bar = state.ohlcv.get(symbol)
                if closed_bar:
                    rec = {
                        "symbol":    symbol,
                        "timestamp": datetime.fromtimestamp(closed_bar[0] / 1000, tz=UTC),
                        "open":      float(closed_bar[1] or 0),
                        "high":      float(closed_bar[2] or 0),
                        "low":       float(closed_bar[3] or 0),
                        "close":     float(closed_bar[4] or 0),
                        "volume":    float(closed_bar[5] or 0),
                        "complete":  True
                    }
                    async with state.completed_lock:
                        state.completed_buffer.append(rec)
                    log.debug(f"[CLOSE] {symbol} @ {rec['timestamp'].isoformat()}")

            # Update state AFTER close detection
            state.prev_ts[symbol] = ts
            state.ohlcv[symbol] = latest

        except Exception as e:
            log.warning(f"[WS] {symbol} error: {e} — retrying in 5s")
            await asyncio.sleep(5)

# =========================
# REST — TICKER REFRESH
# =========================
async def ticker_refresh_loop(
    ex_linear: ccxtpro.bybit,
    ex_spot: ccxtpro.bybit,
    state: MarketState,
    pipeline,
) -> None:
    while True:
        await asyncio.sleep(TICKER_INTERVAL)
        ticker_records: list[dict] = []
        now_utc = datetime.now(UTC)

        for exchange, symbols in [(ex_linear, CRYPTO_SYMBOLS), (ex_spot, XSTOCK_SYMBOLS)]:
            if not symbols:
                continue
            try:
                tickers = await exchange.fetch_tickers(symbols=symbols)
                state.ticker.update(tickers)

                for sym, t in tickers.items():
                    info = t.get("info", {})
                    ticker_records.append({
                        "symbol":        sym,
                        "timestamp":     now_utc,
                        # vwap24h is the Bybit-specific key; fall back to ccxt unified field
                        "vwap":          float(info.get("vwap24h")      or t.get("vwap")        or 0),
                        # turnover24h is quote-currency volume; quoteVolume is the ccxt fallback
                        "turnover_24h":  float(info.get("turnover24h")  or t.get("quoteVolume") or 0),
                        # price24hPcnt arrives as a decimal fraction from Bybit (0.032 = 3.2 %)
                        "price_24h_pct": float(info.get("price24hPcnt") or t.get("percentage")  or 0),
                    })
                log.info(f"[TICKER] Refreshed {len(tickers)} symbols")
            except Exception as e:
                log.error(f"[TICKER] Refresh error: {e}")

        if ticker_records:
            try:
                async with state.pipeline_lock:
                    await asyncio.to_thread(
                        pipeline.run, ticker_records,
                        table_name="bybit_tickers",
                        write_disposition="replace",
                    )
                log.info(f"[TICKER] Replaced {len(ticker_records)} ticker snapshots → bybit_tickers")
            except Exception as e:
                log.error(f"[TICKER] DB write failed: {e}")

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
                    records.append({
                        "symbol":    sym,
                        "timestamp": datetime.fromtimestamp(bar[0] / 1000, tz=UTC),
                        "open":      float(bar[1] or 0),
                        "high":      float(bar[2] or 0),
                        "low":       float(bar[3] or 0),
                        "close":     float(bar[4] or 0),
                        "volume":    float(bar[5] or 0),
                        "complete":  False
                    })

            if records:
                try:
                    async with state.pipeline_lock:
                        await asyncio.to_thread(
                            pipeline.run, records, table_name="bybit_candles",
                            write_disposition="replace",
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
                    async with state.pipeline_lock:
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
            threshold = datetime.now(UTC) - timedelta(hours=CLEANUP_HOURS)

            def _cleanup() -> None:
                with pipeline.sql_client() as client:
                    tname = client.make_qualified_table_name("bybit_candles")
                    client.execute_sql(
                        f"DELETE FROM {tname} WHERE timestamp < %s", threshold
                    )

            try:
                async with state.pipeline_lock:
                    await asyncio.to_thread(_cleanup)
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
    ex_spot   = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "spot"}})

    pipeline = dlt.pipeline(
        pipeline_name="crypto_strategy",
        destination=dlt.destinations.postgres(credentials=DB_URL),
        dataset_name="bybit_data"
    )

    tasks: list[asyncio.Task] = []

    for sym in CRYPTO_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_linear, sym, state), name=f"ws-{sym}"))
    for sym in XSTOCK_SYMBOLS:
        tasks.append(asyncio.create_task(watch_ohlcv_symbol(ex_spot, sym, state), name=f"ws-{sym}"))

    tasks.append(asyncio.create_task(
        ticker_refresh_loop(ex_linear, ex_spot, state, pipeline),   # pipeline added
        name="ticker-refresh"
    ))
    tasks.append(asyncio.create_task(db_writer_loop(pipeline, state), name="db-writer"))

    log.info(
        f"Bot started — {len(CRYPTO_SYMBOLS)} crypto + {len(XSTOCK_SYMBOLS)} xstocks"
        f" | UPSERT {UPSERT_INTERVAL}s | INSERT {INSERT_INTERVAL}s | TICKER {TICKER_INTERVAL}s"
    )

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
