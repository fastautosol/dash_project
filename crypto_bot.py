# crypto-bot.py — ccxt.pro WebSocket edition
# Supports: 25-30 crypto (linear futures) + 15-20 xstocks (spot)
# UPSERT every 75s (forming candles) | INSERT every 5min (completed candles)

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

# Linear futures (perpetuals)
CRYPTO_SYMBOLS: list[str] = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "SUI/USDT", "HYPE/USDT", "LTC/USDT", "ETC/USDT", "COMP/USDT",
    "AVAX/USDT", "AXS/USDT", "LINK/USDT", "BCH/USDT", "TIA/USDT", "ZEN/USDT"]

# Tokenized stocks — traded on Bybit spot market
# ⚠ Verify exact symbol names: exchange.load_markets() and filter for xstock category
XSTOCK_SYMBOLS: list[str] = [
    # "AAPL/USDT", "TSLA/USDT", "NVDA/USDT", "AMZN/USDT", "MSFT/USDT",
    # add up to 20 …
]

UPSERT_INTERVAL  = 75     # seconds — refresh forming candle in DB
INSERT_INTERVAL  = 300    # seconds — flush completed candles to history table
TICKER_INTERVAL  = 300    # seconds — REST batch ticker refresh (funding, OI, 24h)
CLEANUP_INTERVAL = 3600   # seconds — hourly rolling cleanup
CLEANUP_HOURS    = 60     # keep last N hours in bybit_candles


# =========================
# SHARED STATE
# =========================
class MarketState:
    def __init__(self) -> None:
        # Latest OHLCV bar per symbol: [ts, o, h, l, c, v]
        self.ohlcv:  dict[str, list]  = {}
        # Latest ticker info per symbol (from REST batch)
        self.ticker: dict[str, dict]  = {}
        # Last seen candle timestamp per symbol (for close detection)
        self.prev_ts: dict[str, int]  = {}

        # Completed candles waiting to be flushed to history table
        self.completed_buffer: list[dict] = []
        self.completed_lock = asyncio.Lock()

        # Scheduler timestamps
        self.last_upsert:  float = 0.0
        self.last_insert:  float = 0.0
        self.last_cleanup: float = 0.0


# =========================
# HELPERS
# =========================
def _safe_float(value, digits: int = 6) -> float:
    """Convert to float safely, return 0.0 on None / missing / bad value."""
    try:
        return round(float(value), digits) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _build_record(symbol: str, bar: list, ticker: dict, *, complete: bool) -> dict:
    """Build a unified DB record from an OHLCV bar + ticker snapshot."""
    info = ticker.get("info", {})
    return {
        "symbol":       symbol,
        "timestamp":    datetime.fromtimestamp(bar[0] / 1000, tz=UTC),
        "open":         _safe_float(bar[1]),
        "high":         _safe_float(bar[2]),
        "low":          _safe_float(bar[3]),
        "close":        _safe_float(bar[4]),
        "volume":       _safe_float(bar[5], 2),
        "vwap":         _safe_float(ticker.get("vwap")),
        "turnover24h":  _safe_float(info.get("turnover24h"),   2),
        "price24hpcnt": _safe_float(info.get("price24hPcnt")),
        "funding":      _safe_float(info.get("fundingRate") or info.get("lastFundingRate"), 8),
        "oi":           _safe_float(info.get("openInterest"),  2),
        "complete":     complete,
    }


# =========================
# WEBSOCKET — OHLCV WATCHER
# =========================
async def watch_ohlcv_symbol(
    exchange: ccxtpro.bybit,
    symbol: str,
    state: MarketState,
) -> None:
    """
    Infinite loop: subscribes to 5m OHLCV stream for one symbol.
    ccxt.pro multiplexes all symbols over a shared WebSocket pool —
    spawning one coroutine per symbol is the canonical pattern.

    Candle-close detection:
      When the timestamp of ohlcv[-1] changes, the previous bar just completed.
      ohlcv[-2] is that closed candle → appended to completed_buffer.
    """
    while True:
        try:
            # limit=2: keeps previous bar in list so we can capture it on close
            ohlcv = await exchange.watch_ohlcv(symbol, "5m", limit=2)
            if not ohlcv:
                continue

            latest = ohlcv[-1]
            ts     = int(latest[0])
            prev   = state.prev_ts.get(symbol)

            if prev is not None and ts != prev and len(ohlcv) >= 2:
                # Previous candle just closed — grab it before it scrolls out
                closed = ohlcv[-2]
                rec = _build_record(symbol, closed, state.ticker.get(symbol, {}), complete=True)
                async with state.completed_lock:
                    state.completed_buffer.append(rec)
                log.debug(f"[CLOSE] {symbol} @ {rec['timestamp'].isoformat()}")

            state.prev_ts[symbol] = ts
            state.ohlcv[symbol]   = latest

        except ccxtpro.NetworkError as e:
            log.warning(f"[WS] {symbol} network error: {e} — retrying in 5s")
            await asyncio.sleep(5)
        except ccxtpro.ExchangeError as e:
            log.error(f"[WS] {symbol} exchange error: {e} — retrying in 15s")
            await asyncio.sleep(15)
        except Exception as e:
            log.exception(f"[WS] {symbol} unexpected error: {e} — retrying in 5s")
            await asyncio.sleep(5)


async def supervised(coro_fn, *args) -> None:
    """Restart a coroutine on unexpected crash (not on CancelledError)."""
    while True:
        try:
            await coro_fn(*args)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"[SUPERVISOR] {coro_fn.__name__} crashed: {e} — restarting in 5s")
            await asyncio.sleep(5)


# =========================
# REST — TICKER REFRESH
# =========================
async def ticker_refresh_loop(
    ex_linear: ccxtpro.bybit,
    ex_spot:   ccxtpro.bybit,
    state: MarketState,
) -> None:
    """
    Refreshes funding rate, open interest, vwap, and 24h stats every 5min.
    Two batch calls (one per market type) replace N individual REST calls.
    ccxt.pro instances support REST methods alongside WebSocket.
    """
    while True:
        await asyncio.sleep(TICKER_INTERVAL)
        for exchange, symbols in [
            (ex_linear, CRYPTO_SYMBOLS),
            (ex_spot,   XSTOCK_SYMBOLS),
        ]:
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
    """
    Scheduler that runs every second and fires DB writes when intervals elapse:
      • 75s  → UPSERT bybit_candles        (forming candle, merge by [symbol, timestamp])
      • 5min → INSERT bybit_candles_history (completed candles, append-only)
      • 1hr  → DELETE old rows from bybit_candles
    """
    all_symbols = CRYPTO_SYMBOLS + XSTOCK_SYMBOLS

    while True:
        await asyncio.sleep(1)
        now = time.time()

        # ── 75s UPSERT: forming candles ──────────────────────────────────────
        if now - state.last_upsert >= UPSERT_INTERVAL:
            records = [
                _build_record(sym, bar, state.ticker.get(sym, {}), complete=False)
                for sym in all_symbols
                if (bar := state.ohlcv.get(sym)) is not None
            ]
            if records:
                try:
                    await asyncio.to_thread(
                        pipeline.run,
                        records,
                        table_name="bybit_candles",
                        write_disposition="merge",
                        primary_key=["symbol", "timestamp"],
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
                        pipeline.run,
                        to_insert,
                        table_name="bybit_candles_history",
                        write_disposition="append",
                    )
                    log.info(f"[INSERT] {len(to_insert)} completed candles → history")
                except Exception as e:
                    log.error(f"[INSERT] Failed: {e}")
                    # Re-queue on failure so no candles are lost
                    async with state.completed_lock:
                        state.completed_buffer[:0] = to_insert
            state.last_insert = now

        # ── Hourly cleanup ────────────────────────────────────────────────────
        if now - state.last_cleanup >= CLEANUP_INTERVAL:
            try:
                threshold = datetime.now(UTC) - timedelta(hours=CLEANUP_HOURS)
                with pipeline.sql_client() as client:
                    tname = client.make_qualified_table_name("bybit_candles")
                    client.execute_sql(
                        f"DELETE FROM {tname} WHERE timestamp < %s", threshold
                    )
                log.info(f"[CLEANUP] Removed forming candles older than {CLEANUP_HOURS}h")
            except Exception as e:
                log.error(f"[CLEANUP] Failed: {e}")
            state.last_cleanup = now


# =========================
# MAIN
# =========================
async def main() -> None:
    state = MarketState()

    # ── Exchange instances ────────────────────────────────────────────────────
    # ccxt.pro supports both WebSocket (watch_*) and REST (fetch_*) on the same object.
    # Separate instances are required because defaultType affects endpoint routing.
    base_cfg = {"enableRateLimit": True}

    ex_linear = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "linear"}})
    ex_spot   = ccxtpro.bybit({**base_cfg, "options": {"defaultType": "spot"}})

    # ── dlt pipeline ──────────────────────────────────────────────────────────
    pipeline = dlt.pipeline(
        pipeline_name="crypto_strategy",
        destination=dlt.destinations.postgres(credentials=DB_URL),
        dataset_name="bybit_data",
    )

    # ── Build task list ───────────────────────────────────────────────────────
    tasks: list[asyncio.Task] = []

    # One supervised WebSocket watcher per symbol
    # ccxt.pro shares the underlying WS connection across all coroutines
    # on the same exchange instance — no per-symbol connection overhead.
    for sym in CRYPTO_SYMBOLS:
        tasks.append(asyncio.create_task(
            supervised(watch_ohlcv_symbol, ex_linear, sym, state),
            name=f"ws-{sym}",
        ))
    for sym in XSTOCK_SYMBOLS:
        tasks.append(asyncio.create_task(
            supervised(watch_ohlcv_symbol, ex_spot, sym, state),
            name=f"ws-{sym}",
        ))

    # REST ticker refresh (shared instances handle both WS + REST)
    tasks.append(asyncio.create_task(
        supervised(ticker_refresh_loop, ex_linear, ex_spot, state),
        name="ticker-refresh",
    ))

    # DB writer
    tasks.append(asyncio.create_task(
        supervised(db_writer_loop, pipeline, state),
        name="db-writer",
    ))

    log.info(
        f"Bot started — {len(CRYPTO_SYMBOLS)} crypto (linear) "
        f"+ {len(XSTOCK_SYMBOLS)} xstocks (spot) | "
        f"UPSERT {UPSERT_INTERVAL}s | INSERT {INSERT_INTERVAL}s"
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
