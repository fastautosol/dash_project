
# bybit_signal_api.py
# Bybit market scanner -> DLT -> PostgreSQL
# 5-minute market snapshots
#
# Install:
# pip install ccxt "dlt[postgres]"
#
# Run:
# python bybit_signal_api.py

import asyncio
import logging
import math
import os
import time

from datetime import datetime, UTC

import ccxt.async_support as ccxt
import dlt


# ============================================================
# 1. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)

log = logging.getLogger("bybit_signal_api")


# ============================================================
# 2. CONFIGURATION
# ============================================================

# PostgreSQL connection
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgresql"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "n8n"),
    "username": os.getenv("DB_USER", "sql_admin"),
    "password": os.getenv("DB_PASSWORD", "sql_pass"),
    "connect_timeout": 15,
}

# Bybit scanner
TIMEFRAME = "5m"
TIMEFRAME_MS = 5 * 60 * 1000

POLL_INTERVAL = 300  # 5 minutes

MIN_LEV = 25
MIN_TURNOVER = 1_000_000  # USDT / 24h
MIN_R24 = 5.0             # %
MIN_R1 = 0.5              # %

MAX_CONCURRENT_REQUESTS = 7
OHLCV_LIMIT = 12

# DLT
PIPELINE_NAME = "bybit_market_snapshots"
DATASET_NAME = "bybit_data"
TABLE_NAME = "market_snapshots"

PRIMARY_KEY = ["symbol", "snapshot_at"]


# ============================================================
# 3. HELPER FUNCTIONS
# ============================================================

def safe_float(value, default=0.0) -> float:
    """Biztonságos float konverzió."""

    try:
        if value is None:
            return default

        result = float(value)

        if not math.isfinite(result):
            return default

        return result

    except (TypeError, ValueError):
        return default


def calculate_change(current, previous) -> float:
    """Százalékos árfolyamváltozás."""

    current = safe_float(current)
    previous = safe_float(previous)

    if current <= 0 or previous <= 0:
        return 0.0

    return (current / previous - 1) * 100


def get_max_leverage(market: dict) -> float:
    """Maximális leverage lekérése a Bybit market adataiból."""

    info = market.get("info", {}) or {}

    leverage_filter = info.get("leverageFilter", {}) or {}

    max_leverage = safe_float(
        leverage_filter.get("maxLeverage")
    )

    if max_leverage <= 0:
        limits = market.get("limits", {}) or {}
        leverage_limits = limits.get("leverage", {}) or {}

        max_leverage = safe_float(
            leverage_limits.get("max")
        )

    return max_leverage


def is_usdt_linear_swap(market: dict) -> bool:
    """Csak aktív USDT linear perpetual/swap piacokat engedünk."""

    return (
        market.get("active") is not False
        and market.get("swap") is True
        and market.get("linear") is True
        and market.get("quote") == "USDT"
        and market.get("settle") == "USDT"
    )


# ============================================================
# 4. MARKET SCANNER
# ============================================================

async def fetch_symbol_snapshot(
    exchange,
    symbol: str,
    market: dict,
    ticker: dict,
    snapshot_at: datetime,
    semaphore: asyncio.Semaphore,
):
    """
    Egy coin OHLCV adatainak lekérése és market snapshot építése.
    """

    try:
        info = ticker.get("info", {}) or {}

        # ----------------------------------------------------
        # TICKER DATA
        # ----------------------------------------------------

        price = safe_float(
            ticker.get("last"),
            safe_float(info.get("lastPrice"))
        )

        if price <= 0:
            return None

        turnover24h = safe_float(
            info.get("turnover24h"),
            safe_float(ticker.get("quoteVolume"))
        )

        if turnover24h < MIN_TURNOVER:
            return None

        # 1-hour price change
        prev_price_1h = safe_float(
            info.get("prevPrice1h")
        )

        r1h = calculate_change(price, prev_price_1h)

        # 24-hour price change
        prev_price_24h = safe_float(
            info.get("prevPrice24h")
        )

        if prev_price_24h > 0:
            r24h = calculate_change(price, prev_price_24h)
        else:
            # Bybit price24hPcnt fractional value, e.g. -0.05 = -5%
            r24h = safe_float(
                info.get("price24hPcnt")
            ) * 100

        # ----------------------------------------------------
        # FLEXIBLE PRE-FILTER
        # R24 OR R1 must reach the configured threshold.
        # ----------------------------------------------------

        if abs(r24h) < MIN_R24 and abs(r1h) < MIN_R1:
            return None

        max_leverage = get_max_leverage(market)

        if max_leverage < MIN_LEV:
            return None

        # ----------------------------------------------------
        # OHLCV DATA
        # ----------------------------------------------------

        async with semaphore:
            ohlcv = await exchange.fetch_ohlcv(
                symbol,
                timeframe=TIMEFRAME,
                limit=OHLCV_LIMIT,
                params={"category": "linear"},
            )

        if not ohlcv:
            return None

        # Only use fully closed candles.
        now_ms = int(time.time() * 1000)

        closed = [
            candle
            for candle in ohlcv
            if candle[0] + TIMEFRAME_MS <= now_ms
        ]

        # Four closed 5m candles are needed for R15.
        if len(closed) < 4:
            return None

        close_now = safe_float(closed[-1][4])
        close_5 = safe_float(closed[-2][4])
        close_15 = safe_float(closed[-4][4])

        if min(close_now, close_5, close_15) <= 0:
            return None

        r5 = calculate_change(close_now, close_5)
        r15 = calculate_change(close_now, close_15)

        # ----------------------------------------------------
        # ADDITIONAL TICKER DATA
        # ----------------------------------------------------

        change24 = ticker.get("percentage")

        if change24 is None:
            change24 = r24h
        else:
            change24 = safe_float(change24)

        funding = safe_float(
            info.get("fundingRate"),
            safe_float(ticker.get("fundingRate"))
        )

        open_interest = safe_float(
            info.get("openInterest")
        )

        # ----------------------------------------------------
        # SNAPSHOT RECORD
        # ----------------------------------------------------

        return {
            "symbol": symbol,
            "snapshot_at": snapshot_at,

            "price": price,
            "max_leverage": max_leverage,

            "r5": r5,
            "r15": r15,
            "r1h": r1h,
            "r24h": r24h,
            "change24": change24,

            "turnover24h": turnover24h,
            "funding": funding,
            "open_interest": open_interest,
        }

    except Exception as exc:
        log.warning(
            "[OHLCV] %s fetch failed: %s",
            symbol,
            exc
        )
        return None


async def run_bybit_scan(
    exchange,
    semaphore: asyncio.Semaphore,
):
    """
    Teljes Bybit linear market scan.
    """

    # Load markets once; reload only if not available.
    if not exchange.markets:
        await exchange.load_markets()

    markets = exchange.markets

    log.info("[SCAN] Fetching Bybit linear tickers...")

    tickers = await exchange.fetch_tickers(
        params={"category": "linear"}
    )

    snapshot_at = datetime.now(UTC)

    candidates = []

    # --------------------------------------------------------
    # MARKET FILTERING
    # --------------------------------------------------------

    for symbol, market in markets.items():

        if not is_usdt_linear_swap(market):
            continue

        ticker = tickers.get(symbol)

        if not ticker:
            continue

        candidates.append(
            (symbol, market, ticker)
        )

    log.info(
        "[SCAN] Linear USDT markets found: %s",
        len(candidates)
    )

    # --------------------------------------------------------
    # PARALLEL OHLCV FETCH
    # --------------------------------------------------------

    tasks = [
        fetch_symbol_snapshot(
            exchange=exchange,
            symbol=symbol,
            market=market,
            ticker=ticker,
            snapshot_at=snapshot_at,
            semaphore=semaphore,
        )
        for symbol, market, ticker in candidates
    ]

    results = await asyncio.gather(*tasks)

    records = [
        record
        for record in results
        if record is not None
    ]

    log.info(
        "[SCAN] Snapshot candidates: %s",
        len(records)
    )

    return records


# ============================================================
# 5. DLT / POSTGRESQL
# ============================================================

def create_dlt_pipeline():

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dlt.destinations.postgres(
            credentials=DB_CONFIG
        ),
        dataset_name=DATASET_NAME,
    )

    log.info(
        "[DLT] Pipeline initialized: %s.%s",
        DATASET_NAME,
        TABLE_NAME
    )

    return pipeline


async def save_market_snapshots(
    pipeline,
    records: list,
):
    """
    Market snapshots mentése PostgreSQL-be DLT segítségével.
    """

    if not records:
        log.info("[DLT] No records to save.")
        return

    try:
        load_info = await asyncio.to_thread(
            pipeline.run,
            records,
            table_name=TABLE_NAME,
            write_disposition="merge",
            primary_key=PRIMARY_KEY,
        )

        log.info(
            "[DLT] Saved %s market snapshots.",
            len(records)
        )

        log.debug("[DLT] Load info: %s", load_info)

    except Exception:
        log.exception("[DLT] PostgreSQL write failed.")
        raise


# ============================================================
# 6. SCANNER LOOP
# ============================================================

async def scanner_loop(
    exchange,
    pipeline,
):
    """
    Folyamatos scanner.
    Minden futás után a következő 5 perces időhatárig vár.
    """

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT_REQUESTS
    )

    log.info(
        "[START] Bybit scanner started. Interval: %s seconds.",
        POLL_INTERVAL
    )

    while True:

        try:
            start_time = time.time()

            log.info("=" * 60)
            log.info("[SCAN] Starting market scan...")

            records = await run_bybit_scan(
                exchange,
                semaphore
            )

            if records:
                await save_market_snapshots(
                    pipeline,
                    records
                )
            else:
                log.info(
                    "[SCAN] No market snapshots matched the filters."
                )

            elapsed = time.time() - start_time

            log.info(
                "[SCAN] Completed in %.2f seconds. Records: %s",
                elapsed,
                len(records)
            )

        except asyncio.CancelledError:
            log.info("[STOP] Scanner cancellation received.")
            raise

        except Exception:
            log.exception("[SCAN] Scanner cycle failed.")

        # Wait until the next 5-minute wall-clock boundary.
        # A slow scan skips missed intervals instead of overlapping.
        delay = POLL_INTERVAL - (
            time.time() % POLL_INTERVAL
        )

        log.info(
            "[WAIT] Next scan in %.1f seconds.",
            delay
        )

        await asyncio.sleep(delay)


# ============================================================
# 7. MAIN
# ============================================================

async def main():

    exchange = ccxt.bybit({
        "enableRateLimit": True,
        "options": {
            "defaultType": "linear",
        },
    })

    pipeline = create_dlt_pipeline()

    try:
        await scanner_loop(
            exchange,
            pipeline
        )

    finally:
        log.info("[STOP] Closing Bybit connection...")

        await exchange.close()

        log.info("[STOP] Bybit connection closed.")


# ============================================================
# 8. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        log.info("[STOP] Scanner stopped by user.")
