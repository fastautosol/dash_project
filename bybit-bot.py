# 2026.11.20  18.00
import asyncio
import math
import time
import logging
import os
from datetime import datetime, UTC, timedelta
import ccxt.async_support as ccxt
import dlt
from bybit_silver import run_bybit_silver_transformation

os.environ.setdefault("LOAD__DELETE_COMPLETED_JOBS", "true")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger("bybit_bot")

# =========================================================
# 1. CONFIG
# =========================================================
DB_HOST = os.getenv("DB_HOST", "postgresql")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "n8n")
DB_USER = os.getenv("DB_USER", "sql_admin")
DB_PASS = os.getenv("DB_PASSWORD", "sql_pass")
DB_URL  = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

MIN_LEVERAGE       = 25.0
MIN_TURNOVER       = 1_000_000.0  # USDT / 24h
POLL_INTERVAL      = 300
MARKET_REFRESH_SEC = 3600
CLEANUP_HOURS      = 240
T_TICKER           = "bybit_tickers"


def _f(value, default=None):
    try:
        if value is None:
            return default
        x = float(value)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


# =========================================================
# 2. PIACI UNIVERZUM SZŰRÉSE (>= 25x USDT PERPETUAL)
# =========================================================
async def fetch_eligible_markets(exchange) -> dict[str, dict]:
    markets = await exchange.load_markets(reload=True)
    eligible = {}

    for ccxt_sym, m in markets.items():
        if (not m or not m.get("swap") or m.get("quote") != "USDT" or m.get("settle") != "USDT" or m.get("active") is False):
            continue

        info = m.get("info") or {}
        lev = _f((info.get("leverageFilter") or {}).get("maxLeverage"), 0.0)
        if lev < MIN_LEVERAGE:
            continue

        eligible[ccxt_sym] = {"symbol": f"{m['base']}/{m['quote']}", "max_leverage": lev}

    return eligible


# =========================================================
# 3. EGY CIKLUS: összes linear ticker lekérése és szűrése
# =========================================================
async def run_cycle(exchange, pipeline, eligible_markets: dict[str, dict]) -> int:
    step_ms = POLL_INTERVAL * 1000
    now_ms = int(time.time() * 1000)
    target_ms = (now_ms // step_ms) * step_ms
    ts = _dt(target_ms)

    tickers = await exchange.fetch_tickers(params={"category": "linear"})

    recs = []
    for ccxt_sym, meta in eligible_markets.items():
        tk = tickers.get(ccxt_sym)
        if not tk:
            continue

        info = tk.get("info") or {}
        turnover = _f(info.get("turnover24h"), 0.0)
        if turnover < MIN_TURNOVER:
            continue

        funding = _f(info.get("fundingRate"))
        if funding is None:
            funding = _f(info.get("lastFundingRate"))

        recs.append({
            "symbol":         meta["symbol"],
            "timestamp":      ts,
            "max_leverage":   meta["max_leverage"],
            "close":          _f(tk.get("last")),
            "mark_price":     _f(info.get("markPrice")),
            "index_price":    _f(info.get("indexPrice")),
            "high_24h":       _f(info.get("highPrice24h")),
            "low_24h":        _f(info.get("lowPrice24h")),
            "bid":            _f(info.get("bid1Price")),
            "ask":            _f(info.get("ask1Price")),
            "bid1_size":      _f(info.get("bid1Size")),
            "ask1_size":      _f(info.get("ask1Size")),
            "vwap":           _f(tk.get("vwap")),
            "turnover24h":    turnover,
            "prev_price_1h":  _f(info.get("prevPrice1h")),
            "price_24h_pcnt": _f(info.get("price24hPcnt")),
            "funding":        funding,
            "oi":             _f(info.get("openInterest")),
            "oi_value":       _f(info.get("openInterestValue")),
        })

    if recs:
        await asyncio.to_thread(pipeline.run, recs, table_name=T_TICKER, write_disposition="merge", primary_key=["symbol", "timestamp"])
    return len(recs)


# =========================================================
# 4. CLEANUP & INDEX
# =========================================================
async def cleanup(pipeline) -> None:
    threshold = datetime.now(UTC) - timedelta(hours=CLEANUP_HOURS)

    def _c():
        with pipeline.sql_client() as client:
            tname = client.make_qualified_table_name(T_TICKER)
            client.execute_sql(f"CREATE INDEX IF NOT EXISTS idx_bronze_tickers_ts ON {tname} (timestamp);")
            client.execute_sql(f"DELETE FROM {tname} WHERE timestamp < %s", (threshold,))

    try:
        await asyncio.to_thread(_c)
        log.info("[CLEANUP] %dh-nál régebbi adatok törölve", CLEANUP_HOURS)
    except Exception:
        log.exception("[CLEANUP] failed")


# =========================================================
# 5. MAIN
# =========================================================
async def main() -> None:
    exchange = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})
    pipeline = dlt.pipeline(pipeline_name="crypto_candles_bybit", destination=dlt.destinations.postgres(credentials=DB_URL), dataset_name="bronze")
    pipeline.abort_packages()

    eligible_markets: dict[str, dict] = {}
    last_market_refresh = 0.0
    last_cleanup = 0.0

    try:
        while True:
            try:
                print()
                log.info("=" * 45)

                if not eligible_markets or (time.time() - last_market_refresh >= MARKET_REFRESH_SEC):
                    try:
                        eligible_markets = await fetch_eligible_markets(exchange)
                        last_market_refresh = time.time()
                        log.info("[MARKETS] Eligible >=%.0fx USDT swaps: %d", MIN_LEVERAGE, len(eligible_markets))
                    except Exception:
                        if not eligible_markets:
                            raise
                        log.exception("[MARKETS] Frissítés sikertelen, előző lista marad aktív")

                n = await run_cycle(exchange, pipeline, eligible_markets)
                log.info("[CYCLE START] saved_tickers=%d (eligible=%d)", n, len(eligible_markets))
                await asyncio.to_thread(run_bybit_silver_transformation)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("[CYCLE] failed")

            if time.time() - last_cleanup >= 3600:
                await cleanup(pipeline)
                last_cleanup = time.time()

            delay = POLL_INTERVAL - (time.time() % POLL_INTERVAL)
            log.info("[WAIT] Next cycle in %.1f seconds.", delay)
            await asyncio.sleep(delay)
    finally:
        await exchange.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("[STOP] Bot stopped by user.")
