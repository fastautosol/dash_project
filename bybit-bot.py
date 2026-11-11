# 2026.11.11  18.00
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
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"

CRYPTO_SYMBOLS = [
    "AAVE/USDT", "ADA/USDT", "ALGO/USDT", "APT/USDT", "ARB/USDT", "ASTER/USDT", "AVAX/USDT", "AXS/USDT", "BCH/USDT", "BNB/USDT", "BTC/USDT", "COMP/USDT",
    "CRV/USDT", "DASH/USDT", "DOGE/USDT", "DOT/USDT", "EGLD/USDT", "ENA/USDT", "ETC/USDT", "ETH/USDT", "FIL/USDT", "HBAR/USDT", "HYPE/USDT", "ICP/USDT",
    "INJ/USDT", "KAS/USDT", "LINK/USDT", "LIT/USDT", "LTC/USDT", "NEAR/USDT", "ONDO/USDT", "OP/USDT", "QNT/USDT", "PLUME/USDT", "POL/USDT", "PYTH/USDT",
    "RENDER/USDT", "RUNE/USDT", "SAND/USDT", "SOL/USDT", "SEI/USDT", "SUI/USDT", "TIA/USDT", "TAO/USDT", "VIRTUAL/USDT", "ZEC/USDT", "ZEN/USDT", "ZRO/USDT",
    "UNI/USDT", "XPL/USDT", "XLM/USDT", "XMR/USDT", "XRP/USDT", "XTZ/USDT", "W/USDT",
]
CRYPTO_SYMBOLS = list(dict.fromkeys(s.strip() for s in CRYPTO_SYMBOLS if s.strip()))

POLL_INTERVAL = 300      # ciklusidő ÉS gyertyaméret (5m) — ugyanaz a konstans
CLEANUP_HOURS = 72
T_TICKER      = "bybit_tickers"

def to_linear(symbol: str) -> str:
    return f"{symbol}:USDT"

LINEAR_SYMBOLS = [to_linear(s) for s in CRYPTO_SYMBOLS]

def _f(value, default=None):
    """None / NaN / Inf safe float. Hiányzó adatra NULL megy, NEM 0.0."""
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
# 2. EGY CIKLUS: egyetlen fetch_tickers hívás
# =========================================================
async def run_cycle(exchange, pipeline) -> int:
    # a LEZÁRT gyertya timestampje (5 perces rács, epoch-align)
    step_ms = POLL_INTERVAL * 1000
    now_ms = int(time.time() * 1000)
    target_ms = (now_ms // step_ms) * step_ms - step_ms

    tickers = await exchange.fetch_tickers(symbols=LINEAR_SYMBOLS)

    recs = []
    for sym in CRYPTO_SYMBOLS:
        tk = tickers.get(to_linear(sym))
        if not tk:
            continue
        info = tk.get("info", {}) or {}
        funding = _f(info.get("fundingRate"))
        if funding is None:
            funding = _f(info.get("lastFundingRate"))
        recs.append({
            "symbol":         sym,
            "timestamp":      _dt(target_ms),
            "close":          _f(tk.get("last")),        # close-only bar
            "mark_price":     _f(info.get("markPrice")),
            "index_price":    _f(info.get("indexPrice")),
            "bid":            _f(info.get("bid1Price")),
            "ask":            _f(info.get("ask1Price")),
            "vwap":           _f(tk.get("vwap")),
            "turnover24h":    _f(info.get("turnover24h")),
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
# 3. CLEANUP
# =========================================================
async def cleanup(pipeline) -> None:
    threshold = datetime.now(UTC) - timedelta(hours=CLEANUP_HOURS)

    def _c():
        with pipeline.sql_client() as client:
            for t in (T_TICKER, "bybit_ohlcv"):
                try:
                    tname = client.make_qualified_table_name(t)
                    client.execute_sql(f"DELETE FROM {tname} WHERE timestamp < %s", (threshold,))
                except Exception:
                    pass   # az ohlcv tábla lehet, hogy még nem létezik
    try:
        await asyncio.to_thread(_c)
        log.info("[CLEANUP] %dh-nál régebbi adatok törölve", CLEANUP_HOURS)
    except Exception:
        log.exception("[CLEANUP] failed")


# =========================================================
# 4. MAIN — egyszerű loop, a régi delay-mintával
# =========================================================
async def main() -> None:
    exchange = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})
    pipeline = dlt.pipeline(pipeline_name="crypto_candles_bybit", destination=dlt.destinations.postgres(credentials=DB_URL), dataset_name="bronze")
    pipeline.abort_packages()

    last_cleanup = 0.0
    while True:
        try:
            print()
            log.info("=" *45)
            n = await run_cycle(exchange, pipeline)
            log.info("[CYCLE START] tickers=%d", n)
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


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("[STOP] Bot stopped by user.")
