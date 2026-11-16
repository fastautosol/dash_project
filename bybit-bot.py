# 2026.11.16  19.00
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

CRYPTO_SYMBOLS = [
    # --- TOP S Tier ---
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT", "DOT/USDT", "NEAR/USDT", "SUI/USDT", "AAVE/USDT", "RENDER/USDT", "INJ/USDT", "TAO/USDT",

    # --- TOP Alts & Layer-1s & Layer-2s (High Cap, TOP 50) ---
    "DOGE/USDT", "SHIB/USDT", "PEPE/USDT", "WIF/USDT", "BONK/USDT", "FLOKI/USDT", "TON/USDT", "TRX/USDT", "LTC/USDT", "BCH/USDT",
    "LINK/USDT", "UNI/USDT", "LEO/USDT", "ICP/USDT", "APT/USDT", "FET/USDT", "ETC/USDT", "FIL/USDT", "KAS/USDT", "OM/USDT",
    "SEI/USDT", "POL/USDT", "OP/USDT", "ARB/USDT", "GRT/USDT", "AAVE/USDT", "NEAR/USDT", "RNDR/USDT", "VET/USDT", "JUP/USDT",
    "PYTH/USDT", "TIA/USDT", "RUNE/USDT", "STX/USDT", "LDO/USDT", "IMX/USDT", "FTM/USDT", "MNT/USDT", "ATOM/USDT", "THETA/USDT",

    # --- Top 50 - 100 + Bybit kedvencek (Mid-Cap, Nagy volumenűek) ---
    "ALGO/USDT", "FLOW/USDT", "EGLD/USDT", "SAND/USDT", "MANA/USDT", "AXS/USDT", "APE/USDT", "GALA/USDT", "CRV/USDT", "LRC/USDT",
    "ENJ/USDT", "ANKR/USDT", "MINA/USDT", "HBAR/USDT", "QNT/USDT", "CHZ/USDT", "FTT/USDT", "ZEC/USDT", "DASH/USDT", "XMR/USDT",
    "XLM/USDT", "WAVES/USDT", "ROSE/USDT", "MKR/USDT", "COMP/USDT", "LDO/USDT", "RPL/USDT", "FXS/USDT", "DYDX/USDT", "PNDR/USDT",
    "KAVA/USDT", "WOO/USDT", "GMT/USDT", "JASMY/USDT", "IOTA/USDT", "HOT/USDT", "ONE/USDT", "XEM/USDT", "CELO/USDT", "QTUM/USDT",
    "BAT/USDT", "ONT/USDT", "ZIL/USDT", "OMG/USDT", "YFI/USDT", "SUSHI/USDT", "1INCH/USDT", "REI/USDT", "MASK/USDT", "LINA/USDT",

    # --- Trendi & Új, nagy tőkeáramlású coinok (Perps) ---
    "ENA/USDT", "HYPE/USDT", "ONDO/USDT", "W/USDT", "ZRO/USDT", "POPCAT/USDT", "BRETT/USDT", "ME/USDT", "IO/USDT", "ZETA/USDT",
    "BLAST/USDT", "ATH/USDT", "STRK/USDT", "AEVO/USDT", "STG/USDT", "NOT/USDT", "VIRTUAL/USDT", "AIXBT/USDT", "GOAT/USDT", "MOODENG/USDT"]

CRYPTO_SYMBOLS = list(dict.fromkeys(s.strip() for s in CRYPTO_SYMBOLS if s.strip()))

POLL_INTERVAL = 300
CLEANUP_HOURS = 120      
T_TICKER      = "bybit_tickers"

def to_linear(symbol: str) -> str:
    return f"{symbol}:USDT"

LINEAR_SYMBOLS = [to_linear(s) for s in CRYPTO_SYMBOLS]

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
# 2. EGY CIKLUS: egyetlen fetch_tickers hívás
# =========================================================
async def run_cycle(exchange, pipeline) -> int:
    step_ms = POLL_INTERVAL * 1000
    now_ms = int(time.time() * 1000)
    target_ms = (now_ms // step_ms) * step_ms
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
# 3. CLEANUP & INDEX
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
# 4. MAIN
# =========================================================
async def main() -> None:
    exchange = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})
    pipeline = dlt.pipeline(pipeline_name="crypto_candles_bybit", destination=dlt.destinations.postgres(credentials=DB_URL), dataset_name="bronze")
    pipeline.abort_packages()

    last_cleanup = 0.0
    try:
        while True:
            try:
                log.info("=" * 45)
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
    finally:
        await exchange.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("[STOP] Bot stopped by user.")
