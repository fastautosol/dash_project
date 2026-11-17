# 2026.11.17  12.00
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
    # =========================================================
    # 1. A TE 15-ÖS SPOT/FUTURES PORTFÓLIÓD (S-TIER, Mindig aktív)
    # =========================================================
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT",
    "ADA/USDT", "AVAX/USDT", "LINK/USDT", "DOT/USDT", "NEAR/USDT",
    "SUI/USDT", "AAVE/USDT", "RENDER/USDT", "INJ/USDT", "TAO/USDT",

    # =========================================================
    # 2. A BEMÁSOLT ÉS BIZONYÍTOTTAN ÉLES KORÁBBI CRYPTO TICKEREK (85 db)
    # =========================================================
    "HYPE/USDT", "PURR/USDT", "ENS/USDT", "ICP/USDT", "CAKE/USDT",
    "ZRO/USDT", "ATOM/USDT", "LDO/USDT", "HNT/USDT", "CRV/USDT",
    "SNX/USDT", "DYDX/USDT", "DOGE/USDT", "XPL/USDT", "CRO/USDT",
    "1000LUNC/USDT", "PHA/USDT", "KAIA/USDT", "PNUT/USDT", "XDC/USDT",
    "FIDA/USDT", "RARE/USDT", "BICO/USDT", "COTI/USDT", "FLR/USDT",
    "ATH/USDT", "1000PEPE/USDT", "BRETT/USDT", "JASMY/USDT", "GALA/USDT",
    "NOT/USDT", "HMSTR/USDT", "BOME/USDT", "TLM/USDT", "ZIL/USDT",
    "1000BONK/USDT", "SHIB1000/USDT", "BEAM/USDT", "ALT/USDT", "VET/USDT",
    "ZK/USDT", "W/USDT", "FLOW/USDT", "GRT/USDT", "1000FLOKI/USDT",
    "KAS/USDT", "KAVA/USDT", "POPCAT/USDT", "STRK/USDT", "SAND/USDT",
    "SEI/USDT", "PYTH/USDT", "HBAR/USDT", "APE/USDT", "POL/USDT",
    "ALGO/USDT", "OP/USDT", "IMX/USDT", "ENA/USDT", "EIGEN/USDT",
    "ARB/USDT", "THETA/USDT", "WIF/USDT", "XLM/USDT", "XTZ/USDT",
    "JUP/USDT", "WLD/USDT", "STX/USDT", "JTO/USDT", "ONDO/USDT",
    "TIA/USDT", "MNT/USDT", "VIRTUAL/USDT", "ETHFI/USDT", "RUNE/USDT",
    "FIL/USDT", "AXS/USDT", "LTC/USDT", "BCH/USDT", "XMR/USDT",
    "ZEC/USDT", "DASH/USDT", "COMP/USDT", "TRX/USDT", "GRAM/USDT",

    # =========================================================
    # 3. ÚJ, EXTRA 50 DB JÓL TRADED, NAGY VOLUMENŰ CRYPTO (Szigorúan a kapott logodból!)
    # =========================================================
    # --- Top volumenű mémek és új AI/DePIN slágerek ---
    "ORCA/USDT", "MORPHO/USDT", "LITE/USDT", "LIT/USDT", "QNT/USDT", "FLUID/USDT",
    "PENDLE/USDT", "ASTER/USDT", "AERO/USDT", "SPX/USDT", "LSK/USDT", "NBIS/USDT"
    "PUMPFUN/USDT",       # Óriási forgalmú mém Launchpad perp
    "FARTCOIN/USDT",      # Meme
    "MOODENG/USDT",       # Viral Meme
    "GOAT/USDT",          # AI Meme
    "USUAL/USDT",         # RWA/Stable yield perp
    "BIO/USDT",       # Bio-Science AI / DePIN
    "0G/USDT",            # Web3 modular AI
    "SOPH/USDT",          # Sophia AI DePIN
    "PIEVERSE/USDT",      # Pieverse
    "DGAI/USDT",          # Game AI
    "KORU/USDT",          # Koru
    "ARIA/USDT",          # Aria AI
    "KSTR/USDT",          # Star DePIN
    "HUMA/USDT",          # Huma RWA (Kifejezetten nagy napi turnover!)
    "COOKIE/USDT",        # Cookie AI
    "ZK/USDT",            # ZKsync
    "W/USDT",             # Wormhole
    "ME/USDT",            # Magic Eden Token
    "POPCAT/USDT",        # Popcat

    # --- További éles határidős alts & láncok a 311-es listádból (likviditás és volumen alapon) ---
    "DATA/USDT",          # Streamr
    "RIVER/USDT",         # River Network
    "O/USDT",             # O Network
    "DUSK/USDT",          # Dusk Network
    "BASED/USDT",         # Based meme / DeFi
    "SENT/USDT",          # Sentinel
    "ORDER/USDT",         # Orderly Network
    "FIDA/USDT",          # Bonfida
    "RESOLV/USDT",        # Resolv
    "PORTAL/USDT",        # Portal Web3 Game
    "MON/USDT",           # Mon Protocol
    "CYBER/USDT",         # CyberConnect
    "JTO/USDT",           # Jito
    "JELLYJELLY/USDT",    # Jelly perp
    "PORTAL/USDT",        # Ismételt biztos port
    "FLOW/USDT",          # Flow
    "GRT/USDT",           # The Graph
    "XPL/USDT",           # XPLA
    "FIL/USDT",           # Filecoin
    "APT/USDT",           # Aptos
    "STRK/USDT",          # Starknet
    "FIL/USDT",           # Filecoin
    "QTUM/USDT",          # Qtum
    "BAT/USDT",           # Basic Attention
    "ZIL/USDT",           # Zilliqa
    "SUSHI/USDT",         # SushiSwap
    "ETC/USDT",           # 1inch
    "MASK/USDT",          # Mask Network
    "ZK/USDT"             # ZKsync ismét
]


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
                print()
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
