# 2026.09.16  10.00
import asyncio
import logging
import time
from datetime import datetime, UTC

import httpx
import pandas as pd
import pandas_ta_classic as ta
import dlt
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# =========================
# CONFIGURATION
# =========================
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
WEBHOOK_URL = "https://n8n.fastautosol.com/webhook/crypto-alerts"  # adjust if you want a dedicated n8n endpoint for EMA signals

POLL_INTERVAL = 150  

SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "SUI/USDT", "HYPE/USDT", "LTC/USDT", "ETC/USDT", "COMP/USDT",
    "AVAX/USDT", "AXS/USDT", "LINK/USDT", "BCH/USDT", "TIA/USDT", "ZEN/USDT", "NEAR/USDT", "AAVE/USDT", "ICP/USDT",
]

EMA_FAST = 50
EMA_SLOW = 100
CANDLE_LOOKBACK = 300           # 5m bars pulled per symbol (~25h) — plenty for EMA100 to settle
VWAP_MAX_DIST_PCT = 3.0         # only enter within X% of session VWAP — avoids chasing an extended move
MIN_TURNOVER_24H = 3_000_000    # liquidity floor (USDT) — filters out thin books
MIN_PRICE_CHANGE_PCT = 1.0      # below this the 24h move is noise, skip
MAX_PRICE_CHANGE_PCT = 15.0     # above this it's an extended/overheated move, not a fresh trend entry

engine = create_engine(DB_URL)
http_client: httpx.AsyncClient | None = None
pipeline = None
webhook_semaphore = asyncio.Semaphore(3)

# Edge-trigger state per symbol — {"position": "IN"/"OUT", "oi": last_oi}.
# Resets on restart (in-memory only), same limitation as crypto-bot.py's previous_state.
previous_state: dict[str, dict] = {}

# =========================
# DATA — pull candles already collected by candles-bot.py
# =========================
def fetch_symbol_df(symbol: str) -> pd.DataFrame | None:
    sql = text("""
        SELECT timestamp, open, high, low, close, volume,  vwap, turnover24h, price24hpcnt, funding, oi
        FROM bybit_data.bybit_candles WHERE symbol = :sym ORDER BY timestamp DESC LIMIT :lookback
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"sym": symbol, "lookback": CANDLE_LOOKBACK})

    if df.empty or len(df) < EMA_SLOW + 5:
        return None  # not enough history yet for a stable EMA100

    df = df.sort_values("timestamp").reset_index(drop=True)
    for col in ["open", "high", "low", "close", "volume", "vwap"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ema_fast"] = ta.ema(df["close"], length=EMA_FAST)
    df["ema_slow"] = ta.ema(df["close"], length=EMA_SLOW)

    return df

# =========================
# SIGNAL EVALUATION
# =========================
def evaluate_symbol(symbol: str, df: pd.DataFrame) -> dict | None:
    last = df.iloc[-1]

    if pd.isna(last["ema_fast"]) or pd.isna(last["ema_slow"]) or pd.isna(last["vwap"]):
        return None

    close    = float(last["close"])
    ema_fast = float(last["ema_fast"])
    ema_slow = float(last["ema_slow"])
    vwap     = float(last["vwap"])
    turnover = float(last["turnover24h"] or 0)
    pcnt     = float(last["price24hpcnt"] or 0) * 100
    funding  = float(last["funding"] or 0)
    oi       = float(last["oi"] or 0)

    vwap_dist_pct = abs(close - vwap) / vwap * 100 if vwap else 999.0
    trend_up      = close > ema_fast > ema_slow and close > vwap
    trend_break   = close < ema_fast

    liquidity_ok = turnover >= MIN_TURNOVER_24H
    range_ok     = MIN_PRICE_CHANGE_PCT <= abs(pcnt) <= MAX_PRICE_CHANGE_PCT
    vwap_ok      = vwap_dist_pct <= VWAP_MAX_DIST_PCT

    prev = previous_state.get(symbol, {"position": "OUT", "oi": oi})
    oi_rising = oi > prev.get("oi", oi)

    signal = None

    # ---- ENTRY (fires once on the OUT -> IN transition, not every poll) ----
    if prev["position"] == "OUT" and trend_up and liquidity_ok and range_ok and vwap_ok:
        signal = {
            "signal_type": "ENTRY",
            "reason": (f"close {close:.4f} > EMA{EMA_FAST} {ema_fast:.4f} > EMA{EMA_SLOW} {ema_slow:.4f}, "
                       f"VWAP dist {vwap_dist_pct:.2f}%, 24h {pcnt:.2f}%, turnover {turnover:,.0f}"),
            "oi_confirms": oi_rising,  # True = fresh capital entering with the move, not just short-covering
        }
        previous_state[symbol] = {"position": "IN", "oi": oi}

    # ---- EXIT (fires once on the IN -> OUT transition) ----
    elif prev["position"] == "IN" and trend_break:
        signal = {
            "signal_type": "EXIT",
            "reason": f"close {close:.4f} broke below EMA{EMA_FAST} {ema_fast:.4f}",
            "oi_confirms": not oi_rising}
        previous_state[symbol] = {"position": "OUT", "oi": oi}

    else:
        # No transition — just refresh the OI baseline for next poll's comparison.
        previous_state[symbol] = {"position": prev["position"], "oi": oi}

    if signal is None:
        return None

    signal.update({
        "symbol":          symbol,
        "timestamp":       datetime.now(UTC),
        "close":           close,
        "ema_fast":        round(ema_fast, 6),
        "ema_slow":        round(ema_slow, 6),
        "vwap":            round(vwap, 6),
        "vwap_dist_pct":   round(vwap_dist_pct, 3),
        "turnover24h":     round(turnover, 0),
        "price24hpcnt":    round(pcnt, 3),
        "funding":         funding,
        "oi":              oi,
    })
    return signal

# =========================
# WEBHOOK  
# =========================
async def send_webhook(payload: dict):
    async with webhook_semaphore:
        try:
            webhook_payload = {**payload, "timestamp": payload["timestamp"].isoformat()}
            resp = await http_client.post(WEBHOOK_URL, json=webhook_payload)
            if resp.status_code == 200:
                log.info(f"[WEBHOOK] {payload['signal_type']} sent for {payload['symbol']}")
            else:
                log.error(f"[WEBHOOK ERROR] {payload['symbol']} status {resp.status_code}")
        except Exception as e:
            log.error(f"[WEBHOOK FAILED] {payload['symbol']}: {e}")

# =========================
# MAIN LOOP  (structure reused from crypto-bot.py)
# =========================
async def check_all_symbols():
    db_records = []
    for symbol in SYMBOLS:
        df = await asyncio.to_thread(fetch_symbol_df, symbol)
        if df is None:
            continue
        signal = evaluate_symbol(symbol, df)
        if signal:
            asyncio.create_task(send_webhook(signal))
            db_records.append(signal)

    if db_records:
        try:
            await asyncio.to_thread(pipeline.run, db_records, table_name="bybit_ema_signals", write_disposition="append")
            log.info(f"[POSTGRES] Logged {len(db_records)} EMA signal(s)")
        except Exception as e:
            log.error(f"[POSTGRES ERROR] {e}")

async def main():
    global http_client, pipeline
    http_client = httpx.AsyncClient(timeout=10.0)
    pipeline = dlt.pipeline(pipeline_name="crypto_ema_signals", destination=dlt.destinations.postgres(credentials=DB_URL), dataset_name="bybit_data")
    pipeline.abort_packages()

    try:
        log.info(f"EMA signal bot activated. Checking {len(SYMBOLS)} symbols every {POLL_INTERVAL}s.")
        while True:
            await check_all_symbols()
            await asyncio.sleep(POLL_INTERVAL)
    except asyncio.CancelledError:
        log.info("Shutdown requested.")
    finally:
        await http_client.aclose()
        log.info("Connections closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
