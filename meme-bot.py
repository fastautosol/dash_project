# 2026.08.19  18.00

import asyncio, aiohttp, time
from datetime import datetime, UTC, timedelta
import dlt

# ---------------- CONFIG ----------------
N8N_WEBHOOK_URL = "https://n8n.fastautosol.com/webhook/meme-alert"
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
POLL_INTERVAL = 90                # main loop cadence (s)
DISCOVERY_INTERVAL = 300          # discover new tokens every 3 min
WATCHLIST_EXPIRY_MINS = 30        # max time a token stays on watchlist
CLEANUP_HOURS = 12                # delete DB rows older than this
MAX_DISCOVERY_TOKENS = 25         # tokens processed per discovery cycle
MAX_TOKENS_PER_REQUEST = 30       # DexScreener multi-token limit

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20, connect=10, sock_read=15)
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/151.0 Safari/537.36")

DEXSCREENER_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_TOKENS_URL = "https://api.dexscreener.com/tokens/v1/solana/{addresses}"
RUGCHECK_SUMMARY_URL = "https://api.rugcheck.xyz/v1/tokens/{token_address}/report/summary"
DANGEROUS_KEYWORDS = ["large shareholder", "large holder", "freeze authority", "mint authority", "honeypot", "blacklist", "transfer hook"]

# ---------------- STATE ----------------
class MemeState:
    def __init__(self):
        self.watchlist = {}
        self.last_cleanup_time = 0
        self.last_new_tokens_fetch = 0

state = MemeState()


# ---------------- HTTP HELPER ----------------
async def get_json(session, url, *, retries=2):
    """GET JSON with retry/backoff/429 handling. Returns dict/list or None."""
    for attempt in range(1, retries + 2):
        try:
            async with session.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}) as resp:
                if resp.status == 429:
                    try:
                        sleep_s = float(resp.headers.get("Retry-After"))
                    except (TypeError, ValueError):
                        sleep_s = 3.0
                    print(f"[HTTP] 429 rate limit: {url} sleeping {sleep_s:.1f}s")
                    await asyncio.sleep(sleep_s)
                    continue
                if resp.status != 200:
                    body = await resp.text()
                    print(f"[HTTP] ERROR {resp.status}: {url}\n        {body[:300]}")
                    if attempt <= retries:
                        await asyncio.sleep(1.5 * attempt)
                        continue
                    return None
                return await resp.json()
                
        except asyncio.TimeoutError:
            print(f"[HTTP] Timeout attempt {attempt}: {url}")
            if attempt <= retries:
                await asyncio.sleep(1.5 * attempt)
        except aiohttp.ClientError as e:
            print(f"[HTTP] Client error attempt {attempt}: {url} -> {e}")
            if attempt <= retries:
                await asyncio.sleep(1.5 * attempt)
        except Exception as e:
            print(f"[HTTP] Unexpected error attempt {attempt}: {url} -> {repr(e)}")
            if attempt <= retries:
                await asyncio.sleep(1.5 * attempt)
    return None


# ---------------- RUGCHECK ----------------
async def rugcheck_token(session, token_address):
    """Get RugCheck security summary, or None on failure."""
    return await get_json(session, RUGCHECK_SUMMARY_URL.format(token_address=token_address), retries=1)

def token_passes_rugcheck(rug_data):
    """Conservative screening filter. NOT a safety guarantee."""
    if not rug_data:
        return False

    if rug_data.get("mintAuthority"):
        print("[RUGCHECK] Reject: mint authority still enabled")
        return False
    if rug_data.get("freezeAuthority"):
        print("[RUGCHECK] Reject: freeze authority still enabled")
        return False

    for risk in rug_data.get("risks", []) or []:
        name, level = str(risk.get("name", "")).lower(), str(risk.get("level", "")).lower()
        if any(kw in name for kw in DANGEROUS_KEYWORDS):
            print(f"[RUGCHECK] Reject risk: {risk.get('name')}")
            return False
        if level in ("danger", "critical", "error"):
            print(f"[RUGCHECK] Reject risk level: {level} / {risk.get('name')}")
            return False

    return True


# ---------------- TOKEN DISCOVERY ----------------
async def discover_and_filter_tokens(session):
    """Fetch latest Solana token profiles, RugCheck each, add approved ones to watchlist."""
    print("[DISCOVERY] Fetching latest token profiles...")
    profiles = await get_json(session, DEXSCREENER_PROFILES_URL, retries=2)

    if not profiles or not isinstance(profiles, list):
        print(f"[DISCOVERY] No usable profiles received (type={type(profiles)})")
        return

    discovered = approved = 0

    for profile in profiles[:MAX_DISCOVERY_TOKENS]:
        if not isinstance(profile, dict):
            continue

        token_address = profile.get("tokenAddress")
        if profile.get("chainId") != "solana" or not token_address or token_address in state.watchlist:
            continue

        discovered += 1
        print(f"[DISCOVERY] Checking {token_address}")
        await asyncio.sleep(0.25)  # be gentle with RugCheck

        rug_data = await rugcheck_token(session, token_address)
        if rug_data is None:
            print(f"[DISCOVERY] RugCheck unavailable: {token_address}")
            continue
        if not token_passes_rugcheck(rug_data):
            print(f"[DISCOVERY] REJECTED: {token_address}")
            continue

        # Symbol isn't guaranteed in the token-profile endpoint; fill in later.
        state.watchlist[token_address] = {"base_price": None, "base_volume": None,
                                           "added_at": time.time(), "symbol": "MEME"}
        approved += 1
        print(f"[DISCOVERY] APPROVED: {token_address}")

    print(f"[DISCOVERY] Finished | discovered={discovered} | approved={approved} | watchlist={len(state.watchlist)}")


# ---------------- DLT PIPELINE ----------------
def create_dlt_pipeline():
    print("[DLT] Creating pipeline...")
    pipeline = dlt.pipeline(pipeline_name="meme_pump_strategy",
                             destination=dlt.destinations.postgres(credentials=DB_URL),
                             dataset_name="meme_data")
    print(f"[DLT] Pipeline created | dataset={pipeline.dataset_name}")
    return pipeline


def ensure_dlt_table(pipeline):
    """Force-create meme_data.meme_watchlist via a bootstrap insert+delete."""
    print("[DLT] Ensuring meme_watchlist table exists...")
    bootstrap = {"token_address": "__DLT_BOOTSTRAP__", "symbol": "__BOOTSTRAP__",
                 "timestamp": datetime.now(UTC), "price": 0.0, "volume_5m": 0.0,
                 "price_change_from_base": 0.0, "vol_growth_factor": 0.0, "liquidity_usd": 0.0}
    try:
        load_info = pipeline.run([bootstrap], table_name="meme_watchlist", write_disposition="append")
        print(f"[DLT] Bootstrap load successful\n[DLT] {load_info}")
    except Exception as e:
        print(f"[DLT] BOOTSTRAP ERROR\n{repr(e)}")
        raise

    try:
        with pipeline.sql_client() as client:
            table = client.make_qualified_table_name("meme_watchlist")
            client.execute_sql(f"DELETE FROM {table} WHERE token_address = %s", "__DLT_BOOTSTRAP__")
        print("[DLT] Bootstrap row deleted")
    except Exception as e:
        print(f"[DLT] WARNING: Could not delete bootstrap row: {repr(e)}")

    print("[DLT] meme_data.meme_watchlist is ready")


def save_records_with_dlt(pipeline, records):
    """Synchronous DLT load, run via asyncio.to_thread()."""
    if not records:
        print("[DLT] No records to load")
        return None
    print(f"[DLT] Loading {len(records)} records...")
    load_info = pipeline.run(records, table_name="meme_watchlist", write_disposition="append")
    print(f"[DLT] Load finished\n[DLT] {load_info}")
    return load_info


# ---------------- DEXSCREENER MARKET DATA ----------------
async def fetch_token_market_data(session, token_addresses):
    """Fetch market data for up to 30 Solana token addresses."""
    if not token_addresses:
        return []
    addresses = ",".join(token_addresses[:MAX_TOKENS_PER_REQUEST])
    data = await get_json(session, DEXSCREENER_TOKENS_URL.format(addresses=addresses), retries=2)

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("pairs", []) or []
    return []


def _to_float(value, default=0.0):
    try:
        return float(value or default)
    except (ValueError, TypeError):
        return default


# ---------------- ANALYZE WATCHLIST ----------------
async def analyze_watchlist(session, pipeline):
    if not state.watchlist:
        print("[WATCHLIST] Empty")
        return

    # Drop expired tokens
    now_ts = time.time()
    expired = [addr for addr, d in state.watchlist.items() if now_ts - d["added_at"] > WATCHLIST_EXPIRY_MINS * 60]
    for addr in expired:
        del state.watchlist[addr]
        print(f"[WATCHLIST] Expired: {addr}")
    if not state.watchlist:
        return

    addresses = list(state.watchlist.keys())
    print(f"[MARKET] Querying {len(addresses)} watchlist tokens...")
    pairs = await fetch_token_market_data(session, addresses)
    if not pairs:
        print("[MARKET] No pair data received")
        return

    records_to_db, alerts_to_send = [], []

    for pair in pairs:
        if not isinstance(pair, dict):
            continue

        base_token = pair.get("baseToken", {})
        addr = base_token.get("address")
        if not addr or addr not in state.watchlist:
            continue

        symbol = base_token.get("symbol") or state.watchlist[addr].get("symbol", "MEME")
        current_price = _to_float(pair.get("priceUsd"))
        current_vol_5m = _to_float((pair.get("volume") or {}).get("m5"))
        liquidity_usd = _to_float((pair.get("liquidity") or {}).get("usd"))

        if current_price <= 0:
            print(f"[MARKET] No valid price: {symbol} / {addr}")
            continue

        token_state = state.watchlist[addr]

        # Baseline (first observation for this token)
        if token_state["base_price"] is None:
            token_state["base_price"] = current_price
            token_state["base_volume"] = current_vol_5m
            print(f"[BASELINE] {symbol} price={current_price:.10f} volume5m={current_vol_5m:.2f}")

        base_price = float(token_state["base_price"] or current_price)
        base_volume = float(token_state["base_volume"] or 0)

        price_change_pct = ((current_price - base_price) / base_price) * 100 if base_price > 0 else 0.0
        # If initial volume was zero, fall back to a neutral growth factor of 1.0
        volume_growth_factor = (current_vol_5m / base_volume) if base_volume > 0 else 1.0

        timestamp = datetime.now(UTC)
        record = {"token_address": addr, "symbol": symbol, "timestamp": timestamp, "price": current_price,
                  "volume_5m": current_vol_5m, "price_change_from_base": round(price_change_pct, 2),
                  "vol_growth_factor": round(volume_growth_factor, 2), "liquidity_usd": round(liquidity_usd, 2)}
        records_to_db.append(record)

        print(f"[MARKET] {symbol} | price={current_price:.10f} | change={price_change_pct:.2f}% | "
              f"vol5m={current_vol_5m:.2f} | vol_factor={volume_growth_factor:.2f} | liq=${liquidity_usd:,.2f}")

        # Breakout trigger
        if 10.0 <= price_change_pct <= 50.0 and volume_growth_factor >= 2.0:
            print(f"[BREAKOUT] {symbol} {addr} price_change={price_change_pct:.2f}% volume_factor={volume_growth_factor:.2f}")
            state.watchlist.pop(addr, None)  # avoid re-triggering every minute
            alerts_to_send.append({
                **record, "signal": "MEME_PUMP_BREAKOUT",
                # NOTE: placeholder URL — verify real OKX Web3 swap URL format before trading with it.
                "okx_swap_url": f"https://okx.com_{addr}",
                "timestamp": timestamp.isoformat(),
            })

    # Send alerts to n8n
    for alert in alerts_to_send:
        try:
            async with session.post(N8N_WEBHOOK_URL, json=alert, timeout=HTTP_TIMEOUT) as resp:
                if 200 <= resp.status < 300:
                    print(f"[N8N] Alert sent: {alert['symbol']}")
                else:
                    print(f"[N8N] ERROR {resp.status}: {(await resp.text())[:300]}")
        except Exception as e:
            print(f"[N8N] Webhook error: {repr(e)}")

    # Persist to Postgres via DLT
    if records_to_db:
        print(f"[DLT] records_to_db={len(records_to_db)}")
        try:
            await asyncio.to_thread(save_records_with_dlt, pipeline, records_to_db)
        except Exception as e:
            print(f"[DLT] LOAD ERROR\n{repr(e)}")
    else:
        print("[DLT] records_to_db=0")


# ---------------- DATABASE MAINTENANCE ----------------
def cleanup_database(pipeline):
    """Delete DB rows older than CLEANUP_HOURS."""
    threshold = datetime.now(UTC) - timedelta(hours=CLEANUP_HOURS)
    try:
        with pipeline.sql_client() as client:
            table = client.make_qualified_table_name("meme_watchlist")
            print(f"[CLEANUP] Table: {table}")
            client.execute_sql(f"DELETE FROM {table} WHERE timestamp < %s", threshold)
        print("[CLEANUP] Complete")
    except Exception as e:
        print(f"[CLEANUP] ERROR: {repr(e)}")


def test_dlt_database(pipeline):
    """Verify DLT can open its SQL client and resolve the target table."""
    print("[DLT TEST] Testing PostgreSQL connection...")
    try:
        with pipeline.sql_client() as client:
            table = client.make_qualified_table_name("meme_watchlist")
            print(f"[DLT TEST] Qualified table: {table}")
            result = client.execute_sql("SELECT current_database(), current_schema()")
            print(f"[DLT TEST] Database/schema: {result}")
        print("[DLT TEST] PostgreSQL connection OK")
    except Exception as e:
        print(f"[DLT TEST] FAILED\n{repr(e)}")
        raise


# ---------------- MAIN LOOP ----------------
async def main():
    print("=" * 70 + "\nMEME PUMP STRATEGY STARTING\n" + "=" * 70)

    pipeline = create_dlt_pipeline()
    test_dlt_database(pipeline)
    ensure_dlt_table(pipeline)

    try:
        with pipeline.sql_client() as client:
            print(f"[DLT] Target table: {client.make_qualified_table_name('meme_watchlist')}")
    except Exception as e:
        print(f"[DLT] Could not resolve table: {repr(e)}")

    connector = aiohttp.TCPConnector(limit=20, limit_per_host=10, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector, timeout=HTTP_TIMEOUT,
                                      headers={"User-Agent": USER_AGENT, "Accept": "application/json"}) as session:
        print("[MAIN] HTTP session started")

        while True:
            now = time.time()
            try:
                if now - state.last_new_tokens_fetch >= DISCOVERY_INTERVAL:
                    await discover_and_filter_tokens(session)
                    state.last_new_tokens_fetch = now

                await analyze_watchlist(session, pipeline)

                if now - state.last_cleanup_time >= 3600:
                    cleanup_database(pipeline)
                    state.last_cleanup_time = now

            except Exception as e:
                print("=" * 70 + f"\n[MAIN LOOP ERROR]\n{repr(e)}\n" + "=" * 70)

            print(f"[STATUS] watchlist={len(state.watchlist)} | next cycle in {POLL_INTERVAL}s")
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] Stopped by user.")
    except Exception as e:
        print(f"[MAIN] FATAL ERROR:\n{repr(e)}")
