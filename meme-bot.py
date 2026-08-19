# 2026.08.19  18.00
import asyncio
import aiohttp
import time
from datetime import datetime, UTC, timedelta
import dlt

# ============================================================
# CONFIGURATION
# ============================================================
N8N_WEBHOOK_URL = "https://n8n.fastautosol.com/webhook/meme-alert"
DB_URL = "postgresql://sql_admin:sql_pass@postgresql:5432/n8n"
POLL_INTERVAL = 60
DISCOVERY_INTERVAL = 180
WATCHLIST_EXPIRY_MINS = 30
CLEANUP_HOURS = 12
MAX_DISCOVERY_TOKENS = 25
MAX_TOKENS_PER_REQUEST = 30

HTTP_TIMEOUT = aiohttp.ClientTimeout(
    total=20,
    connect=10,
    sock_read=15,
)

USER_AGENT = (
    "Mozilla/5.0 "
    "(Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/151.0 Safari/537.36"
)


# ============================================================
# API ENDPOINTS
# ============================================================
DEXSCREENER_PROFILES_URL = ("https://api.dexscreener.com/token-profiles/latest/v1")
DEXSCREENER_TOKENS_URL = ("https://api.dexscreener.com/tokens/v1/solana/{addresses}")
RUGCHECK_SUMMARY_URL = ("https://api.rugcheck.xyz/v1/tokens/{token_address}/report/summary")

# ============================================================
# APPLICATION STATE
# ============================================================

class MemeState:

    def __init__(self):
        # {
        #   token_address: {
        #       "base_price": float | None,
        #       "base_volume": float | None,
        #       "added_at": float,
        #       "symbol": str
        #   }
        # }
        self.watchlist = {}

        self.last_cleanup_time = 0
        self.last_new_tokens_fetch = 0


state = MemeState()


# ============================================================
# HTTP HELPERS
# ============================================================

async def get_json(session, url, *, retries=2):
    """
    GET JSON with retry handling.
    Returns:
        parsed JSON
        None on failure
    """

    for attempt in range(1, retries + 2):

        try:

            async with session.get(
                url,
                timeout=HTTP_TIMEOUT,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
            ) as resp:

                # ------------------------------------------------
                # Rate limit
                # ------------------------------------------------
                if resp.status == 429:

                    retry_after = resp.headers.get("Retry-After")

                    try:
                        sleep_seconds = float(retry_after)
                    except (TypeError, ValueError):
                        sleep_seconds = 3.0

                    print(
                        f"[HTTP] 429 rate limit: {url} "
                        f"sleeping {sleep_seconds:.1f}s"
                    )

                    await asyncio.sleep(sleep_seconds)

                    continue

                # ------------------------------------------------
                # Other HTTP error
                # ------------------------------------------------
                if resp.status != 200:

                    body = await resp.text()

                    print(
                        f"[HTTP] ERROR {resp.status}: {url}\n"
                        f"        {body[:300]}"
                    )

                    if attempt <= retries:
                        await asyncio.sleep(1.5 * attempt)
                        continue

                    return None

                # ------------------------------------------------
                # JSON
                # ------------------------------------------------
                return await resp.json()

        except asyncio.TimeoutError:

            print(
                f"[HTTP] Timeout attempt {attempt}: {url}"
            )

            if attempt <= retries:
                await asyncio.sleep(1.5 * attempt)

        except aiohttp.ClientError as e:

            print(
                f"[HTTP] Client error attempt {attempt}: "
                f"{url} -> {e}"
            )

            if attempt <= retries:
                await asyncio.sleep(1.5 * attempt)

        except Exception as e:

            print(
                f"[HTTP] Unexpected error attempt {attempt}: "
                f"{url} -> {repr(e)}"
            )

            if attempt <= retries:
                await asyncio.sleep(1.5 * attempt)

    return None


# ============================================================
# RUGCHECK
# ============================================================

async def rugcheck_token(session, token_address):
    """
    Get RugCheck security summary.

    Returns:
        dict | None
    """

    url = RUGCHECK_SUMMARY_URL.format(
        token_address=token_address
    )

    data = await get_json(
        session,
        url,
        retries=1,
    )

    if not data:
        return None

    return data


def token_passes_rugcheck(rug_data):
    """
    Basic security filter.

    This is intentionally conservative.
    RugCheck is a screening layer, NOT a guarantee that
    a token is safe.
    """

    if not rug_data:
        return False

    risks = rug_data.get("risks", []) or []

    # ------------------------------------------------------------
    # Explicit authority checks when available
    # ------------------------------------------------------------

    mint_authority = rug_data.get("mintAuthority")
    freeze_authority = rug_data.get("freezeAuthority")

    if mint_authority:
        print(
            "[RUGCHECK] Reject: mint authority still enabled"
        )
        return False

    if freeze_authority:
        print(
            "[RUGCHECK] Reject: freeze authority still enabled"
        )
        return False

    # ------------------------------------------------------------
    # Risk-name screening
    # ------------------------------------------------------------

    dangerous_keywords = [
        "large shareholder",
        "large holder",
        "freeze authority",
        "mint authority",
        "honeypot",
        "blacklist",
        "transfer hook",
    ]

    for risk in risks:

        risk_name = str(
            risk.get("name", "")
        ).lower()

        risk_level = str(
            risk.get("level", "")
        ).lower()

        if any(
            keyword in risk_name
            for keyword in dangerous_keywords
        ):

            print(
                f"[RUGCHECK] Reject risk: "
                f"{risk.get('name')}"
            )

            return False

        # Reject explicit danger-level risks
        if risk_level in ("danger", "critical", "error"):

            print(
                f"[RUGCHECK] Reject risk level: "
                f"{risk_level} / {risk.get('name')}"
            )

            return False

    return True


# ============================================================
# TOKEN DISCOVERY
# ============================================================

async def discover_and_filter_tokens(session):
    """
    1. Get latest Solana token profiles from DexScreener.
    2. Run RugCheck on each token.
    3. Put approved tokens into watchlist.
    """

    print(
        f"[DISCOVERY] Fetching latest token profiles..."
    )

    profiles = await get_json(
        session,
        DEXSCREENER_PROFILES_URL,
        retries=2,
    )

    if not profiles:

        print(
            "[DISCOVERY] No profiles received."
        )

        return

    if not isinstance(profiles, list):

        print(
            "[DISCOVERY] Unexpected response type:",
            type(profiles)
        )

        return

    discovered = 0
    approved = 0

    # ------------------------------------------------------------
    # Only Solana
    # ------------------------------------------------------------

    for profile in profiles[:MAX_DISCOVERY_TOKENS]:

        if not isinstance(profile, dict):
            continue

        chain_id = profile.get("chainId")
        token_address = profile.get("tokenAddress")

        if chain_id != "solana":
            continue

        if not token_address:
            continue

        if token_address in state.watchlist:
            continue

        discovered += 1

        print(
            f"[DISCOVERY] Checking {token_address}"
        )

        # Small delay between RugCheck requests
        await asyncio.sleep(0.25)

        rug_data = await rugcheck_token(
            session,
            token_address,
        )

        if rug_data is None:

            print(
                f"[DISCOVERY] RugCheck unavailable: "
                f"{token_address}"
            )

            continue

        if not token_passes_rugcheck(rug_data):

            print(
                f"[DISCOVERY] REJECTED: "
                f"{token_address}"
            )

            continue

        # Symbol isn't guaranteed in the token-profile
        # endpoint, so initially use a generic value.
        symbol = "MEME"

        state.watchlist[token_address] = {
            "base_price": None,
            "base_volume": None,
            "added_at": time.time(),
            "symbol": symbol,
        }

        approved += 1

        print(
            f"[DISCOVERY] APPROVED: "
            f"{token_address}"
        )

    print(
        f"[DISCOVERY] Finished | "
        f"discovered={discovered} | "
        f"approved={approved} | "
        f"watchlist={len(state.watchlist)}"
    )


# ============================================================
# DLT PIPELINE
# ============================================================

def create_dlt_pipeline():

    print(
        "[DLT] Creating pipeline..."
    )

    pipeline = dlt.pipeline(
        pipeline_name="meme_pump_strategy",
        destination=dlt.destinations.postgres(
            credentials=DB_URL
        ),
        dataset_name="meme_data",
    )

    print(
        "[DLT] Pipeline created"
    )

    print(
        f"[DLT] Dataset: {pipeline.dataset_name}"
    )

    return pipeline


def ensure_dlt_table(pipeline):
    """
    Force DLT to create:

        meme_data.meme_watchlist

    even if the application hasn't discovered a token yet.

    A bootstrap record is inserted and immediately deleted.
    """

    print(
        "[DLT] Ensuring meme_watchlist table exists..."
    )

    bootstrap_timestamp = datetime.now(UTC)

    bootstrap_record = {
        "token_address": "__DLT_BOOTSTRAP__",
        "symbol": "__BOOTSTRAP__",
        "timestamp": bootstrap_timestamp,
        "price": 0.0,
        "volume_5m": 0.0,
        "price_change_from_base": 0.0,
        "vol_growth_factor": 0.0,
        "liquidity_usd": 0.0,
    }

    try:

        load_info = pipeline.run(
            [bootstrap_record],
            table_name="meme_watchlist",
            write_disposition="append",
        )

        print(
            "[DLT] Bootstrap load successful"
        )

        print(
            f"[DLT] {load_info}"
        )

    except Exception as e:

        print(
            "[DLT] BOOTSTRAP ERROR"
        )

        print(
            repr(e)
        )

        raise

    # ------------------------------------------------------------
    # Delete bootstrap row
    # ------------------------------------------------------------

    try:

        with pipeline.sql_client() as client:

            table_name = (
                client.make_qualified_table_name(
                    "meme_watchlist"
                )
            )

            client.execute_sql(
                f"""
                DELETE FROM {table_name}
                WHERE token_address = %s
                """,
                "__DLT_BOOTSTRAP__",
            )

        print(
            "[DLT] Bootstrap row deleted"
        )

    except Exception as e:

        print(
            "[DLT] WARNING: Could not delete "
            f"bootstrap row: {repr(e)}"
        )

    print(
        "[DLT] meme_data.meme_watchlist is ready"
    )


# ============================================================
# DLT SAVE
# ============================================================

def save_records_with_dlt(
    pipeline,
    records,
):
    """
    Synchronous DLT load function.
    It is executed in asyncio.to_thread().
    """

    if not records:

        print(
            "[DLT] No records to load"
        )

        return None

    print(
        f"[DLT] Loading {len(records)} records..."
    )

    load_info = pipeline.run(
        records,
        table_name="meme_watchlist",
        write_disposition="append",
    )

    print(
        "[DLT] Load finished"
    )

    print(
        f"[DLT] {load_info}"
    )

    return load_info


# ============================================================
# DEXSCREENER MARKET DATA
# ============================================================

async def fetch_token_market_data(
    session,
    token_addresses,
):
    """
    Fetch market data for up to 30 Solana token addresses.

    DexScreener endpoint:
        /tokens/v1/solana/{tokenAddresses}
    """

    if not token_addresses:
        return []

    # DexScreener supports max 30 per request
    token_addresses = token_addresses[
        :MAX_TOKENS_PER_REQUEST
    ]

    addresses = ",".join(token_addresses)

    url = DEXSCREENER_TOKENS_URL.format(
        addresses=addresses
    )

    data = await get_json(
        session,
        url,
        retries=2,
    )

    if data is None:
        return []

    # The documented v1 token endpoint returns an array.
    if isinstance(data, list):
        return data

    # Be tolerant if API returns an object with pairs.
    if isinstance(data, dict):
        return data.get("pairs", []) or []

    return []


# ============================================================
# ANALYZE WATCHLIST
# ============================================================

async def analyze_watchlist(
    session,
    pipeline,
):

    if not state.watchlist:

        print(
            "[WATCHLIST] Empty"
        )

        return

    # ========================================================
    # Remove expired tokens
    # ========================================================

    now_ts = time.time()

    expired = [
        address
        for address, data in state.watchlist.items()
        if now_ts - data["added_at"]
        > WATCHLIST_EXPIRY_MINS * 60
    ]

    for address in expired:

        del state.watchlist[address]

        print(
            f"[WATCHLIST] Expired: {address}"
        )

    if not state.watchlist:

        return

    # ========================================================
    # Fetch current market data
    # ========================================================

    addresses = list(state.watchlist.keys())

    print(
        f"[MARKET] Querying {len(addresses)} "
        f"watchlist tokens..."
    )

    pairs = await fetch_token_market_data(
        session,
        addresses,
    )

    if not pairs:

        print(
            "[MARKET] No pair data received"
        )

        return

    # ========================================================
    # Prepare database records
    # ========================================================

    records_to_db = []
    alerts_to_send = []

    for pair in pairs:

        if not isinstance(pair, dict):
            continue

        base_token = pair.get(
            "baseToken",
            {},
        )

        addr = base_token.get("address")

        if not addr:
            continue

        if addr not in state.watchlist:
            continue

        symbol = (
            base_token.get("symbol")
            or state.watchlist[addr].get(
                "symbol",
                "MEME",
            )
        )

        # ----------------------------------------------------
        # Price
        # ----------------------------------------------------

        try:

            current_price = float(
                pair.get("priceUsd") or 0
            )

        except (ValueError, TypeError):

            current_price = 0.0

        # ----------------------------------------------------
        # 5-minute volume
        # ----------------------------------------------------

        volume_data = pair.get(
            "volume",
            {},
        ) or {}

        try:

            current_vol_5m = float(
                volume_data.get("m5") or 0
            )

        except (ValueError, TypeError):

            current_vol_5m = 0.0

        # ----------------------------------------------------
        # Liquidity
        # ----------------------------------------------------

        liquidity_data = pair.get(
            "liquidity",
            {},
        ) or {}

        try:

            liquidity_usd = float(
                liquidity_data.get("usd") or 0
            )

        except (ValueError, TypeError):

            liquidity_usd = 0.0

        # ----------------------------------------------------
        # Ignore tokens that have no usable price
        # ----------------------------------------------------

        if current_price <= 0:

            print(
                f"[MARKET] No valid price: "
                f"{symbol} / {addr}"
            )

            continue

        token_state = state.watchlist[addr]

        # ====================================================
        # BASELINE
        # ====================================================

        if token_state["base_price"] is None:

            token_state["base_price"] = (
                current_price
            )

            token_state["base_volume"] = (
                current_vol_5m
            )

            print(
                f"[BASELINE] {symbol} "
                f"price={current_price:.10f} "
                f"volume5m={current_vol_5m:.2f}"
            )

        # ====================================================
        # ANALYTICS
        # ====================================================

        base_price = float(
            token_state["base_price"]
            or current_price
        )

        base_volume = float(
            token_state["base_volume"]
            or 0
        )

        if base_price > 0:

            price_change_pct = (
                (current_price - base_price)
                / base_price
            ) * 100

        else:

            price_change_pct = 0.0

        # Avoid divide-by-zero
        if base_volume > 0:

            volume_growth_factor = (
                current_vol_5m / base_volume
            )

        else:

            # If initial volume was zero, use the
            # current volume as a sensible baseline.
            volume_growth_factor = 1.0

        timestamp = datetime.now(UTC)

        # ====================================================
        # DB RECORD
        # ====================================================

        record = {
            "token_address": addr,
            "symbol": symbol,
            "timestamp": timestamp,
            "price": current_price,
            "volume_5m": current_vol_5m,
            "price_change_from_base": round(
                price_change_pct,
                2,
            ),
            "vol_growth_factor": round(
                volume_growth_factor,
                2,
            ),
            "liquidity_usd": round(
                liquidity_usd,
                2,
            ),
        }

        records_to_db.append(record)

        print(
            f"[MARKET] {symbol} | "
            f"price={current_price:.10f} | "
            f"change={price_change_pct:.2f}% | "
            f"vol5m={current_vol_5m:.2f} | "
            f"vol_factor={volume_growth_factor:.2f} | "
            f"liq=${liquidity_usd:,.2f}"
        )

        # ====================================================
        # BREAKOUT TRIGGER
        # ====================================================

        if (
            10.0
            <= price_change_pct
            <= 50.0
            and volume_growth_factor >= 2.0
        ):

            print(
                f"[BREAKOUT] {symbol} "
                f"{addr} "
                f"price_change={price_change_pct:.2f}% "
                f"volume_factor={volume_growth_factor:.2f}"
            )

            # Remove from watchlist so it doesn't
            # repeatedly trigger every minute.
            state.watchlist.pop(
                addr,
                None,
            )

            payload = {
                **record,
                "signal": "MEME_PUMP_BREAKOUT",

                # IMPORTANT:
                # This is kept as your original custom
                # placeholder URL. Verify the actual OKX
                # Web3 URL format before using it for trading.
                "okx_swap_url": (
                    f"https://okx.com_{addr}"
                ),

                "timestamp": timestamp.isoformat(),
            }

            alerts_to_send.append(payload)

    # ========================================================
    # SEND ALERTS TO N8N
    # ========================================================

    for alert in alerts_to_send:

        try:

            async with session.post(
                N8N_WEBHOOK_URL,
                json=alert,
                timeout=HTTP_TIMEOUT,
            ) as resp:

                if 200 <= resp.status < 300:

                    print(
                        f"[N8N] Alert sent: "
                        f"{alert['symbol']}"
                    )

                else:

                    body = await resp.text()

                    print(
                        f"[N8N] ERROR {resp.status}: "
                        f"{body[:300]}"
                    )

        except Exception as e:

            print(
                f"[N8N] Webhook error: "
                f"{repr(e)}"
            )

    # ========================================================
    # DLT / POSTGRES
    # ========================================================

    if records_to_db:

        print(
            f"[DLT] records_to_db="
            f"{len(records_to_db)}"
        )

        try:

            await asyncio.to_thread(
                save_records_with_dlt,
                pipeline,
                records_to_db,
            )

        except Exception as e:

            print(
                "[DLT] LOAD ERROR"
            )

            print(
                repr(e)
            )

    else:

        print(
            "[DLT] records_to_db=0"
        )


# ============================================================
# DATABASE CLEANUP
# ============================================================

def cleanup_database(
    pipeline,
):
    """
    Remove old records from the DLT table.
    """

    threshold = (
        datetime.now(UTC)
        - timedelta(
            hours=CLEANUP_HOURS
        )
    )

    try:

        with pipeline.sql_client() as client:

            table_name = (
                client.make_qualified_table_name(
                    "meme_watchlist"
                )
            )

            print(
                f"[CLEANUP] Table: "
                f"{table_name}"
            )

            client.execute_sql(
                f"""
                DELETE FROM {table_name}
                WHERE timestamp < %s
                """,
                threshold,
            )

        print(
            "[CLEANUP] Complete"
        )

    except Exception as e:

        print(
            f"[CLEANUP] ERROR: {repr(e)}"
        )


# ============================================================
# DLT DATABASE TEST
# ============================================================

def test_dlt_database(
    pipeline,
):
    """
    Verify that DLT can actually open its SQL client
    and resolve the target table.
    """

    print(
        "[DLT TEST] Testing PostgreSQL connection..."
    )

    try:

        with pipeline.sql_client() as client:

            table_name = (
                client.make_qualified_table_name(
                    "meme_watchlist"
                )
            )

            print(
                f"[DLT TEST] Qualified table: "
                f"{table_name}"
            )

            # PostgreSQL schema/table existence test.
            result = client.execute_sql(
                """
                SELECT
                    current_database(),
                    current_schema()
                """
            )

            print(
                f"[DLT TEST] Database/schema: "
                f"{result}"
            )

        print(
            "[DLT TEST] PostgreSQL connection OK"
        )

    except Exception as e:

        print(
            "[DLT TEST] FAILED"
        )

        print(
            repr(e)
        )

        raise


# ============================================================
# MAIN LOOP
# ============================================================

async def main():

    print("=" * 70)
    print("MEME PUMP STRATEGY STARTING")
    print("=" * 70)

    # ========================================================
    # Create DLT pipeline
    # ========================================================

    pipeline = create_dlt_pipeline()

    # ========================================================
    # Test PostgreSQL / DLT connection
    # ========================================================

    test_dlt_database(
        pipeline
    )

    # ========================================================
    # FORCE TABLE CREATION
    # ========================================================

    ensure_dlt_table(
        pipeline
    )

    # ========================================================
    # Show qualified table
    # ========================================================

    try:

        with pipeline.sql_client() as client:

            qualified_table = (
                client.make_qualified_table_name(
                    "meme_watchlist"
                )
            )

            print(
                f"[DLT] Target table: "
                f"{qualified_table}"
            )

    except Exception as e:

        print(
            f"[DLT] Could not resolve table: "
            f"{repr(e)}"
        )

    # ========================================================
    # HTTP SESSION
    # ========================================================

    connector = aiohttp.TCPConnector(
        limit=20,
        limit_per_host=10,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=HTTP_TIMEOUT,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
    ) as session:

        print(
            "[MAIN] HTTP session started"
        )

        # ====================================================
        # MAIN LOOP
        # ====================================================

        while True:

            now = time.time()

            try:

                # ==============================================
                # 1. TOKEN DISCOVERY
                # ==============================================

                if (
                    now
                    - state.last_new_tokens_fetch
                    >= DISCOVERY_INTERVAL
                ):

                    await discover_and_filter_tokens(
                        session
                    )

                    state.last_new_tokens_fetch = now

                # ==============================================
                # 2. WATCHLIST ANALYSIS
                # ==============================================

                await analyze_watchlist(
                    session,
                    pipeline,
                )

                # ==============================================
                # 3. DATABASE CLEANUP
                # ==============================================

                if (
                    now
                    - state.last_cleanup_time
                    >= 3600
                ):

                    cleanup_database(
                        pipeline
                    )

                    state.last_cleanup_time = now

            except Exception as e:
                print("=" * 70)
                print("[MAIN LOOP ERROR]")
                print(repr(e))
                print("=" * 70)

            # ==============================================
            # STATUS
            # ==============================================

            print(f"[STATUS] watchlist={len(state.watchlist)} | next cycle in {POLL_INTERVAL}s")
            await asyncio.sleep(POLL_INTERVAL)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print( "\n[MAIN] Stopped by user.")
    except Exception as e:
        print("[MAIN] FATAL ERROR:")
        print(repr(e))
