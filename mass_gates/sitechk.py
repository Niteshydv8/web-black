import asyncio
import random
import aiohttp
import json
import os
import time
import re
import logging
import io

# Absolute path to sites.txt — always correct regardless of launch directory
SITES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sites.txt")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AIogram Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from aiogram import types, F, Router, Bot
from aiogram.filters import Command
from aiogram.types import FSInputFile

from shopify_api import call_shopify_api, APIStatusError

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ADD YOUR TELEGRAM ID HERE
# Read ADMIN_IDS from environment variable
_admin_ids_str = os.getenv("ADMIN_IDS", "").strip()
if _admin_ids_str:
    try:
        ADMIN_IDS = set()
        for id_str in _admin_ids_str.split(","):
            id_str = id_str.strip()
            if id_str and id_str.isdigit():
                ADMIN_IDS.add(int(id_str))
    except Exception as e:
        import logging
        logging.error(f"Failed to parse ADMIN_IDS: {e}")
        ADMIN_IDS = set()
else:
    ADMIN_IDS = set() 

# Test card to verify site functionality
TEST_CARD = "4000223372377978|05|29|651"

# ━━━ TEST CARDS POOL (60 random cards, rotation per check) ━━━━━━━━━━━
# All exp dates are FUTURE (today = Jul 2026). No expired cards.
TEST_CARDS = [
    # Visa
    "4242424242424242|11|27|123",
    "4111111111111111|11|27|123",
    "4012888888881881|11|27|123",
    "4222222222222222|11|27|123",
    "4000111111111115|11|27|123",
    "4242424242424242|03|28|321",
    "4111111111111111|03|28|321",
    "4012888888881881|03|28|321",
    "4222222222222222|03|28|321",
    "4000111111111115|03|28|321",
    "4242424242424242|08|28|456",
    "4111111111111111|08|28|456",
    "4012888888881881|08|28|456",
    "4222222222222222|08|28|456",
    "4000111111111115|08|28|456",
    "4242424242424242|06|29|789",
    "4111111111111111|06|29|789",
    "4012888888881881|06|29|789",
    "4222222222222222|06|29|789",
    "4000111111111115|06|29|789",
    "4242424242424242|09|29|321",
    "4111111111111111|09|29|321",
    "4012888888881881|09|29|321",
    "4222222222222222|09|29|321",
    "4000111111111115|09|29|321",
    # Mastercard
    "5555555555554444|11|27|123",
    "5105105105105100|11|27|123",
    "5454545454545454|11|27|123",
    "5111111111111111|11|27|123",
    "5200828282828210|11|27|123",
    "5555555555554444|03|28|321",
    "5105105105105100|03|28|321",
    "5454545454545454|03|28|321",
    "5111111111111111|03|28|321",
    "5200828282828210|03|28|321",
    "5555555555554444|08|28|456",
    "5105105105105100|08|28|456",
    "5454545454545454|08|28|456",
    "5111111111111111|08|28|456",
    "5200828282828210|08|28|456",
    "5555555555554444|06|29|789",
    "5105105105105100|06|29|789",
    "5454545454545454|06|29|789",
    "5111111111111111|06|29|789",
    "5200828282828210|06|29|789",
    # Amex (4-digit CVV)
    "378282246310005|11|27|1234",
    "371449635398431|11|27|1234",
    "378734493671000|11|27|1234",
    "378282246310005|03|28|4321",
    "371449635398431|03|28|4321",
    "378734493671000|03|28|4321",
    "378282246310005|08|28|8765",
    "371449635398431|08|28|8765",
    # Discover
    "6011111111111117|03|28|123",
    "6011000990139424|03|28|123",
    "6011111111111117|06|29|321",
    "6011000990139424|06|29|321",
    "6011111111111117|09|29|456",
    "6011000990139424|09|29|456",
    # JCB
    "3530111333300000|06|29|123",
    "3566002020360505|06|29|123",
    "3530111333300000|09|29|321",
]  # 60 cards total — all FUTURE exp dates

# API Configuration (admin-specified URL — used ONLY for site checks)
SITE_CHECK_API_URL = "http://38.247.64.215:5000/shopify"
API_TIMEOUT = 30
PRICE_FETCH_TIMEOUT = 8  # short timeout — price pre-fetch should be a "quick probe", not block site-check

# Proxy List - Rotated randomly for each check
PROXY_LIST = [
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@cz-pra.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@ph-man.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@nz-auc.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@co-bog.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@cl-san.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@il-tel.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@hu-bud.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@lt-sia.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@ro-buk.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@ee-tal.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@ie-dub.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@fi-esp.pvdata.host:8080",
    "http://g2rTXpNfPdcw2fzGtWKp62yH:nizar1elad2@jp-tok.pvdata.host:8080",
    "http://OR1673915314:LMf4JcDV@208.196.99.128:8813",
    "http://naveed:Qwerty_123ABC@196.244.48.124:12345"
]

# Bad proxies that failed - removed from rotation
BAD_PROXIES = set()

# DEAD ERRORS LIST (Including all step failures)
DEAD_ERRORS = [
    # Original errors
    'site error! status: 404', 'site error! status: 500', 'site error! status: 402', 
    'site error! status: 502', 'site error! 503', 'site error! status: 503',
    'site not supported for now!', 'connection error', 'connection error!', 
    'error processing card', 'failed to get token', 'failed to get checkout', 
    'failed to add to cart', 'site overloaded', 'site rate limited',
    'failed to get session token', 'unable to get payment token', 'no valid products', 
    'site error! status: 403', 'payment method is not shopify!', 'not shopify!', 
    'site error! status: 401', 'site requires login!',
    'timeout', 'http error', 'json', 'proxy', 'curl error', 'could not resolve',
    'connect tunnel failed', 'max retries', 'GENERIC_ERROR',
    
    # Step failures (Site Broken/Dead)
    'step 1 failed', 'step 0 failed', 'step 2 failed', 'step 3 failed', 'step 4 failed',
    'step 5 failed', 'step 6 failed', 'step 7 failed', 'step 9 failed', 'step 10 failed',
    'missing stableid', 'missing buildid', 'missing sourcetoken',
    'could not extract private_access_token',
    'could not find actions js url',
    'missing proposal', 'missing submit id',
    'retryable: inventory reservation failure',
    'exceeded 30 poll attempts',
    'could not extract queuetoken',
    'could not extract identification signature',
    'could not extract session id',
    'could not extract delivery handle',
    'could not extract signedhandles',
    'could not extract shipping amount',
    'could not extract total amount',
    'could not extract receiptid',
    'could not extract sessiontoken',
    'errstoreincompatible', 'errmissingreceiptid'
]

# ━━━ DEAD KEYWORDS (admin-editable — match in response = site dead) ━━━━
# Add site-error keywords here. If ANY keyword is found in the API response,
# the site is marked as DEAD. Edit freely — case-insensitive substring match.
DEAD_KEYWORDS = [
    # NOTE: MERCHANDISE_EXPECTED_PRICE_MISMATCH was here; now handled by
    # RETRYABLE_KEYWORDS below (price mismatch is NOT a dead indicator).
    "<b>Site Error! Status: 429</b>",
    "PAYMENTS_PAYMENT_FLEXIBILITY_TERMS_ID_MISMATCH",
    "Processing Error",
    "DECISION_RULE_BLOCK",
]  # PLACEHOLDER_RESOLVED


# ━━━ RETRYABLE KEYWORDS (price/currency transient mismatches) ━━━━━━━━━
# When these appear in the response, the site is ALIVE — the issue is on
# the test card's price vs. the site price (sale, multi-currency, etc.).
# The check retries with a different test card up to MAX_RETRIES times.
# If all retries still hit a retryable error, the site is treated as KEEP
# (alive) — never DEAD — because price mismatch indicates a working site,
# just with a price the bot couldn't validate.
RETRYABLE_KEYWORDS = [
    "MERCHANDISE_EXPECTED_PRICE_MISMATCH",  # Shopify GraphQL cartCreate
    "EXPECTED_PRICE_MISMATCH",
    "EXPECTED_LINE_ITEM_PRICE",
    "CURRENCY_MISMATCH",
    "CURRENCY_NOT_SUPPORTED",
    "SALE_PRICE_MISMATCH",
    "SALE_ENDED",
    "PRICE_OUT_OF_RANGE",
    "EXPECTED_PRICE",
]  # PLACEHOLDER_RESOLVED


# ━━━ PRICE PRE-FETCH (avoid MERCHANDISE_EXPECTED_PRICE_MISMATCH retry) ━━━
# To avoid the price-mismatch retry loop entirely, we pre-fetch the site's
# product price from the API before doing the real check, then pass that
# price as a 5th segment of the cc parameter (card|mm|yy|cvv|$X.XX).
# When price matches, MERCHANDISE_EXPECTED_PRICE_MISMATCH never fires.
# Falls back silently to retry logic if the price endpoint isn't supported.

def _parse_price(price_val) -> float | None:
    """Parse various price formats to float. Returns None if invalid."""
    if price_val is None:
        return None
    if isinstance(price_val, (int, float)):
        return float(price_val)
    if not isinstance(price_val, str):
        return None
    s = re.sub(r'[^\d.]', '', str(price_val))
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


async def _fetch_site_price(site_url: str, proxy: str) -> float | None:
    """
    Best-effort price pre-fetch from the site-check API.
    Tries several endpoint patterns to maximize compatibility.
    Returns float price or None if not available.

    Designed to be FAST — single 8s timeout cap per variant, max 2 variants.
    If price endpoint doesn't exist or proxy/network stalls, we bail out
    quickly so the main check proceeds without delay.
    """
    try:
        from shopify_api import _get_shared_session, _proxy_for_api
    except Exception:
        return None

    session = await _get_shared_session()
    proxy_param = _proxy_for_api(proxy) if proxy else ""
    base = SITE_CHECK_API_URL.rstrip("/")
    proxy_for_aiohttp = proxy if (proxy and proxy.strip()) else None
    quick_timeout = aiohttp.ClientTimeout(total=PRICE_FETCH_TIMEOUT)

    # Just two patterns — both common, returns on first 200 + valid price.
    variants = [
        ("{base}/price",                  {"site": site_url, "proxy": proxy_param}),
        ("{base}?info=price",             {"site": site_url, "proxy": proxy_param, "info": "price"}),
    ]
    for url_template, params in variants:
        url = url_template.format(base=base)
        try:
            async with session.get(
                url,
                params=params,
                proxy=proxy_for_aiohttp,
                timeout=quick_timeout,
            ) as resp:
                if resp.status != 200:
                    continue
                text = await resp.text()
                if not text or not text.strip():
                    continue
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if not isinstance(data, dict):
                    continue
                for key, value in data.items():
                    if key.lower() in ("price", "amount", "expectedprice",
                                        "expected_price", "total", "totalamount"):
                        parsed = _parse_price(value)
                        if parsed is not None and 0 < parsed <= 1000:
                            return parsed
                # 200 OK but no price field — endpoint exists but doesn't
                # return a price for this site. Don't try more variants.
                if data:
                    return None
        except Exception:
            continue
    return None


# List of valid gateway responses (Site is Alive)
SUCCESS_RESPONSES = [
    'CARD_DECLINED', 'INVALID_CVC', 'INCORRECT_CVV', 'INSUFFICIENT_FUNDS', 
    'GENERIC_DECLINE', 'DO NOT HONOR', 'UNKNOWN_ERROR', 'EXPIRED_CARD',
    'PICK_UP_CARD', 'FRAUD_SUSPECTED', '3DS_REQUIRED', 'AMOUNT_TOO_SMALL',
    'INVALID_PURCHASE_TYPE', 'INVALID_PAYMENT_METHOD', 'ORDER_PAID', 'INCORRECT_NUMBER',
]

# Router for this module
router = Router()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPER FUNCTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def is_admin(user_id):
    """Check if user is admin"""
    return user_id in ADMIN_IDS

def read_sites():
    """Read sites from sites.txt file - returns list without duplicates"""
    if not os.path.exists(SITES_FILE):
        return []
    with open(SITES_FILE, "r", encoding="utf-8") as f:
        # Automatically remove duplicates when reading
        sites = list(set([line.strip() for line in f if line.strip()]))
    return sites

def write_sites(sites_list):
    """Write sites to sites.txt file - ENSURES NO DUPLICATES"""
    # Convert to set to remove duplicates, then back to list
    unique_sites = list(set(sites_list))
    
    with open(SITES_FILE, "w", encoding="utf-8") as f:
        for site in unique_sites:
            f.write(f"{site}\n")
    
    return len(unique_sites)

def normalize_url(url: str) -> str:
    """
    Normalize URL to prevent duplicates with slight variations
    
    Examples:
    - https://example.com/ -> https://example.com
    - https://EXAMPLE.COM -> https://example.com
    - https://example.com/// -> https://example.com
    """
    url = url.strip().lower()
    
    # Remove trailing slash
    url = url.rstrip('/')
    
    # Remove www. prefix for consistency
    if url.startswith('www.'):
        url = url[4:]
    
    return url

def get_random_proxy():
    """Get a random proxy that isn't in the bad list"""
    global BAD_PROXIES
    available = [p for p in PROXY_LIST if p not in BAD_PROXIES]
    if not available:
        # Reset bad proxies if all are bad
        BAD_PROXIES.clear()
        available = PROXY_LIST
    return random.choice(available)

def mark_proxy_bad(proxy):
    """Mark a proxy as bad"""
    global BAD_PROXIES
    BAD_PROXIES.add(proxy)


# ━━━ SMART ERROR RESPONSE DETECTION ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Generic patterns that ALWAYS indicate a dead/broken site (regardless of
# DEAD_ERRORS / DEAD_KEYWORDS). Catches Shopify taxonomy error codes and
# diagnostic messages that the API may return on a "successful" call.
SHOPIFY_ERROR_PREFIXES = (
    "PAYMENTS_", "DELIVERY_", "CHECKOUT_", "CART_",
    "ORDER_", "RISK_", "VALIDATION_",
)

GENERIC_ERROR_MARKERS = (
    "site not",
    "cart failed",
    "site error",
    "step ",
)

EMPTY_DEFAULT_RESPONSES = ("unknown", "")


def _looks_like_error_response(response_msg: str) -> bool:
    """Return True if the API response looks like an error/diagnostic
    message rather than a legitimate gateway decision.
    """
    if response_msg is None:
        return True
    msg = response_msg.strip()
    if not msg:
        return True
    msg_lower = msg.lower()
    msg_upper = msg.upper()

    # Empty or default "Unknown" placeholder
    if msg_lower in EMPTY_DEFAULT_RESPONSES:
        return True

    # Generic error prefixes: "error:", "error ...", "site error..."
    if msg_lower.startswith("error:") or msg_lower.startswith("error "):
        return True

    # Generic error markers (substring match)
    for marker in GENERIC_ERROR_MARKERS:
        if marker in msg_lower:
            return True

    # Shopify uppercase taxonomy error codes (PAYMENTS_, DELIVERY_, ...)
    # — but allow codes that are explicitly listed as success.
    if any(msg_upper.startswith(pre) for pre in SHOPIFY_ERROR_PREFIXES):
        is_known_success = any(s in msg_upper for s in SUCCESS_RESPONSES)
        if not is_known_success:
            return True

    return False

async def call_site_check_api(site_url: str, cc_formatted: str, proxy: str) -> dict:
    """
    Call the site-check API URL with smart price-hint retry.

    Flow:
      1. First call WITHOUT price hint — capture actual site price from
         response (Shopify API returns `Price` field even on price-mismatch
         errors).
      2. If response was MERCHANDISE_EXPECTED_PRICE_MISMATCH (or similar)
         AND the response carries a valid site price:
         Second call WITH that price passed as `&price=$X.XX` URL param.
         The API uses our price instead of running the merchant-side
         expected-price comparison, eliminating the mismatch on shot #2.

    Persistent mismatch after the price-hint retry falls through to the
    RETRYABLE_KEYWORDS handler in check_site_status (which keeps the site
    alive — never marks it dead for a price mismatch).
    """
    from shopify_api import _proxy_for_api

    proxy_param = _proxy_for_api(proxy) if proxy else ""
    proxy_for_aiohttp = proxy if (proxy and proxy.strip()) else None

    # ── CALL 1: standard request without price hint ──
    result1 = await _do_site_api_call(
        site_url=site_url,
        cc=cc_formatted,
        proxy_param=proxy_param,
        proxy_for_aiohttp=proxy_for_aiohttp,
        price_hint=None,
    )

    response_msg = result1.get("response", "Unknown")
    response_upper = response_msg.upper()

    # ── SMART RETRY: mismatch + valid price in response → pass price ──
    is_price_mismatch = (
        "MERCHANDISE_EXPECTED_PRICE_MISMATCH" in response_upper
        or "EXPECTED_PRICE_MISMATCH" in response_upper
        or "SALE_PRICE_MISMATCH" in response_upper
        or "PRICE_OUT_OF_RANGE" in response_upper
        or "EXPECTED_LINE_ITEM_PRICE" in response_upper
    )

    if is_price_mismatch:
        first_price = _parse_price(result1.get("price"))
        if first_price is not None and first_price > 0:
            # Second call WITH price hint — bypass merchant price check
            result2 = await _do_site_api_call(
                site_url=site_url,
                cc=cc_formatted,
                proxy_param=proxy_param,
                proxy_for_aiohttp=proxy_for_aiohttp,
                price_hint=f"{first_price:.2f}",
            )
            return result2

    return result1


async def _do_site_api_call(
    site_url: str,
    cc: str,
    proxy_param: str,
    proxy_for_aiohttp: str | None,
    price_hint: str | None,
) -> dict:
    """Single HTTP call to SITE_CHECK_API_URL. Internal helper."""
    try:
        from shopify_api import _get_shared_session

        params: dict = {
            "site": site_url,
            "cc": cc,
            "proxy": proxy_param,
        }
        if price_hint:
            # Pass `price=` URL param so the API uses our value and skips
            # the merchant-side expected-price check that causes
            # MERCHANDISE_EXPECTED_PRICE_MISMATCH.
            params["price"] = price_hint

        session = await _get_shared_session()
        async with session.get(
            SITE_CHECK_API_URL,
            params=params,
            proxy=proxy_for_aiohttp,
            timeout=aiohttp.ClientTimeout(total=API_TIMEOUT),
        ) as resp:
            text = await resp.text()
            if resp.status in (408, 429, 500, 502, 503, 504):
                raise APIStatusError(resp.status, text)
            if not text or not text.strip():
                raise APIStatusError(resp.status, "<empty body>")
            try:
                data = json.loads(text)
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(e.msg, e.doc, e.pos) from e

        response_msg = data.get("Response", "Unknown")
        price_str = data.get("Price", "-1.0")
        proxy_raw = data.get("Proxy", "Dead")
        gateway = data.get("Gateway", "")
        status = data.get("Status", "false")

        if "live" in str(proxy_raw).lower():
            proxy_status = "Live"
        else:
            proxy_status = "Dead"

        return {
            "success": True,
            "response": response_msg,
            "price": price_str,
            "proxy_status": proxy_status,
            "gateway": gateway,
            "status": status,
            "error": None,
        }

    except asyncio.TimeoutError:
        return {
            "success": False,
            "response": "Timeout Error",
            "price": "-1.0",
            "proxy_status": "Dead",
            "gateway": "",
            "error": "TIMEOUT"
        }
    except aiohttp.ClientResponseError as e:
        return {
            "success": False,
            "response": f"HTTP Error {e.status}",
            "price": "-1.0",
            "proxy_status": "Dead",
            "gateway": "",
            "error": f"HTTP_{e.status}"
        }
    except APIStatusError as e:
        return {
            "success": False,
            "response": f"HTTP Error {e.status}",
            "price": "-1.0",
            "proxy_status": "Dead",
            "gateway": "",
            "error": f"HTTP_{e.status}"
        }
    except json.JSONDecodeError:
        return {
            "success": False,
            "response": "Invalid JSON Response",
            "price": "-1.0",
            "proxy_status": "Dead",
            "gateway": "",
            "error": "JSON_PARSE_ERROR"
        }
    except aiohttp.ClientConnectorError as e:
        error_str = str(e).lower()
        if "proxy" in error_str or "tunnel" in error_str:
            return {
                "success": False,
                "response": f"Proxy Error: {str(e)[:60]}",
                "price": "-1.0",
                "proxy_status": "Dead",
                "gateway": "",
                "error": "PROXY_ERROR"
            }
        return {
            "success": False,
            "response": f"Connection Error: {str(e)[:60]}",
            "price": "-1.0",
            "proxy_status": "Dead",
            "gateway": "",
            "error": "CONNECTION_ERROR"
        }
    except aiohttp.ClientError as e:
        return {
            "success": False,
            "response": f"Client Error: {str(e)[:60]}",
            "price": "-1.0",
            "proxy_status": "Dead",
            "gateway": "",
            "error": "CLIENT_ERROR"
        }
    except Exception as e:
        return {
            "success": False,
            "response": f"Error: {str(e)[:60]}",
            "price": "-1.0",
            "proxy_status": "Dead",
            "gateway": "",
            "error": "UNKNOWN_ERROR"
        }

async def check_site_status(site_url: str) -> tuple:
    """
    Checks a single site using the API with a test card.
    
    Returns: (site_url, status, data_dict, final_response_string)
    - status: "KEEP", "REMOVE", "ERROR"
    """
    MAX_RETRIES = 3
    
    for attempt in range(MAX_RETRIES):
        proxy = get_random_proxy()
        test_cc = random.choice(TEST_CARDS)
        
        result = await call_site_check_api(
            site_url=site_url,
            cc_formatted=test_cc,
            proxy=proxy
        )
        
        response_msg = result.get("response", "Unknown")
        price_str = result.get("price", "-1.0")
        proxy_status = result.get("proxy_status", "Dead")
        error_type = result.get("error")
        
        if proxy_status and proxy_status.lower() != "live":
            mark_proxy_bad(proxy)
            
            if error_type in ["PROXY_ERROR", "TIMEOUT", "CONNECTION_ERROR"]:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(0.5)
                    continue
        
        # Check for Hard Errors (Site Dead)
        is_dead = False
        response_lower = response_msg.lower()
        
        for err in DEAD_ERRORS:
            if err.lower() in response_lower:
                is_dead = True
                break
        
        # Check admin-defined DEAD_KEYWORDS (case-insensitive substring match)
        if not is_dead:
            for kw in DEAD_KEYWORDS:
                if kw.lower() in response_lower:
                    is_dead = True
                    break
        
        # Smart error detection: taxonomy codes, generic error markers
        if not is_dead and _looks_like_error_response(response_msg):
            is_dead = True
        
        if not result.get("success"):
            if error_type in ["JSON_PARSE_ERROR", "HTTP_500", "HTTP_502", "HTTP_503", "HTTP_404"]:
                is_dead = True
        
        if is_dead:
            return site_url, "REMOVE", {"Price": -1.0}, response_msg

        # ━━━ RETRYABLE: price/currency transient mismatches ━━━━━━━━━━━━━━━━
        # These responses indicate the SITE IS ALIVE but the test card's
        # price doesn't match (sale discount / multi-currency / dynamic
        # pricing / sale ended). Retry with a different random test card from
        # TEST_CARDS pool, then a jittered pause so the price/sale state
        # can update server-side. After MAX_RETRIES the site is treated as
        # KEEP (alive) — price-mismatch is NEVER a dead indicator.
        is_retryable = False
        for kw in RETRYABLE_KEYWORDS:
            if kw.lower() in response_lower:
                is_retryable = True
                break

        if is_retryable:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(0.3 + random.uniform(0.0, 0.4))
                continue
            # All retries exhausted on retryable errors — site is alive,
            # just price-mismatched. Mark KEEP with price=0 (unknown).
            return site_url, "KEEP", {"Price": 0.0}, f"Price Mismatch (Retries Exhausted) | {response_msg}"

        # Check for Valid Gateway Response (Site is Alive)
        if any(x in response_msg.upper() for x in SUCCESS_RESPONSES):
            actual_price = -1.0
            if price_str and price_str != "-1.0":
                clean_price = re.sub(r'[^\d.]', '', str(price_str))
                if clean_price:
                    try:
                        actual_price = float(clean_price)
                    except ValueError:
                        actual_price = -1.0

            # PRICE CONSTRAINT: Must be between $0 and $20
            if 0.00 <= actual_price <= 20.00:
                msg_display = f"${actual_price:.2f} | {response_msg}"
                return site_url, "KEEP", {"Price": actual_price}, msg_display
            else:
                return site_url, "REMOVE", {"Price": actual_price}, f"Price ${actual_price:.2f} (Rejected) | {response_msg}"

        # Fallback - got a valid response but not in SUCCESS list.
        # Only KEEP if the response does NOT look like an error/diagnostic.
        if result.get("success") and not _looks_like_error_response(response_msg):
            return site_url, "KEEP", {"Price": 0.0}, f"Unknown Response: {response_msg}"

        return site_url, "REMOVE", {"Price": -1.0}, response_msg

    return site_url, "ERROR", {"Price": -1.0}, "Max Retries Reached"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BACKGROUND WORKER FUNCTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def run_site_checker(bot: Bot, chat_id: int, sites_to_check, command_name="Audit", status_message_id=None):
    """Run site checking process in background with DUPLICATE PREVENTION"""
    global BAD_PROXIES
    # Reset bad proxies for fresh start
    BAD_PROXIES.clear()
    
    total_sites = len(sites_to_check)
    valid_sites = []
    working_sites_content = []
    checked_count = 0
    live_count = 0
    dead_count = 0
    duplicate_count = 0
    
    last_edit_time = 0
    MIN_EDIT_INTERVAL = 2.0
    CHECKS_PER_UPDATE = 10 
    sem = asyncio.Semaphore(20)
    
    # Get existing sites for duplicate checking (for /addsite command)
    existing_sites = set()
    if command_name == "Adding":
        existing_sites = set(await asyncio.to_thread(read_sites))
        print(f"[SITECHK] Found {len(existing_sites)} existing sites for duplicate check")

    async def worker(site):
        async with sem:
            return await check_site_status(site)
    
    tasks = [worker(site) for site in sites_to_check]
    
    for future in asyncio.as_completed(tasks):
        try:
            site, status, data, resp_msg = await future
        except Exception as e:
            checked_count += 1
            dead_count += 1
            print(f"[LOG] {site} | Error: {e}")
            continue
            
        checked_count += 1
        print(f"[LOG] {site} | {resp_msg}")

        if status == "KEEP":
            # NORMALIZE URL FOR DUPLICATE CHECK
            normalized_site = normalize_url(site)
            
            # Check if already exists (for Adding mode)
            if command_name == "Adding":
                normalized_existing = {normalize_url(s) for s in existing_sites}
                if normalized_site in normalized_existing:
                    duplicate_count += 1
                    print(f"[DUPLICATE SKIPPED] {site} already exists!")
                    continue
                
                # Also check if already added in this batch
                normalized_valid = {normalize_url(s) for s in valid_sites}
                if normalized_site in normalized_valid:
                    duplicate_count += 1
                    print(f"[DUPLICATE SKIPPED] {site} duplicate in batch!")
                    continue
            
            live_count += 1
            valid_sites.append(site)
            price = data.get("Price", "0.00") if isinstance(data, dict) else "0.00"
            if isinstance(price, float):
                price = f"${price:.2f}"
            working_sites_content.append(f"{site} | Price: {price} | Response: {resp_msg}")
        else:
            dead_count += 1

        current_time = time.time()
        if (current_time - MIN_EDIT_INTERVAL > last_edit_time) or (checked_count % CHECKS_PER_UPDATE == 0):
            try:
                if status_message_id:
                    dup_text = ""
                    if duplicate_count > 0:
                        dup_text = f"\n🔄 <b>Duplicates Skipped:</b> <code>{duplicate_count}</code>"
                    
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message_id,
                        text=f"🔄 <b>{command_name}ing {total_sites} Sites...</b>\n"
                        f"<b>━━━━━━━━━━━━━━━━━━━━━━</b>\n"
                        f"✅ <b>Kept ($0-20):</b> <code>{live_count}</code>\n"
                        f"❌ <b>Rejected:</b> <code>{dead_count}</code>\n"
                        f"🔄 <b>Checked:</b> <code>{checked_count}/{total_sites}</code>\n"
                        f"🌐 <b>Proxies Available:</b> <code>{len(PROXY_LIST) - len(BAD_PROXIES)}/{len(PROXY_LIST)}</code>"
                        f"{dup_text}",
                        parse_mode="HTML"
                    )
                    last_edit_time = current_time
            except Exception:
                pass 

    # FINAL DEDUPLICATION before saving
    final_unique_sites = []
    seen_normalized = set()
    
    for site in valid_sites:
        normalized = normalize_url(site)
        if normalized not in seen_normalized:
            seen_normalized.add(normalized)
            final_unique_sites.append(site)
    
    removed_dupes = len(valid_sites) - len(final_unique_sites)
    if removed_dupes > 0:
        print(f"[SITECHK] Removed {removed_dupes} internal duplicates before saving")
    
    # Save results based on command type
    if command_name == "Audit":
        saved_count = await asyncio.to_thread(write_sites, final_unique_sites)
    elif command_name == "Adding":
        # Merge with existing, ensuring no duplicates
        existing = await asyncio.to_thread(read_sites)
        existing_normalized = {normalize_url(s) for s in existing}
        
        combined = list(existing)
        for new_site in final_unique_sites:
            norm_new = normalize_url(new_site)
            if norm_new not in existing_normalized:
                combined.append(new_site)
                existing_normalized.add(norm_new)
        
        saved_count = await asyncio.to_thread(write_sites, combined)

    # Generate report file
    filename = f"report_{command_name.lower()}_{int(time.time())}.txt"
    
    file_content = "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    file_content += f"TOTAL CHECKED: {total_sites}\n"
    file_content += f"WORKING SITES (Price $0-20): {len(final_unique_sites)}\n"
    file_content += f"REJECTED (Dead/High Price): {dead_count}\n"
    if duplicate_count > 0 or removed_dupes > 0:
        file_content += f"DUPLICATES SKIPPED: {duplicate_count + removed_dupes}\n"
    file_content += f"PROXIES USED: {len(PROXY_LIST)} | BAD: {len(BAD_PROXIES)}\n"
    file_content += "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    if working_sites_content:
        file_content += "\n".join(working_sites_content)
    else:
        file_content += "No valid sites found within price range!"

    try:
        def _write_report():
            with open(filename, "w", encoding="utf-8") as f:
                f.write(file_content)
        
        await asyncio.to_thread(_write_report)
        
        if status_message_id:
            try:
                dup_final = ""
                if (duplicate_count + removed_dupes) > 0:
                    dup_final = f"\n🚫 <b>Duplicates Blocked:</b> <code>{duplicate_count + removed_dupes}</code>"
                
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=status_message_id,
                    text=f"✅ <b>{command_name} Complete!</b>\n\n"
                    f"<b>Total Checked:</b> {total_sites}\n"
                    f"<b>Valid ($0-20):</b> {len(final_unique_sites)} ✅\n"
                    f"<b>Rejected:</b> {dead_count} ❌\n"
                    f"<b>━━━━━━━━━━━━━━━━━━━━━━</b>\n"
                    f"🌐 <b>Proxies Used:</b> {len(PROXY_LIST)} | <b>Bad:</b> {len(BAD_PROXIES)}"
                    f"{dup_final}",
                    parse_mode="HTML"
                )
            except Exception:
                pass

        await bot.send_document(
            chat_id=chat_id,
            document=FSInputFile(filename),
            caption=f"📜 <b>{command_name} Report (Deduplicated)</b>",
            parse_mode="HTML"
        )
        
        try:
            os.remove(filename)
        except:
            pass
            
    except Exception as e:
        await bot.send_message(chat_id=chat_id, text=f"❌ <b>Error:</b> {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND 1: /sitechk (Audit & Clean Sites)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("sitechk"))
async def sitechk_command(message: types.Message):
    """Audit existing sites - removes dead ones and deduplicates"""
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("⛔ <b>You are not authorized.</b>", parse_mode="HTML")
        return

    bot = message.bot
    chat_id = message.chat.id

    sites = await asyncio.to_thread(read_sites)
    if not sites:
        await message.answer("📭 <b>No sites found in sites.txt</b>", parse_mode="HTML")
        return

    status_msg = await message.answer(
        f"🔄 <b>Starting Audit on {len(sites)} Sites...</b>\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        f"🔄 <b>Checked:</b> <code>0/{len(sites)}</code>\n"
        f"✅ <b>Kept ($0-20):</b> <code>0</code>\n"
        f"❌ <b>Rejected:</b> <code>0</code>\n"
        f"🌐 <b>Proxies:</b> <code>{len(PROXY_LIST)}</code>",
        parse_mode="HTML"
    )

    asyncio.create_task(
        run_site_checker(
            bot, 
            chat_id,
            sites,
            command_name="Audit",
            status_message_id=status_msg.message_id
        )
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND 2: /addsite (Add & Verify New Sites)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("addsite"))
async def addsite_command(message: types.Message):
    """Add new sites from uploaded file - automatically skips duplicates"""
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("⛔ <b>You are not authorized.</b>", parse_mode="HTML")
        return

    bot = message.bot
    chat_id = message.chat.id

    doc = message.document
    if not doc:
        if message.reply_to_message:
            doc = message.reply_to_message.document
    
    if not doc:
        await message.answer(
            "⚠️ <b>Please reply to a file or upload a file containing sites with /addsite.</b>",
            parse_mode="HTML"
        )
        return

    # Download file content
    try:
        file_info = await bot.get_file(doc.file_id)
        
        destination = io.BytesIO()
        await bot.download_file(file_info.file_path, destination)
        
        destination.seek(0)
        byte_content = destination.read()
        text = byte_content.decode('utf-8', errors='ignore')
        
        # Extract URLs
        url_pattern = r'(https?://\S+)'
        new_sites = []
        lines = text.split('\n')
        
        for line in lines:
            match = re.search(url_pattern, line)
            if match:
                url = match.group(1)
                url = url.rstrip('.,;:!?)\'"')
                new_sites.append(url)
        
        # Remove duplicates from uploaded file itself
        new_sites = list(set(new_sites))
        
        if not new_sites:
            await message.answer("❌ <b>No valid sites found in file.</b>", parse_mode="HTML")
            return
            
    except Exception as e:
        logging.error(f"Error downloading file: {e}", exc_info=True)
        await message.answer(f"❌ <b>Error reading file:</b> {e}", parse_mode="HTML")
        return

    # Send initial status
    status_msg = await message.answer(
        f"🔄 <b>Starting Addition of {len(new_sites)} Sites...</b>\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        f"🔄 <b>Checked:</b> <code>0/{len(new_sites)}</code>\n"
        f"✅ <b>Added ($0-20):</b> <code>0</code>\n"
        f"❌ <b>Rejected:</b> <code>0</code>\n"
        f"🚫 <b>Duplicates:</b> <code>0</code>\n"
        f"🌐 <b>Proxies:</b> <code>{len(PROXY_LIST)}</code>",
        parse_mode="HTML"
    )

    asyncio.create_task(
        run_site_checker(
            bot,
            chat_id,
            new_sites,
            command_name="Adding",
            status_message_id=status_msg.message_id
        )
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND 3: /siteall (List All Sites)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("siteall"))
async def siteall_command(message: types.Message):
    """Download full list of all sites (automatically deduplicated)"""
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("⛔ <b>You are not authorized.</b>", parse_mode="HTML")
        return

    sites = await asyncio.to_thread(read_sites)
    if not sites:
        await message.answer("📭 <b>sites.txt is empty.</b>", parse_mode="HTML")
        return

    filename = f"full_sites_list_{int(time.time())}.txt"
    
    try:
        def _write_file():
            with open(filename, "w", encoding="utf-8") as f:
                f.write(f"Total Sites: {len(sites)} (Deduplicated)\n\n")
                f.write("\n".join(sites))
        
        await asyncio.to_thread(_write_file)
        
        await message.answer_document(
            document=FSInputFile(filename),
            caption=f"📜 <b>Total Sites:</b> <code>{len(sites)}</code> ✨ (No Duplicates)",
            parse_mode="HTML"
        )
        os.remove(filename)
    except Exception as e:
        await message.answer(f"❌ <b>Error:</b> {e}", parse_mode="HTML")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND 4: /removeall (Remove All Sites)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("removeall"))
async def removeall_command(message: types.Message):
    """Clear all sites from sites.txt"""
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("⛔ <b>You are not authorized.</b>", parse_mode="HTML")
        return

    sites = await asyncio.to_thread(read_sites)
    if not sites:
        await message.answer("📭 <b>sites.txt is already empty.</b>", parse_mode="HTML")
        return

    try:
        def _clear_file():
            with open("sites.txt", "w", encoding="utf-8") as f:
                pass
        
        await asyncio.to_thread(_clear_file)
        
        await message.answer(
            f"✅ <b>All sites have been successfully removed.</b>\n\n"
            f"<b>Removed:</b> <code>{len(sites)}</code> sites",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"❌ <b>Error:</b> {e}", parse_mode="HTML")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND 5: /dedupe (Force Deduplicate)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("dedupe"))
async def dedupe_command(message: types.Message):
    """Force deduplicate sites.txt"""
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("⛔ <b>You are not authorized.</b>", parse_mode="HTML")
        return
    
    sites = await asyncio.to_thread(read_sites)
    
    if not sites:
        await message.answer("📭 <b>sites.txt is empty.</b>", parse_mode="HTML")
        return
    
    original_count = len(sites)
    
    # Force write (which auto-deduplicates)
    final_count = await asyncio.to_thread(write_sites, sites)
    
    removed = original_count - final_count
    
    if removed > 0:
        await message.answer(
            f"✨ <b>Deduplication Complete!</b>\n\n"
            f"<b>Original:</b> <code>{original_count}</code>\n"
            f"<b>Removed:</b> <code>{removed}</code> duplicates\n"
            f"<b>Final:</b> <code>{final_count}</code> unique sites",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            f"✅ <b>No duplicates found!</b>\n\n"
            f"<b>Total Sites:</b> <code>{final_count}</code> (All Unique)",
            parse_mode="HTML"
        )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND 6: /proxyinfo (Check Proxy Status)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("proxyinfo"))
async def proxyinfo_command(message: types.Message):
    """Show proxy statistics"""
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("⛔ <b>You are not authorized.</b>", parse_mode="HTML")
        return

    available = len(PROXY_LIST) - len(BAD_PROXIES)
    
    text = (
        f"🌐 <b>Proxy Information</b>\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━</b>\n"
        f"📊 <b>Total Proxies:</b> <code>{len(PROXY_LIST)}</code>\n"
        f"✅ <b>Available:</b> <code>{available}</code>\n"
        f"❌ <b>Bad/Dead:</b> <code>{len(BAD_PROXIES)}</code>\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
    )
    
    for i, proxy in enumerate(PROXY_LIST, 1):
        status = "❌ Dead" if proxy in BAD_PROXIES else "✅ Live"
        if "@" in proxy:
            parts = proxy.split("@")
            host_part = parts[1] if len(parts) > 1 else proxy
            text += f"<code>{i}.</code> {host_part} - {status}\n"
        else:
            text += f"<code>{i}.</code> {proxy[:30]}... - {status}\n"
    
    await message.answer(text, parse_mode="HTML")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND 7: /resetproxy (Reset Bad Proxies)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("resetproxy"))
async def resetproxy_command(message: types.Message):
    """Reset all bad proxies back to available"""
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer("⛔ <b>You are not authorized.</b>", parse_mode="HTML")
        return

    global BAD_PROXIES
    cleared_count = len(BAD_PROXIES)
    BAD_PROXIES.clear()
    
    await message.answer(
        f"✅ <b>Proxy List Reset!</b>\n\n"
        f"<b>Cleared:</b> <code>{cleared_count}</code> bad proxies\n"
        f"<b>Available Now:</b> <code>{len(PROXY_LIST)}</code>",
        parse_mode="HTML"
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GATEWAY EXTRACTION (Tier 3)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def check_payment_gateway(content_type: str, headers: dict, html: str, cookies: dict) -> str:
    """Extract payment gateway from site response."""
    gateway_keywords = {
        'shopify': ['shopify', 'myshopify', 'shopify.com', 'checkout.shopify.com'],
        'stripe': ['stripe', 'checkout.stripe.com', 'js.stripe.com'],
        'paypal': ['paypal', 'paypal.com', 'checkout.js'],
        'square': ['square', 'squareup.com', 'connect.squareup.com'],
        'adyen': ['adyen', 'adyen.com', 'adyen-payment'],
        'braintree': ['braintree', 'braintreegateway.com'],
        'mollie': ['mollie', 'api.mollie.com'],
        'worldpay': ['worldpay', 'secure.worldpay.com'],
        'cybersource': ['cybersource', 'cybersource.com'],
        'authorize.net': ['authorize.net', 'authorizenet.com'],
    }
    
    content_str = str(content_type).lower()
    headers_str = str(headers).lower()
    html_lower = html.lower()
    cookies_str = str(cookies).lower()
    
    for gateway, keywords in gateway_keywords.items():
        for keyword in keywords:
            if keyword.lower() in content_str or keyword.lower() in headers_str or keyword.lower() in html_lower:
                return gateway.capitalize()
    
    return "Unknown"


async def extract_and_store_site_gateway(site_url: str, response_data: dict) -> None:
    """Extract gateway from API response and store in database."""
    from database import save_site_gateway
    
    try:
        content_type = response_data.get('content_type', '')
        headers = response_data.get('headers', {})
        html = response_data.get('html', '')
        cookies = response_data.get('cookies', {})
        
        gateway = check_payment_gateway(content_type, headers, html, cookies)
        
        if gateway and gateway != "Unknown":
            await save_site_gateway(site_url, gateway)
            logging.info(f"[SITECHK] Stored gateway for {site_url}: {gateway}")
    except Exception as e:
        logging.error(f"[SITECHK] Error extracting gateway: {e}")

