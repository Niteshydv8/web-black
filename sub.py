import asyncio
from datetime import datetime, timedelta
import random
import string
import logging
import io

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AIogram Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from aiogram import types, F, Router
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile

router = Router()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIGURATION & IMPORTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
try:
    from database import _premium_cache, _premium_cache_ttl, get_collection
    _USE_POOL = True
except ImportError:
    _USE_POOL = False


def _invalidate_premium_cache(user_id: int):
    """
    Drop the cached premium status for a user. MUST be called after every
    write that changes is_premium / premium_expiry — otherwise the TTL
    cache will keep returning the stale (pre-update) value and the user
    sees their subscription as "not unlocked" right after claiming/paying.
    """
    if _USE_POOL:
        _premium_cache.pop(user_id, None)


def _refresh_premium_cache(user_id: int):
    """
    Drop the cached premium status AND immediately re-read from DB so the
    next caller sees the fresh value. Use this right after a write that
    sets is_premium=1 so the very next /msh call recognises the user as
    premium (instead of waiting for the TTL to expire).
    """
    _invalidate_premium_cache(user_id)
    # Re-query DB and repopulate the cache with the fresh value.
    get_premium_status(user_id)

ADMIN_IDS = {8502412301, 8952038376, 7814400733}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PREMIUM EMOJI IDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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

def e(eid, fallback):
    return f'<tg-emoji emoji-id="{eid}">{fallback}</tg-emoji>'
LOG_CHANNEL_ID = -1003838614236  # fallback numeric — overridden by resolver at startup
LOG_CHANNEL_HANDLE = "@blackulogs"   # public handle resolved to ID on startup

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


def _resolve_user_id_sync(target_input: str):
    """
    Resolve a user_id from either a numeric ID or @username string.
    Tries multiple strategies in order, since usernames can be stored with
    different casing (Telegram preserves original case) or contain regex
    special chars (. $ ^ etc.) that break naive pattern matching.
    """
    if not target_input:
        return None
    s = str(target_input).strip()
    # Numeric ID — direct return
    if s.lstrip("-").isdigit():
        return int(s)
    if s.startswith("@"):
        # Strip the @ but KEEP the original case for exact-match lookups;
        # we'll also try a lowercased variant because the bot may have
        # stored the username lowercased depending on entry path.
        raw_uname = s[1:]
        uname = raw_uname.lower()
        users = get_collection("users")

        # Strategy 1: exact match on the username as-provided (preserves case).
        # This catches users whose DB record has the same casing as Telegram.
        doc = users.find_one({"username": raw_uname}, {"user_id": 1})
        if doc:
            return doc.get("user_id")

        # Strategy 2: exact match on lowercased username.
        # Catches users whose DB record was stored lowercased (e.g. via
        # _ensure_user_for_admin_sync which doesn't normalise case).
        if uname != raw_uname:
            doc = users.find_one({"username": uname}, {"user_id": 1})
            if doc:
                return doc.get("user_id")

        # Strategy 3: case-insensitive regex with PROPER escaping.
        # Previous version used f"^{uname}$" which broke on usernames
        # containing regex special chars (. matches any char, $ anchors
        # end-of-line, ^ anchors start, etc.). `re.escape` makes the
        # pattern literal so usernames like "user.name" match exactly.
        import re as _re
        try:
            pattern = f"^{_re.escape(uname)}$"
            doc = users.find_one(
                {"username": {"$regex": pattern, "$options": "i"}},
                {"user_id": 1},
            )
            if doc:
                return doc.get("user_id")
        except Exception as e:
            logging.debug(f"[resolve_user] regex lookup failed: {e}")
    return None


def _ensure_user_for_admin_sync(user_id: int, username: str, first_name: str):
    """Pre-create a user record so /sub etc. work for users who haven't /start yet."""
    from datetime import datetime
    users = get_collection("users")
    users.update_one(
        {"user_id": user_id},
        {
            "$setOnInsert": {
                "user_id": user_id,
                "username": username or "Unknown",
                "credits": 0,
                "joined_at": datetime.now(),
            },
            "$set": {"first_name": first_name or "User"},
        },
        upsert=True,
    )
    if username:
        users.update_one({"user_id": user_id}, {"$set": {"username": username}})

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATABASE HELPERS (MONGODB)
# All DB functions are synchronous — always call via asyncio.to_thread()
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SYNC LOGIC HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_receipt_id():
    random_str = ''.join(random.choices(string.digits, k=6))
    return f"BLC-{random_str}-PLR"

def mask_receipt_id(receipt_id):
    parts = receipt_id.split('-')
    if len(parts) == 3:
        middle = parts[1]
        if len(middle) >= 2:
            masked_middle = middle[:2] + "XX" + middle[4:]
            return f"{parts[0]}-{masked_middle}-{parts[2]}"
    return receipt_id

def get_premium_status(user_id):
    import time as _t
    if _USE_POOL:
        now = _t.monotonic()
        cached = _premium_cache.get(user_id)
        if cached is not None and now < cached[1]:
            return cached[0]
    col = get_collection("users")
    doc = col.find_one({"user_id": user_id})
    if not doc:
        return False, None
    is_premium_flag = doc.get("is_premium", 0)
    expiry = doc.get("premium_expiry")
    result = (False, None)
    if is_premium_flag == 1:
        if expiry:
            if datetime.now() < expiry:
                result = (True, expiry)
            else:
                col.update_one(
                    {"user_id": user_id},
                    {"$set": {"is_premium": 0, "premium_expiry": None, "credits": 150}}
                )
                result = (False, None)
        else:
            col.update_one({"user_id": user_id}, {"$set": {"is_premium": 0}})
            result = (False, None)
    elif expiry:
        if datetime.now() > expiry:
            col.update_one(
                {"user_id": user_id},
                {"$set": {"premium_expiry": None, "credits": 150}}
            )
            result = (False, None)
    if _USE_POOL:
        _premium_cache[user_id] = (result, _t.monotonic() + _premium_cache_ttl)
    return result

def _sub_db_sync(target_id, display_name, plan_name, days, credits, amount):
    """Full /sub DB update. Returns receipt_id or raises on error."""
    expiry_date = datetime.now() + timedelta(days=days)
    receipt_id = generate_receipt_id()
    purchased_on = datetime.now()
    users = get_collection("users")
    receipts = get_collection("receipts")
    # Upsert user
    users.update_one(
        {"user_id": target_id},
        {"$setOnInsert": {"user_id": target_id, "username": "Unknown", "credits": 0, "joined_at": datetime.now()}},
        upsert=True
    )
    users.update_one({"user_id": target_id}, {"$set": {"first_name": display_name}})
    users.update_one(
        {"user_id": target_id},
        {"$set": {"is_premium": 1, "premium_expiry": expiry_date}}
    )
    # Drop + refresh cached premium status so the user is recognised as
    # premium immediately on their next command — without this the cache
    # keeps returning the stale (False, None) value.
    _refresh_premium_cache(target_id)
    users.update_one({"user_id": target_id}, {"$inc": {"credits": credits}})
    receipts.insert_one({
        "receipt_id": receipt_id,
        "user_id": target_id,
        "plan": plan_name,
        "amount_paid": amount,
        "purchased_on": purchased_on,
        "expires_on": expiry_date
    })
    return receipt_id

def _adcr_db_sync(target_id, display_name, add_credits):
    """Add credits to user. Returns new total."""
    users = get_collection("users")
    users.update_one(
        {"user_id": target_id},
        {"$setOnInsert": {"user_id": target_id, "username": "Unknown", "first_name": display_name, "credits": 0, "joined_at": datetime.now()}},
        upsert=True
    )
    users.update_one({"user_id": target_id}, {"$inc": {"credits": add_credits}})
    doc = users.find_one({"user_id": target_id}, {"credits": 1})
    return doc["credits"] if doc else 0

def _rsub_db_sync(target_id):
    """Remove premium from user, reset credits to 150. Returns user details dict or None."""
    users = get_collection("users")
    users.update_one(
        {"user_id": target_id},
        {"$set": {"is_premium": 0, "premium_expiry": None, "credits": 150}}
    )
    # Drop cached premium status so the user's premium gate goes away
    # immediately instead of waiting up to 60s for the cache to expire.
    _invalidate_premium_cache(target_id)
    return users.find_one({"user_id": target_id}, {"first_name": 1, "username": 1})

def _rc_db_sync(receipt_id):
    """Fetch receipt + user details. Returns dict or None."""
    receipts = get_collection("receipts")
    users = get_collection("users")
    r = receipts.find_one({"receipt_id": receipt_id})
    if not r:
        return None
    u = users.find_one({"user_id": r["user_id"]}, {"first_name": 1, "is_premium": 1, "credits": 1})
    if u:
        r.update({"first_name": u.get("first_name"), "is_premium": u.get("is_premium"), "credits": u.get("credits")})
    return r

def _info_db_sync(user_id, username, first_name):
    """Fetch info row, creating user if missing. Returns (user_dict, receipt_dict)."""
    get_premium_status(user_id)  # resets expired plans
    users = get_collection("users")
    receipts = get_collection("receipts")
    users.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {"user_id": user_id, "username": username, "first_name": first_name, "credits": 0, "joined_at": datetime.now()}},
        upsert=True
    )
    u_row = users.find_one({"user_id": user_id})
    receipt_row = None
    if u_row and u_row.get("is_premium") == 1:
        receipt_row = receipts.find_one(
            {"user_id": user_id},
            sort=[("purchased_on", -1)]
        )
    return u_row, receipt_row

def _suball_db_sync():
    users = get_collection("users")
    receipts = get_collection("receipts")
    now = datetime.now()
    premium_users = list(users.find(
        {"is_premium": 1, "premium_expiry": {"$gt": now}},
        {"user_id": 1, "username": 1, "premium_expiry": 1, "credits": 1}
    ))
    rows = []
    for u in premium_users:
        r = receipts.find_one({"user_id": u["user_id"]}, sort=[("purchased_on", -1)])
        rows.append({
            "user_id": u.get("user_id"),
            "username": u.get("username"),
            "receipt_id": r.get("receipt_id") if r else None,
            "purchased_on": r.get("purchased_on") if r else None,
            "plan": r.get("plan") if r else None,
            "premium_expiry": u.get("premium_expiry"),
            "credits": u.get("credits")
        })
    return rows

def _g_code_db_sync(amount):
    generated = []
    codes = get_collection("codes")
    for _ in range(amount):
        code = "CARD-" + ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        try:
            codes.insert_one({"code": code, "credits": 100, "claimed_by": None, "claimed_at": None})
            generated.append(code)
        except Exception as e:
            logging.error(f"Error generating code: {e}")
    return generated


def _g_access_db_sync(count: int, days: int):
    """
    Generate `count` time-based access codes. Each code, when claimed,
    grants the user `days` days of premium (is_premium=1, premium_expiry=now+days).
    """
    generated = []
    codes = get_collection("codes")
    for _ in range(count):
        code = "ACCESS-" + ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        try:
            codes.insert_one({
                "code": code,
                "type": "access",
                "days": days,
                "claimed_by": None,
                "claimed_at": None,
            })
            generated.append(code)
        except Exception as e:
            logging.error(f"Error generating access code: {e}")
    return generated

def _claim_db_sync(user_id, code):
    """
    Returns one of:
      ("invalid",  None)
      ("claimed",  None)
      ("premium",  None)
      ("ok",       credits_added)
      ("access",   days_granted)
      ("error",    None)
    """
    from datetime import timedelta
    codes = get_collection("codes")
    users = get_collection("users")
    try:
        row = codes.find_one({"code": code})
        if not row:
            return "invalid", None
        if row.get("claimed_by") is not None:
            return "claimed", None
        is_prem, _ = get_premium_status(user_id)
        if is_prem:
            return "premium", None
        # Mark as claimed first (atomic-ish)
        codes.update_one(
            {"code": code},
            {"$set": {"claimed_by": user_id, "claimed_at": datetime.now()}}
        )

        # Time-based access code (ACCESS-XXXX) → set premium for N days
        if row.get("type") == "access":
            days = int(row.get("days", 0) or 0)
            if days <= 0:
                return "error", None
            expiry = datetime.now() + timedelta(days=days)
            # CRITICAL: use upsert=True. Previously this was a plain
            # update_one which silently did nothing if the user record
            # didn't exist (e.g. user /start-ed but DB write failed, or
            # record was deleted). Without upsert the user's premium
            # status was never actually written — the success message
            # printed but DB stayed unchanged.
            users.update_one(
                {"user_id": user_id},
                {
                    "$set": {"is_premium": 1, "premium_expiry": expiry},
                    "$setOnInsert": {
                        "user_id": user_id,
                        "username": "Unknown",
                        "credits": 0,
                        "joined_at": datetime.now(),
                    },
                },
                upsert=True,
            )
            # CRITICAL: drop AND refresh cached premium status so the user
            # is recognised as premium on their very next check — without
            # this the cache keeps returning the stale (False, None) value.
            _refresh_premium_cache(user_id)
            return "access", days

        # Default: legacy credit code (CARD-XXXX)
        credits = int(row.get("credits", 0) or 0)
        users.update_one({"user_id": user_id}, {"$inc": {"credits": credits}})
        return "ok", credits
    except Exception as e:
        logging.error(f"Error claiming code: {e}")
        return "error", None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND: /sub
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/sub"))
async def sub_command(message: types.Message):
    user = message.from_user
    if user.id not in ADMIN_IDS:
        await message.reply("❌ 𝗬𝗼𝘂 𝗮𝗿𝗲 𝗻𝗼𝘁 𝗮𝘂𝘁𝗵𝗼𝗿𝗶𝘇𝗲𝗱 𝘁𝗼 𝘂𝘀𝗲 𝘁𝗵𝗶𝘀.")
        return

    args = message.text.split()[1:]
    if len(args) < 2:
        await message.reply("❌ 𝗨𝘀𝗮𝗴𝗲: /sub {user_id/username} {plan}\nPlans: core, elite, root\nExample: /sub 123456 elite")
        return

    target_input, plan = args[0], args[1].lower()

    plan_map = {
        "core":  ("Core 🛠️",  7,  13000, 10),
        "elite": ("Elite ⭐", 15, 23000, 15),
        "root":  ("Root 👑",  30, 53000, 30),
    }
    if plan not in plan_map:
        await message.reply("❌ 𝗜𝗻𝘃𝗮𝗹𝗶𝗱 𝗽𝗹𝗮𝗻. 𝗣𝗹𝗮𝗻𝘀: <b>Core</b>, <b>Elite</b>, <b>Root</b>", parse_mode="HTML")
        return

    # Resolve target ID in thread (DB lookup first)
    target_id = await asyncio.to_thread(_resolve_user_id_sync, target_input)
    if not target_id and target_input.startswith("@"):
        # DB miss — try Telegram's API as fallback. Telegram resolves
        # usernames case-insensitively so we don't need to lowercase.
        try:
            chat = await message.bot.get_chat(target_input)
            target_id = chat.id
            try:
                await asyncio.to_thread(_ensure_user_for_admin_sync, target_id, chat.username or "", chat.first_name or "User")
            except Exception as db_e:
                logging.error(f"Could not pre-create user record: {db_e}")
        except Exception as tg_e:
            logging.warning(f"Could not resolve @username via Telegram for {target_input}: {tg_e}")
            # Last-ditch: try without the @ prefix in case bot.get_chat
            # is strict about the format.
            try:
                chat = await message.bot.get_chat(target_input.lstrip("@"))
                target_id = chat.id
                try:
                    await asyncio.to_thread(_ensure_user_for_admin_sync, target_id, chat.username or "", chat.first_name or "User")
                except Exception as db_e:
                    logging.error(f"Could not pre-create user record: {db_e}")
            except Exception as tg_e2:
                logging.warning(f"Could not resolve @username via Telegram (no-@) for {target_input}: {tg_e2}")
    if not target_id:
        await message.reply(
            f"❌ 𝗖𝗼𝘂𝗹𝗱 𝗻𝗼𝘁 𝗳𝗶𝗻𝗱 𝘂𝘀𝗲𝗿 <code>{target_input}</code>.\n"
            f"𝗨𝘀𝗲𝗿 𝗺𝘂𝘀𝘁 𝗵𝗮𝘃𝗲 𝘀𝘁𝗮𝗿𝘁𝗲𝗱 𝘁𝗵𝗲 𝗯𝗼𝘁 (or be a public @username), "
            f"𝗼𝗿 𝘂𝘀𝗲 𝘁𝗵𝗲𝗶𝗿 𝗻𝘂𝗺𝗲𝗿𝗶𝗰 𝗜𝗗 𝗶𝗻𝘀𝘁𝗲𝗮𝗱.",
            parse_mode="HTML",
        )
        return

    plan_name, days, credits, amount = plan_map[plan]

    # Fetch display name from Telegram API (non-blocking)
    display_name = "User"
    try:
        chat = await message.bot.get_chat(target_id)
        display_name = chat.first_name or chat.username or "User"
    except Exception:
        pass

    user_link = f'<a href="tg://user?id={target_id}">{display_name}</a>'

    # DB update in thread
    try:
        receipt_id = await asyncio.to_thread(_sub_db_sync, target_id, display_name, plan_name, days, credits, amount)
    except Exception as e:
        logging.error(f"Error updating subscription: {e}")
        await message.reply("❌ 𝗗𝗮𝘁𝗮𝗯𝗮𝘀𝗲 𝗘𝗿𝗿𝗼𝗿.")
        return

    masked_id = mask_receipt_id(receipt_id)

    caption = (
        f"𝐂𝐨𝐧𝐠𝐫𝐚𝐭𝐮𝐥𝐚𝐭𝐢𝐨𝐧𝐬!🎉 𝐲𝐨𝐮𝐫 𝐚𝐜𝐜𝐞𝐬𝐬 𝐡𝐚𝐬 𝐛𝐞𝐞𝐧 𝐚𝐜𝐭𝐢𝐯𝐚𝐭𝐞𝐝.\n"
        f"𝗨𝘀𝗲𝗿 ➛ {user_link}\n"
        f"𝗔𝗰𝗰𝗲𝘀𝘀 ➛ <b>{plan_name}</b>\n"
        f"𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻 ➛ {days} Days\n"
        f"𝗖𝗿𝗲𝗱𝗶𝘁𝘀 𝗔𝗱𝗱𝗲𝗱 ➛ +{credits:,}\n"
        f"𝗥𝗲𝗰𝗲𝗶𝗽𝘁 𝗜𝗗 ➛ <code>{receipt_id}</code>\n"
        f"𝗽𝗹𝗲𝗮𝘀𝗲 𝘀𝗮𝘃𝗲 𝘁𝗵𝗶𝘀 𝗿𝗲𝗰𝗲𝗶𝗽𝘁 𝗜𝗗."
    )
    support_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="𝗦𝘂𝗽𝗽𝗼𝗿𝘁", url="https://t.me/blacklistedcarder1")]
    ])
    log_text = (
        f"<b>NEW PLAN PURCHASED 🛒</b>\n"
        f"<b>User ➛</b> {user_link}\n"
        f"<b>Access ➛</b> <b>{plan_name}</b>\n"
        f"<b>Amount ➛</b> <b>{amount} USD</b>\n"
        f"<b>Receipt ID ➛</b> <code>{masked_id}</code>"
    )
    log_kb = None

    # Send DM to user + log to channel + admin confirm — all concurrently
    async def _dm_user():
        try:
            await message.bot.send_message(chat_id=target_id, text=caption, parse_mode="HTML", reply_markup=support_kb)
        except Exception as e:
            logging.error(f"Could not DM user {target_id}: {e}")

    async def _log_channel():
        try:
            await message.bot.send_message(chat_id=LOG_CHANNEL_ID, text=log_text, parse_mode="HTML", reply_markup=log_kb)
        except Exception as e:
            logging.error(f"Could not log to channel: {e}")

    async def _admin_reply():
        try:
            await message.reply(f"✅ Premium granted to {target_id} for {days} days with {credits:,} credits.")
        except Exception as e:
            logging.error(f"Could not reply to admin: {e}")

    await asyncio.gather(_dm_user(), _log_channel(), _admin_reply())

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND: /adcr
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/adcr"))
async def adcr_command(message: types.Message):
    user = message.from_user
    if user.id not in ADMIN_IDS:
        await message.reply("❌ 𝗬𝗼𝘂 𝗮𝗿𝗲 𝗻𝗼𝘁 𝗮𝘂𝘁𝗵𝗼𝗿𝗶𝘇𝗲𝗱 𝘁𝗼 𝘂𝘀𝗲 𝘁𝗵𝗶𝘀.")
        return

    args = message.text.split()[1:]
    if len(args) < 2:
        await message.reply("❌ 𝗨𝘀𝗮𝗴𝗲: /adcr {user_id} {amount}\nExample: /adcr 123456789 5000")
        return

    target_input, amount_input = args[0], args[1]

    try:
        add_credits = int(amount_input)
        if add_credits <= 0:
            await message.reply("❌ 𝗔𝗺𝗼𝘂𝗻𝘁 𝗺𝘂𝘀𝘁 𝗯𝗲 𝗮 𝗽𝗼𝘀𝗶𝘁𝗶𝘃𝗲 𝗻𝘂𝗺𝗯𝗲𝗿.")
            return
    except ValueError:
        await message.reply("❌ 𝗜𝗻𝘃𝗮𝗹𝗶𝗱 𝗮𝗺𝗼𝘂𝗻𝘁. 𝗣𝗹𝗲𝗮𝘀𝗲 𝗲𝗻𝘁𝗲𝗿 𝗮 𝗻𝘂𝗺𝗯𝗲𝗿.")
        return

    target_id = await asyncio.to_thread(_resolve_user_id_sync, target_input)
    if not target_id and target_input.startswith("@"):
        try:
            chat = await message.bot.get_chat(target_input)
            target_id = chat.id
            try:
                await asyncio.to_thread(_ensure_user_for_admin_sync, target_id, chat.username or "", chat.first_name or "User")
            except Exception as db_e:
                logging.error(f"Could not pre-create user record: {db_e}")
        except Exception as tg_e:
            logging.warning(f"Could not resolve @username via Telegram: {tg_e}")
    if not target_id:
        await message.reply("❌ 𝗖𝗼𝘂𝗹𝗱 𝗻𝗼𝘁 𝗳𝗶𝗻𝗱 𝘂𝘀𝗲𝗿. 𝗨𝘀𝗲𝗿 𝗺𝘂𝘀𝘁 𝗵𝗮𝘃𝗲 𝘀𝘁𝗮𝗿𝘁𝗲𝗱 𝘁𝗵𝗲 𝗯𝗼𝘁 𝗼𝗿 𝗯𝗲 𝗿𝗲𝗮𝗰𝗵𝗮𝗯𝗹𝗲 𝗯𝘆 @username.")
        return

    display_name = "User"
    try:
        chat = await message.bot.get_chat(target_id)
        display_name = chat.first_name or chat.username or "User"
    except Exception:
        pass

    try:
        new_total = await asyncio.to_thread(_adcr_db_sync, target_id, display_name, add_credits)
    except Exception as e:
        logging.error(f"Error adding credits: {e}")
        await message.reply("❌ 𝗗𝗮𝘁𝗮𝗯𝗮𝘀𝗲 𝗘𝗿𝗿𝗼𝗿.")
        return

    user_link = f'<a href="tg://user?id={target_id}">{display_name}</a>'
    user_text = (
        f"𝗖𝗿𝗲𝗱𝗶𝘁𝘀 𝗔𝗱𝗱𝗲𝗱! ✅\n"
        f"𝗨𝘀𝗲𝗿 ➛ {user_link}\n"
        f"𝗖𝗿𝗲𝗱𝗶𝘁𝘀 𝗔𝗱𝗱𝗲𝗱 ➛ <b>+{add_credits:,}</b>\n"
        f"𝗧𝗼𝘁𝗮𝗹 𝗖𝗿𝗲𝗱𝗶𝘁𝘀 ➛ <b>{new_total:,}</b>"
    )
    support_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="𝗦𝘂𝗽𝗽𝗼𝗿𝘁", url="https://t.me/blacklistedcarder1")]
    ])

    async def _dm_user():
        try:
            await message.bot.send_message(chat_id=target_id, text=user_text, parse_mode="HTML", reply_markup=support_kb)
        except Exception as e:
            logging.error(f"Could not DM user {target_id}: {e}")

    await asyncio.gather(
        _dm_user(),
        asyncio.ensure_future(message.reply(
            f"✅ Added <b>{add_credits:,}</b> credits to {user_link}.\n"
            f"𝗡𝗲𝘄 𝗧𝗼𝘁𝗮𝗹 ➛ <b>{new_total:,}</b>",
            parse_mode="HTML"
        )),
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND: /rsub
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/rsub"))
async def rsub_command(message: types.Message):
    user = message.from_user
    if user.id not in ADMIN_IDS:
        await message.reply("❌ 𝗬𝗼𝘂 𝗮𝗿𝗲 𝗻𝗼𝘁 𝗮𝘂𝘁𝗵𝗼𝗿𝗶𝘇𝗲𝗱 𝘁𝗼 𝘂𝘀𝗲 𝘁𝗵𝗶𝘀.")
        return

    args = message.text.split()[1:]
    if not args:
        await message.reply("❌ 𝗨𝘀𝗮𝗴𝗲: /rsub {user_id/username}")
        return

    target_id = await asyncio.to_thread(_resolve_user_id_sync, args[0])
    if not target_id and args[0].startswith("@"):
        try:
            chat = await message.bot.get_chat(args[0])
            target_id = chat.id
            try:
                await asyncio.to_thread(_ensure_user_for_admin_sync, target_id, chat.username or "", chat.first_name or "User")
            except Exception as db_e:
                logging.error(f"Could not pre-create user record: {db_e}")
        except Exception as tg_e:
            logging.warning(f"Could not resolve @username via Telegram: {tg_e}")
    if not target_id:
        await message.reply("❌ 𝗖𝗼𝘂𝗹𝗱 𝗻𝗼𝘁 𝗳𝗶𝗻𝗱 𝘂𝘀𝗲𝗿. 𝗨𝘀𝗲𝗿 𝗺𝘂𝘀𝘁 𝗵𝗮𝘃𝗲 𝘀𝘁𝗮𝗿𝘁𝗲𝗱 𝘁𝗵𝗲 𝗯𝗼𝘁 𝗼𝗿 𝗯𝗲 𝗿𝗲𝗮𝗰𝗵𝗮𝗯𝗹𝗲 𝗯𝘆 @username.")
        return

    try:
        user_details = await asyncio.to_thread(_rsub_db_sync, target_id)
    except Exception as e:
        logging.error(f"Error removing subscription: {e}")
        await message.reply("❌ 𝗗𝗮𝘁𝗮𝗯𝗮𝘀𝗲 𝗘𝗿𝗿𝗼𝗿.")
        return

    if not user_details:
        await message.reply(f"✅ Premium removed from {target_id} and credits reset to 150.")
        return

    display_name = "User"
    temp_name = user_details['first_name'] if user_details['first_name'] else user_details['username']
    if temp_name and temp_name not in ["Unknown", "User"]:
        display_name = temp_name
    else:
        try:
            chat = await message.bot.get_chat(target_id)
            display_name = chat.first_name or "User"
        except Exception:
            pass

    dm_text = (
        f"𝗬𝗼𝘂𝗿 𝗮𝗰𝗰𝗲𝘀𝘀 𝗵𝗮𝘀 𝗯𝗲𝗲𝗻 𝗲𝗻𝗱𝗲𝗱. 𝗧𝗵𝗮𝗻𝗸 𝘆𝗼𝘂 𝗳𝗼𝗿 𝘂𝘀𝗶𝗻𝗴.\n\n"
        f"𝗨𝘀𝗲𝗿 ➛ {display_name}\n"
        f"𝗨𝘀𝗲𝗿 𝗜𝗗 ➛ <code>{target_id}</code>\n"
        f"𝘀𝘁𝗮𝘁𝘂𝘀 ➛ <b>Trial</b>\n"
        f"𝗖𝗿𝗲𝗱𝗶𝘁𝘀 ➛ <b>150</b>"
    )
    buy_kb = None

    async def _dm_user():
        try:
            await message.bot.send_message(chat_id=target_id, text=dm_text, parse_mode="HTML", reply_markup=buy_kb)
        except Exception as e:
            logging.error(f"Could not DM user {target_id}: {e}")

    await asyncio.gather(
        _dm_user(),
        asyncio.ensure_future(message.reply(f"✅ Premium removed from {target_id} and credits reset to 150.")),
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND: /rc
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/rc"))
async def rc_command(message: types.Message):
    user = message.from_user
    if user.id not in ADMIN_IDS:
        await message.reply("❌ 𝗬𝗼𝘂 𝗮𝗿𝗲 𝗻𝗼𝘁 𝗮𝘂𝘁𝗵𝗼𝗿𝗶𝘇𝗲𝗱.")
        return

    args = message.text.split()[1:]
    if not args:
        await message.reply("❌ 𝗨𝘀𝗮𝗴𝗲: /rc {receipt_id}")
        return

    receipt_id = args[0]
    row = await asyncio.to_thread(_rc_db_sync, receipt_id)

    if not row:
        await message.reply("❌ 𝗥𝗲𝗰𝗲𝗶𝗽𝘁 𝗜𝗗 𝗻𝗼𝘁 𝗳𝗼𝘂𝗻𝗱.")
        return

    uid = row['user_id']
    fname = row['first_name']
    if not fname or fname in ["User", "Unknown"]:
        try:
            chat = await message.bot.get_chat(uid)
            fname = chat.first_name or "Unknown"
        except Exception:
            fname = "Unknown"

    user_link = f'<a href="tg://user?id={uid}">{fname}</a>'
    amount_val = row.get('amount_paid') if row.get('amount_paid') is not None else row.get('amount', 0)
    date_str    = row['purchased_on'].strftime('%Y-%m-%d') if row['purchased_on'] else "N/A"
    expires_str = row['expires_on'].strftime('%Y-%m-%d') if row.get('expires_on') else "N/A"
    is_prem_rc, _ = get_premium_status(uid)
    credits_str = "Unlimited" if is_prem_rc else f"{row['credits']:,}"

    text = (
        f"𝗨𝘀𝗲𝗿 ➛ {user_link}\n"
        f"𝗨𝘀𝗲𝗿 𝗜𝗗 ➛ <code>{uid}</code>\n"
        f"𝗔𝗰𝗰𝗲𝘀𝘀 ➛ <b>{row['plan']}</b>\n"
        f"𝗣𝘂𝗿𝗰𝗵𝗮𝘀𝗲𝗱 𝗢𝗻 ➛ {date_str}\n"
        f"𝗘𝘅𝗽𝗶𝗿𝗲𝘀 𝗢𝗻 ➛ <b>{expires_str}</b>\n"
        f"𝗔𝗺𝗼𝘂𝗻𝘁 ➛ {amount_val} USD\n"
        f"𝗖𝘂𝗿𝗿𝗲𝗻𝘁 𝗖𝗿𝗲𝗱𝗶𝘁𝘀 ➛ {credits_str}\n"
        f"𝗥𝗲𝗰𝗲𝗶𝗽𝘁 𝗜𝗗 ➛ <code>{row['receipt_id']}</code>"
    )
    await message.reply(text, parse_mode="HTML")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND: /info
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/info"))
async def info_command(message: types.Message):
    user = message.from_user
    user_id = user.id

    u_row, r_row = await asyncio.to_thread(_info_db_sync, user_id, user.username, user.first_name)

    raw_name = "N/A"
    if user.username:
        raw_name = f"@{user.username}"
    elif user.first_name:
        raw_name = user.first_name
    elif u_row.get('username') and u_row['username'].lower() not in ['unknown', 'user', 'none']:
        raw_name = u_row['username']
    elif u_row.get('first_name') and u_row['first_name'].lower() not in ['unknown', 'user', 'none']:
        raw_name = u_row['first_name']

    username_link = f'<a href="tg://user?id={user_id}">{raw_name}</a>' if raw_name != "N/A" else raw_name

    access_str    = "Trial"
    purchased_str = "N/A"
    ending_str    = "N/A"

    _is_prem = u_row.get('is_premium', 0)
    if _is_prem == 1 and r_row:
        access_str    = r_row.get('plan', 'Premium')
        purchased_str = r_row['purchased_on'].strftime('%Y-%m-%d') if r_row.get('purchased_on') else "N/A"
        if u_row.get('premium_expiry'):
            ending_str = u_row['premium_expiry'].strftime('%Y-%m-%d')
    elif _is_prem == 1:
        access_str = "Premium"

    joined_disp  = u_row['joined_at'].strftime('%Y-%m-%d') if u_row.get('joined_at') else "N/A"
    credits_disp = f"{u_row['credits']:,}"

    # Admins always show as Elite with Unlimited credits
    if user_id in ADMIN_IDS:
        access_str    = "Elite"
        credits_disp  = "Unlimited"
        purchased_str = "Admin"
        ending_str    = "Lifetime"

    text = (
        f"𝗨𝘀𝗲𝗿 ➛ {username_link} <tg-emoji emoji-id=\"{EMOJI_BLUE_TICK}\">✅</tg-emoji>\n"
        f"𝗨𝘀𝗲𝗿 𝗜𝗗 ➛ <code>{user_id}</code>\n"
        f"𝗔𝗰𝗰𝗲𝘀𝘀 ➛ <b>{access_str}</b> <tg-emoji emoji-id=\"{EMOJI_EPIC}\">✨</tg-emoji>\n"
        f"𝗣𝘂𝗿𝗰𝗵𝗮𝘀𝗲𝗱 𝗼𝗻 ➛ <b>{purchased_str}</b> <tg-emoji emoji-id=\"{EMOJI_GUN}\">🔫</tg-emoji>\n"
        f"𝗘𝗻𝗱𝗶𝗻𝗴 𝗼𝗻 ➛ <b>{ending_str}</b> <tg-emoji emoji-id=\"{EMOJI_DRAGON}\">🐉</tg-emoji>\n"
        f"𝗖𝗿𝗲𝗱𝗶𝘁𝘀 ➛ <b>{credits_disp}</b> <tg-emoji emoji-id=\"{EMOJI_FIRE}\">🔥</tg-emoji>\n"
        f"𝗝𝗼𝗶𝗻𝗲𝗱 ➛ <b>{joined_disp}</b> <tg-emoji emoji-id=\"{EMOJI_WHITE_STAR}\">⭐</tg-emoji>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"<tg-emoji emoji-id=\"{EMOJI_CROWN}\">👑</tg-emoji> <b><a href=\"https://t.me/blacklistedcarder1\">Blacklisted Carder</a></b>"
    )
    await message.reply(text, parse_mode="HTML", disable_web_page_preview=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND: /suball
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/suball"))
async def suball_command(message: types.Message):
    user = message.from_user
    if user.id not in ADMIN_IDS:
        return

    rows = await asyncio.to_thread(_suball_db_sync)

    if not rows:
        await message.reply("𝗡𝗼 𝗽𝗿𝗲𝗺𝗶𝘂𝗺 𝘂𝘀𝗲𝗿𝘀 𝗳𝗼𝘂𝗻𝗱.")
        return

    output = io.BytesIO()
    output.write("𝗽𝗿𝗲𝗺𝗶𝘂𝗺 𝘂𝘀𝗲𝗿𝘀 𝗟𝗶𝘀𝘁\n\n".encode('utf-8'))
    for row in rows:
        expiry_date = row['premium_expiry'].strftime('%Y-%m-%d') if row['premium_expiry'] else "N/A"
        line = (
            f"ID: {row['user_id']}\n"
            f"Username: {row['username'] or 'N/A'}\n"
            f"Plan: {row['plan'] or 'N/A'}\n"
            f"Credits: {row['credits']}\n"
            f"Expiry: {expiry_date}\n"
            f"{'-'*30}\n"
        ).encode('utf-8')
        output.write(line)

    filename = f"premium_users_{datetime.now().strftime('%Y%m%d')}.txt"
    document = BufferedInputFile(output.getvalue(), filename=filename)
    await message.reply_document(document=document)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND: /g_code
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/g_code"))
async def g_code_command(message: types.Message):
    user = message.from_user
    if user.id not in ADMIN_IDS:
        await message.reply("❌ 𝗬𝗼𝘂 𝗮𝗿𝗲 𝗻𝗼𝘁 𝗮𝘂𝘁𝗵𝗼𝗿𝗶𝘇𝗲𝗱.")
        return

    args = message.text.split()[1:]
    if not args:
        await message.reply("❌ 𝗨𝘀𝗮𝗴𝗲: /g_code {amount}")
        return

    try:
        amount = int(args[0])
    except ValueError:
        await message.reply("❌ 𝗜𝗻𝘃𝗮𝗹𝗶𝗱 𝗮𝗺𝗼𝘂𝗻𝘁.")
        return

    generated_codes = await asyncio.to_thread(_g_code_db_sync, amount)
    formatted_codes = "\n".join([f"<code>{c}</code>" for c in generated_codes])
    response = (
        f"𝗔𝗺𝗼𝘂𝗻𝘁 ➛ {amount}\n"
        f"𝗖𝗿𝗲𝗱𝗶𝘁𝘀 ➛ 100\n\n"
        f"{formatted_codes}\n\n"
        f"𝗨𝘀𝗲 /𝗰𝗹𝗮𝗶𝗺 𝘁𝗼 𝗴𝗲𝘁 𝘆𝗼𝘂𝗿 𝗰𝗿𝗲𝗱𝗶𝘁𝘀."
    )
    await message.reply(response, parse_mode="HTML")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND: /claim
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/claim"))
async def claim_command(message: types.Message):
    user = message.from_user
    user_id = user.id

    args = message.text.split()[1:]
    if not args:
        await message.reply("❌ 𝗨𝘀𝗮𝗴𝗲: /claim {code}")
        return

    code = args[0].upper()
    status, credits_added = await asyncio.to_thread(_claim_db_sync, user_id, code)

    if status == "invalid":
        await message.reply("❌ 𝗜𝗻𝘃𝗮𝗹𝗶𝗱 𝗖𝗼𝗱𝗲.")
    elif status == "claimed":
        await message.reply("❌ 𝗧𝗵𝗶𝘀 𝗰𝗼𝗱𝗲 𝗵𝗮𝘀 𝗯𝗲𝗲𝗻 𝗮𝗹𝗿𝗲𝗮𝗱𝘆 𝗰𝗹𝗮𝗶𝗺𝗲𝗱.")
    elif status == "premium":
        await message.reply("❌ 𝗨𝘀𝗲𝗿𝘀 𝘄𝗶𝘁𝗵 𝗮𝗻 𝗮𝗰𝘁𝗶𝘃𝗲 𝗽𝗹𝗮𝗻 𝗰𝗮𝗻𝗻𝗼𝘁 𝗿𝗲𝗱𝗲𝗲𝗺 𝗰𝗼𝗱𝗲𝘀.")
    elif status == "ok":
        await message.reply(f"✅ 𝗦𝘂𝗰𝗰𝗲𝘀𝘀𝗳𝘂𝗹𝗹𝘆 𝗖𝗹𝗮𝗶𝗺𝗲𝗱 {credits_added} 𝗰𝗿𝗲𝗱𝗶𝘁𝘀!")
    elif status == "access":
        await message.reply(
            f"{e(EMOJI_BLUE_TICK, '✅')} <b>𝗣𝗿𝗲𝗺𝗶𝘂𝗺 𝗮𝗰𝗰𝗲𝘀𝘀 𝗮𝗰𝘁𝗶𝘃𝗮𝘁𝗲𝗱 𝗳𝗼𝗿 {credits_added} 𝗱𝗮𝘆𝘀!</b>\n"
            f"{e(EMOJI_CROWN, '👑')} 𝗔𝗹𝗹 𝗴𝗮𝘁𝗲𝘀 𝘂𝗻𝗹𝗼𝗰𝗸𝗲𝗱.\n"
            f"{e(EMOJI_FIRE, '🔥')} 𝗘𝗻𝗷𝗼𝘆 {e(EMOJI_WHITE_STAR, '⭐')}",
            parse_mode="HTML",
        )
    else:
        await message.reply("❌ 𝗘𝗿𝗿𝗼𝗿 𝗖𝗹𝗮𝗶𝗺𝗶𝗻𝗴 𝗰𝗼𝗱𝗲. 𝗖𝗼𝗻𝘁𝗮𝗰𝘁 𝘀𝘂𝗽𝗽𝗼𝗿𝘁.")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND: /gencodes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/gencodes"))
async def g_access_command(message: types.Message):
    """
    /gencodes <count> <days>
      count = how many codes to generate
      days  = how many days of premium each code grants (1, 7, 30, ...)
    Each code is one-time-use; claiming sets is_premium=1 + premium_expiry=now+days.
    """
    user = message.from_user
    if user.id not in ADMIN_IDS:
        await message.reply("❌ 𝗬𝗼𝘂 𝗮𝗿𝗲 𝗻𝗼𝘁 𝗮𝘂𝘁𝗵𝗼𝗿𝗶𝘇𝗲𝗱.")
        return

    args = message.text.split()[1:]
    if len(args) < 2:
        await message.reply(
            "❌ 𝗨𝘀𝗮𝗴𝗲: <code>/gencodes &lt;count&gt; &lt;days&gt;</code>\n"
            "𝗘𝘅𝗮𝗺𝗽𝗹𝗲: <code>/gencodes 10 1</code>  →  10 codes, 1 day each",
            parse_mode="HTML",
        )
        return

    try:
        count = int(args[0])
        days = int(args[1])
    except ValueError:
        await message.reply("❌ 𝗜𝗻𝘃𝗮𝗹𝗶𝗱 𝗻𝘂𝗺𝗯𝗲𝗿𝘀. 𝗕𝗼𝘁𝗵 𝗮𝗿𝗴𝘀 𝗺𝘂𝘀𝘁 𝗯𝗲 𝗶𝗻𝘁𝗲𝗴𝗲𝗿𝘀.")
        return

    if count <= 0 or count > 500:
        await message.reply("❌ 𝗖𝗼𝘂𝗻𝘁 𝗺𝘂𝘀𝘁 𝗯𝗲 𝗯𝗲𝘁𝘄𝗲𝗲𝗻 𝟭 𝗮𝗻𝗱 𝟱𝟬𝟬.")
        return
    if days <= 0 or days > 3650:
        await message.reply("❌ 𝗗𝗮𝘆𝘀 𝗺𝘂𝘀𝘁 𝗯𝗲 𝗯𝗲𝘁𝘄𝗲𝗲𝗻 𝟭 𝗮𝗻𝗱 𝟯𝟲𝟱𝟬 (𝟭𝟬 𝘆𝗲𝗮𝗿𝘀).")
        return

    generated_codes = await asyncio.to_thread(_g_access_db_sync, count, days)

    if not generated_codes:
        await message.reply("❌ 𝗙𝗮𝗶𝗹𝗲𝗱 𝘁𝗼 𝗴𝗲𝗻𝗲𝗿𝗮𝘁𝗲 𝗰𝗼𝗱𝗲𝘀. 𝗖𝗵𝗲𝗰𝗸 𝗹𝗼𝗴𝘀.")
        return

    formatted_codes = "\n".join([f"<code>{c}</code>" for c in generated_codes])
    day_word = "𝗱𝗮𝘆" if days == 1 else "𝗱𝗮𝘆𝘀"
    code_word = "𝗰𝗼𝗱𝗲" if count == 1 else "𝗰𝗼𝗱𝗲𝘀"
    response = (
        f"{e(EMOJI_CROWN, '👑')} <b>𝗔𝗰𝗰𝗲𝘀𝘀 𝗖𝗼𝗱𝗲𝘀 𝗚𝗲𝗻𝗲𝗿𝗮𝘁𝗲𝗱</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{e(EMOJI_FIRE, '🔥')} 𝗔𝗺𝗼𝘂𝗻𝘁 ➛ <b>{count}</b> {code_word}\n"
        f"{e(EMOJI_LIGHTNING, '⚡')} 𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻 ➛ <b>{days}</b> {day_word} 𝗲𝗮𝗰𝗵\n"
        f"{e(EMOJI_EPIC, '✨')} 𝗧𝘆𝗽𝗲 ➛ 𝗣𝗿𝗲𝗺𝗶𝘂𝗺 𝗮𝗰𝗰𝗲𝘀𝘀 (all gates unlocked)\n"
        f"━━━━━━━━━━━━━━━━━━━\n\n"
        f"{formatted_codes}\n\n"
        f"{e(EMOJI_WHITE_STAR, '⭐')} 𝗨𝘀𝗲𝗿𝘀 𝗿𝗲𝗱𝗲𝗲𝗺 𝘄𝗶𝘁𝗵 <code>/claim ACCESS-XXXX</code>"
    )
    await message.reply(response, parse_mode="HTML")
