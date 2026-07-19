import asyncio
import logging
from typing import Callable, Dict, Any, Awaitable
from aiogram import Router, types, BaseMiddleware
from aiogram.filters import Command
from database import get_collection

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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

router = Router()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SYNC DB HELPERS (run via asyncio.to_thread)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _is_banned_sync(user_id: int) -> bool:
    col = get_collection("banned_users")
    return col.find_one({"user_id": user_id}) is not None

def _ban_user_sync(user_id: int):
    col = get_collection("banned_users")
    col.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {"user_id": user_id, "banned_at": __import__("datetime").datetime.now()}},
        upsert=True
    )

def _unban_user_sync(user_id: int) -> bool:
    col = get_collection("banned_users")
    result = col.delete_one({"user_id": user_id})
    return result.deleted_count > 0

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /ban COMMAND   —  /ban 123456789
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.message(Command("ban"))
async def ban_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("⛔ Admin only.")
        return

    parts = message.text.split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.reply("Usage: /ban [user_id]")
        return

    target_id = int(parts[1])
    if target_id in ADMIN_IDS:
        await message.reply("❌ Cannot ban an admin.")
        return

    await asyncio.to_thread(_ban_user_sync, target_id)
    await message.reply(
        f"🚫 User <code>{target_id}</code> has been <b>banned</b>.",
        parse_mode="HTML"
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /unban COMMAND  —  /unban 123456789
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@router.message(Command("unban"))
async def unban_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("⛔ Admin only.")
        return

    parts = message.text.split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.reply("Usage: /unban [user_id]")
        return

    target_id = int(parts[1])
    removed = await asyncio.to_thread(_unban_user_sync, target_id)

    if removed:
        await message.reply(
            f"✅ User <code>{target_id}</code> has been <b>unbanned</b>.",
            parse_mode="HTML"
        )
    else:
        await message.reply(
            f"⚠️ User <code>{target_id}</code> is <b>not</b> in the ban list.",
            parse_mode="HTML"
        )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BAN MIDDLEWARE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BAN_WARNING = (
    "🚫 <b>𝗬𝗼𝘂 𝗵𝗮𝘃𝗲 𝗯𝗲𝗲𝗻 𝗯𝗮𝗻𝗻𝗲𝗱 𝗳𝗿𝗼𝗺 𝘂𝘀𝗶𝗻𝗴 𝘁𝗵𝗶𝘀 𝗯𝗼𝘁.</b>\n\n"
    "𝗜𝗳 𝘆𝗼𝘂 𝗯𝗲𝗹𝗶𝗲𝘃𝗲 𝘁𝗵𝗶𝘀 𝗶𝘀 𝗮 𝗺𝗶𝘀𝘁𝗮𝗸𝗲, 𝗰𝗼𝗻𝘁𝗮𝗰𝘁 𝘀𝘂𝗽𝗽𝗼𝗿𝘁."
)

class BanMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[types.Message, Dict[str, Any]], Awaitable[Any]],
        event: types.Message,
        data: Dict[str, Any]
    ) -> Any:
        user = event.from_user
        if not user or user.id in ADMIN_IDS:
            return await handler(event, data)

        banned = await asyncio.to_thread(_is_banned_sync, user.id)
        if banned:
            try:
                await event.reply(BAN_WARNING, parse_mode="HTML")
            except Exception:
                pass
            return  # stop processing

        return await handler(event, data)
