"""
Admin API Management Commands
Allows admins to manage API endpoints from the bot
Commands:
- /addapi <name> <endpoint>
- /apis
- /checkapi <name>
- /removeapi <name>
"""

import asyncio
import logging
from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import api_manager
from admin import ADMIN_IDS, EMOJI_LIGHTNING, EMOJI_FIRE, EMOJI_RED_TICK, EMOJI_BLUE_TICK, EMOJI_CROWN

router = Router()

_SEP = "━━━━━━━━━━━━━━━━"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /addapi <name> <endpoint>
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("addapi"))
async def addapi_command(message: types.Message):
    """Admin: /addapi <name> <endpoint> - Add new API"""
    red = f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji>'
    green = f'<tg-emoji emoji-id="{EMOJI_BLUE_TICK}">✅</tg-emoji>'
    
    if message.from_user.id not in ADMIN_IDS:
        await message.reply(f"{red} <b>Admin only</b>")
        return
    
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.reply(
            f"{red} <b>Usage:</b>\n"
            f"<code>/addapi &lt;name&gt; &lt;endpoint&gt;</code>\n\n"
            f"<b>Example:</b>\n"
            f"<code>/addapi shopify https://api.example.com/check</code>"
        )
        return
    
    name = args[1].lower()
    endpoint = args[2]
    
    # Validate endpoint is URL
    if not endpoint.startswith(("http://", "https://")):
        await message.reply(f"{red} <b>Invalid endpoint</b>\nMust start with <code>http://</code> or <code>https://</code>")
        return
    
    # Add to database
    success = await api_manager.add_api(name, endpoint)
    
    if success:
        await message.reply(
            f"{green} <b>API '{name}' added</b>\n"
            f"Endpoint: <code>{endpoint}</code>\n"
            f"Health check performed"
        )
    else:
        await message.reply(
            f"{red} <b>Failed to add API</b>\n"
            f"API '{name}' may already exist"
        )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /apis - List all APIs
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("apis"))
async def apis_command(message: types.Message):
    """Admin: /apis - List all configured APIs with status"""
    crown = f'<tg-emoji emoji-id="{EMOJI_CROWN}">👑</tg-emoji>'
    lightning = f'<tg-emoji emoji-id="{EMOJI_LIGHTNING}">⚡</tg-emoji>'
    
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Admin only")
        return
    
    apis = await api_manager.get_all_apis()
    
    if not apis:
        await message.reply(f"{crown} <b>🔌 APIs</b>\n{_SEP}\n📭 No APIs configured")
        return
    
    lines = [f"{crown} <b>🔌 Configured APIs</b>", _SEP]
    
    for api in apis:
        status_emoji = "🟢" if api["status"] == "UP" else "🔴"
        checked = api.get("last_checked")
        if checked:
            checked_str = checked.strftime("%H:%M:%S")
        else:
            checked_str = "never"
        
        lines.append(
            f"{status_emoji} <b>{api['name'].upper()}</b>\n"
            f"   URL: <code>{api['endpoint']}</code>\n"
            f"   Response: {api.get('response_time_ms', '?')}ms\n"
            f"   Last checked: {checked_str}"
        )
    
    lines.append(_SEP)
    lines.append(f"{lightning} Use <code>/checkapi &lt;name&gt;</code> to check health")
    
    text = "\n".join(lines)
    await message.reply(text, parse_mode="HTML")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /checkapi <name>
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("checkapi"))
async def checkapi_command(message: types.Message):
    """Admin: /checkapi <name> - Health check single API"""
    red = f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji>'
    fire = f'<tg-emoji emoji-id="{EMOJI_FIRE}">🔥</tg-emoji>'
    
    if message.from_user.id not in ADMIN_IDS:
        await message.reply(f"{red} <b>Admin only</b>")
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.reply(f"{red} <b>Usage:</b> <code>/checkapi &lt;name&gt;</code>")
        return
    
    name = args[1].lower()
    
    # Perform health check
    api = await api_manager.check_and_update_api(name)
    
    if not api:
        await message.reply(f"{red} <b>API '{name}' not found</b>")
        return
    
    status_emoji = "🟢" if api["status"] == "UP" else "🔴"
    error_text = f"\n❌ <b>Error:</b> <code>{api.get('error')}</code>" if api.get("error") else ""
    
    text = (
        f"{status_emoji} <b>{api['name'].upper()}</b>\n"
        f"{_SEP}\n"
        f"<b>Endpoint:</b> <code>{api['endpoint']}</code>\n"
        f"<b>Status:</b> <b>{api['status']}</b>\n"
        f"<b>Response Time:</b> <b>{api['response_time_ms']}ms</b>"
        f"{error_text}\n"
        f"{_SEP}\n"
        f"{fire}"
    )
    
    await message.reply(text, parse_mode="HTML")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /removeapi <name>
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(Command("removeapi"))
async def removeapi_command(message: types.Message):
    """Admin: /removeapi <name> - Remove API"""
    red = f'<tg-emoji emoji-id="{EMOJI_RED_TICK}">❌</tg-emoji>'
    green = f'<tg-emoji emoji-id="{EMOJI_BLUE_TICK}">✅</tg-emoji>'
    
    if message.from_user.id not in ADMIN_IDS:
        await message.reply(f"{red} <b>Admin only</b>")
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.reply(f"{red} <b>Usage:</b> <code>/removeapi &lt;name&gt;</code>")
        return
    
    name = args[1].lower()
    success = await api_manager.remove_api(name)
    
    if success:
        await message.reply(f"{green} <b>API '{name}' removed</b>")
    else:
        await message.reply(f"{red} <b>API '{name}' not found</b>")

