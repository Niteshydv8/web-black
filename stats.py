import asyncio
import logging

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AIogram Imports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from aiogram import types, F, Router

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB import — leaderboard now lives in MongoDB Atlas so it
# survives VPS redeploys (mshh.txt / mst.txt were wiped every update).
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from database import get_charged_leaderboard

# Router for this module
router = Router()

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
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_leaderboard_data():
    """
    Fetches the top 10 users ranked by total charged card hits.
    Data is aggregated from the `charged_hits` collection in
    MongoDB Atlas — persistent across VPS redeploys.
    """
    return get_charged_leaderboard(limit=10)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND: /stats
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@router.message(F.text.startswith("/stats"))
async def stats_command(message: types.Message):
    """
    Displays the top 10 users ranked by total Charged cards.
    Pulls data from the charged_hits collection (MongoDB Atlas).
    """
    
    # Create a status message while fetching
    status_msg = await message.answer(
        f"<tg-emoji emoji-id=\"{EMOJI_LIGHTNING}\">⏳</tg-emoji> <b>Fetching Leaderboard...</b>",
        parse_mode="HTML"
    )

    try:
        # Run file reading in thread to prevent blocking
        top_users, user_details = await asyncio.to_thread(get_leaderboard_data)

        # 1. Format the Message
        if not top_users:
            text = (
                f"<b>🏆 𝗟𝗲𝗮𝗱𝗲𝗿𝗯𝗼𝗮𝗿𝗱 (𝗖𝗵𝗮𝗿𝗴𝗲𝗱)</b> <tg-emoji emoji-id=\"{EMOJI_DRAGON}\">🐉</tg-emoji>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "No charged cards found yet.\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                f"<tg-emoji emoji-id=\"{EMOJI_CROWN}\">👑</tg-emoji> <b><a href=\"https://t.me/blacklistedcarder1\">Blacklisted Carder</a></b> <tg-emoji emoji-id=\"{EMOJI_EPIC}\">✨</tg-emoji>"
            )
        else:
            header = (
                f"<b>🏆 𝗟𝗲𝗮𝗱𝗲𝗿𝗯𝗼𝗮𝗿𝗱 (𝗖𝗵𝗮𝗿𝗴𝗲𝗱)</b> <tg-emoji emoji-id=\"{EMOJI_DRAGON}\">🐉</tg-emoji>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
            )

            list_text = ""
            for index, (user_id, count) in enumerate(top_users, 1):
                # Get details safely
                details = user_details.get(user_id, {})
                fname = details.get('first_name', 'Unknown')
                uname = details.get('username')

                # Create a clickable link to the user's profile
                if uname:
                    user_link = f'<a href="https://t.me/{uname}">{fname}</a>'
                else:
                    user_link = f'<a href="tg://user?id={user_id}">{fname}</a>'

                # Rank Medal
                medal = "🥇" if index == 1 else ("🥈" if index == 2 else ("🥉" if index == 3 else f"{index}."))

                list_text += f"{medal} {user_link} ➛ <b>{count}</b> <tg-emoji emoji-id=\"{EMOJI_FIRE}\">🔥</tg-emoji>\n"

            footer = (
                "━━━━━━━━━━━━━━━━━━━━━\n"
                f"<tg-emoji emoji-id=\"{EMOJI_CROWN}\">👑</tg-emoji> <b><a href=\"https://t.me/blacklistedcarder1\">Blacklisted Carder</a></b> <tg-emoji emoji-id=\"{EMOJI_EPIC}\">✨</tg-emoji>"
            )

            text = header + list_text + footer

        # 2. Edit the original message with the result
        await status_msg.edit_text(text, parse_mode="HTML", disable_web_page_preview=True)

    except Exception as e:
        logging.error(f"Error in /stats command: {e}")
        await status_msg.edit_text(f"❌ <b>Error fetching stats.</b> <tg-emoji emoji-id=\"{EMOJI_RED_TICK}\">❌</tg-emoji>", parse_mode="HTML")
