import sub
# Simulate the gencodes response template
count, days = 3, 7
day_word = "day" if days == 1 else "days"
code_word = "code" if count == 1 else "codes"
sample_codes = ["ACCESS-ABC123XYZ0", "ACCESS-DEF456WXY1", "ACCESS-GHI789ZAB2"]
formatted_codes = "\n".join([f"<code>{c}</code>" for c in sample_codes])

response = (
    f"{sub.e(sub.EMOJI_CROWN, 'Crown')} <b>Access Codes Generated</b>\n"
    f"-------------------\n"
    f"{sub.e(sub.EMOJI_FIRE, 'Fire')} Amount: <b>{count}</b> {code_word}\n"
    f"{sub.e(sub.EMOJI_LIGHTNING, 'Lightning')} Duration: <b>{days}</b> {day_word} each\n"
    f"{sub.e(sub.EMOJI_EPIC, 'Epic')} Type: Premium access (all gates unlocked)\n"
    f"-------------------\n\n"
    f"{formatted_codes}\n\n"
    f"{sub.e(sub.EMOJI_WHITE_STAR, 'Star')} Users redeem with <code>/claim ACCESS-XXXX</code>"
)

# Simulate /claim access success response
claim_response = (
    f"{sub.e(sub.EMOJI_BLUE_TICK, 'Tick')} <b>Premium access activated for 7 days!</b>\n"
    f"{sub.e(sub.EMOJI_CROWN, 'Crown')} All gates unlocked.\n"
    f"{sub.e(sub.EMOJI_FIRE, 'Fire')} Enjoy {sub.e(sub.EMOJI_WHITE_STAR, 'Star')}"
)

with open('_tmp_prem.txt', 'w', encoding='utf-8') as f:
    f.write("=== /gencodes 3 7 ===\n")
    f.write(response)
    f.write("\n\n=== /claim ACCESS-XXX (success) ===\n")
    f.write(claim_response)
