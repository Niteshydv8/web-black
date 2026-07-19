from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.async_telebot import AsyncTeleBot
import requests
import asyncio

BOT_TOKEN = "#Ur Bot Token Here"
bot = AsyncTeleBot(BOT_TOKEN)

def Check_Gateway(content_type, headers, html, cookies):
    Gateway_Keywords = {
        'mollie': ['mollie', 'api.mollie.com', 'mollie.com', 'mollie-payment', 'mollie-checkout', 'mollie-form', 'mollie-sdk', 'mollie-subscription', 'mollie-token', 'mollie-merchant', 'mollie-billing', 'mollie-gateway'],
        'square': ['square', 'squareup.com', 'square-payment', 'square-checkout', 'square-form', 'square-sdk', 'square-subscription', 'square-token', 'square-merchant', 'square-billing', 'square-gateway','connect.squareup.com','connect.squareup.com/v2/analytics','connect.squareup.com/v2/analytics/verifications'],
        'cybersource': ['cybersource', 'cybersource.com', 'cybersource-payment', 'cybersource-checkout', 'cybersource-form', 'cybersource-sdk', 'cybersource-subscription', 'cybersource-token', 'cybersource-merchant', 'cybersource-billing', 'cybersource-gateway'],
        '2checkout': ['2checkout', '2checkout.com', '2checkout-payment', '2checkout-checkout', '2checkout-form', '2checkout-sdk', '2checkout-subscription', '2checkout-token', '2checkout-merchant', '2checkout-billing', '2checkout-gateway'],
        'eway': ['eway', 'eway.com', 'eway-payment', 'eway-checkout', 'eway-form', 'eway-sdk', 'eway-subscription', 'eway-token', 'eway-merchant', 'eway-billing', 'eway-gateway'],
        'stripe': ['stripe', 'checkout.stripe.com', 'js.stripe.com', 'stripe.com', 'stripe-elements', 'stripe-js-v3',
                   'stripe-button', 'stripe-payment', 'stripe-checkout', 'stripe-form', 'stripe-sdk', 'stripe-pay',
                   'stripe-card', 'stripe-subscription', 'stripe-checkout-button', 'stripe-elements', 'stripe-token'],
        'paypal': ['paypal', 'paypal.com', 'smart/buttons.js', 'checkout.js', 'paypal-checkout', 'paypal-button',
                   'paypal-payment', 'paypal-express', 'paypal-form', 'paypal-sdk', 'paypal-checkout-button',
                   'paypal-subscription', 'paypal-token', 'paypal-merchant', 'paypal-billing', 'paypal-braintree'],
        'braintree': ['https://js.braintreegateway.com/js/braintree-2.32.1.min.js','braintree', 'braintreegateway.com', 'braintree-api.com', 'data-braintree-name', 'braintree.js',
                      'braintree-payment', 'braintree-button', 'braintree-form', 'braintree-sdk', 'braintree-checkout',
                      'braintree-subscription', 'braintree-token', 'braintree-merchant', 'braintree-billing'],
        'worldpay': ['worldpay', 'worldpay.com', 'secure.worldpay.com', 'wp-e-commerce', 'worldpay-button',
                     'worldpay-payment', 'worldpay-express', 'worldpay-form', 'worldpay-sdk', 'worldpay-checkout',
                     'worldpay-subscription', 'worldpay-token', 'worldpay-merchant', 'worldpay-billing'],
        'authnet': ['authnet', 'authorize.net', 'authorizenet.com', 'accept-sdk', 'anet', 'authnet-button',
                    'authnet-payment', 'authnet-express', 'authnet-form', 'authnet-sdk', 'authnet-checkout',
                    'authnet-subscription', 'authnet-token', 'authnet-merchant', 'authnet-billing'],
        'recurly': ['recurly', 'recurly.com', 'recurly.js', 'recurly-integration', 'recurly-button', 'recurly-payment',
                    'recurly-checkout', 'recurly-form', 'recurly-sdk', 'recurly-express', 'recurly-subscription',
                    'recurly-token', 'recurly-merchant', 'recurly-billing'],
        'shopify': ['shopify', 'myshopify', 'shopify.com', 'checkout.shopify.com', 'shopify-checkout', 'shopify-payment-button',
                    'shopify-payment', 'shopify-checkout-button', 'shopify-express', 'shopify-form', 'shopify-sdk',
                    'shopify-subscription', 'shopify-token', 'shopify-merchant', 'shopify-billing'],
        'adyen': ['adyen', 'adyen.com', 'adyen-payment', 'adyen-express', 'adyen-form', 'adyen-sdk',
                  'adyen-checkout', 'adyen-subscription', 'adyen-token', 'adyen-merchant', 'adyen-billing'],
    }

    Found_Gateways = []

    for keyword, values in Gateway_Keywords.items():
        if (keyword in str(content_type) or
                any(key in str(headers) or key in html or keyword in str(cookies) for key in values)):
            Found_Gateways.append(keyword.capitalize())

    return Found_Gateways

def Check_Cloudflare(response_text):
    Cloudflare_Markers = [
        'checking your browser', 'cf-ray', 'cloudflare',
        '__cfduid', '__cflb', '__cf_bm', 'cf_clearance'
    ]

    for marker in Cloudflare_Markers:
        if marker in response_text:
            return True

    return False

def Check_Captcha(response_text):
    Captcha_Markers = [
        'recaptcha', 'g-recaptcha', 'data-sitekey',
        'captcha', 'cf_captcha', 'arkoselabs'
    ]

    for marker in Captcha_Markers:
        if marker in response_text:
            return True

    return False

def Check_Graphql(response_text):
    Graphql_Markers = ['graphql', 'application/graphql']

    for marker in Graphql_Markers:
        if marker in response_text:
            return True

    return False

def Check_Platform(response_text):
    Platform_Markers = {
        'woocommerce': ['woocommerce', 'wc-cart', 'wc-ajax'],
        'magento': ['magento', 'mageplaza'],
        'shopify': ['shopify', 'myshopify'],
        'prestashop': ['prestashop', 'addons.prestashop'],
        'opencart': ['opencart', 'route=common/home'],
        'bigcommerce': ['bigcommerce', 'stencil'],
        'wordpress': ['wordpress', 'wp-content'],
        'drupal': ['drupal', 'sites/all'],
        'joomla': ['joomla', 'index.php?option=com_']
    }

    for platform, markers in Platform_Markers.items():
        if any(marker in response_text.lower() for marker in markers):
            return platform.capitalize()

    return None

def Analyze_Site(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        http_status = response.status_code
        html = response.text.lower()
        response_headers = dict(response.headers)
        cookies = response.cookies
        
        payment_gateways = Check_Gateway(response_headers.get('content-type', ''), response_headers, html, cookies)
        cloudflare = Check_Cloudflare(html)
        captcha = Check_Captcha(html)
        graphql = Check_Graphql(html)
        platform = Check_Platform(html)
        
        return {
            'Http_Status': http_status,
            'Gateways': payment_gateways,
            'Cloudflare': cloudflare,
            'Captcha': captcha,
            'Graphql': graphql,
            'Platform': platform,
            'Error': None
        }
        
    except requests.exceptions.RequestException as e:
        return {
            'Http_Status': None,
            'Gateways': [],
            'Cloudflare': False,
            'Captcha': False,
            'Graphql': False,
            'Platform': None,
            'Error': str(e)
        }

@bot.message_handler(commands=['start'])
async def start_command(message):
    text = f"""<b><tg-emoji emoji-id="5042101437237036298">🔗</tg-emoji> Welcome to URL Analyzer!

I can help you analyze websites and detect:

<tg-emoji emoji-id="5800696196192277572">🔍</tg-emoji> Analysis Features:
<tg-emoji emoji-id="5195410990652474023">💳</tg-emoji> Payment Systems
<tg-emoji emoji-id="5330194932781050507">🌟</tg-emoji> Cloudflare Protection
<tg-emoji emoji-id="6147666194351529665">🛡</tg-emoji> Captcha Systems
<tg-emoji emoji-id="5992531713627004633">⌨️</tg-emoji> Platform Detection
<tg-emoji emoji-id="6001546944470587024">📊</tg-emoji> GraphQL Endpoints

<tg-emoji emoji-id="6271512469684883558">💎</tg-emoji> Usage:
• Use /url [link]</b>"""

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton('𝗗𝗲𝘃', url="tg://openmessage?user_id=5191787565", style="primary", icon_custom_emoji_id="5039727497143387500"))
    await bot.reply_to(message, text, reply_markup=markup, parse_mode="HTML")
       
@bot.message_handler(commands=['url'])
async def analyze_url_command(message):
    processing_msg = None
    try:        
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            error_text = f"""<b><tg-emoji emoji-id="5039665997506675838">⚠️</tg-emoji> Invalid Format
            
Usage : /url [site]</b>"""
            await bot.reply_to(message, error_text, parse_mode="HTML")
            return
        
        url = args[1].strip()
        
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
                
        processing_msg = await bot.reply_to(message, f"<b><tg-emoji emoji-id=\"5796283422238314412\">💎</tg-emoji> Analyzing <code>{url}</code></b>", parse_mode="HTML")
        
        result = Analyze_Site(url)
                
        if processing_msg:
            await bot.delete_message(chat_id=message.chat.id, message_id=processing_msg.message_id)
                                       
        if result.get('Error'):
            status_text = f"<tg-emoji emoji-id=\"5981335554225081686\">🔴</tg-emoji> Error"
        elif result['Http_Status'] and 200 <= result['Http_Status'] < 400:
            status_text = f"<tg-emoji emoji-id=\"5981066684977384749\">🟢</tg-emoji> {result['Http_Status']}"
        else:
            status_text = f"<tg-emoji emoji-id=\"5981335554225081686\">🔴</tg-emoji> {result['Http_Status']}" if result['Http_Status'] else "<tg-emoji emoji-id=\"5981335554225081686\">🔴</tg-emoji> Unknown"
        
        payment_text = ', '.join(result['Gateways']) if result['Gateways'] else "None"
        
        captcha_text = "True <tg-emoji emoji-id=\"5039613856603702817\">🙂</tg-emoji>" if result['Captcha'] else "False <tg-emoji emoji-id=\"5834775782433492415\">🔥</tg-emoji>"
        
        cloudflare_text = "True <tg-emoji emoji-id=\"5039613856603702817\">🙂</tg-emoji>" if result['Cloudflare'] else "False <tg-emoji emoji-id=\"5834775782433492415\">🔥</tg-emoji>"
        
        graphql_text = "True <tg-emoji emoji-id=\"5039613856603702817\">🙂</tg-emoji>" if result['Graphql'] else "False <tg-emoji emoji-id=\"5834775782433492415\">🔥</tg-emoji>"
                
        platform_text = result['Platform'] if result['Platform'] else "None"
                        
        result_text = f"""<b><tg-emoji emoji-id=\"5800696196192277572\">🔍</tg-emoji> Website Analysis Result
━━━━━━━━━━━━━
<tg-emoji emoji-id=\"6269566961168944843\">🚀</tg-emoji> URL: <code>{url}</code>
<tg-emoji emoji-id=\"6269566961168944843\">🚀</tg-emoji> Gateways: {payment_text}
<tg-emoji emoji-id=\"6269566961168944843\">🚀</tg-emoji> Captcha: {captcha_text}
<tg-emoji emoji-id=\"6269566961168944843\">🚀</tg-emoji> Cloudflare: {cloudflare_text}
<tg-emoji emoji-id=\"6269566961168944843\">🚀</tg-emoji> GraphQL: {graphql_text}
<tg-emoji emoji-id=\"6269566961168944843\">🚀</tg-emoji> Platform: {platform_text}
<tg-emoji emoji-id=\"6269566961168944843\">🚀</tg-emoji> Status: {status_text}
━━━━━━━━━━━━━
<tg-emoji emoji-id=\"5769547529993588669\">👑</tg-emoji> Dev -> <a href='tg://user?id=5191787565'>𝗔𝗟𝗢𝗡𝗘</a></b>"""
                                        
        await bot.reply_to(message, result_text, parse_mode="HTML")
        
    except Exception as e:            
        await bot.reply_to(message, f"<b>Error: {str(e)}</b>", parse_mode="HTML")

async def main():
    me = await bot.get_me()
    print(f"""
╔══════════════════════════╗
║  ✨ 𝗕𝗢𝗧 𝗜𝗦 𝗥𝗨𝗡𝗡𝗜𝗡𝗚 ✨            ║
╚══════════════════════════╝""")
    await bot.polling(non_stop=True, timeout=60)

if __name__ == "__main__":
    asyncio.run(main())
