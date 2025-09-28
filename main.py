import logging
import asyncio
import json
import os
import re
from datetime import datetime
from typing import Optional, Tuple, List

from langdetect import detect, LangDetectException

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, ConversationHandler, filters, AIORateLimiter
)
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

# --- Configuration ---
TELEGRAM_BOT_TOKEN = "8406044266:AAGjItZuDNzNiSMYdTODvCjF4NLj9wW6JnI"
ADMIN_USER_IDS = [7921994434]
ADMIN_GROUP_ID = -4812708792
YOUR_PUBLIC_ADMIN_ID = "@fack_baby_fack"

# --- File-based Data Storage ---
DATA_FOLDER = "data"
BOT_DATA_FILE = os.path.join(DATA_FOLDER, "bot_data.json")

# --- Conversation States ---
(
    LOGIN, MAIN_MENU, ADMIN_PANEL, AWAITING_USERID_TO_AUTH, AWAITING_USERID_TO_UNAUTH,
    AWAITING_BOT_LINK, AWAITING_CHANNEL_USERNAME, AWAITING_CHANNEL_VERIFICATION,
    ADMIN_ADD_PANEL, AWAITING_USERID_TO_ADD_ADMIN, AWAITING_USERID_TO_REMOVE_ADMIN,
    SELECT_AUTH_ACTION, AWAITING_DATA_UPLOAD
) = range(13)

# --- Logging & Sessions ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
user_sessions = {}

# --- Helper Functions for Centralized Data ---
def load_bot_data():
    """Loads all data from the centralized JSON file."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    
    default_data = {
        "authorized_users": list(ADMIN_USER_IDS),
        "all_users": [],
        "bot_link": "https://t.me/wsotp200bot?start=u",
        "channel_username": "BotAccessTask",
        "admin_user_ids": list(ADMIN_USER_IDS)
    }

    if not os.path.exists(BOT_DATA_FILE):
        return default_data

    try:
        with open(BOT_DATA_FILE, 'r') as f:
            data = json.load(f)
            for key, value in default_data.items():
                if key not in data:
                    data[key] = value
            if 'banned_users' in data:
                del data['banned_users']
            return data
    except (json.JSONDecodeError, FileNotFoundError):
        logging.warning(f"Could not load or parse {BOT_DATA_FILE}. Using default data.")
        return default_data

def save_bot_data(data):
    """Saves all data to the centralized JSON file."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    default_data = {
        "authorized_users": list(ADMIN_USER_IDS),
        "all_users": [],
        "bot_link": "https://t.me/wsotp200bot?start=u",
        "channel_username": "BotAccessTask",
        "admin_user_ids": list(ADMIN_USER_IDS)
    }
    for key, value in default_data.items():
        if key not in data:
            data[key] = value
            
    with open(BOT_DATA_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def escape_markdown_v2(text: str) -> str:
    """Escapes characters for Telegram's MarkdownV2 parse mode."""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def extract_credentials(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Extracts Twilio SID and Token from text using regex, supporting various separators."""
    match = re.search(r'(AC[a-fA-F0-9]{32})[\s,:_-]+([a-fA-F0-9]{32})', text)
    if match:
        return match.group(1), match.group(2)
    return None, None

def parse_user_ids(text: str) -> List[int]:
    """Extracts all numbers from a string, treating them as user IDs."""
    return [int(uid) for uid in re.findall(r'\d+', text)]

async def remove_previous_inline_keyboard(context: ContextTypes.DEFAULT_TYPE):
    last_message_info = context.user_data.pop('last_inline_message', None)
    if last_message_info:
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=last_message_info['chat_id'],
                message_id=last_message_info['message_id'],
                reply_markup=None
            )
        except Exception as e:
            logging.info(f"Could not remove previous inline keyboard (it may have been deleted): {e}")


# --- Keyboard Menus ---
def get_main_menu(user_id):
    bot_data = load_bot_data()
    admins = bot_data.get('admin_user_ids', [])
    menu = [
        [KeyboardButton("📩 Message"), KeyboardButton("📞 My Number")],
        [KeyboardButton("🚪 Logout"), KeyboardButton("🚀 Start")]
    ]
    if user_id in admins:
        menu.insert(0, [KeyboardButton('👑 Admin Panel')])
    return ReplyKeyboardMarkup(menu, resize_keyboard=True)

def get_admin_menu(user_id):
    buttons = [
        [KeyboardButton('✅ GIVE ACCESS')],
        [KeyboardButton('👥 USER LIST'), KeyboardButton('D/U'), KeyboardButton('⚙️ SETTINGS')],
    ]
    if user_id in ADMIN_USER_IDS: 
        buttons.append([KeyboardButton('➕ Admin Add')])
    buttons.append([KeyboardButton('⬅️ Back to Main Menu')])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def get_admin_add_menu():
    return ReplyKeyboardMarkup([
        [KeyboardButton('➕ Add New Admin'), KeyboardButton('➖ Remove Admin')],
        [KeyboardButton('⬅️ Back to Admin Panel')]
    ], resize_keyboard=True)

def get_admin_settings_menu():
    return ReplyKeyboardMarkup([
        [KeyboardButton('📝 Change Bot Link'), KeyboardButton('📝 Change Channel Username')],
        [KeyboardButton('⬅️ Back to Admin Panel')]
    ], resize_keyboard=True)

def get_start_only_menu():
    return ReplyKeyboardMarkup([[KeyboardButton("🚀 Start")]], resize_keyboard=True)

# --- Authorization & Core Logic ---
async def check_subscription(user_id, bot):
    bot_data = load_bot_data()
    channel_username = bot_data.get("channel_username")
    admins = bot_data.get("admin_user_ids", [])
    if user_id in admins: 
        return True
    if not channel_username: 
        return True 
    try:
        member = await bot.get_chat_member(f"@{channel_username}", user_id)
        return member.status in ['member', 'creator', 'administrator']
    except Exception as e:
        logging.error(f"Error checking subscription for user {user_id}: {e}")
        return False

# --- Login, Start & Authentication ---
async def _login_user(update: Update, context: ContextTypes.DEFAULT_TYPE, sid: str, token: str):
    user_id = update.effective_user.id
    try:
        processing_msg = await update.message.reply_text("🔐 Verifying your credentials and fetching numbers, please wait...")
        client = await asyncio.to_thread(Client, sid, token)
        await asyncio.to_thread(lambda: client.api.v2010.accounts(sid).fetch())
        incoming_numbers = await asyncio.to_thread(client.incoming_phone_numbers.list)
        active_numbers = [num.phone_number for num in incoming_numbers]
        last_number = active_numbers[0] if active_numbers else None
        user_sessions[user_id] = {
            'client': client, 'sid': sid, 'token': token, 'status': 'active',
            'purchased_count': 0, 'total_sms_received': 0, 
            'active_numbers': active_numbers,
            'last_number': last_number,
            'shown_sms_sids': set()
        }
        await processing_msg.edit_text('✅ Login successful!')
        await update.message.reply_text(
            'Welcome to the main menu.',
            reply_markup=get_main_menu(user_id)
        )
        return MAIN_MENU
    except TwilioRestException as e:
        logging.warning(f"Failed login for user {user_id} with SID {sid}. Error: {e}")
        await processing_msg.delete()
        await update.message.reply_text(
            'This SID is banned or invalid.',
            reply_markup=get_main_menu(user_id) if user_id in load_bot_data().get("authorized_users", []) else ReplyKeyboardRemove()
        )
        current_state = context.user_data.get('state')
        return current_state if current_state else MAIN_MENU
    except Exception as e:
        logging.error(f"Unexpected error during login for user {user_id}: {e}")
        await processing_msg.delete()
        await update.message.reply_text('An unexpected error occurred. Please try again later.')
        return ConversationHandler.END

async def receive_credentials(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sid, token = extract_credentials(update.message.text)
    if not (sid and token):
        await update.message.reply_text(
            'Invalid format\\. Please send your SID and Token separated by a space or other separator\\.',
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return LOGIN
    return await _login_user(update, context, sid, token)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    bot_data = load_bot_data()

    # Add user to the list of all users who have interacted with the bot
    if user_id not in bot_data.get("all_users", []):
        bot_data.setdefault("all_users", []).append(user_id)
        save_bot_data(bot_data)

    # --- 1. Admin Check ---
    # Admins get direct access to the main menu.
    admins = bot_data.get('admin_user_ids', [])
    if user_id in admins:
        await update.message.reply_text("Welcome back, Admin.", reply_markup=get_main_menu(user_id))
        return MAIN_MENU

    # --- 2. Subscription Check (for all non-admins) ---
    # Check if the user is subscribed to the required channel.
    is_subscribed = await check_subscription(user_id, context.bot)
    if not is_subscribed:
        channel_username = bot_data.get('channel_username', 'your_channel')
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton('🔗 Join Channel', url=f"https://t.me/{channel_username}")],
            [InlineKeyboardButton('✅ I have joined', callback_data='verify_join')]
        ])
        await update.message.reply_text(
            f"🚫 To continue, please join our channel: @{channel_username}",
            reply_markup=kb
        )
        return AWAITING_CHANNEL_VERIFICATION

    # --- 3. Authorization Check (for subscribed users) ---
    # If the user is subscribed, now check if they are authorized by an admin.
    authorized_users = bot_data.get("authorized_users", [])
    if user_id not in authorized_users:
        # User is subscribed but not on the authorized list.
        unauthorized_text = (
            "You are not authorized to use this bot\\. Please contact the admin: {admin_id_public}"
        ).format(
            admin_id_public=escape_markdown_v2(YOUR_PUBLIC_ADMIN_ID)
        )
        await update.message.reply_text(
            unauthorized_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=ReplyKeyboardRemove()
        )
        # Notify admins that a subscribed user is waiting for authorization.
        try:
            user_info = update.effective_user
            username = f"@{user_info.username}" if user_info.username else "Not available"
            notification_text = (
                f"User joined channel but is awaiting authorization:\n"
                f"Username: {username}\n"
                f"User ID: `{user_info.id}`" # Using backticks for easy copy
            )
            await context.bot.send_message(
                chat_id=ADMIN_GROUP_ID,
                text=notification_text,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except Exception as e:
            logging.error(f"Failed to send unauthorized (but subscribed) notification: {e}")
        return ConversationHandler.END

    # --- 4. Proceed to Login (for subscribed and authorized users) ---
    await update.message.reply_text(
        '👋 Welcome\\! Please send your Twilio SID and Auth Token to log in, like so:\\n\\n`ACxxxxxxxxxxxxxxxxx xxxxxxxxxxxxxxxxx`',
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=ReplyKeyboardRemove()
    )
    return LOGIN


async def main_menu_or_number_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await remove_previous_inline_keyboard(context)
    user_id = update.effective_user.id
    text = update.message.text
    context.user_data['state'] = MAIN_MENU
    bot_data = load_bot_data()
    admins = bot_data.get('admin_user_ids', [])
    if user_id in admins and text == '👑 Admin Panel':
        await update.message.reply_text("Welcome to the Admin Panel.", reply_markup=get_admin_menu(user_id))
        return ADMIN_PANEL
    if text == "🚪 Logout":
        return await logout(update, context)
    if text == "🚀 Start":
        return await start(update, context)
    sid, token = extract_credentials(text)
    if sid and token:
        return await _login_user(update, context, sid, token)
    sess = user_sessions.get(user_id)
    if not sess:
        await update.message.reply_text('Your session has expired. Please /start again or send your credentials to log in.')
        return ConversationHandler.END
    if text == "📩 Message":
        await show_sms(update, context)
        return MAIN_MENU
    if text == "📞 My Number":
        await my_number(update, context)
        return MAIN_MENU
    if sess.get('status') == 'banned':
        purchased_count = sess.get('purchased_count', 0)
        sms_count = sess.get('total_sms_received', 0)
        ban_message = (
            "Your SID-Token Got banned please login via new SID_TOKEN.\n"
            f"Numbers Purchased:{purchased_count}\n"
            f"sms receved:{sms_count}"
        )
        await update.message.reply_text(ban_message, reply_markup=get_start_only_menu())
        if user_id in user_sessions: del user_sessions[user_id]
        return ConversationHandler.END
    numbers_found = re.findall(r'(\+?\d{10,15})', text)
    if numbers_found:
        for num in numbers_found:
            safe_num_text = escape_markdown_v2(num)
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(f'🛒 Buy {num}', callback_data=f'BUY:{num}')]])
            await update.message.reply_text(f"Number Found: `{safe_num_text}`", reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)
        return MAIN_MENU
    await update.message.reply_text("Unrecognized command. Please use a menu button or send credentials/phone number.", reply_markup=get_main_menu(user_id))
    return MAIN_MENU

async def my_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    sess = user_sessions.get(user_id)
    if not sess or 'client' not in sess:
        await update.message.reply_text('Your session has expired. Please /start again or send your credentials.')
        return
    client = sess['client']
    try:
        await update.message.reply_text("🔍 Checking your account for active numbers...")
        incoming_numbers = await asyncio.to_thread(client.incoming_phone_numbers.list)
        if not incoming_numbers:
            await update.message.reply_text("You don't have any active numbers on this Twilio account.")
            return
        sess['active_numbers'] = [num.phone_number for num in incoming_numbers]
        if incoming_numbers: sess['last_number'] = incoming_numbers[0].phone_number
        response_text = "Your active number\\(s\\) on this account:\n\n"
        for number_obj in incoming_numbers:
            safe_number = escape_markdown_v2(number_obj.phone_number)
            response_text += f"➡️ `{safe_number}`\n"
        await update.message.reply_text(response_text, parse_mode=ParseMode.MARKDOWN_V2)
    except TwilioRestException as e:
        logging.error(f"Twilio API error fetching numbers for user {user_id}: {e}")
        await update.message.reply_text("❌ Could not fetch your numbers due to a Twilio API error.")
    except Exception as e:
        logging.error(f"Unexpected error in my_number for user {user_id}: {e}")
        await update.message.reply_text("An unexpected system error occurred.")

def get_lang_name(code: str) -> str:
    lang_map = {
        'af': 'Afrikaans', 'ar': 'Arabic', 'bg': 'Bulgarian', 'bn': 'Bengali', 'ca': 'Catalan',
        'cs': 'Czech', 'cy': 'Welsh', 'da': 'Danish', 'de': 'German', 'el': 'Greek', 'en': 'English',
        'es': 'Spanish', 'et': 'Estonian', 'fa': 'Persian', 'fi': 'Finnish', 'fr': 'French',
        'gu': 'Gujarati', 'he': 'Hebrew', 'hi': 'Hindi', 'hr': 'Croatian', 'hu': 'Hungarian',
        'id': 'Indonesian', 'it': 'Italian', 'ja': 'Japanese', 'kn': 'Kannada', 'ko': 'Korean',
        'lt': 'Lithuanian', 'lv': 'Latvian', 'mk': 'Macedonian', 'ml': 'Malayalam', 'mr': 'Marathi',
        'ne': 'Nepali', 'nl': 'Dutch', 'no': 'Norwegian', 'pa': 'Punjabi', 'pl': 'Polish',
        'pt': 'Portuguese', 'ro': 'Romanian', 'ru': 'Russian', 'sk': 'Slovak', 'sl': 'Slovenian',
        'so': 'Somali', 'sq': 'Albanian', 'sv': 'Swedish', 'sw': 'Swahili', 'ta': 'Tamil',
        'te': 'Telugu', 'th': 'Thai', 'tl': 'Tagalog', 'tr': 'Turkish', 'uk': 'Ukrainian',
        'ur': 'Urdu', 'vi': 'Vietnamese', 'zh-cn': 'Chinese (Simplified)', 'zh-tw': 'Chinese (Traditional)'
    }
    return lang_map.get(code, "Unknown")

def _extract_and_format_code(body: str) -> Optional[str]:
    """Finds common OTP/code patterns in a message body and formats a copyable line."""
    # Pattern for codes like "123-456", "123 456", or "123456" (and other lengths)
    # This looks for a sequence of 4 to 8 digits, possibly split by a single space or hyphen.
    match = re.search(r'\b(\d{3})[\s-]?(\d{3})\b|\b(\d{4,8})\b', body)
    
    if match:
        raw_code = match.group(0)
        # Remove any spaces or hyphens to create a clean, copyable code
        clean_code = re.sub(r'[\s-]', '', raw_code)
        # Format for one-click copy in Telegram
        return f'Copy Code : `{clean_code}`'
        
    return None

def format_sms_text(messages: list, number: str) -> str:
    """Formats a list of SMS messages according to the specified layout."""
    if not messages:
        return f'{escape_markdown_v2(number)} Active\n\n📭 No SMS received yet\\.'

    message_lines = []
    # Sort messages from newest to oldest
    for msg in sorted(messages, key=lambda m: m.date_sent or datetime.min, reverse=True):
        sent_date = msg.date_sent.strftime("%Y-%m-%d %H:%M") if msg.date_sent else "Unknown"
        
        lang_name = "N/A"
        try:
            if msg.body:
                lang_code = detect(msg.body)
                lang_name = get_lang_name(lang_code)
        except LangDetectException:
            lang_name = "Unknown"
            
        # Build the main message block
        main_block = (
            f'MSG lan: {escape_markdown_v2(lang_name)}\n'
            f'Date: {escape_markdown_v2(sent_date)}\n'
            f'{escape_markdown_v2(msg.body)}'
        )
        
        # Check for a code and create the copyable line if found
        copy_line = _extract_and_format_code(msg.body)
        
        if copy_line:
            # Add the copyable code line to the message block
            full_message = f'{main_block}\n\n{copy_line}'
            message_lines.append(full_message)
        else:
            message_lines.append(main_block)

    # Join all formatted messages with a separator
    all_messages_text = '\n\n---\n\n'.join(message_lines)
    
    # Combine the header with the formatted messages
    final_text = (
        f'{escape_markdown_v2(number)} Active\n\n'
        f'{all_messages_text}'
    )
    return final_text


async def show_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    sess = user_sessions.get(user_id)
    if not sess or 'client' not in sess:
        await update.message.reply_text('Your session has expired. Please /start again or send your credentials.')
        return
    last_number = sess.get('last_number')
    if not last_number:
        await update.message.reply_text('❌ No active number found. Please purchase one first or use "My Number" to check your account.')
        return
    client = sess['client']
    try:
        await update.message.reply_text(f"🔄 Checking messages for `{escape_markdown_v2(last_number)}`\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)
        messages = await asyncio.to_thread(client.messages.list, to=last_number, limit=20)
        text = format_sms_text(messages, last_number)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton('🔄 Refresh', callback_data=f'CHECKSMS:{last_number}'), InlineKeyboardButton('🗑️ Close', callback_data='DELETE_MSG')]
        ])
        sent_message = await update.message.reply_text(text=text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)
        context.user_data['last_inline_message'] = {'chat_id': sent_message.chat_id, 'message_id': sent_message.message_id}
    except TwilioRestException as e:
        logging.error(f"Twilio API error in show_sms for user {user_id}: {e}")
        await update.message.reply_text("❌ Could not check messages due to a Twilio API error.")
    except Exception as e:
        logging.error(f"Unexpected error in show_sms for user {user_id}: {e}")
        await update.message.reply_text("An unexpected system error occurred.")

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_sessions:
        del user_sessions[user_id]
    await update.message.reply_text("You have been logged out. Send /start to begin a new session.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    if user_id in user_sessions:
        del user_sessions[user_id]
    await update.message.reply_text('Operation cancelled. You can start over with /start.', reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    logging.info(f"User {user_id} triggered callback: {data}")

    if data.startswith('CHECKSMS:') or data.startswith('BUY:'):
        context.user_data.pop('last_inline_message', None)

    if data == 'verify_join':
        if await check_subscription(user_id, context.bot):
            await query.edit_message_text('✅ Verified! You now have access. Please tap /start to begin your session.')
        else:
            await query.answer('🚫 You have not joined the channel yet. Please join and try again.', show_alert=True)
        return

    if data == 'DELETE_MSG':
        try:
            await query.message.delete()
        except Exception as e:
            logging.error(f"Error deleting message for user {user_id}: {e}")
            await query.answer("Could not delete message.", show_alert=True)
        return

    sess = user_sessions.get(user_id)
    if not sess or 'client' not in sess or sess.get('status') == 'banned':
        await query.message.reply_text('Your session has expired or is invalid. Please start again.', reply_markup=get_start_only_menu())
        return

    client = sess['client']
    try:
        if data.startswith('BUY:'):
            number_to_buy = data.split(':', 1)[1]
            if not number_to_buy.startswith('+'): number_to_buy = f"+{number_to_buy}"
            safe_number_text = escape_markdown_v2(number_to_buy)
            await query.edit_message_text(f"🔍 Purchasing `{safe_number_text}`\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)
            for old_number in await asyncio.to_thread(client.incoming_phone_numbers.list):
                await asyncio.to_thread(old_number.delete)
            purchased = await asyncio.to_thread(client.incoming_phone_numbers.create, phone_number=number_to_buy)
            sess['purchased_count'] += 1
            sess['last_number'] = purchased.phone_number
            sess['shown_sms_sids'] = set()
            kb = InlineKeyboardMarkup([[InlineKeyboardButton('🔄 Show/Refresh SMS', callback_data=f'CHECKSMS:{purchased.phone_number}'), InlineKeyboardButton('🗑️ Delete', callback_data='DELETE_MSG')]])
            safe_purchased_number = escape_markdown_v2(purchased.phone_number)
            await query.edit_message_text(f'✅ Purchased: `{safe_purchased_number}`', reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)

        elif data.startswith('CHECKSMS:'):
            number = data.split(':', 1)[1]
            await query.answer(f"Checking messages for {number}...")
            messages = await asyncio.to_thread(client.messages.list, to=number, limit=20)
            all_message_sids = {msg.sid for msg in messages}
            shown_sids = sess.get('shown_sms_sids', set())
            if all_message_sids == shown_sids and messages:
                await query.answer("No new messages received.")
                return
            newly_shown_count = len(all_message_sids - shown_sids)
            sess['shown_sms_sids'] = all_message_sids
            if newly_shown_count > 0: sess['total_sms_received'] += newly_shown_count
            text = format_sms_text(messages, number)
            kb = InlineKeyboardMarkup([[InlineKeyboardButton('🔄 Refresh', callback_data=f'CHECKSMS:{number}'), InlineKeyboardButton('🗑️ Delete', callback_data='DELETE_MSG')]])
            await query.edit_message_text(text=text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)
    except TwilioRestException as e:
        if e.status == 401:
            logging.warning(f"SID Banned for user {user_id}. Error: {e}")
            purchased_count=sess.get('purchased_count',0)
            sms_count=sess.get('total_sms_received',0)
            sess['status']='banned'
            ban_message = (f"Your SID-Token Got banned please login via new SID_TOKEN.\nNumbers Purchased:{purchased_count}\nsms receved:{sms_count}")
            try:
                await query.edit_message_text(text=ban_message, reply_markup=None)
            except Exception as edit_error:
                logging.warning(f"Could not edit original message on ban: {edit_error}")
            await query.message.reply_text("You have been logged out. Please start again.", reply_markup=get_start_only_menu())
            return
        logging.error(f"--- Twilio API error for user {user_id}. Code: {e.code}, Message: {e.msg} ---")
        error_msg = f"❌ Action Failed: `{escape_markdown_v2(e.msg)}`"
        try:
            await query.edit_message_text(error_msg, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception:
            await query.message.reply_text(error_msg, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        if 'message is not modified' not in str(e).lower():
            logging.error(f"--- UNHANDLED ERROR in callback for user {user_id}: {e} ---")
            await query.message.reply_text(f"❌ An unexpected system error occurred.")

# --- Admin Panel ---
async def admin_panel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await remove_previous_inline_keyboard(context)
    text = update.message.text
    user_id = update.effective_user.id
    context.user_data['state'] = ADMIN_PANEL
    if text == '✅ GIVE ACCESS':
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton('Authorize User(s)', callback_data='auth_authorize')],
            [InlineKeyboardButton('Unauthorize User(s)', callback_data='auth_unauthorize')]
        ])
        sent_message = await update.message.reply_text("Please choose an action:", reply_markup=kb)
        context.user_data['last_inline_message'] = {'chat_id': sent_message.chat_id, 'message_id': sent_message.message_id}
        return SELECT_AUTH_ACTION
    elif text == '👥 USER LIST':
        bot_data = load_bot_data()
        authorized_users = bot_data.get("authorized_users", [])
        if not authorized_users:
            await update.message.reply_text("No users are currently authorized.")
        else:
            await update.message.reply_text(f"✅ Sending the list of {len(authorized_users)} authorized users one by one:")
            for user in authorized_users:
                # Send each user ID in a separate message for easy copying
                await update.message.reply_text(f"`{user}`", parse_mode=ParseMode.MARKDOWN_V2)
        return ADMIN_PANEL
    elif text == 'D/U':
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton('⬇️ Download Data', callback_data='admin_data_download')],
            [InlineKeyboardButton('⬆️ Upload Data', callback_data='admin_data_upload')]
        ])
        await update.message.reply_text("Select an option for the bot's data file:", reply_markup=kb)
        return ADMIN_PANEL
    elif text == '⚙️ SETTINGS':
        bot_data = load_bot_data()
        msg = ("*Current Settings:*\n"
               f"Bot Deep Link Prefix: `{escape_markdown_v2(bot_data.get('bot_link'))}`\n"
               f"Channel Username: `@{escape_markdown_v2(bot_data.get('channel_username'))}`\n\n"
               "What would you like to change?")
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_admin_settings_menu())
        return ADMIN_PANEL
    elif text == '➕ Admin Add' and user_id in ADMIN_USER_IDS:
        bot_data = load_bot_data()
        admins = bot_data.get('admin_user_ids', [])
        escaped_admins = [escape_markdown_v2(str(u)) for u in sorted(admins)]
        await update.message.reply_text("Admin Add/Remove Panel\\.\n\n"
                                        f"Current Admins:\n`{', '.join(escaped_admins)}`",
                                        parse_mode=ParseMode.MARKDOWN_V2,
                                        reply_markup=get_admin_add_menu())
        return ADMIN_ADD_PANEL
    elif text == '📝 Change Bot Link':
        await update.message.reply_text("Please send the new bot deep link prefix (e.g., `https://t.me/your_bot?start=u`).")
        return AWAITING_BOT_LINK
    elif text == '📝 Change Channel Username':
        await update.message.reply_text("Please send the new channel username (e.g., `BotAccessTask`). Do not include the `@` symbol.")
        return AWAITING_CHANNEL_USERNAME
    elif text == '⬅️ Back to Main Menu':
        await update.message.reply_text("Returning...", reply_markup=get_main_menu(user_id))
        return MAIN_MENU
    elif text == '⬅️ Back to Admin Panel':
        await update.message.reply_text("Returning...", reply_markup=get_admin_menu(user_id))
        return ADMIN_PANEL
    else:
        await update.message.reply_text("Invalid option.", reply_markup=get_admin_menu(user_id))
        return ADMIN_PANEL

async def admin_data_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    """Handles the 'Download/Upload Data' inline keyboard callbacks and transitions state."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    logging.info(f"Admin {user_id} triggered conversational callback: {data}")
    if data == 'admin_data_download':
        try:
            await context.bot.send_document(chat_id=user_id, document=open(BOT_DATA_FILE, 'rb'))
            await query.message.delete()
        except FileNotFoundError:
            await query.answer("Data file not found on server.", show_alert=True)
        except Exception as e:
            logging.error(f"Failed to send data file to admin {user_id}: {e}")
            await query.answer("Could not send data file.", show_alert=True)
        return ADMIN_PANEL
    elif data == 'admin_data_upload':
        await query.edit_message_text("Please upload the `bot_data.json` file now.")
        return AWAITING_DATA_UPLOAD
    return ADMIN_PANEL


async def select_auth_action_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    context.user_data.pop('last_inline_message', None)
    if data == 'auth_authorize':
        await query.edit_message_text("Please send the Telegram User ID (or a list of IDs) to authorize.")
        return AWAITING_USERID_TO_AUTH
    elif data == 'auth_unauthorize':
        await query.edit_message_text("Please send the Telegram User ID (or a list of IDs) to unauthorize.")
        return AWAITING_USERID_TO_UNAUTH
    return ConversationHandler.END

async def admin_add_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.effective_user.id
    if text == '➕ Add New Admin':
        await update.message.reply_text("Please send the User ID of the new admin.")
        return AWAITING_USERID_TO_ADD_ADMIN
    elif text == '➖ Remove Admin':
        await update.message.reply_text("Please send the User ID of the admin to remove.")
        return AWAITING_USERID_TO_REMOVE_ADMIN
    elif text == '⬅️ Back to Admin Panel':
        await update.message.reply_text("Returning to Admin Panel.", reply_markup=get_admin_menu(user_id))
        return ADMIN_PANEL
    else:
        await update.message.reply_text("Invalid option.", reply_markup=get_admin_add_menu())
        return ADMIN_ADD_PANEL

async def add_new_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        new_admin_id=int(update.message.text.strip())
        bot_data=load_bot_data()
        admins=set(bot_data.get("admin_user_ids",[]))
        safe_admin_id=escape_markdown_v2(str(new_admin_id))
        if new_admin_id in admins:
            await update.message.reply_text(f"User `{safe_admin_id}` is already an admin\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_admin_add_menu())
        else:
            admins.add(new_admin_id)
            bot_data['admin_user_ids']=list(admins)
            save_bot_data(bot_data)
            await update.message.reply_text(f"✅ User `{safe_admin_id}` has been added as a new admin\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_admin_add_menu())
            await context.bot.send_message(chat_id=new_admin_id, text="🎉 You have been granted admin rights! Send /start.")
    except Exception as e:
        await update.message.reply_text(f"An error occurred: {e}", reply_markup=get_admin_add_menu())
    return ADMIN_ADD_PANEL

async def remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        admin_id_to_remove=int(update.message.text.strip())
        bot_data=load_bot_data()
        admins=set(bot_data.get("admin_user_ids",[]))
        safe_admin_id=escape_markdown_v2(str(admin_id_to_remove))
        if admin_id_to_remove not in admins:
            await update.message.reply_text(f"User `{safe_admin_id}` is not an admin\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_admin_add_menu())
        elif admin_id_to_remove in ADMIN_USER_IDS:
            await update.message.reply_text("You cannot remove a master admin.", reply_markup=get_admin_add_menu())
        else:
            admins.discard(admin_id_to_remove)
            bot_data['admin_user_ids']=list(admins)
            save_bot_data(bot_data)
            await update.message.reply_text(f"✅ User `{safe_admin_id}` has been removed from admin status\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_admin_add_menu())
            await context.bot.send_message(chat_id=admin_id_to_remove, text="❌ Your admin rights have been revoked.")
    except Exception as e:
        await update.message.reply_text(f"An error occurred: {e}", reply_markup=get_admin_add_menu())
    return ADMIN_ADD_PANEL

async def give_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_ids_to_auth = parse_user_ids(update.message.text)
    if not user_ids_to_auth:
        await update.message.reply_text(
            "I couldn't find any valid User IDs in your message\\. Please provide a list of numeric IDs\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_admin_menu(update.effective_user.id)
        )
        return ADMIN_PANEL

    bot_data = load_bot_data()
    authorized_users = set(bot_data.get("authorized_users", []))
    
    newly_authorized = []
    already_authorized = []

    for user_id in user_ids_to_auth:
        if user_id in authorized_users:
            already_authorized.append(user_id)
        else:
            authorized_users.add(user_id)
            newly_authorized.append(user_id)
            try:
                await context.bot.send_message(chat_id=user_id, text="🎉 You have been granted access! Send /start.")
            except Exception as e:
                logging.warning(f"Could not notify user {user_id} about authorization: {e}")

    if newly_authorized:
        bot_data['authorized_users'] = list(authorized_users)
        save_bot_data(bot_data)
        try:
            notification_text = (
                f"✅ {len(newly_authorized)} user(s) have been authorized by admin {update.effective_user.id}:\n"
                f"{', '.join(map(str, newly_authorized))}"
            )
            await context.bot.send_message(chat_id=ADMIN_GROUP_ID, text=notification_text)
        except Exception as e:
            logging.error(f"Failed to send bulk authorization notification to admin group: {e}")

    # --- Build and send summary to admin ---
    summary_parts = [f"*Authorization Summary for {len(user_ids_to_auth)} ID\\(s\\)*"]
    if newly_authorized:
        summary_parts.append(f"\n✅ *Authorized Successfully \\({len(newly_authorized)}\\):*\n`{', '.join(map(str, newly_authorized))}`")
    if already_authorized:
        summary_parts.append(f"\n⚠️ *Already Authorized \\({len(already_authorized)}\\):*\n`{', '.join(map(str, already_authorized))}`")

    await update.message.reply_text(
        "\n".join(summary_parts),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_admin_menu(update.effective_user.id)
    )
    return ADMIN_PANEL


async def unauthorize_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_ids_to_unauth = parse_user_ids(update.message.text)
    if not user_ids_to_unauth:
        await update.message.reply_text(
            "I couldn't find any valid User IDs in your message\\. Please provide a list of numeric IDs\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=get_admin_menu(update.effective_user.id)
        )
        return ADMIN_PANEL

    bot_data = load_bot_data()
    authorized_users = set(bot_data.get("authorized_users", []))
    admins = set(bot_data.get("admin_user_ids", []))

    unauthorized_successfully = []
    not_authorized = []
    cannot_unauthorize = []
    
    changed = False
    for user_id in user_ids_to_unauth:
        if user_id in ADMIN_USER_IDS:
            cannot_unauthorize.append(user_id)
            continue
        
        if user_id in authorized_users:
            authorized_users.discard(user_id)
            admins.discard(user_id) # Also remove from admins if they are one
            unauthorized_successfully.append(user_id)
            changed = True
            try:
                await context.bot.send_message(chat_id=user_id, text="❌ Your access to this bot has been revoked.")
                if user_id in user_sessions:
                    del user_sessions[user_id]
            except Exception as e:
                logging.warning(f"Could not notify user {user_id} about deauthorization: {e}")
        else:
            not_authorized.append(user_id)

    if changed:
        bot_data['authorized_users'] = list(authorized_users)
        bot_data['admin_user_ids'] = list(admins)
        save_bot_data(bot_data)

    # --- Build and send summary to admin ---
    summary_parts = [f"*Unauthorization Summary for {len(user_ids_to_unauth)} ID\\(s\\)*"]
    if unauthorized_successfully:
        summary_parts.append(f"\n🚫 *Unauthorized Successfully \\({len(unauthorized_successfully)}\\):*\n`{', '.join(map(str, unauthorized_successfully))}`")
    if not_authorized:
        summary_parts.append(f"\n⚠️ *Were Not Authorized \\({len(not_authorized)}\\):*\n`{', '.join(map(str, not_authorized))}`")
    if cannot_unauthorize:
        summary_parts.append(f"\n❌ *Cannot Unauthorize Master Admins \\({len(cannot_unauthorize)}\\):*\n`{', '.join(map(str, cannot_unauthorize))}`")
    
    await update.message.reply_text(
        "\n".join(summary_parts),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=get_admin_menu(update.effective_user.id)
    )
    return ADMIN_PANEL


async def set_bot_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_link = update.message.text.strip()
    if not new_link.startswith("https://t.me/"):
        await update.message.reply_text("⚠️ Invalid link format. Please provide a link starting with `https://t.me/`.")
        return AWAITING_BOT_LINK
    bot_data = load_bot_data()
    bot_data['bot_link'] = new_link
    save_bot_data(bot_data)
    await update.message.reply_text(f"✅ Bot link updated to: `{escape_markdown_v2(new_link)}`", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_admin_settings_menu())
    return ADMIN_PANEL

async def set_channel_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_username = update.message.text.strip().lstrip('@')
    if not re.match(r'^[a-zA-Z0-9_]+$', new_username):
        await update.message.reply_text("⚠️ Invalid username. Please provide a valid Telegram username without the `@` symbol.")
        return AWAITING_CHANNEL_USERNAME
    bot_data = load_bot_data()
    bot_data['channel_username'] = new_username
    save_bot_data(bot_data)
    await update.message.reply_text(f"✅ Channel username updated to: `@ {escape_markdown_v2(new_username)}`", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_admin_settings_menu())
    return ADMIN_PANEL

async def handle_data_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    document = update.message.document
    admin_menu = get_admin_menu(user_id)
    
    if document.file_name != 'bot_data.json':
        await update.message.reply_text(
            f"❌ Invalid file. Please upload a file named `{escape_markdown_v2('bot_data.json')}`.",
            reply_markup=admin_menu,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return ADMIN_PANEL

    backup_path = f"{BOT_DATA_FILE}.bak"
    # Ensure no old backup file is lying around from a previous crash
    if os.path.exists(backup_path):
        os.remove(backup_path)

    try:
        # 1. Backup the current file if it exists
        if os.path.exists(BOT_DATA_FILE):
            os.rename(BOT_DATA_FILE, backup_path)

        # 2. Download the new file
        bot_file = await document.get_file()
        await bot_file.download_to_drive(BOT_DATA_FILE)

        # 3. Validate the new file by trying to load it
        load_bot_data() # This will raise JSONDecodeError if invalid

        # 4. If successful, send confirmation and remove the backup
        await update.message.reply_text(
            f"✅ `{escape_markdown_v2('bot_data.json')}` has been successfully uploaded and loaded\\.",
            reply_markup=admin_menu,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        if os.path.exists(backup_path):
            os.remove(backup_path)

    except (json.JSONDecodeError, Exception) as e:
        # This block will catch JSON errors and any other exceptions during the process.
        logging.error(f"Failed to process uploaded data file: {e}")
        restore_msg = ""

        # Attempt to restore from backup
        if os.path.exists(backup_path):
            try:
                # FIX: Remove the invalid downloaded file before restoring the backup
                if os.path.exists(BOT_DATA_FILE):
                    os.remove(BOT_DATA_FILE)
                os.rename(backup_path, BOT_DATA_FILE)
                restore_msg = "The previous data has been successfully restored."
            except Exception as restore_error:
                logging.error(f"CRITICAL: Failed to restore backup file: {restore_error}")
                restore_msg = "CRITICAL: Failed to restore the previous data file."
        else:
            restore_msg = "No previous data was available to restore."

        # Prepare user-friendly error message
        error_type_msg = "The uploaded file is not a valid JSON." if isinstance(e, json.JSONDecodeError) else "An unexpected error occurred during processing."
        
        try:
            escaped_details = escape_markdown_v2(f"{error_type_msg}\n{restore_msg}")
            formatted_error_text = f"❌ *Upload Failed*\n{escaped_details}"

            await update.message.reply_text(
                text=formatted_error_text,
                reply_markup=admin_menu,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except Exception as send_error:
            logging.error(f"Failed to send formatted error message, falling back. Original error: {e}. Send error: {send_error}")
            plain_text_error = f"Upload Failed: {error_type_msg}\n{restore_msg}\n\nDetails: {e}"
            await update.message.reply_text(
                text=plain_text_error,
                reply_markup=admin_menu,
                parse_mode=None
            )
            
    return ADMIN_PANEL

# --- Main Application Setup ---
def main():
    bot_data = load_bot_data()
    save_bot_data(bot_data)
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).rate_limiter(AIORateLimiter()).build()
    
    admin_menu_texts = ['✅ GIVE ACCESS', '👥 USER LIST', 'D/U', '⚙️ SETTINGS', '➕ Admin Add', '📝 Change Bot Link', '📝 Change Channel Username', '⬅️ Back to Main Menu', '⬅️ Back to Admin Panel']
    admin_add_menu_texts = ['➕ Add New Admin', '➖ Remove Admin', '⬅️ Back to Admin Panel']
    
    admin_button_handler = MessageHandler(filters.Regex(f"^({'|'.join(re.escape(btn) for btn in admin_menu_texts)})$"), admin_panel_handler)
    admin_add_button_handler = MessageHandler(filters.Regex(f"^({'|'.join(re.escape(btn) for btn in admin_add_menu_texts)})$"), admin_add_handler)
    
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            LOGIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_credentials)],
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu_or_number_handler)],
            ADMIN_PANEL: [
                admin_button_handler,
                CallbackQueryHandler(admin_data_callback_handler, pattern='^admin_data_')
            ],
            ADMIN_ADD_PANEL: [admin_add_button_handler],
            AWAITING_CHANNEL_VERIFICATION: [CallbackQueryHandler(handle_callback, pattern='^verify_join$')],
            
            SELECT_AUTH_ACTION: [
                CallbackQueryHandler(select_auth_action_handler, pattern='^auth_'),
                admin_button_handler 
            ],
            AWAITING_DATA_UPLOAD: [
                MessageHandler(filters.Document.FileExtension("json"), handle_data_upload),
                admin_button_handler
            ],
            AWAITING_USERID_TO_AUTH: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, give_access),
                admin_button_handler
            ],
            AWAITING_USERID_TO_UNAUTH: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, unauthorize_user),
                admin_button_handler
            ],
            AWAITING_USERID_TO_ADD_ADMIN: [
                MessageHandler(filters.Regex(r'^\d+$'), add_new_admin),
                admin_add_button_handler
            ],
            AWAITING_USERID_TO_REMOVE_ADMIN: [
                MessageHandler(filters.Regex(r'^\d+$'), remove_admin),
                admin_add_button_handler
            ],
            AWAITING_BOT_LINK: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, set_bot_link),
                admin_button_handler
            ],
            AWAITING_CHANNEL_USERNAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, set_channel_username),
                admin_button_handler
            ],
        },
        fallbacks=[CommandHandler('start', start), CommandHandler('cancel', cancel)],
        allow_reentry=True
    )
    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    logging.info("Bot started successfully.")
    application.run_polling()

if __name__ == '__main__':

    main()


