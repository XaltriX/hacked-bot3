import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.exceptions import TelegramAPIError, TelegramConflictError
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import os
import psutil
import re
from pathlib import Path
import logging

# --- Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Constants
TOKEN_FILES = ["token1.txt"]
USER_IDS_FILE = "user_ids.txt"
REMAINING_TOKENS_FILE = "remaining_tokens.txt"
CONFIG_FILE = "bot_config.txt"
ADMIN_ID = 5706788169
DASHBOARD_TOKEN = "7557269432:AAF1scybLhu5sX4E6xkktd5jGXtCFzOz1n0"
BATCH_SIZE = 50
DELAY_BETWEEN_BATCHES = 10
MAX_RETRIES = 3
RETRY_DELAY = 2
BOTS_PER_PAGE = 50
MAX_BOTS_LIMIT = 100  # Heroku-friendly default

CUSTOM_REPLY_TEXT = """
🎬 MOVIE & ENTERTAINMENT HUB 🍿  
✨ Your Ultimate Destination for Movies & Daily Entertainment!

━━━━━━━━━━━━━━━━━━━━━━━━━

🎥 Request your favorite movies
🔥 Exclusive unseen drops  
💎 High-quality premium content
🌑 Rare & bold videos
📅 Fresh movies every day

━━━━━━━━━━━━━━━━━━━━━━━━━

👇 Click the buttons below to join! 👇
"""

CUSTOM_REPLY_BUTTONS = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🎥 Movie Request Group", url="https://t.me/MOVIE_REQUESTX")],
    [InlineKeyboardButton(text="💥 Daily MMS Le@k", url="https://t.me/+Br0s4neTgL0xM2I8")],
    [InlineKeyboardButton(text="💎 Premium MMS C0rn", url="https://t.me/+VWdELS83oeMxMWI1")],
    [InlineKeyboardButton(text="🌑 D@rk Web Vide0s", url="https://t.me/+we2VaRaOfr5lM2M0")],
    [InlineKeyboardButton(text="🎞️ New Movie Daily", url="https://t.me/+vkh5MVQqJzs4OGU0")],
    [InlineKeyboardButton(text="🌐 Full Hub Access", url="https://linkzwallah.netlify.app/")]
])

user_ids = set()
bots = {}
bot_stats = {}
bot_tasks = {}
broadcast_cancelled = False

def extract_tokens(text):
    pattern = r'\d{6,10}:[A-Za-z0-9_-]{20,}'
    return re.findall(pattern, text)

def load_config():
    global MAX_BOTS_LIMIT
    try:
        if Path(CONFIG_FILE).exists():
            with open(CONFIG_FILE, "r") as f:
                content = f.read().strip()
                if content.isdigit():
                    MAX_BOTS_LIMIT = int(content)
                    logger.info(f"Loaded bot limit: {MAX_BOTS_LIMIT}")
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        MAX_BOTS_LIMIT = 100

def save_config():
    try:
        with open(CONFIG_FILE, "w") as f:
            f.write(str(MAX_BOTS_LIMIT))
        logger.info(f"Saved bot limit: {MAX_BOTS_LIMIT}")
    except Exception as e:
        logger.error(f"Error saving config: {e}")

def save_remaining_tokens(tokens):
    try:
        if tokens:
            with open(REMAINING_TOKENS_FILE, "w", encoding="utf-8") as f:
                for token in tokens:
                    f.write(f"{token}\n")
            logger.info(f"Saved {len(tokens)} remaining tokens to {REMAINING_TOKENS_FILE}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error saving remaining tokens: {e}")
        return False

def get_vps_capacity():
    try:
        cpu_count = psutil.cpu_count()
        ram_gb = psutil.virtual_memory().total / (1024**3)
        ram_based_capacity = int(ram_gb * 1024 / 15)
        cpu_based_capacity = cpu_count * 100
        estimated_capacity = min(ram_based_capacity, cpu_based_capacity)
        is_heroku = os.environ.get('DYNO') is not None
        return {
            "cpu_cores": cpu_count,
            "ram_gb": round(ram_gb, 2),
            "estimated_capacity": estimated_capacity,
            "current_bots": len(bots),
            "current_limit": MAX_BOTS_LIMIT,
            "available_slots": MAX_BOTS_LIMIT - len(bots),
            "is_heroku": is_heroku,
            "platform": "Heroku" if is_heroku else "VPS"
        }
    except Exception as e:
        logger.error(f"Error calculating capacity: {e}")
        return None

def load_user_ids():
    try:
        if Path(USER_IDS_FILE).exists():
            with open(USER_IDS_FILE, "r") as f:
                for line in f:
                    chat_id = line.strip()
                    if chat_id.isdigit():
                        user_ids.add(int(chat_id))
        logger.info(f"Loaded {len(user_ids)} user IDs")
    except Exception as e:
        logger.error(f"Error loading user IDs: {e}")

def save_user_id(chat_id):
    try:
        if chat_id not in user_ids:
            user_ids.add(chat_id)
            with open(USER_IDS_FILE, "a") as f:
                f.write(f"{chat_id}\n")
    except Exception as e:
        logger.error(f"Error saving user ID {chat_id}: {e}")

async def delete_webhook(token):
    bot = None
    try:
        bot = Bot(token)
        await bot.delete_webhook(drop_pending_updates=True)
    except TelegramConflictError:
        logger.warning(f"Webhook conflict for token {token[:10]}...")
        try:
            await asyncio.sleep(2)
            if bot:
                await bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            logger.error(f"Error deleting webhook: {e}")
    except Exception as e:
        logger.error(f"Error deleting webhook: {e}")
    finally:
        if bot:
            try:
                await bot.session.close()
            except Exception:
                pass

async def get_bot_username(token):
    for attempt in range(MAX_RETRIES):
        bot = None
        try:
            bot = Bot(token)
            me = await bot.get_me()
            return me.username
        except TelegramConflictError:
            logger.warning(f"Conflict error getting bot info (attempt {attempt+1}/{MAX_RETRIES})")
            await asyncio.sleep(RETRY_DELAY)
        except Exception as e:
            logger.error(f"Error getting bot username: {e}")
            return None
        finally:
            if bot:
                try:
                    await bot.session.close()
                except Exception:
                    pass
    return None

async def startup_bots(tokens, dashboard_bot=None, notify_chat_id=None):
    global MAX_BOTS_LIMIT
    current_bots = len(bots)
    available_slots = MAX_BOTS_LIMIT - current_bots
    if available_slots <= 0:
        logger.warning(f"Bot limit reached! Current: {current_bots}/{MAX_BOTS_LIMIT}")
        if dashboard_bot and notify_chat_id:
            await dashboard_bot.send_message(
                notify_chat_id,
                f"⚠️ BOT LIMIT REACHED!\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Current Bots: {current_bots}\n"
                f"Max Limit: {MAX_BOTS_LIMIT}\n\n"
                f"❌ Cannot deploy more bots!\n"
                f"Use /setlimit to increase limit."
            )
        return 0
    tokens_to_deploy = tokens[:available_slots]
    remaining_tokens = tokens[available_slots:]
    if remaining_tokens:
        logger.info(f"Limiting to {len(tokens_to_deploy)} tokens. {len(remaining_tokens)} tokens remaining.")
        saved = save_remaining_tokens(remaining_tokens)
        if dashboard_bot and notify_chat_id and saved:
            await dashboard_bot.send_document(
                notify_chat_id,
                types.FSInputFile(REMAINING_TOKENS_FILE),
                caption=(
                    f"⚠️ BOT LIMIT WARNING\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔢 Total Tokens: {len(tokens)}\n"
                    f"✅ Deploying: {len(tokens_to_deploy)}\n"
                    f"📋 Remaining: {len(remaining_tokens)}\n\n"
                    f"⚡ Current: {current_bots}/{MAX_BOTS_LIMIT}\n"
                    f"🎯 After Deployment: {current_bots + len(tokens_to_deploy)}/{MAX_BOTS_LIMIT}\n\n"
                    f"📎 Attached: Remaining tokens that couldn't be deployed\n"
                    f"💡 Use /setlimit to increase capacity"
                )
            )
    started = 0
    failed = 0
    total = len(tokens_to_deploy)
    logger.info(f"Starting {total} bots in batches of {BATCH_SIZE}")
    batch_num = 1
    for i in range(0, total, BATCH_SIZE):
        batch = tokens_to_deploy[i:i+BATCH_SIZE]
        logger.info(f"Starting batch {batch_num}/{(total+BATCH_SIZE-1)//BATCH_SIZE}...")
        tasks = [start_single_bot(token) for token in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if result is True:
                started += 1
            else:
                failed += 1
        logger.info(f"[{started}/{total}] bots started, {failed} failed. Sleeping {DELAY_BETWEEN_BATCHES}s...")
        print_resource_usage()
        batch_num += 1
        await asyncio.sleep(DELAY_BETWEEN_BATCHES)
    logger.info(f"Bot startup complete: {started} successful, {failed} failed")
    return started

async def start_single_bot(token):
    try:
        if len(bots) >= MAX_BOTS_LIMIT:
            logger.warning(f"Bot limit reached: {len(bots)}/{MAX_BOTS_LIMIT}")
            return False
        await delete_webhook(token)
        await asyncio.sleep(1)
        username = await get_bot_username(token)
        if not username:
            logger.error(f"Token invalid or unreachable: {token[:10]}...")
            return False
        if username in bots:
            logger.warning(f"Already running: @{username}")
            return False
        bot = Bot(token)
        dp = Dispatcher()
        bots[username] = bot
        bot_stats[username] = {"messages": 0, "users": set()}
        @dp.message()
        async def handler(msg: types.Message):
            try:
                bot_stats[username]["messages"] += 1
                bot_stats[username]["users"].add(msg.from_user.id)
                save_user_id(msg.from_user.id)
                # --- FIXED: Button reply!
                await msg.answer(CUSTOM_REPLY_TEXT, reply_markup=CUSTOM_REPLY_BUTTONS)
            except Exception as e:
                logger.error(f"Error handling message for @{username}: {e}")
        async def poll_with_error_handling():
            try:
                await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
            except Exception as e:
                logger.error(f"Error polling @{username}: {e}")
            finally:
                try:
                    await bot.session.close()
                except Exception:
                    pass
        task = asyncio.create_task(poll_with_error_handling())
        bot_tasks[username] = task
        logger.info(f"@{username}: OK")
        return True
    except Exception as e:
        logger.error(f"Critical error starting bot: {e}")
        return False

def load_all_tokens():
    all_tokens = []
    try:
        for token_file in TOKEN_FILES:
            if not Path(token_file).exists():
                continue
            try:
                with open(token_file, "r", encoding="utf-8") as f:
                    content = f.read()
                    tokens = extract_tokens(content)
                    all_tokens.extend(tokens)
                    logger.info(f"Loaded {len(tokens)} tokens from {token_file}")
            except Exception as e:
                logger.error(f"Error reading {token_file}: {e}")
        logger.info(f"Total tokens loaded: {len(all_tokens)}")
        return all_tokens
    except Exception as e:
        logger.error(f"Error loading tokens: {e}")
        return []

def print_resource_usage():
    try:
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        try:
            disk = psutil.disk_usage('/').percent
        except Exception:
            disk = 0
        logger.info(f"Resources - CPU: {cpu}% | RAM: {ram}% | Disk: {disk}%")
    except Exception as e:
        logger.error(f"Error getting resource usage: {e}")

def get_resource_usage_str():
    try:
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        try:
            disk = psutil.disk_usage('/').percent
            return f"CPU: {cpu}% | RAM: {ram}% | Disk: {disk}%"
        except Exception:
            return f"CPU: {cpu}% | RAM: {ram}%"
    except Exception as e:
        return "Resource usage unavailable"

def get_bot_list_page(page=0):
    try:
        bot_list = list(bots.keys())
        if not bot_list:
            return "No bots running", None
        total_bots = len(bot_list)
        total_pages = (total_bots + BOTS_PER_PAGE - 1) // BOTS_PER_PAGE
        start_idx = page * BOTS_PER_PAGE
        end_idx = min(start_idx + BOTS_PER_PAGE, total_bots)
        page_bots = bot_list[start_idx:end_idx]
        bot_text = f"🤖 Bot List (Page {page+1}/{total_pages})\n"
        bot_text += f"📊 Total Bots: {total_bots}/{MAX_BOTS_LIMIT}\n"
        bot_text += "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for idx, uname in enumerate(page_bots, start=start_idx+1):
            user_count = len(bot_stats.get(uname, {}).get("users", set()))
            bot_text += f"{idx}. @{uname} - 👥 {user_count} users\n"
        buttons = []
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(text="◀️ Previous", callback_data=f"botlist_{page-1}"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"botlist_{page+1}"))
        if nav_row:
            buttons.append(nav_row)
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
        return bot_text, keyboard
    except Exception as e:
        logger.error(f"Error getting bot list: {e}")
        return "Error getting bot list", None

def get_stats():
    try:
        total_users = len(user_ids)
        total_bots = len(bots)
        total_messages = sum(stat["messages"] for stat in bot_stats.values())
        return (
            f"📊 SYSTEM STATISTICS\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 Bots Running: {total_bots}/{MAX_BOTS_LIMIT}\n"
            f"📈 Available Slots: {MAX_BOTS_LIMIT - total_bots}\n"
            f"👥 Total Users (all bots): {total_users}\n"
            f"📨 Total Messages: {total_messages}\n"
            f"💻 {get_resource_usage_str()}"
        )
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return "Stats unavailable"

async def dashboard():
    dashboard_bot = None
    try:
        await delete_webhook(DASHBOARD_TOKEN)
        dashboard_bot = Bot(DASHBOARD_TOKEN)
        dp = Dispatcher()

        @dp.message(Command("start"))
        async def cmd_start(msg: types.Message):
            if msg.from_user.id != ADMIN_ID:
                await msg.answer("Unauthorized.")
                return
            await msg.answer(
                "🎛️ DASHBOARD COMMANDS\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "/stats - Show statistics\n"
                "/bots - List all bots (paginated)\n"
                "/topbots - Top 20 bots by users\n"
                "/capacity - Check VPS/Heroku capacity\n"
                "/setlimit <number> - Set bot limit\n"
                "/gettoken @botname - Get bot token\n"
                "/broadcast <msg> - Broadcast to all users\n"
                "\n📤 Send a .txt file to upload tokens."
            )

        @dp.message(Command("stats"))
        async def cmd_stats(msg: types.Message):
            if msg.from_user.id != ADMIN_ID:
                await msg.answer("Unauthorized.")
                return
            await msg.answer(get_stats())

        @dp.message(Command("capacity"))
        async def cmd_capacity(msg: types.Message):
            if msg.from_user.id != ADMIN_ID:
                await msg.answer("Unauthorized.")
                return
            capacity = get_vps_capacity()
            if not capacity:
                await msg.answer("❌ Error calculating capacity!")
                return
            usage_percent = (capacity['current_bots'] / capacity['current_limit'] * 100) if capacity['current_limit'] > 0 else 0
            if usage_percent < 50:
                status = "🟢 Excellent"
            elif usage_percent < 70:
                status = "🟡 Good"
            elif usage_percent < 90:
                status = "🟠 Warning"
            else:
                status = "🔴 Critical"
            platform_emoji = "☁️" if capacity['is_heroku'] else "💻"
            response = (
                f"{platform_emoji} {capacity['platform'].upper()} CAPACITY REPORT\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🖥️ System Resources:\n"
                f"   • CPU Cores: {capacity['cpu_cores']}\n"
                f"   • RAM: {capacity['ram_gb']} GB\n"
                f"   • Estimated Max Capacity: {capacity['estimated_capacity']} bots\n"
            )
            if capacity['is_heroku']:
                response += f"   • Platform: Heroku Dyno\n"
            response += (
                f"\n📊 Current Status:\n"
                f"   • Active Bots: {capacity['current_bots']}\n"
                f"   • Set Limit: {capacity['current_limit']}\n"
                f"   • Available Slots: {capacity['available_slots']}\n"
                f"   • Usage: {usage_percent:.1f}% {status}\n\n"
                f"💡 Recommendation:\n"
            )
            if capacity['current_limit'] > capacity['estimated_capacity']:
                response += f"   ⚠️ Current limit ({capacity['current_limit']}) exceeds estimated capacity!\n"
                response += f"   🎯 Recommended limit: {capacity['estimated_capacity']} bots\n"
                response += f"   Use /setlimit {capacity['estimated_capacity']} for optimal performance"
            elif capacity['available_slots'] < 50:
                response += f"   ⚠️ Low capacity! Only {capacity['available_slots']} slots remaining\n"
                response += "   Consider increasing limit if system can handle more"
            else:
                response += "   ✅ System running optimally!\n"
                response += f"   💪 Can handle {capacity['available_slots']} more bots"
            response += f"\n\n💻 {get_resource_usage_str()}"
            await msg.answer(response)

        @dp.message(Command("setlimit"))
        async def cmd_setlimit(msg: types.Message):
            global MAX_BOTS_LIMIT
            if msg.from_user.id != ADMIN_ID:
                await msg.answer("Unauthorized.")
                return
            args = msg.text.split(None, 1)
            if len(args) < 2 or not args[1].isdigit():
                await msg.answer(
                    "❌ Usage: /setlimit <number>\n\n"
                    "Example: /setlimit 500\n\n"
                    f"Current limit: {MAX_BOTS_LIMIT}"
                )
                return
            new_limit = int(args[1])
            if new_limit < 1:
                await msg.answer("❌ Limit must be at least 1!")
                return
            if new_limit < len(bots):
                await msg.answer(
                    f"⚠️ Cannot set limit lower than current bots!\n"
                    f"Current bots: {len(bots)}\n"
                    f"Requested limit: {new_limit}\n\n"
                    f"Stop some bots first to reduce limit."
                )
                return
            old_limit = MAX_BOTS_LIMIT
            MAX_BOTS_LIMIT = new_limit
            save_config()
            capacity = get_vps_capacity()
            warning = ""
            if capacity and new_limit > capacity['estimated_capacity']:
                warning = (
                    f"\n\n⚠️ WARNING:\n"
                    f"New limit ({new_limit}) exceeds estimated capacity ({capacity['estimated_capacity']})!\n"
                    f"System may become unstable with too many bots."
                )
            await msg.answer(
                f"✅ BOT LIMIT UPDATED\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Old Limit: {old_limit}\n"
                f"New Limit: {new_limit}\n"
                f"Current Bots: {len(bots)}\n"
                f"Available Slots: {new_limit - len(bots)}"
                f"{warning}"
            )

        @dp.message(Command("bots"))
        async def cmd_bots(msg: types.Message):
            if msg.from_user.id != ADMIN_ID:
                await msg.answer("Unauthorized.")
                return
            bot_text, keyboard = get_bot_list_page(0)
            await msg.answer(bot_text, reply_markup=keyboard)

        @dp.message(Command("topbots"))
        async def cmd_topbots(msg: types.Message):
            if msg.from_user.id != ADMIN_ID:
                await msg.answer("Unauthorized.")
                return
            bot_user_counts = []
            for uname in bots.keys():
                user_count = len(bot_stats.get(uname, {}).get("users", set()))
                bot_user_counts.append((uname, user_count))
            bot_user_counts.sort(key=lambda x: x[1], reverse=True)
            if not bot_user_counts:
                await msg.answer("No bots running!")
                return
            top_bots = bot_user_counts[:20]
            text = "🏆 TOP BOTS (By Users)\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for idx, (uname, count) in enumerate(top_bots, 1):
                medal = "🥇" if idx == 1 else "🥈" if idx == 2 else "🥉" if idx == 3 else f"{idx}."
                text += f"{medal} @{uname}\n   👥 {count} users\n\n"
            total_users = sum(count for _, count in bot_user_counts)
            text += f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"📊 Total Bots: {len(bot_user_counts)}/{MAX_BOTS_LIMIT}\n"
            text += f"👥 Total Users: {total_users}"
            await msg.answer(text)

        @dp.message(Command("gettoken"))
        async def cmd_gettoken(msg: types.Message):
            if msg.from_user.id != ADMIN_ID:
                await msg.answer("Unauthorized.")
                return
            args = msg.text.split(None, 1)
            if len(args) < 2:
                await msg.answer("❌ Usage: /gettoken @botusername\n\nExample: /gettoken @mybot")
                return
            bot_username = args[1].strip().lstrip('@')
            found_token = None
            found_in_file = None
            for token_file in TOKEN_FILES:
                if not Path(token_file).exists():
                    continue
                try:
                    with open(token_file, "r", encoding="utf-8") as f:
                        content = f.read()
                        tokens = extract_tokens(content)
                        for token in tokens:
                            bot_instance = None
                            try:
                                bot_instance = Bot(token)
                                me = await bot_instance.get_me()
                                if me.username.lower() == bot_username.lower():
                                    found_token = token
                                    found_in_file = token_file
                                    break
                            except Exception:
                                pass
                            finally:
                                if bot_instance:
                                    try:
                                        await bot_instance.session.close()
                                    except Exception:
                                        pass
                        if found_token:
                            break
                except Exception as e:
                    logger.error(f"Error reading {token_file}: {e}")
            if found_token:
                user_count = len(bot_stats.get(bot_username, {}).get("users", set()))
                msg_count = bot_stats.get(bot_username, {}).get("messages", 0)
                response = (
                    f"🔐 BOT TOKEN FOUND\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🤖 Bot: @{bot_username}\n"
                    f"📁 File: {found_in_file}\n"
                    f"👥 Users: {user_count}\n"
                    f"📨 Messages: {msg_count}\n\n"
                    f"🔑 Token:\n`{found_token}`"
                )
                await msg.answer(response, parse_mode="Markdown")
            else:
                await msg.answer(f"❌ Token not found for @{bot_username}")

        @dp.callback_query(lambda c: c.data.startswith("botlist_"))
        async def handle_bot_pagination(callback: CallbackQuery):
            if callback.from_user.id != ADMIN_ID:
                await callback.answer("Unauthorized.", show_alert=True)
                return
            page = int(callback.data.split("_")[1])
            bot_text, keyboard = get_bot_list_page(page)
            await callback.message.edit_text(bot_text, reply_markup=keyboard)
            await callback.answer()

        @dp.callback_query(lambda c: c.data == "cancel_broadcast")
        async def handle_cancel_broadcast(callback: CallbackQuery):
            global broadcast_cancelled
            if callback.from_user.id != ADMIN_ID:
                await callback.answer("Unauthorized.", show_alert=True)
                return
            broadcast_cancelled = True
            await callback.answer("🛑 Broadcast cancellation requested!", show_alert=True)
            await callback.message.edit_text(
                callback.message.text + "\n\n🛑 CANCELLATION REQUESTED..."
            )

        @dp.message(Command("broadcast"))
        async def cmd_broadcast(msg: types.Message):
            global broadcast_cancelled
            if msg.from_user.id != ADMIN_ID:
                await msg.answer("Unauthorized.")
                return
            txt = msg.text.split(None, 1)
            if len(txt) < 2:
                await msg.answer("Usage: /broadcast <message>")
                return
            message = txt[1]
            if not bots:
                await msg.answer("❌ No bots available for broadcast!")
                return
            broadcast_cancelled = False
            cancel_btn = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛑 Cancel Broadcast", callback_data="cancel_broadcast")]
            ])
            total_messages = sum(len(bot_stats.get(uname, {}).get("users", set())) for uname in bots.keys())
            status_msg = await msg.answer(
                "🚀 BROADCAST STARTING\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🤖 Active Bots: {len(bots)}\n"
                f"📨 Total Messages to Send: {total_messages}\n\n"
                "⏳ Processing...",
                reply_markup=cancel_btn
            )
            total_successful = 0
            total_failed = 0
            bots_processed = 0
            for uname, bot_instance in bots.items():
                if broadcast_cancelled:
                    await dashboard_bot.edit_message_text(
                        chat_id=msg.chat.id,
                        message_id=status_msg.message_id,
                        text=f"🛑 BROADCAST CANCELLED\n"
                             f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                             f"✅ Successful: {total_successful}\n"
                             f"❌ Failed: {total_failed}\n"
                             f"🤖 Bots Processed: {bots_processed}/{len(bots)}"
                    )
                    break
                bot_users = list(bot_stats.get(uname, {}).get("users", set()))
                if not bot_users:
                    continue
                successful = 0
                failed = 0
                for uid in bot_users:
                    if broadcast_cancelled:
                        break
                    try:
                        await bot_instance.send_message(uid, message)
                        successful += 1
                        total_successful += 1
                    except Exception as e:
                        failed += 1
                        total_failed += 1
                        logger.error(f"Failed to send to user {uid}: {e}")
                    if (total_successful + total_failed) % 50 == 0:
                        try:
                            progress = f"({total_successful + total_failed}/{total_messages})"
                            success_rate = (total_successful/(total_successful+total_failed)*100) if (total_successful+total_failed) > 0 else 0
                            await dashboard_bot.edit_message_text(
                                chat_id=msg.chat.id,
                                message_id=status_msg.message_id,
                                text=f"🚀 BROADCAST IN PROGRESS\n"
                                     f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                     f"🤖 Current Bot: @{uname}\n"
                                     f"📊 Bots Processed: {bots_processed+1}/{len(bots)}\n"
                                     f"📨 Total Messages: {total_messages}\n\n"
                                     f"✅ Successful: {total_successful}\n"
                                     f"❌ Failed: {total_failed}\n"
                                     f"⏳ Progress: {progress}\n"
                                     f"📈 Success Rate: {success_rate:.1f}%",
                                reply_markup=cancel_btn
                            )
                        except Exception as e:
                            logger.error(f"Error updating status: {e}")
                    if (successful + failed) % 30 == 0:
                        await asyncio.sleep(1)
                bots_processed += 1
                logger.info(f"Bot @{uname}: {successful} sent, {failed} failed")
            if broadcast_cancelled:
                return
            success_rate = (total_successful / (total_successful + total_failed) * 100) if (total_successful + total_failed) > 0 else 0
            final_report = (
                "✅ BROADCAST COMPLETED\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🤖 Bots Used: {bots_processed}\n"
                f"📨 Total Messages: {total_successful + total_failed}\n\n"
                f"✅ Successful: {total_successful}\n"
                f"❌ Failed: {total_failed}\n"
                f"📈 Success Rate: {success_rate:.1f}%"
            )
            try:
                await dashboard_bot.edit_message_text(
                    chat_id=msg.chat.id,
                    message_id=status_msg.message_id,
                    text=final_report
                )
            except Exception as e:
                logger.error(f"Error sending final report: {e}")
                await msg.answer(final_report)
            logger.info(f"Broadcast completed: {total_successful} successful, {total_failed} failed")

        @dp.message()
        async def handle_document(msg: types.Message):
            try:
                if msg.from_user.id != ADMIN_ID:
                    return
                if msg.document and msg.document.file_name.endswith(".txt"):
                    file = await dashboard_bot.get_file(msg.document.file_id)
                    dest = f"uploads/{msg.document.file_name}"
                    os.makedirs("uploads", exist_ok=True)
                    await dashboard_bot.download_file(file.file_path, dest)
                    await msg.answer(f"📥 File uploaded. Extracting tokens...")
                    with open(dest, "r", encoding="utf-8") as f:
                        content = f.read()
                    tokens = extract_tokens(content)
                    if not tokens:
                        await msg.answer("❌ No valid tokens found in file!")
                        return
                    await msg.answer(
                        f"✅ Found {len(tokens)} tokens.\n"
                        f"🤖 Current bots: {len(bots)}/{MAX_BOTS_LIMIT}\n"
                        f"📊 Available slots: {MAX_BOTS_LIMIT - len(bots)}\n\n"
                        f"⏳ Starting bots..."
                    )
                    started = await startup_bots(tokens, dashboard_bot, msg.chat.id)
                    await msg.answer(
                        f"🎉 Deployment complete!\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"✅ Bots started: {started}\n"
                        f"🤖 Total active: {len(bots)}/{MAX_BOTS_LIMIT}\n"
                        f"📊 Available slots: {MAX_BOTS_LIMIT - len(bots)}"
                    )
                elif msg.text and re.match(r'^\d{6,10}:[A-Za-z0-9_-]{20,}$', msg.text.strip()):
                    token = msg.text.strip()
                    if len(bots) >= MAX_BOTS_LIMIT:
                        await msg.answer(
                            f"⚠️ BOT LIMIT REACHED!\n"
                            f"Current: {len(bots)}/{MAX_BOTS_LIMIT}\n\n"
                            f"Use /setlimit to increase capacity."
                        )
                        return
                    await msg.answer("🔄 Token received. Starting bot...")
                    started = await startup_bots([token], dashboard_bot, msg.chat.id)
                    if started > 0:
                        await msg.answer(
                            f"✅ Bot started successfully!\n"
                            f"🤖 Total: {len(bots)}/{MAX_BOTS_LIMIT}"
                        )
                    else:
                        await msg.answer("❌ Failed to start bot!")
            except Exception as e:
                logger.error(f"Error handling document: {e}")
                await msg.answer(f"❌ Error: {str(e)}")

        await dp.start_polling(dashboard_bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
    finally:
        if dashboard_bot:
            try:
                await dashboard_bot.session.close()
            except Exception:
                pass

async def main():
    try:
        load_config()
        load_user_ids()
        all_tokens = load_all_tokens()
        if not all_tokens:
            logger.warning("No tokens found!")
        asyncio.create_task(dashboard())
        await startup_bots(all_tokens)
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logger.error(f"Main error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
