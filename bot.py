import os
import re
import time
import logging
import asyncio
import threading
from flask import Flask
from waitress import serve
from dotenv import load_dotenv
from collections import defaultdict
from aiogram.filters import Command
from typing import DefaultDict, Set
from search_index import search_files
from utils import download_youtube_video
from telethon.sync import TelegramClient
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, FSInputFile
from datetime import datetime, timedelta, timezone
from telethon.errors import RPCError, AuthKeyDuplicatedError
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from firebase import (
    add_new_user,
    process_removal_queue,
    remove_user,
    add_extra_7_days,
    get_expiring_users,
    get_stats,
)
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    ChatInviteLink,
)

# Initialize Flask and Aiogram
app = Flask(__name__)

load_dotenv()


# Load environment variables
def get_env(key: str) -> str:
    value = os.getenv(key)
    if value is None:
        raise EnvironmentError(f"{key} is missing in .env")
    return value


OWNER_ID = int(get_env("OWNER_ID"))  # User Account ID
TELEGRAM_API_ID = int(get_env("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = get_env("TELEGRAM_API_HASH")
BOT_ID = int(get_env("BOT_ID"))  # Bot ID
BOT_USERNAME = get_env("BOT_USERNAME")  # Bot username
BOT_API_TOKEN = get_env("BOT_API_TOKEN")  # Bot Token
PUBLIC_GROUP_ID = int(get_env("PUBLIC_GROUP_ID"))  # Public Group
PRIVATE_GROUP_ID = int(get_env("PRIVATE_GROUP_ID"))  # Private Request Group ID
PRIVATE_GROUP_URL = get_env("PRIVATE_GROUP_URL")  # Private Request Group URL
DATABASE_ID = int(get_env("DATABASE_ID"))  # Private Database
SESSION_NAME = "my_session2"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

# Mark the query as active and add the task to the set
bot = Bot(token=BOT_API_TOKEN)

dp = Dispatcher()
router = Router()

# Register the router with the dispatcher
dp.include_router(router)

# Regex to validate the query format
VALID_QUERY_REGEX = r"^[A-Za-z]+(?: [A-Za-z]+)*(?:: [A-Za-z0-9]+(?: [A-Za-z0-9]+)*)?(?: (?:S\d{2}|\d{4}))?$"

# Global variables
is_shutting_down = False
active_searches: DefaultDict[str, Set[asyncio.Task]] = defaultdict(set)
search_msg = False


@dp.message(Command("start"))
async def send_welcome(message: Message):
    """
    Send a welcome message when the bot starts.
    """
    try:
        # Block /start from groups except PRIVATE_GROUP_ID
        # Ignore all the messages in database group
        if message.chat.id == DATABASE_ID:
            return

        # List of allowed groups where the bot should stay
        allowed_group_ids = [PRIVATE_GROUP_ID, DATABASE_ID]

        # Check if the message is from a group/channel
        if message.chat.type in ["group", "supergroup", "channel"]:
            # If the group is NOT in the allowed list, make the bot leave
            if message.chat.id not in allowed_group_ids:
                response_msg = await message.reply(
                    "❌ *This bot is restricted to specific groups. Leaving...*"
                )
                asyncio.create_task(delete_message_after_delay(response_msg, delay=5))
                await asyncio.sleep(6)
                await bot.leave_chat(message.chat.id)
                return

        if message.chat.id == PRIVATE_GROUP_ID:
            # Send bot's response
            response_msg = await message.answer(
                "✨ *Welcome to StreamTap!* 🎬\n\n"
                "🔍 *How to request files:*\n"
                "━━━━━━━━━━━━━━━━━\n\n"
                "• 🎥 *Movies:* `Title Year`\n"
                "   Example: `Rustom 2016`\n\n"
                "• 📺 *Series:* `Title SXX`\n"
                "   Example: `Paatal Lok S01`\n\n",
                parse_mode="Markdown",
            )

            # Delete user's /start command first
            await asyncio.sleep(2)
            await message.delete()

            # Schedule deletion of bot's response after 20 seconds
            asyncio.create_task(delete_message_after_delay(response_msg, delay=20))

        else:
            # Send bot's response
            response_msg = await message.answer(
                "🔥 <b>Munna Bhaiya bol rahe hain...</b>\n\n"
                "🔍 <b>How to request files:</b>\n"
                "━━━━━━━━━━━━━━━━━━━\n\n"
                "• 🎥 <b>Movies:</b> <code>Title Year</code>\n"
                "   Example: <code>Rustom 2016</code>\n\n"
                "• 📺 <b>Series:</b> <code>Title SXX</code>\n"
                "   Example: <code>Paatal Lok S01</code>\n\n"
                "<u>Note</u>: Request sirf <b>StreamTap Group</b> mein hi karna. Aur kahin nahi. 🫡",
                parse_mode="HTML",
            )

            # Delete user's /start command first
            await asyncio.sleep(2)
            await message.delete()

            # Schedule deletion of bot's response after 20 seconds
            asyncio.create_task(delete_message_after_delay(response_msg, delay=20))
            return

    except TelegramBadRequest as e:
        if "message to delete not found" in str(e):
            logger.debug("User's message already deleted")
        else:
            logger.error(f"Error in /start handler: {e}")


# Generate one time invite link
@dp.message(F.text == "/get_link")
async def generate_token(message: Message):
    # Make sure user id is not None
    if message.from_user is None:
        return

    member = await bot.get_chat_member(
        chat_id=PRIVATE_GROUP_ID, user_id=message.from_user.id
    )

    if member.status in ("administrator", "creator"):
        try:
            invite_link: ChatInviteLink = await bot.create_chat_invite_link(
                chat_id=PRIVATE_GROUP_ID,
                expire_date=datetime.now(timezone.utc) + timedelta(minutes=1),
                member_limit=1,
                creates_join_request=False,
            )

            response_msg = await message.answer(
                text=f"🔗 *Your invite link is ready!*\n\n"
                "*Use this link within the next 1 minute:*\n"
                f"*[🚀 Join Now]*({invite_link.invite_link})\n\n"
                "_Note:_ This is for *1 person only*.\n"
                "If shared and used by someone else, no new links or refunds will be provided.\n\n"
                "*Thank you for your understanding!* 💙",
                parse_mode="Markdown",
            )

            asyncio.create_task(delete_message_after_delay(response_msg, delay=70))
            return

        except Exception as e:
            logging.error(f"Failed to create link: {e}")
            response_msg = await message.reply(
                "❌ *Falied to create invite link.*", parse_mode="Markdown"
            )

            asyncio.create_task(delete_message_after_delay(response_msg, delay=30))
            return
    else:
        response_msg = await message.answer(
            "❌ *You are not allowed to use this command.*",
            parse_mode="Markdown",
        )

        asyncio.create_task(delete_message_after_delay(response_msg, delay=7))
        return


@dp.message(F.text == "/stats")
async def send_stats(message: Message):
    # Make sure user id is not None
    if message.from_user is None:
        return

    member = await bot.get_chat_member(
        chat_id=PRIVATE_GROUP_ID, user_id=message.from_user.id
    )

    if member.status in ("administrator", "creator"):
        try:
            user_count, total_amt = get_stats()
            response_msg = await message.answer(
                f"📊 <b>Stats:\n\n👥 Users: {user_count}\n💰 Total Collected: ₹{total_amt}</b>",
                parse_mode="HTML",
            )

            asyncio.create_task(delete_message_after_delay(response_msg, delay=30))
            return

        except Exception as e:
            response_msg = await message.reply(
                "❌ *Falied to get stats.*", parse_mode="Markdown"
            )

            asyncio.create_task(delete_message_after_delay(response_msg, delay=30))
            return
    else:
        response_msg = await message.answer(
            "❌ *You are not allowed to use this command.*",
            parse_mode="Markdown",
        )

        asyncio.create_task(delete_message_after_delay(response_msg, delay=7))
        return


# TODO Pending to check bot activation in private chat
@dp.message(F.new_chat_members)
async def on_user_joined(message: Message):
    # First extrat all new members before deleting
    new_members = message.new_chat_members

    # Then delete the join telegram service message
    try:
        logger.info("Deleting the `joined chat` telegram serivce message.")
        await message.delete()
    except Exception as e:
        logger.error(f"Failed to delete join message: {e}")

    # Object can be None
    if not new_members:
        return

    # Welcome to new memeber
    for member in new_members:
        first_name = member.first_name or "there"
        try:
            # Create inline button with bot link
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="👉 Start Bot",
                            url=f"https://t.me/{BOT_USERNAME}?start=start",
                        )
                    ]
                ]
            )

            response_msg = await message.answer(
                f"🎉 *Hey {first_name}, welcome to StreamTap* ✌️🎥\n\n"
                "🔒 *To use this bot, you must start it in private first.*\n"
                "👇 *Tap the button below to open the bot and press the START button there.*",
                parse_mode="Markdown",
                reply_markup=keyboard,
            )

            # Make sure user id is not None
            if message.new_chat_members is None:
                return

            # Add the user to firebase database
            for user in message.new_chat_members:
                # Skip bots
                if user.is_bot:
                    continue

                member = await bot.get_chat_member(
                    chat_id=PRIVATE_GROUP_ID, user_id=user.id
                )
                if member.status in ("administrator", "creator"):
                    continue

                add_new_user(str(user.id))

            asyncio.create_task(delete_message_after_delay(response_msg, delay=20))
            return

        except TelegramBadRequest as e:
            logger.error(f"Failed to send welcome messgae: {e}")

    # Check bot has started in private chat or not
    for user in new_members:
        if not await has_user_started_bot(user.id):
            btn = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="👉 Start Bot",
                            url=f"https://t.me/{BOT_USERNAME}?start=start",
                        )
                    ]
                ]
            )

            warn_msg = await message.answer(
                "⚠️ *Please start the bot in private chat first!*\n\n"
                "Just click the button below and press *START*.\nThen try again in the group.",
                parse_mode="Markdown",
                reply_markup=btn,
            )
            asyncio.create_task(delete_message_after_delay(warn_msg, delay=20))
            return


@dp.message(F.left_chat_member)
async def on_user_left(message: Message):
    try:
        logger.info("Deleting the `left chat` telegram service message.")
        await message.delete()
    except Exception as e:
        logger.error(f"Failed to delete leave message: {e}")


@dp.message(F.text.startswith("/seven_days"))
async def add_seven_days_handler(message: Message):
    # Make sure user id is not None
    if message.from_user is None:
        return

    if message.text is None:
        return

    member = await bot.get_chat_member(
        chat_id=PRIVATE_GROUP_ID, user_id=message.from_user.id
    )

    if member.status in ("administrator", "creator"):
        try:
            parts = message.text.strip().split()

            if len(parts) != 2:
                response_msg = await message.answer(
                    "⚠️ *You forgot to give user id with command.*",
                    parse_mode="Markdown",
                )

                asyncio.create_task(delete_message_after_delay(response_msg, delay=30))
                return

            user_id = parts[1]

            success = add_extra_7_days(user_id)
            await message.delete()

            if success:
                response_msg = await message.answer(
                    "✅ *7 extra days added to your subscription!*",
                    parse_mode="Markdown",
                )

                asyncio.create_task(delete_message_after_delay(response_msg, delay=30))
                return

            else:
                response_msg = await message.answer(
                    "❌ *You are not registered or your subscription was not found.*",
                    parse_mode="Markdown",
                )

                asyncio.create_task(delete_message_after_delay(response_msg, delay=30))
                return

        except Exception as e:
            response_msg = await message.reply(
                "❌ *Falied to add 7 days extra.*", parse_mode="Markdown"
            )

            asyncio.create_task(delete_message_after_delay(response_msg, delay=30))
            return

    else:
        response_msg = await message.answer(
            "❌ *You are not allowed to use this command.*",
            parse_mode="Markdown",
        )

        asyncio.create_task(delete_message_after_delay(response_msg, delay=7))
        return


@dp.message(F.text.startswith("/ydl"))
async def download_upload(message: Message):
    # 1) Permission & parsing
    if not message.text or not message.from_user:
        return

    parts = message.text.strip().split()
    if len(parts) != 2:
        return await message.answer(
            "⚠️ *Usage:* `/ydl <YouTube_URL>`", parse_mode="Markdown"
        )

    url = parts[1]
    status_msg = await message.answer(
        "⏳ *Received your request...*", parse_mode="Markdown"
    )
    chat_id = status_msg.chat.id
    chat_type = message.chat.type

    member = await bot.get_chat_member(
        chat_id=PRIVATE_GROUP_ID, user_id=message.from_user.id
    )

    if member.status in ("administrator", "creator"):
        if chat_type == "private":
            try:
                # 2) Show downloading status
                await status_msg.edit_text("📥 *Downloading...*", parse_mode="Markdown")

                # 3) Download video
                if "youtube.com" in url or "youtu.be" in url:
                    video_path, thumb_path, title, duration, width, height = (
                        await asyncio.to_thread(download_youtube_video, url, 1080)
                    )
                else:
                    await status_msg.edit_text(
                        "❌ *This is not a valid link of YouTube video.*",
                        parse_mode="Markdown",
                    )

                if not video_path:
                    return await status_msg.edit_text(
                        "❌ *Failed to download video.*", parse_mode="Markdown"
                    )

                # 4) Show upload start
                await status_msg.edit_text("📤 *Uploading...*", parse_mode="Markdown")

                # 5) Send video to Telegram
                if thumb_path is not None:
                    thumbnail = FSInputFile(thumb_path)

                video_file = FSInputFile(video_path)

                try:
                    await bot.send_video(
                        chat_id=chat_id,
                        video=video_file,
                        thumbnail=thumbnail,
                        caption=f"<b>{title}</b>\n\n<a href='{url}'><b>Watch on YouTube</b></a>",
                        duration=duration,
                        width=width,
                        height=height,
                        supports_streaming=True,
                        request_timeout=600,
                        parse_mode="HTML",
                    )

                    await status_msg.delete()

                    await bot.send_message(
                        chat_id, "✅ *Uploaded successfully!*", parse_mode="Markdown"
                    )

                except Exception as e:
                    await status_msg.edit_text(
                        "❌ *Failed to upload video.*", parse_mode="Markdown"
                    )
                    logger.error(f"Failed to send video: {e}")

                # 6) Cleanup after 5 minutes
                async def _cleanup():
                    await asyncio.sleep(420)
                    for p in (video_path, thumb_path):
                        if p:
                            await delete_file_with_retry(p)

                asyncio.create_task(_cleanup())

            except Exception as e:
                logger.error(f" Error: {e}")

        else:
            await message.delete()
            await status_msg.delete()
            response_msg = await message.answer(
                "❌ *This command can only be used in private chat*",
                parse_mode="Markdown",
            )

            asyncio.create_task(delete_message_after_delay(response_msg, delay=7))
            return

    else:
        await message.delete()
        await status_msg.delete()
        response_msg = await message.answer(
            "❌ *You are not allowed to use this command.*",
            parse_mode="Markdown",
        )

        asyncio.create_task(delete_message_after_delay(response_msg, delay=7))
        return


async def delete_file_with_retry(path, max_retries=5, delay=1):
    for _ in range(max_retries):
        try:
            os.remove(path)
            logger.info(f"Deleted {path}")
            return
        except PermissionError:
            logger.warning(f"PermissionError deleting {path}, retrying...")
            await asyncio.sleep(delay)
        except FileNotFoundError:
            logger.info(f"File {path} not found")
            return
        except Exception as e:
            logger.error(f"Error deleting {path}: {e}")
            return
    logger.error(f"Failed to delete {path} after {max_retries} retries")


async def has_user_started_bot(user_id: int) -> bool:
    try:
        await bot.send_chat_action(user_id, "typing")
        return True
    except TelegramForbiddenError:
        return False


def start_expiry_monitoring(loop):
    while True:
        try:
            logger.info("Expiry monitoring check...")
            expiring_data = (
                get_expiring_users()
            )  # For testing pass test_mode=True as argument

            if expiring_data is None:
                time.sleep(30)
                continue

            # Notify users whose plans are expiring in 7 days
            for user_id, end_date in expiring_data["soon"]:
                asyncio.run_coroutine_threadsafe(
                    notify_user_plan_expiry(user_id, end_date, days_left=7), loop
                )

            # Notify and users whose plans have expired
            for user_id, end_date in expiring_data["expired"]:
                asyncio.run_coroutine_threadsafe(
                    notify_user_plan_expiry(user_id, end_date, days_left=0), loop
                )

                # Remove user from the database
                remove_user(user_id)

                # Remove from the private group and database
                asyncio.run_coroutine_threadsafe(
                    remove_user_from_private_group(user_id, PRIVATE_GROUP_ID), loop
                )

        except Exception as e:
            logger.error(f"💥 Error during monitoring loop: {e}")

        # Wait 30 seconds before the next check
        time.sleep(30)


async def notify_user_plan_expiry(user_id: str, end_date: str, days_left: int):
    try:
        if days_left == 7:
            text = (
                f"⏳ <b>Your plan will expire on <i>{end_date}</i> (in 7 days).</b>\n\n"
                "<b>Please renew soon to continue enjoying our service.</b>"
            )
        else:
            text = (
                f"❌ <b>Your plan expired on <i>{end_date}</i>.</b>\n\n"
                "<b>Access has been removed. Please renew to continue.</b>"
            )

        await bot.send_message(int(user_id), text, parse_mode="HTML")
        logger.info(f"Sent expiry message to user {user_id}")

    except Exception as e:
        logger.error(f"Failed to notify user {user_id}: {e}")


async def remove_user_from_private_group(user_id: int, group_chat_id: int):
    try:
        await bot.ban_chat_member(group_chat_id, int(user_id))
        await bot.unban_chat_member(group_chat_id, int(user_id))
        logger.info(f"Removed user {user_id} from private group {group_chat_id}")
    except Exception as e:
        logger.error(f"❌ Failed to remove user {user_id} from group: {e}")


# TODO Add new keywords in ignore_keywords
async def validate_search_query(
    query: str, bot: Bot, receiver: int, original_message_id: int
) -> bool:
    """
    Validate the search query and send error messages if invalid.

    Args:
        query: The search query to validate
        bot: The bot instance for sending messages
        receiver: The message receiver
        original_message_id: ID of the original message

    Returns:
        bool: True if query is valid, False if invalid
    """
    query = query.strip()

    # Check for special characters (allow only space, colon)
    contains_special_chars = any(not c.isalnum() and c not in " :" for c in query)
    is_plain_text = query.strip().isalnum()

    # Check if it's valid format
    if contains_special_chars and not is_plain_text:
        await send_invalid_format_message(bot, receiver, original_message_id)
        return False

    # Keywords to ignore
    ignore_keywords = [
        "mp4",
        "mkv",
        "zip",
        "Cam",
        "HDHub",
        "Print",
        "CamRec",
        "PreDVD",
        "Part01",
        "Part02",
        "Part03",
        "Part04",
        "HDPrint",
        "Part001",
        "Part002",
        "Part003",
        "Part004",
        "CineVood",
        "Bollyflix",
        "Vegamovies",
    ]

    # Case-insensitive regex search
    if any(
        re.search(re.escape(keyword), query, re.IGNORECASE)
        for keyword in ignore_keywords
    ):
        await send_invalid_format_message(bot, receiver, original_message_id)
        return False

    return True


async def send_invalid_format_message(bot, receiver, original_message_id):
    message = (
        "*❗Invalid request format!*\n\n"
        "*Send in this format:*\n"
        "• 🎥 *Movies:* `Title Year`\n"
        "   Example: `Rustom 2016`\n\n"
        "• 📺 *Series:* `Title SXX`\n"
        "   Example: `Paatal Lok S01`\n\n"
    )
    response_msg = await bot.send_message(
        receiver,
        message,
        parse_mode="Markdown",
        reply_to_message_id=original_message_id,
    )
    asyncio.create_task(delete_message_after_delay(response_msg, delay=20))


async def fetch_and_send_file(
    query: str,
    reply_chat_id: int,
    receiver: int,
    original_message_id: int,
    requester_name: str,
):
    """
    Fetch and send the files to respective user to their personal chat.
    """
    logger.info(f"Query found in fetch_and_send_file: {query}")
    global search_msg

    try:
        if not await validate_search_query(
            query, bot, reply_chat_id, original_message_id
        ):
            return

        # Check if the query contains Hindi characters (Devanagari Unicode block)
        hindi_pattern = re.compile("[\u0900-\u097F]")  # Devanagari block for Hindi

        # If the query contains Hindi characters
        if hindi_pattern.search(query):
            response_msg = await bot.send_message(
                reply_chat_id,
                "❗ *Sorry, I only understand English. Please wait for the admin's reply.*\n\n"
                "❗ *क्षमा करें, मैं केवल अंग्रेजी समझता हूं। कृपया व्यवस्थापक के उत्तर की प्रतीक्षा करें।*",
                parse_mode="Markdown",
                reply_to_message_id=original_message_id,
            )
            asyncio.create_task(delete_message_after_delay(response_msg, delay=20))
            return

        # Initial search message
        searching_msg = await bot.send_message(
            reply_chat_id,
            f"🔍 *Searching for '{query}'\nPlease wait...*",
            parse_mode="Markdown",
            reply_to_message_id=original_message_id,
        )

        # Send the query to search for fetching the file from database
        results = search_files(query)
        print(f"Result from db: {results}")

        # If files found then send it to respective user
        if results:
            # 🔁 Edit the "Searching..." message to say "Sending..."
            await bot.edit_message_text(
                chat_id=reply_chat_id,
                message_id=searching_msg.message_id,
                text=f"📤 *Found {len(results)} result(s) for '{query}'*\n_Sending files..._",
                parse_mode="Markdown",
            )

            file_count = 0

            # Data present in result = (title, message_id, quality)
            for result in results:
                title = result[0]
                msg_id = result[1]

                try:
                    await bot.copy_message(
                        chat_id=receiver,
                        from_chat_id=DATABASE_ID,
                        message_id=msg_id,
                        protect_content=True,
                    )

                    file_count += 1
                    logger.info(f"{file_count} => Sent: {title}")

                except Exception as e:
                    logger.info(f"⚠️ Failed to sent `{title}` (ID: {msg_id}): {e}")

            # Reply to the user's original message with their first name
            first_name = requester_name.split()[0]

            if file_count > 0:
                response_msg = await bot.send_message(
                    reply_chat_id,
                    f"*Hey {first_name}, check your DM! I've sent total {file_count} files there. 📂*",
                    parse_mode="Markdown",
                    reply_to_message_id=original_message_id,
                )
                asyncio.create_task(delete_message_after_delay(response_msg, delay=10))
            else:
                response_msg = await bot.send_message(
                    reply_chat_id,
                    f"❌ *Hey {first_name}, failed to send the files to your DM. Something went wrong.*",
                    parse_mode="Markdown",
                    reply_to_message_id=original_message_id,
                )
                asyncio.create_task(delete_message_after_delay(response_msg, delay=10))

        else:
            response_msg = await bot.send_message(
                reply_chat_id,
                "*🚫 No files found.*\n\n"
                "*Please check your spelling and try again.*\n\n"
                "*Not released on OTT.*\n\n*If the issue continues, contact the Owner/Admin.💡*\n\n"
                "*Send in this format:*\n"
                "• 🎥 *Movies:* `Title Year`\n"
                "   Example: `Rustom 2016`\n\n"
                "• 📺 *Series:* `Title SXX`\n"
                "   Example: `Paatal Lok S01`\n\n",
                parse_mode="Markdown",
                reply_to_message_id=original_message_id,
            )

            search_msg = True
            asyncio.create_task(delete_message_after_delay(response_msg, delay=25))

        # Delete the searching message after completion
        if searching_msg:
            await bot.delete_message(
                chat_id=reply_chat_id, message_id=searching_msg.message_id
            )

    except RPCError as e:
        logger.info(f"RPC Error: {e}")
        response_msg = await bot.send_message(
            reply_chat_id,
            "❌ *An error occurred while processing your request.* 💻",
            parse_mode="Markdown",
            reply_to_message_id=original_message_id,
        )
        search_msg = True
        asyncio.create_task(delete_message_after_delay(response_msg, delay=20))

    except Exception as e:
        logger.info(f"Unexpected error: {e}")
        response_msg = await bot.send_message(
            reply_chat_id,
            "❌ *An unexpected error occurred. Please try again later.*",
            parse_mode="Markdown",
            reply_to_message_id=original_message_id,
        )
        search_msg = True
        asyncio.create_task(delete_message_after_delay(response_msg, delay=20))


@dp.message()
async def handle_query(message: Message):
    """
    Process user queries.
    """
    logger.info(f"\n\nQuery found in handle_query: {message}\n\n")
    global active_searches, is_shutting_down

    # Ignore the `Bot left` service message
    if message.left_chat_member and message.left_chat_member.id == bot.id:
        return

    # Ignore all the messages in database group
    if message.chat.id == DATABASE_ID:
        return

    # List of allowed groups where the bot should stay
    allowed_group_ids = [PRIVATE_GROUP_ID, DATABASE_ID]

    # Check if the message is from a group/channel
    if message.chat.type in ["group", "supergroup", "channel"]:
        # If the group is NOT in the allowed list, make the bot leave
        if message.chat.id not in allowed_group_ids:
            response_msg = await message.reply(
                "❌ *This bot is restricted to specific groups. Leaving...*",
                parse_mode="Markdown",
            )

            asyncio.create_task(delete_message_after_delay(response_msg, delay=5))
            await asyncio.sleep(7)
            await bot.leave_chat(message.chat.id)
            return

    # Make sure user id and user name is not None
    if message.from_user is None:
        return

    chat_type = message.chat.type
    reply_chat_id = message.chat.id
    requester_id = message.from_user.id
    original_message_id = message.message_id
    requester_name = message.from_user.first_name

    # Not allowed to chat with bot
    if chat_type == "private" and requester_id != OWNER_ID:
        response_msg = await bot.send_message(
            reply_chat_id,
            "❌ *You are not allowed to use this bot in private chat. Please use it in the group.*",
            parse_mode="Markdown",
        )

        await message.delete()
        asyncio.create_task(delete_message_after_delay(response_msg, delay=3))
        return

    """
        Check the message before processing it, 
        message is not sent by the bot, 
        message is sent by user not by bot using the credentials of user
    """
    if (
        not message.text
        or (message.from_user is not None and message.from_user.is_bot)
        or message.sender_chat
    ):
        return

    # Fetch user's status in this chat
    try:
        member = await bot.get_chat_member(
            chat_id=message.chat.id,
            user_id=message.from_user is not None and message.from_user.id,
        )
    except TelegramBadRequest as e:
        logger.warning(f"Could not get chat member status: {e}")
        return

    # Ignore all the messages sent by admins/owners that are NOT /turnoff
    if member.status in (
        "administrator",
        "creator",
    ) and not message.text.lower().startswith("/turnoff"):
        logger.info("Ignored admin/owner message.")
        return

    if message.text.lower().startswith("/turnoff"):
        if message.chat.id != PRIVATE_GROUP_ID:
            response_msg = await message.answer(
                "⚠️ *This command can only be used by admins in the StreamTap group.*",
                parse_mode="Markdown",
            )
            asyncio.create_task(delete_message_after_delay(response_msg, delay=20))
            return

        # Move this here so it's only checked inside the correct group
        if member.status not in ("administrator", "creator"):
            response_msg = await message.answer(
                "❌ *You are not allowed to use this command.*", parse_mode="Markdown"
            )
            await asyncio.sleep(3)
            await message.delete()
            asyncio.create_task(delete_message_after_delay(response_msg, delay=20))
            return

        # Now it's safe to proceed
        if is_shutting_down:
            await message.answer(
                "*The bot is already shutting down.*", parse_mode="Markdown"
            )
            return

        is_shutting_down = True
        logger.info("Shutting down the bot...")
        await message.delete()
        response_msg = await message.answer(
            "🛑 *Bot ab band ho raha hai, agli baar phir se milte hain. Shukriya* 🙏",
            parse_mode="Markdown",
        )

        asyncio.create_task(delete_message_after_delay(response_msg, delay=3))
        await asyncio.sleep(3)

        await dp.stop_polling()
        await bot.session.close()
        await dp.storage.close()

        os._exit(0)

    # Check if the message start with /ignore
    if message.text.lower().startswith("/ignore"):
        # Ignore messages that match "/ignore + Plain Text"
        if re.match(r"^/ignore\s+\w.*$", message.text.strip(), re.IGNORECASE):
            print("Ignored /ignore command with plain text.")
            return
        else:
            response_msg = await message.answer(
                "*Oops! You forgot to add something after /ignore. Please type some text.*",
                parse_mode="Markdown",
            )

            await asyncio.sleep(3)
            await message.delete()
            asyncio.create_task(delete_message_after_delay(response_msg, delay=20))
            return

    # Validate the query format
    query = message.text.strip()
    if not re.match(VALID_QUERY_REGEX, query):
        print(f"Handle query: {query}")
        response_msg = await message.reply(
            "*❗Invalid request format!*\n\n"
            "*Send in this format:*\n"
            "• 🎥 *Movies:* `Title Year`\n"
            "   Example: `Rustom 2016`\n\n"
            "• 📺 *Series:* `Title SXX`\n"
            "   Example: `Paatal Lok S01`\n\n",
            parse_mode="Markdown",
        )
        asyncio.create_task(delete_message_after_delay(response_msg, delay=20))
        return

    # Check if the query is already being processed
    if query in active_searches:
        print(f"Duplicate request ignored: {query}")

    # Mark the query as active
    active_searches[query].add(
        asyncio.create_task(
            fetch_and_send_file(
                query=query,
                reply_chat_id=reply_chat_id,
                receiver=requester_id,
                original_message_id=original_message_id,
                requester_name=requester_name,
            )
        )
    )

    # Wait for the task to complete
    try:
        for task in active_searches[query]:  # Iterate over each task in the set
            await task  # Await each individual task
    except Exception as e:
        logger.info(f"Error during processing query {query}: {e}")
    finally:
        # Remove the query from active searches
        del active_searches[query]


async def delete_message_after_delay(message: Message, delay: int):
    """
    Deletes a bot message after a specified delay.
    """
    await asyncio.sleep(delay)
    try:
        member = await bot.get_chat_member(message.chat.id, bot.id)
        if member.status not in ["left", "kicked"]:
            await bot.delete_message(
                chat_id=message.chat.id, message_id=message.message_id
            )
    except TelegramForbiddenError:
        logger.info(f"Bot is not a member of this group anymore. Skipping deletion.")
    except TelegramBadRequest as e:
        logger.warning(f"Bad request while deleting message: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while deleting message: {e}")


async def discard_db_group_updates():
    """
    Discards all pending updates from the DB group before polling starts.
    """
    try:
        # Get the VERY LAST update in Telegram's server
        last_updates = await bot.get_updates(
            offset=-1, limit=1, timeout=1, allowed_updates=["message"]
        )

        if not last_updates:
            logger.info("No updates to process.")
            return

        # Find the highest update_id across all messages
        max_update_id = last_updates[-1].update_id

        # Immediately acknowledge ALL updates up to max_update_id
        await bot.get_updates(offset=max_update_id + 1)
        logger.info(
            f"Skipped ALL updates from DB group up to {max_update_id} in one request."
        )
    except Exception as e:
        logger.error(f"Error skipping updates: {e}")


async def bot_start_message(chat_id: int):
    response_msg = await bot.send_message(
        chat_id,
        "🚬 Munna Bhaiya yaha hain. \n\nKya chahiye? movie, series, ya goli?",
    )

    asyncio.create_task(delete_message_after_delay(response_msg, delay=12))
    return


@app.route("/")
def index():
    return "StreamTap bot is running with flask and threading!"


# TODO
def start_server():
    # logger.info("Starting Flask...")
    serve(app, host="0.0.0.0", port=8000)


def run_server():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start_server()


async def run_aiogram():
    logger.info("Starting Aiogram...")
    try:
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        logger.info("Polling cancelled. Shutting down Aiogram...")
    finally:
        await bot.session.close()
        logger.info("Aiogram bot session closed.")


async def main():
    """
    Start the bot with concurrent tasks for Telethon and Aiogram.
    """
    # Initialize the client
    client = TelegramClient(SESSION_NAME, TELEGRAM_API_ID, TELEGRAM_API_HASH)

    try:
        logger.info("Starting Telethon user client...")
        await client.connect()
        logger.info("Telethon user client is running!")

        # Clean DB group messages before polling
        await discard_db_group_updates()

        logger.info("Sending bot startup message...")
        await bot_start_message(chat_id=PRIVATE_GROUP_ID)

        # Run Flask server in a separate thread
        server_thread = threading.Thread(target=run_server)
        server_thread.daemon = True
        server_thread.start()

        loop = asyncio.get_running_loop()
        monitoring_thread = threading.Thread(
            target=start_expiry_monitoring, args=(loop,)
        )
        monitoring_thread.daemon = True
        monitoring_thread.start()

        removal_thread = threading.Thread(target=process_removal_queue)
        removal_thread.daemon = True
        removal_thread.start()

        logger.info("Starting Aiogram polling...")
        await dp.start_polling(bot)

        # Keep the event loop running
        await client.run_until_disconnected()  # type: ignore

    except AuthKeyDuplicatedError:
        logger.error("AuthKeyDuplicatedError detected! Shutting down service.")
        os._exit(1)  # Force exit to stop the Render service

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        os._exit(1)  # Exit the process to suspend the service

    finally:
        client.disconnect()  # Disconnect safely


if __name__ == "__main__":
    try:
        asyncio.run(main())  # Run the main function
    except KeyboardInterrupt:
        logger.info("Graceful shutdown initiated...")
    finally:
        logger.info("Existing the application.")
