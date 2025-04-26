import os
import re
import json
import asyncio
import sqlite3
from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()

# Telegram API credentials
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "None")
SESSION_NAME = "my_session2"
CHANNEL_NAME = "sfudjodgpehjghlalbkldoijkoska"

DB_FILE = "index.db"
LAST_INDEXED_FILE = "last_indexed.json"

QUALITY_PATTERN = re.compile(r"(\d{3,4}p|HDRip|WEB-DL|PreDVD)", re.IGNORECASE)
EPISODE_PATTERN = re.compile(r"(S\d+E\d+|EP?\s?\d+|Episode\s+\d+)", re.IGNORECASE)


def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS files USING fts5(
            base_title,
            original_title,
            description,
            quality,
            message_id UNINDEXED
        )
        """
    )
    conn.commit()
    conn.close()


def extract_metadata(title: str) -> str:
    base_title = QUALITY_PATTERN.sub("", title)
    base_title = re.sub(r"\s+", " ", base_title).strip()
    base_title = re.sub(r"[-\s]+$", "", base_title)
    return base_title


def add_to_index(title: str, description: str, message_id: int):
    base_title = extract_metadata(title)
    quality = ",".join(QUALITY_PATTERN.findall(title))

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Avoid duplicate entries by deleting any existing ones with the same message_id
    cursor.execute("DELETE FROM files WHERE message_id = ?", (message_id,))
    cursor.execute(
        """
            INSERT INTO files (base_title, original_title, description, quality, message_id) VALUES (?, ?, ?, ?, ?)
        """,
        (base_title, title, description, quality, message_id),
    )

    conn.commit()
    conn.close()


def load_last_indexed() -> int:
    if not os.path.exists(LAST_INDEXED_FILE):
        return 0
    with open(LAST_INDEXED_FILE, "r") as f:
        return json.load(f).get("last_indexed_id", 0)


def save_last_index(message_id: int):
    with open(LAST_INDEXED_FILE, "w") as f:
        json.dump({"last_message_id": message_id}, f, indent=2)


async def scan_group(
    client: TelegramClient, group_username: str, batch_size: int = 100
):
    last_indexed_id = load_last_indexed()
    print(f"Starting from message ID: {last_indexed_id}")

    messages_batch = []

    async for message in client.iter_messages(
        group_username, min_id=last_indexed_id, reverse=True
    ):
        if message.document or message.video:
            title = message.file.name if message.file else "Unknown"
            description = message.text or ""

            if not title:
                continue

            messages_batch.append((title, description, message.id))

            if len(messages_batch) >= batch_size:
                for title, description, message_id in messages_batch:
                    add_to_index(title, description, message_id)
                    save_last_index(message_id)
                    print(f"Indexed: {title} (ID: {message_id})")
                messages_batch.clear()

    # Process any remaining
    for title, description, message_id in messages_batch:
        add_to_index(title, description, message_id)
        save_last_index(message_id)
        print(f"Indexed: {title} (ID: {message_id})")

    print("Indexing completed!!")


async def main():
    client: TelegramClient

    init_db()
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        await scan_group(client, CHANNEL_NAME)


if __name__ == "__main__":
    asyncio.run(main())
