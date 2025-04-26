import time
import pytz
import logging
import firebase_admin
from datetime import datetime, timedelta
from firebase_admin import credentials, db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

cred = credentials.Certificate("firebase_credentials.json")
firebase_admin.initialize_app(
    cred, {"databaseURL": "https://telegram-payment-db-default-rtdb.firebaseio.com/"}
)

# Get IST timezone
india = pytz.timezone("Asia/Kolkata")


# Add new user
def add_new_user(user_id: str):
    # Get current IST datetime
    now_ist = datetime.now(india)

    # Calculate 30-days end date
    end_ist = now_ist + timedelta(days=30)

    # Convert to string
    start_date_str = now_ist.strftime("%d-%m-%Y %I:%M:%S %p")
    end_date_str = end_ist.strftime("%d-%m-%Y %I:%M:%S %p")

    # Firebase reference
    user_ref = db.reference(f"users/{user_id}")

    # Prevent overwrite
    if user_ref.get() is not None:
        logger.warning(
            f"⚠️  User {user_id} already exists. Skipping add to avoid overwrite."
        )
        return

    # Set user data
    user_ref.set(
        {"start_date": start_date_str, "end_date": end_date_str, "extra_days": 0}
    )

    logger.info(
        f"✅ User {user_id} added with premium access from {start_date_str} to {end_date_str}"
    )


# Add 7 days extra
def add_extra_7_days(user_id: str):
    user_ref = db.reference(f"users/{user_id}")
    user_data = user_ref.get()

    if not user_data:
        logger.warning(f"❌ Cannot add extra days. User {user_id} not found.")
        return False

    # Confirm it's dict (not tuple, not None)
    if not isinstance(user_data, dict):
        logger.warning(f"❌ Unexpected data type for user {user_id}: {type(user_data)}")
        return False

    # Parse end_date
    current_end = datetime.strptime(user_data["end_date"], "%d-%m-%Y %I:%M:%S %p")
    current_end = india.localize(current_end)

    # Add 7 days
    new_end = current_end + timedelta(days=7)
    new_end_str = new_end.strftime("%d-%m-%Y %I:%M:%S %p")

    # Update firebase
    user_ref.update(
        {"end_date": new_end_str, "extra_days": user_data.get("extra_days", 0) + 7}
    )

    logger.info(
        f"➕  Added 7 extra days to user {user_id}. New end date: {new_end_str}"
    )
    return True


def remove_user(user_id: str):
    removal_queue_ref = db.reference(f"removal_queue")

    now = datetime.now(india)
    timestamp_str = now.strftime("%d-%m-%Y %I:%M:%S %p")

    # Add to queue instead of deleting immediately
    removal_queue_ref.child(str(user_id)).set({"timestamp": timestamp_str})

    logger.info(f"🕒 Queued user {user_id} for removal after 24 hours.")


def process_removal_queue():
    while True:
        try:
            removal_queue_ref = db.reference("removal_queue")
            all_items = removal_queue_ref.get() or {}

            if not all_items:
                time.sleep(30)
                continue

            if not isinstance(all_items, dict):
                logger.warning("⚠️  Unexpected removal queue format.")
                time.sleep(30)
                continue

            now = datetime.now(india)

            for user_id, item in all_items.items():
                timestamp_str = item.get("timestamp")

                if not user_id or not timestamp_str:
                    continue

                try:
                    removal_time = datetime.strptime(
                        timestamp_str, "%d-%m-%Y %I:%M:%S %p"
                    )
                    removal_time = india.localize(removal_time)

                except Exception as e:
                    logger.error(f"Timestamp parsing error: {e}")
                    continue

                if now >= removal_time + timedelta(
                    hours=24
                ):  # For testing use minutes=1
                    logger.info(
                        f"⏳ For user: {user_id}. Now: {now.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    logger.info(
                        f"Scheduled (Added to removal queue): {removal_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    logger.info(
                        f"Deadline: {(removal_time + timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')}"
                    )

                    db.reference(f"users/{user_id}").delete()
                    logger.info(f"✅ Removed user {user_id} after 24 hours grace.")
                    removal_queue_ref.child(user_id).delete()
                else:
                    logger.info(
                        f"✅ Ready to delete {user_id}. Now: {now.strftime('%Y-%m-%d %H:%M:%S')}, "
                        f"Removal time: {removal_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )

        except Exception as e:
            logger.error(f"💥 Error processing removal queue: {e}")

        time.sleep(30)


def get_expiring_users():  # For testing pass test_mode=False as parameter
    users_ref = db.reference("users")
    all_users = users_ref.get()
    expiring_users = {"soon": [], "expired": []}

    if all_users is None:
        logger.info("✅ Firebase Telegram Payment DB is empty.")
        return
    elif not isinstance(all_users, dict):
        logger.warning("⚠️  Unexpected data format in Firebase. Skipping...")
        return

    # if test_mode:
    #     now = datetime.strptime("23-05-2025 10:35:00 AM", "%d-%m-%Y %I:%M:%S %p")
    #     now = india.localize(now)
    # else:
    now = datetime.now(india)

    for user_id, data in all_users.items():
        if not isinstance(data, dict):
            return

        try:
            end_date = datetime.strptime(data["end_date"], "%d-%m-%Y %I:%M:%S %p")
            end_date = india.localize(end_date)
            days_left = (end_date - now).days

            already_notified = data.get("notified", "")

            if days_left == 7 and already_notified != "soon":
                expiring_users["soon"].append((user_id, data["end_date"]))
                users_ref.child(user_id).update({"notified": "soon"})

            elif days_left <= 0 and already_notified != "expired":
                expiring_users["expired"].append((user_id, data["end_date"]))
                users_ref.child(user_id).update({"notified": "expired"})

        except Exception as e:
            logger.error(f"🔥 Error checking user {user_id}: {e}")

    return expiring_users


def get_stats():
    users_ref = db.reference("users")
    all_users = users_ref.get()

    if not all_users:
        logger.info("No users found in the database.")
        return 0, 0  # return both user count and amount as 0

    total_user = len(all_users)
    total_amount = total_user * 40

    logger.info(f"📊 Total active users: {total_user}, Total amount: {total_amount}")
    return total_user, total_amount


# add_new_user("123456789")
# add_extra_7_days("123456789")
# remove_user("123456789")
# get_expiring_users()
# get_stats()
