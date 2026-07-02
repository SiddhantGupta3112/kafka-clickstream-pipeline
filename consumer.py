from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import os
import time
import json
import logging
from datetime import datetime

from db import get_db

# ---------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Environment Variables
# ---------------------------------------------------------------------
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

assert KAFKA_BROKER is not None, "KAFKA_BROKER environment variable is not set"
assert KAFKA_TOPIC is not None, "KAFKA_TOPIC environment variable is not set"

logger.info(f"Kafka Broker: {KAFKA_BROKER}")
logger.info(f"Kafka Topic : {KAFKA_TOPIC}")

# ---------------------------------------------------------------------
# Kafka Consumer
# ---------------------------------------------------------------------
consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "clickstream-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)

def on_assign(consumer, partitions):
    print(f"ASSIGNED: {partitions}")

def on_revoke(consumer, partitions):
    print(f"REVOKED: {partitions}")

consumer.subscribe(
    [KAFKA_TOPIC],
    on_assign=on_assign,
    on_revoke=on_revoke
)

logger.info("Kafka consumer started and subscribed to topic.")

# ---------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            time.sleep(1)
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue

            logger.error(f"Kafka consumer error: {msg.error()}")
            continue

        if msg.value() is None:
            logger.warning("Received Kafka message with no value. Skipping.")
            consumer.commit(message=msg, asynchronous=False)
            continue

        try:
            click_event = json.loads(msg.value().decode("utf-8"))
            logger.info(f"Received click event: {click_event}")

        except Exception:
            logger.exception("Failed to decode Kafka message.")
            consumer.commit(message=msg, asynchronous=False)
            continue

        select_query = """
            SELECT last_event_at
            FROM active_sessions
            WHERE user_id = %s
        """

        try:
            logger.info(
                f"Connecting to database for user {click_event['user_id']}..."
            )

            with get_db() as conn:
                logger.info("Database connection established.")

                with conn.cursor() as cursor:

                    logger.info(
                        f"Checking existing session for user {click_event['user_id']}."
                    )

                    cursor.execute(select_query, (click_event["user_id"],))
                    row = cursor.fetchone()

                    event_time = click_event["timestamp"]
                    parsed_event_time = datetime.fromisoformat(event_time)

                    if row is None:
                        logger.info(
                            f"No active session found for {click_event['user_id']}. "
                            "Creating a new session."
                        )

                        cursor.execute(
                            """
                            INSERT INTO active_sessions
                            (user_id, session_start, last_event_at, event_count)
                            VALUES (%s, %s, %s, 1)
                            """,
                            (
                                click_event["user_id"],
                                event_time,
                                event_time,
                            ),
                        )

                    else:
                        last_event_at = row[0]

                        inactivity = (
                            parsed_event_time - last_event_at
                        ).total_seconds()

                        logger.info(
                            f"Previous activity was {inactivity:.2f} seconds ago."
                        )

                        if inactivity > 1800:
                            logger.info(
                                "Session expired (>30 min). Starting a new session."
                            )

                            cursor.execute(
                                """
                                UPDATE active_sessions
                                SET session_id = gen_random_uuid(),
                                    session_start = %s,
                                    last_event_at = %s,
                                    event_count = 1
                                WHERE user_id = %s
                                """,
                                (
                                    event_time,
                                    event_time,
                                    click_event["user_id"],
                                ),
                            )

                        else:
                            logger.info(
                                "Existing session active. Incrementing event count."
                            )

                            cursor.execute(
                                """
                                UPDATE active_sessions
                                SET last_event_at = %s,
                                    event_count = event_count + 1
                                WHERE user_id = %s
                                  AND last_event_at < %s
                                """,
                                (
                                    event_time,
                                    click_event["user_id"],
                                    event_time,
                                ),
                            )

                conn.commit()
                logger.info(
                    f"Database transaction committed for user {click_event['user_id']}."
                )

            consumer.commit(message=msg, asynchronous=False)
            logger.info("Kafka offset committed.")

        except Exception:
            logger.exception("Failed while processing click event.")

except KeyboardInterrupt:
    logger.info("Keyboard interrupt received. Shutting down consumer.")

finally:
    consumer.close()
    logger.info("Kafka consumer closed.")