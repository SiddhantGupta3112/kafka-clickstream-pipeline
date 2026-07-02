from confluent_kafka import Producer
from dotenv import load_dotenv
import os
import uuid
import random
from datetime import datetime, timezone
import time
import json
import logging

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
# Kafka Producer
# ---------------------------------------------------------------------
producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKER,
    }
)

logger.info("Kafka producer initialized.")

# ---------------------------------------------------------------------
# Test Users
# ---------------------------------------------------------------------
user_list = [str(uuid.uuid4()) for _ in range(20)]

logger.info(f"Generated {len(user_list)} simulated users.")

# ---------------------------------------------------------------------
# Delivery Callback
# ---------------------------------------------------------------------
def delivery_report(err, msg):
    if err is not None:
        logger.error(
            f"Delivery failed | User={msg.key().decode()} | Error={err}"
        )
    else:
        logger.info(
            f"Delivered | Topic={msg.topic()} | "
            f"Partition={msg.partition()} | "
            f"Offset={msg.offset()} | "
            f"User={msg.key().decode()}"
        )

# ---------------------------------------------------------------------
# Produce Messages
# ---------------------------------------------------------------------
try:
    logger.info("Starting event generation...")

    while True:

        user_id = random.choice(user_list)
        clicks = random.randint(3, 8)

        logger.info(
            f"Generating {clicks} click events for user {user_id}"
        )

        for i in range(clicks):

            click_event = {
                "user_id": user_id,
                "page": random.choice(
                    [
                        "home",
                        "about",
                        "contact",
                        "products",
                        "services",
                    ]
                ),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            logger.info(
                f"Producing event {i + 1}/{clicks}: {click_event}"
            )

            producer.produce(
                KAFKA_TOPIC,
                key=user_id,
                value=json.dumps(click_event),
                callback=delivery_report,
            )

            # Trigger delivery callbacks
            producer.poll(0)

            time.sleep(random.uniform(0.5, 2.0))

        logger.info("Flushing producer...")
        producer.flush()
        logger.info("Producer flushed successfully.")

except KeyboardInterrupt:
    logger.info("Keyboard interrupt received. Shutting down producer...")

except Exception:
    logger.exception("Unexpected error occurred in producer.")

finally:
    logger.info("Final producer flush...")
    producer.flush()
    logger.info("Producer shutdown complete.")