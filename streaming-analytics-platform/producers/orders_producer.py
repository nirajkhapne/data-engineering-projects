import random
import time
from datetime import datetime, timezone

from configs.settings import settings
from utils.kafka import create_producer
from utils.logging import get_logger

logger = get_logger(__name__)


def build_order(order_number: int) -> dict:
    timestamp = datetime.now(timezone.utc).isoformat()
    return {
        "order_id": f"order_{order_number}",
        "order_date": timestamp,
        "created_at": timestamp,
        "customer_id": f"customer_{random.randint(1, 10)}",
        "amount": random.randint(100, 1000),
    }


def main() -> None:
    producer = create_producer()
    try:
        for order_number in range(1, 6):
            order = build_order(order_number)
            producer.send(settings.order_topic, value=order).get(timeout=10)
            logger.info("Published order: %s", order)
            time.sleep(5)
        producer.flush()
    except Exception:
        logger.exception("Order producer failed")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    main()
