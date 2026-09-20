import random
import time
import uuid
from datetime import datetime, timezone

from configs.settings import settings
from utils.kafka import create_producer
from utils.logging import get_logger

logger = get_logger(__name__)


def build_payment(order_number: int) -> dict:
    timestamp = datetime.now(timezone.utc).isoformat()
    return {
        "payment_id": str(uuid.uuid4()),
        "order_id": f"order_{order_number}",
        "payment_date": timestamp,
        "created_at": timestamp,
        "amount": random.randint(100, 1000),
    }


def main() -> None:
    producer = create_producer()
    try:
        for order_number in range(1, 6):
            payment = build_payment(order_number)
            producer.send(settings.payment_topic, value=payment).get(timeout=10)
            logger.info("Published payment: %s", payment)
            time.sleep(7)
        producer.flush()
    except Exception:
        logger.exception("Payment producer failed")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    main()
