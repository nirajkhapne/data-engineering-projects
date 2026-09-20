from mongodb.mongo_client import get_collection, ping
from utils.logging import get_logger

logger = get_logger(__name__)


def create_indexes() -> None:
    ping()
    collection = get_collection()
    collection.create_index("order_id", unique=True)
    collection.create_index("customer_id")
    logger.info("MongoDB indexes created")


if __name__ == "__main__":
    try:
        create_indexes()
    except Exception:
        logger.exception("Failed to create MongoDB indexes")
        raise
