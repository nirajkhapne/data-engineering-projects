from typing import Any

from pymongo import UpdateOne

from mongodb.mongo_client import get_collection, ping
from monitoring.metrics import processed_records
from utils.logging import get_logger

logger = get_logger(__name__)


def upsert_order_batch(rows: list[dict[str, Any]]) -> int:
    """Idempotently upsert joined orders into MongoDB using order_id as the key."""
    if not rows:
        return 0

    ping()
    operations = [
        UpdateOne({"order_id": row["order_id"]}, {"$set": row}, upsert=True)
        for row in rows
        if row.get("order_id")
    ]

    if not operations:
        return 0

    result = get_collection().bulk_write(operations, ordered=False)
    processed = result.upserted_count + result.modified_count
    processed_records.inc(processed)
    return processed
