from pymongo import MongoClient
from pymongo.collection import Collection

from configs.settings import settings
from utils.logging import get_logger

logger = get_logger(__name__)

_client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
db = _client[settings.mongo_db]
collection = db[settings.mongo_collection]


def ping() -> None:
    """Fail fast when MongoDB is unavailable."""
    _client.admin.command("ping")


def get_collection() -> Collection:
    return collection


def close() -> None:
    _client.close()
