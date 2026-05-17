from pymongo import MongoClient
from configs.settings import settings

client = MongoClient(settings.MONGO_URI)

collection = client[
    settings.MONGO_DB
][settings.MONGO_COLLECTION]

collection.create_index("order_id", unique=True)
