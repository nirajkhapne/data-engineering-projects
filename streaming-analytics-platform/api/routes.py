from fastapi import APIRouter
from pymongo import MongoClient

from configs.settings import settings

router = APIRouter()

client = MongoClient(settings.MONGO_URI)

collection = client[
    settings.MONGO_DB
][settings.MONGO_COLLECTION]


@router.get("/orders/{order_id}")
def get_order(order_id: str):

    return list(collection.find(
        {"order_id": order_id},
        {"_id": 0}
    ))
