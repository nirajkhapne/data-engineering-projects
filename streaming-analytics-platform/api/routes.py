from fastapi import APIRouter

from mongodb.mongo_client import collection

router = APIRouter()


@router.get("/orders/{order_id}")
def get_order(order_id: str):

    return list(collection.find(
        {"order_id": order_id},
        {"_id": 0}
    ))
