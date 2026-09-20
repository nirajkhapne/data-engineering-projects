from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from mongodb.mongo_client import get_collection
from utils.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/orders/{order_id}")
def get_order(order_id: str):
    try:
        order = get_collection().find_one({"order_id": order_id}, {"_id": 0})
        if order is None:
            raise HTTPException(status_code=404, detail="Order not found")
        return order
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to fetch order %s", order_id)
        raise HTTPException(status_code=500, detail="Failed to fetch order") from exc
