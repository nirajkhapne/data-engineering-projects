from fastapi import APIRouter
from api.database import collection

router = APIRouter()


@router.get("/vehicle/{vehicle_no}")
def get_vehicle(vehicle_no: str):

    docs = list(collection.find(
        {"vehicle_no": vehicle_no},
        {"_id": 0}
    ))

    return docs


@router.get("/gps-provider-count")
def gps_provider_count():

    pipeline = [
        {
            "$group": {
                "_id": "$GpsProvider",
                "count": {"$sum": 1}
            }
        }
    ]

    return list(collection.aggregate(pipeline))
