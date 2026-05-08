import os
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))

collection = client[
    os.getenv("MONGO_DB")
][os.getenv("MONGO_COLLECTION")]

collection.create_index("BookingID", unique=True)


def write_to_mongo(record):

    collection.bulk_write([
        UpdateOne(
            {"BookingID": record["BookingID"]},
            {"$set": record},
            upsert=True
        )
    ])
