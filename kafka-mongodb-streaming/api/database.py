import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))

collection = client[
    os.getenv("MONGO_DB")
][os.getenv("MONGO_COLLECTION")]
