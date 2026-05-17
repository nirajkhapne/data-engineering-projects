from dotenv import load_dotenv
import os

load_dotenv()


class Settings:

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")

    MONGO_URI = os.getenv("MONGO_URI")

    MONGO_DB = os.getenv("MONGO_DB")

    MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")


settings = Settings()
