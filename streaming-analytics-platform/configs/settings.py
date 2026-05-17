from dotenv import load_dotenv
import os

load_dotenv()


class Settings:

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

    MONGO_URI = os.getenv("MONGO_URI")

    MONGO_DB = os.getenv("MONGO_DB")

    MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")


settings = Settings()
