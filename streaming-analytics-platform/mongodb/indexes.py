from mongodb.mongo_client import collection

collection.create_index(
    "order_id",
    unique=True
)
