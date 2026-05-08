from fastapi import FastAPI
from api.routes import router

app = FastAPI(title="Logistics Streaming API")

app.include_router(router)
