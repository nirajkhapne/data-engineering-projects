from fastapi import FastAPI
from api.routes import router

app = FastAPI(title="Streaming Analytics APIs")

app.include_router(router)
