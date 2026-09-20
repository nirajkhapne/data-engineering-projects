from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import router
from mongodb.mongo_client import ping
from utils.logging import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        ping()
        logger.info("MongoDB connection verified")
        yield
    except Exception:
        logger.exception("MongoDB health check failed")
        raise


app = FastAPI(
    title="Streaming Analytics API",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(router)
