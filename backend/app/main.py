from dotenv import load_dotenv
load_dotenv()

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from contextlib import asynccontextmanager

from app.routers import bigquery, credentials, endpoints, export, extract, oauth, platforms, schedule, schema


@asynccontextmanager
async def lifespan(app_instance):
    """Start the scheduler on startup and stop it on shutdown."""
    from app.core.scheduler import start_scheduler, stop_scheduler

    try:
        start_scheduler()
    except Exception:
        import logging
        logging.getLogger("ecommerce_data_extractor").warning("Scheduler failed to start", exc_info=True)
    yield
    try:
        stop_scheduler()
    except Exception:
        pass

app = FastAPI(
    title="EC Data Extractor",
    description="Read-only data extraction from multiple e-commerce platforms",
    version="0.1.0",
    lifespan=lifespan,
)

_cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Platform discovery
app.include_router(platforms.router, prefix="/api")
# Endpoints and schema are nested under /api/platforms/{id}/...
app.include_router(endpoints.router, prefix="/api")
app.include_router(schema.router, prefix="/api")
# Extract and export
app.include_router(extract.router, prefix="/api")
app.include_router(export.router, prefix="/api")
app.include_router(credentials.router, prefix="/api")
app.include_router(oauth.router, prefix="/api")
app.include_router(schedule.router, prefix="/api")
app.include_router(bigquery.router, prefix="/api")


@app.get("/")
async def health_check() -> dict:
    return {"status": "ok", "app": "EC Data Extractor"}
