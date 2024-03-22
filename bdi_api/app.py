import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uptrace
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from starlette import status
from starlette.responses import JSONResponse

import bdi_api
from bdi_api.examples import v0_router
from bdi_api.s1.exercise import s1
from bdi_api.s4.exercise import s4
from bdi_api.s8.exercise import s8

from bdi_api.settigns import Settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator:
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    logger.info("Application started. You can check the documentation in https://localhost:8000/docs/")
    yield
    # Shut Down
    logger.warning("Application shutdown")


app = FastAPI(
    title=bdi_api.__name__,
    version=bdi_api.__version__,
)

uptrace.configure_opentelemetry(
    # Copy DSN here or use UPTRACE_DSN env var.
    dsn=Settings().telemetry_dsn,
    service_name=bdi_api.__name__,
    service_version=bdi_api.__version__,
    logging_level=logging.INFO,
)
FastAPIInstrumentor.instrument_app(app)
app.include_router(v0_router)
app.include_router(s1)
app.include_router(s4)
app.include_router(s8)


@app.get("/health", status_code=200)
async def get_health() -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content="ok",
    )


@app.get("/version", status_code=200)
async def get_version() -> dict:
    return {"version": bdi_api.__version__}


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000, access_log=False)


if __name__ == "__main__":
    main()
