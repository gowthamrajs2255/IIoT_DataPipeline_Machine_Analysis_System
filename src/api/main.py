from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from api.db import create_pool, close_pool
from api.analytics import router as analytics_router

app = FastAPI(title="Factory Analytics API")
Instrumentator().instrument(app).expose(app)


@app.on_event("startup")
async def startup():
    await create_pool()


@app.on_event("shutdown")
async def shutdown():
    await close_pool()


@app.get("/")
def root():
    return {"status": "Factory analytics service running"}


app.include_router(analytics_router)