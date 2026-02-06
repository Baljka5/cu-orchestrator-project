from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routes import router as api_router
from app.api.ui import router as ui_router

app = FastAPI(title="CU Orchestrator", version="1.0.0")

app.include_router(api_router, prefix="/api")
app.include_router(ui_router)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
