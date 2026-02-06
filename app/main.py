from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api.routes import router as api_router
from app.api.ui import router as ui_router  # байгаа бол

app = FastAPI(title="CU Orchestrator", version="1.0.0")

# API
app.include_router(api_router, prefix="/api")

# UI (хэрэв ui.py байгаа бол)
app.include_router(ui_router)

# Static + templates
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
