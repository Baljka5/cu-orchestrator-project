from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from app.config import APP_NAME
from app.core.logging import setup_logging
from app.api.routes import router

setup_logging()

app = FastAPI(title=APP_NAME)
app.include_router(router, prefix="/api")

app.mount("/ui", StaticFiles(directory="/app/ui", html=True), name="ui")

@app.get("/", response_class=HTMLResponse)
def home():
    with open("/app/ui/index.html", "r", encoding="utf-8") as f:
        return f.read()
