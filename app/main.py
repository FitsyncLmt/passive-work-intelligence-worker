import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from xroiq_store import (
    init_db,
    list_actions,
    list_devices,
    list_events,
    list_sessions,
)
from xroiq_work_intelligence_service import DEFAULT_CONFIG_PATH, DEFAULT_DATABASE_PATH


APP_NAME = "XROIQ Work Intelligence Dashboard"
STATIC_DIR = Path(__file__).with_name("static")

log = logging.getLogger(APP_NAME)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def get_database_path() -> Path:
    config_path = Path(os.getenv("XROIQ_CONFIG_PATH", DEFAULT_CONFIG_PATH))
    if config_path.exists():
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
            database_path = config.get("database_path") or DEFAULT_DATABASE_PATH
            return Path(database_path).expanduser()
        except Exception:
            log.exception("Failed to read dashboard config; using default database path")
    return Path(DEFAULT_DATABASE_PATH).expanduser()


def database_status() -> Dict[str, Any]:
    db_path = get_database_path()
    try:
        init_db(db_path)
        return {
            "connected": True,
            "database_path": str(db_path),
            "error": None,
        }
    except Exception as exc:
        log.exception("Dashboard SQLite check failed")
        return {
            "connected": False,
            "database_path": str(db_path),
            "error": str(exc),
        }


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    status = database_status()
    if status["connected"]:
        log.info("Dashboard started with SQLite database: %s", status["database_path"])
    else:
        log.error("Dashboard started without SQLite connection: %s", status["error"])
    yield


app = FastAPI(title=APP_NAME, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8765",
        "http://localhost:8765",
    ],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> Dict[str, Any]:
    status = database_status()
    return {
        "service": APP_NAME,
        "status": "ok" if status["connected"] else "degraded",
        "db_connected": status["connected"],
        "database_path": status["database_path"],
        "error": status["error"],
    }


@app.get("/api/events")
def events(limit: int = 100) -> Dict[str, Any]:
    rows = list_events(get_database_path(), limit=limit)
    return {"items": rows, "count": len(rows)}


@app.get("/api/sessions")
def sessions(limit: int = 100) -> Dict[str, Any]:
    rows = list_sessions(get_database_path(), limit=limit)
    return {"items": rows, "count": len(rows)}


@app.get("/api/devices")
def devices() -> Dict[str, Any]:
    rows = list_devices(get_database_path())
    return {"items": rows, "count": len(rows)}


@app.get("/api/actions")
def actions(limit: int = 100) -> Dict[str, Any]:
    rows = list_actions(get_database_path(), limit=limit)
    return {"items": rows, "count": len(rows)}


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="dashboard")
