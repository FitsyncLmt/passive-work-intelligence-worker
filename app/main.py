import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from xroiq_store import (
    get_event_counts_by_category,
    get_latest_event_time,
    get_recent_activity_summary,
    get_session_totals_by_category,
    init_db,
    insert_action,
    list_actions,
    list_devices,
    list_events,
    list_sessions,
)
from xroiq_device_intelligence import refresh_configured_device_health
from xroiq_founder_intelligence import generate_founder_intelligence_report
from xroiq_reports import backup_sqlite_database, generate_daily_report
from xroiq_storage_decisions import get_storage_decisions
from xroiq_work_intelligence_service import (
    DEFAULT_CONFIG,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATABASE_PATH,
)


APP_NAME = "XROIQ Work Intelligence Dashboard"
STATIC_DIR = Path(__file__).with_name("static")

log = logging.getLogger(APP_NAME)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def get_dashboard_config() -> Dict[str, Any]:
    config_path = Path(os.getenv("XROIQ_CONFIG_PATH", DEFAULT_CONFIG_PATH))
    if config_path.exists():
        try:
            loaded = json.loads(config_path.read_text(encoding="utf-8"))
            config = dict(DEFAULT_CONFIG)
            config.update(loaded)
            return config
        except Exception:
            log.exception("Failed to read dashboard config; using default database path")
    return dict(DEFAULT_CONFIG)


def get_database_path() -> Path:
    config = get_dashboard_config()
    database_path = config.get("database_path") or DEFAULT_DATABASE_PATH
    return Path(database_path).expanduser()


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
    allow_methods=["GET", "POST"],
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


@app.get("/api/device-health")
def device_health() -> Dict[str, Any]:
    rows = refresh_configured_device_health(get_dashboard_config(), get_database_path())
    return {"items": rows, "count": len(rows)}


@app.get("/api/summary")
def summary() -> Dict[str, Any]:
    db_path = get_database_path()
    recent = get_recent_activity_summary(db_path, hours=24)
    return {
        "events_by_category": get_event_counts_by_category(db_path),
        "session_minutes_by_category": get_session_totals_by_category(db_path),
        "latest_event_time": get_latest_event_time(db_path),
        "last_24h_event_count": recent["event_count"],
    }


@app.get("/api/storage-decisions")
def storage_decisions() -> Dict[str, Any]:
    return get_storage_decisions(get_dashboard_config(), get_database_path())


@app.post("/api/actions/refresh-device-health")
def action_refresh_device_health() -> Dict[str, Any]:
    db_path = get_database_path()
    items = refresh_configured_device_health(get_dashboard_config(), db_path)
    return _record_action(
        db_path,
        action_type="refresh_device_health",
        status="ok",
        files_processed=len(items),
        errors="",
        notes=f"Refreshed {len(items)} configured devices.",
        success=True,
    )


@app.post("/api/actions/backup-sqlite")
def action_backup_sqlite() -> Dict[str, Any]:
    db_path = get_database_path()
    try:
        destination = backup_sqlite_database(db_path, _backup_root())
        return _record_action(
            db_path,
            action_type="backup_sqlite",
            status="ok",
            files_processed=1,
            errors="",
            notes=f"SQLite snapshot created: {destination}",
            success=True,
            destination_path=str(destination),
        )
    except Exception as exc:
        log.exception("SQLite backup action failed")
        return _record_action(
            db_path,
            action_type="backup_sqlite",
            status="failed",
            files_processed=0,
            errors=str(exc),
            notes="SQLite snapshot was not created.",
            success=False,
            destination_path=None,
        )


@app.post("/api/actions/generate-daily-report")
def action_generate_daily_report() -> Dict[str, Any]:
    db_path = get_database_path()
    try:
        report_path = generate_daily_report(
            db_path=db_path,
            backup_root=_backup_root(),
            config=get_dashboard_config(),
        )
        return _record_action(
            db_path,
            action_type="generate_daily_report",
            status="ok",
            files_processed=1,
            errors="",
            notes=f"Daily report created: {report_path}",
            success=True,
            report_path=str(report_path),
        )
    except Exception as exc:
        log.exception("Daily report action failed")
        return _record_action(
            db_path,
            action_type="generate_daily_report",
            status="failed",
            files_processed=0,
            errors=str(exc),
            notes="Daily report was not created.",
            success=False,
            report_path=None,
        )


@app.post("/api/actions/generate-founder-intelligence-report")
def action_generate_founder_intelligence_report() -> Dict[str, Any]:
    db_path = get_database_path()
    try:
        report_path = generate_founder_intelligence_report(
            get_dashboard_config(),
            db_path,
            output_root=_backup_root(),
        )
        return _record_action(
            db_path,
            action_type="generate_founder_intelligence_report",
            status="ok",
            files_processed=1,
            errors="",
            notes=f"Founder intelligence report created: {report_path}",
            success=True,
            path=str(report_path),
        )
    except Exception as exc:
        log.exception("Founder intelligence report action failed")
        return _record_action(
            db_path,
            action_type="generate_founder_intelligence_report",
            status="failed",
            files_processed=0,
            errors=str(exc),
            notes="Founder intelligence report was not created.",
            success=False,
            path=None,
        )


@app.post("/api/actions/open-logs")
def action_open_logs() -> Dict[str, Any]:
    db_path = get_database_path()
    logs_path = _backup_root()
    logs_path.mkdir(parents=True, exist_ok=True)
    if os.name == "nt" and hasattr(os, "startfile"):
        try:
            os.startfile(str(logs_path))  # type: ignore[attr-defined]
            return _record_action(
                db_path,
                action_type="open_logs",
                status="ok",
                files_processed=1,
                errors="",
                notes=f"Opened logs folder: {logs_path}",
                success=True,
                logs_path=str(logs_path),
            )
        except Exception as exc:
            log.exception("Open logs action failed")
            return _record_action(
                db_path,
                action_type="open_logs",
                status="failed",
                files_processed=0,
                errors=str(exc),
                notes=f"Failed to open logs folder: {logs_path}",
                success=False,
                logs_path=str(logs_path),
            )

    return _record_action(
        db_path,
        action_type="open_logs",
        status="unsupported",
        files_processed=0,
        errors="unsupported",
        notes=f"Opening folders is unsupported on this platform: {logs_path}",
        success=False,
        logs_path=str(logs_path),
    )


@app.get("/api/actions")
def actions(limit: int = 100) -> Dict[str, Any]:
    rows = list_actions(get_database_path(), limit=limit)
    return {"items": rows, "count": len(rows)}


def _backup_root() -> Path:
    config = get_dashboard_config()
    return Path(config.get("backup_root") or DEFAULT_CONFIG["backup_root"]).expanduser()


def _record_action(
    db_path,
    *,
    action_type: str,
    status: str,
    files_processed: int,
    errors: str,
    notes: str,
    success: bool,
    **extra: Any,
) -> Dict[str, Any]:
    payload = {
        "action_time": datetime.now().isoformat(),
        "action_type": action_type,
        "status": status,
        "files_processed": files_processed,
        "errors": errors,
        "notes": notes,
    }
    try:
        init_db(db_path)
        insert_action(db_path, payload)
    except Exception:
        log.exception("Failed to write dashboard action row")
    response = {
        "success": success,
        "action_type": action_type,
        "files_processed": files_processed,
        "errors": errors,
        "notes": notes,
    }
    response.update(extra)
    return response


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="dashboard")
