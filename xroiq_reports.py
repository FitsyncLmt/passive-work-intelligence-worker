import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from xroiq_device_intelligence import refresh_configured_device_health
from xroiq_store import (
    get_event_counts_by_category,
    get_latest_event_time,
    get_recent_activity_summary,
    get_session_totals_by_category,
    init_db,
    list_events,
)


def backup_sqlite_database(db_path, backup_root) -> Path:
    init_db(db_path)
    source = Path(db_path)
    snapshots_dir = Path(backup_root).expanduser() / "db_snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destination = snapshots_dir / f"xroiq_work_intelligence_{timestamp}.db"
    shutil.copy2(source, destination)
    return destination


def generate_daily_report(
    *,
    db_path,
    backup_root,
    config: Dict[str, Any],
    sqlite_backup_status: Optional[str] = None,
) -> Path:
    init_db(db_path)
    reports_dir = Path(backup_root).expanduser() / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_date = datetime.now().strftime("%Y%m%d")
    report_path = reports_dir / f"xroiq_daily_report_{report_date}.md"
    device_items = refresh_configured_device_health(config, db_path)
    content = build_daily_report_markdown(
        db_path=db_path,
        device_items=device_items,
        sqlite_backup_status=sqlite_backup_status,
    )
    report_path.write_text(content, encoding="utf-8")
    return report_path


def build_daily_report_markdown(
    *,
    db_path,
    device_items: List[Dict[str, Any]],
    sqlite_backup_status: Optional[str] = None,
) -> str:
    latest_event_time = get_latest_event_time(db_path)
    recent_summary = get_recent_activity_summary(db_path, hours=24)
    events_by_category = get_event_counts_by_category(db_path)
    session_minutes = get_session_totals_by_category(db_path)
    recent_events = list_events(db_path, limit=10)
    missing_devices = [item for item in device_items if item.get("status") == "missing"]

    lines = [
        "# XROIQ Daily Work Intelligence Report",
        "",
        f"Date: {datetime.now().date().isoformat()}",
        f"Database: {Path(db_path)}",
        f"Latest Event: {latest_event_time or '-'}",
        f"Last 24h Event Count: {recent_summary['event_count']}",
        "",
        "## Events by Category",
    ]
    lines.extend(_mapping_lines(events_by_category, "No events recorded."))
    lines.extend(["", "## Session Minutes by Category"])
    lines.extend(_mapping_lines(session_minutes, "No sessions recorded."))
    lines.extend(["", "## Recent Events"])
    lines.extend(_recent_event_lines(recent_events))
    lines.extend(["", "## Device Health"])
    lines.extend(_device_lines(device_items))
    lines.extend(["", "## Warnings"])
    lines.extend(
        _warning_lines(
            missing_devices=missing_devices,
            last_24h_event_count=recent_summary["event_count"],
            sqlite_backup_status=sqlite_backup_status,
        )
    )
    return "\n".join(lines) + "\n"


def _mapping_lines(values: Dict[str, Any], empty_message: str) -> List[str]:
    if not values:
        return [f"- {empty_message}"]
    return [f"- {key}: {value}" for key, value in sorted(values.items())]


def _recent_event_lines(events: List[Dict[str, Any]]) -> List[str]:
    if not events:
        return ["- No recent events recorded."]
    return [
        "- {time} | {category} | {event_type} | {file_name}".format(
            time=event.get("event_time") or "-",
            category=event.get("category") or "-",
            event_type=event.get("event_type") or "-",
            file_name=event.get("file_name") or "-",
        )
        for event in events
    ]


def _device_lines(device_items: List[Dict[str, Any]]) -> List[str]:
    if not device_items:
        return ["- No devices recorded."]
    return [
        "- {name} | {role} | {status} | {free} GB free | {path}".format(
            name=item.get("name") or item.get("device_id") or "-",
            role=item.get("role") or "-",
            status=item.get("status") or "-",
            free=item.get("free_space_gb") if item.get("free_space_gb") is not None else "-",
            path=item.get("drive_path") or item.get("drive_letter") or "-",
        )
        for item in device_items
    ]


def _warning_lines(
    *,
    missing_devices: List[Dict[str, Any]],
    last_24h_event_count: int,
    sqlite_backup_status: Optional[str],
) -> List[str]:
    warnings = []
    if missing_devices:
        names = ", ".join(item.get("name") or item.get("device_id") or "-" for item in missing_devices)
        warnings.append(f"- Missing devices: {names}")
    if last_24h_event_count == 0:
        warnings.append("- No events in last 24h.")
    if sqlite_backup_status:
        warnings.append(f"- SQLite backup status: {sqlite_backup_status}")
    if not warnings:
        warnings.append("- No warnings.")
    return warnings
