from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

from xroiq_device_intelligence import collect_configured_device_health
from xroiq_store import init_db, list_events


def get_storage_decisions(config: Dict[str, Any], db_path, limit: int = 100) -> Dict[str, Any]:
    init_db(db_path)
    devices = collect_configured_device_health(config)
    device_by_id = {device.get("device_id"): device for device in devices}
    warnings = _device_warnings(config, device_by_id)
    items = [
        _decision_for_event(event, config, device_by_id)
        for event in list_events(db_path, limit=limit)
    ]
    return {"items": items, "count": len(items), "warnings": warnings}


def top_recommendation(decisions: Dict[str, Any]) -> str:
    priority = [
        "warning_missing_device",
        "warning_no_recent_backup",
        "archive_to_wd",
        "sync_to_microsd",
        "keep_on_laptop",
        "ignore",
    ]
    values = [warning.get("recommendation") for warning in decisions.get("warnings", [])]
    values.extend(item.get("recommendation") for item in decisions.get("items", []))
    for recommendation in priority:
        if recommendation in values:
            return recommendation
    return "none"


def _device_warnings(config: Dict[str, Any], device_by_id: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    warnings = []
    if config.get("copy_recent_to_microsd", True) and _device_missing(device_by_id, "STR-003"):
        warnings.append(
            {
                "device_id": "STR-003",
                "recommendation": "warning_missing_device",
                "target": "microsd",
                "reason": "microSD is configured for recent file sync but is missing.",
            }
        )
    if config.get("copy_stable_to_wd", True) and _device_missing(device_by_id, "STR-001"):
        warnings.append(
            {
                "device_id": "STR-001",
                "recommendation": "warning_missing_device",
                "target": "wd",
                "reason": "WD archive is configured for stable file archive but is missing.",
            }
        )
    if not _has_recent_backup(config):
        warnings.append(
            {
                "device_id": "DB-001",
                "recommendation": "warning_no_recent_backup",
                "target": "none",
                "reason": "No SQLite snapshot was found in the last 24 hours.",
            }
        )
    return warnings


def _decision_for_event(
    event: Dict[str, Any],
    config: Dict[str, Any],
    device_by_id: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    event_type = str(event.get("event_type") or "").lower()
    event_time = str(event.get("event_time") or "")
    recommendation, reason, target = _recommendation(event, event_type, event_time, config, device_by_id)
    return {
        "file_name": event.get("file_name") or "",
        "full_path": event.get("full_path") or "",
        "category": event.get("category") or "",
        "event_type": event.get("event_type") or "",
        "event_time": event_time,
        "recommendation": recommendation,
        "reason": reason,
        "target": target,
    }


def _recommendation(
    event: Dict[str, Any],
    event_type: str,
    event_time: str,
    config: Dict[str, Any],
    device_by_id: Dict[str, Dict[str, Any]],
) -> Tuple[str, str, str]:
    if event_type == "deleted":
        return "ignore", "Deleted events are retained as history only.", "none"

    if event_type == "moved":
        full_path = event.get("full_path")
        if full_path and not Path(full_path).exists():
            return "ignore", "Moved destination is not present; no storage action recommended.", "none"
        return "keep_on_laptop", "Moved event should remain on laptop until manually reviewed.", "laptop"

    when = _parse_event_time(event_time)
    age_days = (datetime.now() - when).days if when else 0
    is_recent = bool(when and when >= datetime.now() - timedelta(hours=48))
    microsd_available = _device_available(device_by_id, "STR-003")
    wd_available = _device_available(device_by_id, "STR-001")

    if is_recent:
        if (
            event_type in {"created", "modified"}
            and config.get("copy_recent_to_microsd", True)
            and microsd_available
        ):
            return (
                "sync_to_microsd",
                "Recent active file should stay on laptop and be mirrored to available microSD.",
                "microsd",
            )
        return "keep_on_laptop", "Recent active file should remain on laptop.", "laptop"

    stable_days = int(config.get("stable_days_before_wd", 7))
    if age_days >= stable_days and config.get("copy_stable_to_wd", True) and wd_available:
        return "archive_to_wd", f"File event is at least {stable_days} days old and WD archive is available.", "wd"

    return "keep_on_laptop", "No deterministic external storage action is currently recommended.", "laptop"


def _parse_event_time(value: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _device_available(device_by_id: Dict[str, Dict[str, Any]], device_id: str) -> bool:
    return not _device_missing(device_by_id, device_id)


def _device_missing(device_by_id: Dict[str, Dict[str, Any]], device_id: str) -> bool:
    return device_by_id.get(device_id, {}).get("status") != "available"


def _has_recent_backup(config: Dict[str, Any]) -> bool:
    backup_root = config.get("backup_root")
    if not backup_root:
        return False
    snapshots_dir = Path(backup_root).expanduser() / "db_snapshots"
    if not snapshots_dir.exists():
        return False
    cutoff = datetime.now() - timedelta(hours=24)
    for snapshot in snapshots_dir.glob("*.db"):
        try:
            if datetime.fromtimestamp(snapshot.stat().st_mtime) >= cutoff:
                return True
        except OSError:
            continue
    return False
