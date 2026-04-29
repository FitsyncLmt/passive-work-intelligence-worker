import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from xroiq_store import init_db, upsert_device


DEVICE_CONFIGS = [
    {
        "config_key": "laptop_root",
        "device_id": "LAP-002",
        "name": "Karne Alienware Workstation",
        "device_type": "workstation",
        "role": "execution_core",
    },
    {
        "config_key": "microsd_root",
        "device_id": "STR-003",
        "name": "SanDisk Extreme PRO microSDXC",
        "device_type": "storage",
        "role": "active_working_memory",
    },
    {
        "config_key": "wd_root",
        "device_id": "STR-001",
        "name": "WD My Book 6TB",
        "device_type": "storage",
        "role": "cold_archive",
    },
    {
        "config_key": "backup_root",
        "device_id": "LOG-001",
        "name": "XROIQ Logs Folder",
        "device_type": "folder",
        "role": "operational_logs",
    },
    {
        "config_key": "database_path",
        "device_id": "DB-001",
        "name": "SQLite Memory Store",
        "device_type": "database",
        "role": "machine_memory",
        "use_parent": True,
    },
]


def refresh_configured_device_health(config: Dict[str, Any], db_path) -> List[Dict[str, Any]]:
    init_db(db_path)
    items = collect_configured_device_health(config)
    for item in items:
        upsert_device(db_path, item)
    return items


def collect_configured_device_health(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    checked_at = datetime.now().isoformat()
    return [_build_device_record(device_config, config, checked_at) for device_config in DEVICE_CONFIGS]


def _build_device_record(
    device_config: Dict[str, Any],
    config: Dict[str, Any],
    checked_at: str,
) -> Dict[str, Any]:
    raw_value = config.get(device_config["config_key"])
    path = _device_path(raw_value, use_parent=bool(device_config.get("use_parent")))
    exists = bool(path and path.exists())
    status = "available" if exists else "missing"
    free_space_gb = _free_space_gb(path) if exists and path is not None else None
    drive_path = str(path) if path is not None else None

    return {
        "device_id": device_config["device_id"],
        "name": device_config["name"],
        "device_type": device_config["device_type"],
        "role": device_config["role"],
        "drive_letter": drive_path,
        "drive_path": drive_path,
        "status": status,
        "exists": exists,
        "mounted": status,
        "free_space_gb": free_space_gb,
        "last_seen": checked_at,
        "notes": "",
        "updated_at": checked_at,
    }


def _device_path(raw_value: Any, *, use_parent: bool) -> Optional[Path]:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    path = Path(raw_value).expanduser()
    return path.parent if use_parent else path


def _free_space_gb(path: Path) -> Optional[float]:
    try:
        usage = shutil.disk_usage(path)
    except Exception:
        return None
    return round(usage.free / (1024 ** 3), 2)
