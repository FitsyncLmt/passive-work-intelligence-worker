import json
import sys
from pathlib import Path

import pytest
from openpyxl import Workbook

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from xroiq_work_intelligence_service import (  # noqa: E402
    REQUIRED_ACTIVITY_HEADERS,
    REQUIRED_SESSION_HEADERS,
    WorkIntelligenceService,
)


def create_workbook(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    activity = workbook.active
    activity.title = "Activity_Log"
    activity.append(REQUIRED_ACTIVITY_HEADERS)
    sessions = workbook.create_sheet("Sessions")
    sessions.append(REQUIRED_SESSION_HEADERS)
    workbook.save(path)
    workbook.close()
    return path


def make_config(tmp_path: Path, **overrides) -> dict:
    work_root = tmp_path / "work"
    backup_root = tmp_path / "logs"
    workbook_path = tmp_path / "ops" / "test_workbook.xlsx"
    database_path = tmp_path / "data" / "xroiq_work_intelligence.db"
    work_root.mkdir(parents=True, exist_ok=True)
    backup_root.mkdir(parents=True, exist_ok=True)
    create_workbook(workbook_path)

    config = {
        "watch_paths": [str(work_root)],
        "ignore_dirs": [".git", "node_modules", "__pycache__", ".venv", "venv"],
        "ignore_extensions": [".tmp", ".log", ".cache", ".lock"],
        "session_gap_minutes": 10,
        "event_debounce_seconds": 8,
        "heartbeat_interval_seconds": 300,
        "shutdown_drain_timeout_seconds": 1,
        "queue_warning_size": 500,
        "owner": "Tester",
        "device": "TEST-DEVICE",
        "laptop_root": str(work_root),
        "microsd_root": str(tmp_path / "missing_microsd"),
        "wd_root": str(tmp_path / "missing_wd"),
        "copy_recent_to_microsd": False,
        "copy_stable_to_wd": False,
        "stable_days_before_wd": 7,
        "workbook_path": str(workbook_path),
        "backup_root": str(backup_root),
        "database_path": str(database_path),
        "category_rules": {
            "Build": {"extensions": [".py", ".js", ".json"]},
            "R&D": {"extensions": [".md", ".txt", ".pdf"]},
            "Admin": {"extensions": [".docx", ".xlsx"]},
            "Communications": {"path_keywords": ["mail", "teams", "slack"]},
        },
        "importance_keywords": {
            "High": ["final", "production", "contract"],
            "Medium": ["draft", "review", "notes"],
            "Low": [],
        },
    }
    config.update(overrides)
    return config


def write_config(tmp_path: Path, config: dict) -> Path:
    path = tmp_path / "xroiq_work_intelligence_config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


@pytest.fixture
def service_factory(tmp_path):
    def factory(**overrides):
        config = make_config(tmp_path, **overrides)
        config_path = write_config(tmp_path, config)
        return WorkIntelligenceService(config_path, config_path_was_explicit=True)

    return factory
