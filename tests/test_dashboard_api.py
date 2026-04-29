import json
from datetime import datetime, timedelta

from fastapi.testclient import TestClient

import app.main as dashboard_main
from app.main import app
from xroiq_store import init_db, insert_event, insert_session, list_actions, list_devices


def write_config(tmp_path):
    db_path = tmp_path / "data" / "dashboard.db"
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"database_path": str(db_path)}), encoding="utf-8")
    return config_path, db_path


def write_device_config(tmp_path):
    db_path = tmp_path / "data" / "dashboard.db"
    laptop_root = tmp_path / "work"
    backup_root = tmp_path / "logs"
    laptop_root.mkdir(parents=True)
    backup_root.mkdir(parents=True)
    config = {
        "database_path": str(db_path),
        "laptop_root": str(laptop_root),
        "microsd_root": str(tmp_path / "missing_microsd"),
        "wd_root": str(tmp_path / "missing_wd"),
        "backup_root": str(backup_root),
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    return config_path, db_path


def test_api_health(monkeypatch, tmp_path):
    config_path, db_path = write_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))

    response = TestClient(app).get("/api/health")

    assert response.status_code == 200
    body = response.json()
    assert body["db_connected"] is True
    assert body["database_path"] == str(db_path)


def test_api_events_returns_list(monkeypatch, tmp_path):
    config_path, db_path = write_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))
    init_db(db_path)
    insert_event(db_path, {"event_id": "EVT-000001", "event_type": "created"})

    response = TestClient(app).get("/api/events")

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["items"], list)
    assert body["items"][0]["event_id"] == "EVT-000001"


def test_api_sessions_returns_list(monkeypatch, tmp_path):
    config_path, db_path = write_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))
    init_db(db_path)
    insert_session(db_path, {"session_key": "SESSION-1", "category": "Build"})

    response = TestClient(app).get("/api/sessions")

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["items"], list)
    assert body["items"][0]["session_key"] == "SESSION-1"


def test_api_device_health_returns_configured_devices(monkeypatch, tmp_path):
    config_path, db_path = write_device_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))

    response = TestClient(app).get("/api/device-health")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 5
    device_ids = {item["device_id"] for item in body["items"]}
    assert device_ids == {"LAP-002", "STR-003", "STR-001", "LOG-001", "DB-001"}
    assert len(list_devices(db_path)) == 5


def test_api_device_health_marks_missing_optional_device(monkeypatch, tmp_path):
    config_path, _db_path = write_device_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))

    response = TestClient(app).get("/api/device-health")

    assert response.status_code == 200
    body = response.json()
    microsd = next(item for item in body["items"] if item["device_id"] == "STR-003")
    assert microsd["status"] == "missing"
    assert microsd["exists"] is False
    assert microsd["free_space_gb"] is None


def test_api_summary_returns_expected_category_counts(monkeypatch, tmp_path):
    config_path, db_path = write_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))
    init_db(db_path)
    now = datetime.now()
    old = now - timedelta(hours=30)
    insert_event(
        db_path,
        {
            "event_id": "EVT-000001",
            "event_time": now.isoformat(),
            "category": "Build",
        },
    )
    insert_event(
        db_path,
        {
            "event_id": "EVT-000002",
            "event_time": now.isoformat(),
            "category": "Build",
        },
    )
    insert_event(
        db_path,
        {
            "event_id": "EVT-000003",
            "event_time": old.isoformat(),
            "category": "R&D",
        },
    )
    insert_session(db_path, {"session_key": "SESSION-1", "category": "Build", "duration_minutes": 12})
    insert_session(db_path, {"session_key": "SESSION-2", "category": "R&D", "duration_minutes": 8})

    response = TestClient(app).get("/api/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["events_by_category"] == {"Build": 2, "R&D": 1}
    assert body["session_minutes_by_category"] == {"Build": 12.0, "R&D": 8.0}
    assert body["latest_event_time"] == now.isoformat()
    assert body["last_24h_event_count"] == 2


def test_existing_endpoints_still_pass(monkeypatch, tmp_path):
    config_path, db_path = write_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))
    init_db(db_path)

    client = TestClient(app)

    assert client.get("/api/health").status_code == 200
    assert client.get("/api/events").status_code == 200
    assert client.get("/api/sessions").status_code == 200
    assert client.get("/api/storage-decisions").status_code == 200


def test_action_refresh_device_health_writes_action(monkeypatch, tmp_path):
    config_path, db_path = write_device_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))

    response = TestClient(app).post("/api/actions/refresh-device-health")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["action_type"] == "refresh_device_health"
    assert body["files_processed"] == 5
    actions = list_actions(db_path)
    assert actions[0]["action_type"] == "refresh_device_health"
    assert actions[0]["status"] == "ok"


def test_action_backup_sqlite_creates_snapshot_and_action(monkeypatch, tmp_path):
    config_path, db_path = write_device_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))
    init_db(db_path)
    insert_event(db_path, {"event_id": "EVT-BACKUP", "event_time": datetime.now().isoformat()})

    response = TestClient(app).post("/api/actions/backup-sqlite")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    destination = tmp_path / "logs" / "db_snapshots"
    snapshot_path = body["destination_path"]
    assert snapshot_path.startswith(str(destination))
    assert snapshot_path.endswith(".db")
    assert dashboard_main.Path(snapshot_path).exists()
    assert db_path.exists()
    actions = list_actions(db_path)
    assert actions[0]["action_type"] == "backup_sqlite"
    assert actions[0]["files_processed"] == 1


def test_action_generate_daily_report_creates_markdown_and_action(monkeypatch, tmp_path):
    config_path, db_path = write_device_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))
    init_db(db_path)
    insert_event(
        db_path,
        {
            "event_id": "EVT-REPORT",
            "event_time": datetime.now().isoformat(),
            "event_type": "created",
            "file_name": "report.py",
            "category": "Build",
        },
    )
    insert_session(db_path, {"session_key": "SESSION-REPORT", "category": "Build", "duration_minutes": 15})

    response = TestClient(app).post("/api/actions/generate-daily-report")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    report_path = dashboard_main.Path(body["report_path"])
    assert report_path.exists()
    assert report_path.name.startswith("xroiq_daily_report_")
    content = report_path.read_text(encoding="utf-8")
    assert "# XROIQ Daily Work Intelligence Report" in content
    assert "## Events by Category" in content
    assert "## Device Health" in content
    actions = list_actions(db_path)
    assert actions[0]["action_type"] == "generate_daily_report"
    assert actions[0]["files_processed"] == 1


def test_action_open_logs_returns_gracefully_and_writes_action(monkeypatch, tmp_path):
    config_path, db_path = write_device_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))
    monkeypatch.setattr(dashboard_main.os, "startfile", lambda _path: None, raising=False)

    response = TestClient(app).post("/api/actions/open-logs")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["action_type"] == "open_logs"
    assert body["errors"] == ""
    actions = list_actions(db_path)
    assert actions[0]["action_type"] == "open_logs"
    assert actions[0]["status"] == "ok"


def test_api_storage_decisions_returns_missing_device_warnings(monkeypatch, tmp_path):
    config_path, _db_path = write_device_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))

    response = TestClient(app).get("/api/storage-decisions")

    assert response.status_code == 200
    body = response.json()
    warning_ids = {warning["device_id"] for warning in body["warnings"]}
    assert {"STR-003", "STR-001"}.issubset(warning_ids)
    assert body["count"] == 0


def test_api_storage_decisions_recent_created_recommends_sync_to_microsd(monkeypatch, tmp_path):
    config_path, db_path = write_device_config(tmp_path)
    microsd_root = tmp_path / "microsd"
    microsd_root.mkdir()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["microsd_root"] = str(microsd_root)
    config_path.write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))
    init_db(db_path)
    insert_event(
        db_path,
        {
            "event_id": "EVT-KEEP",
            "event_time": datetime.now().isoformat(),
            "event_type": "created",
            "file_name": "active.py",
            "full_path": str(tmp_path / "work" / "active.py"),
            "category": "Build",
        },
    )

    response = TestClient(app).get("/api/storage-decisions")

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["recommendation"] == "sync_to_microsd"
    assert body["items"][0]["target"] == "microsd"
    assert body["items"][0]["reason"] == "Recent active file should stay on laptop and be mirrored to available microSD."


def test_api_storage_decisions_deleted_event_recommends_ignore(monkeypatch, tmp_path):
    config_path, db_path = write_device_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))
    init_db(db_path)
    insert_event(
        db_path,
        {
            "event_id": "EVT-IGNORE",
            "event_time": datetime.now().isoformat(),
            "event_type": "deleted",
            "file_name": "removed.txt",
            "full_path": str(tmp_path / "work" / "removed.txt"),
            "category": "R&D",
        },
    )

    response = TestClient(app).get("/api/storage-decisions")

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["recommendation"] == "ignore"
    assert body["items"][0]["target"] == "none"


def test_founder_intelligence_report_creates_markdown(monkeypatch, tmp_path):
    config_path, db_path = write_device_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))
    init_db(db_path)
    insert_event(
        db_path,
        {
            "event_id": "EVT-FOUNDER",
            "event_time": datetime.now().isoformat(),
            "event_type": "modified",
            "file_name": "evidence.md",
            "category": "R&D",
        },
    )

    response = TestClient(app).post("/api/actions/generate-founder-intelligence-report")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    report_path = dashboard_main.Path(body["path"])
    assert report_path.exists()
    content = report_path.read_text(encoding="utf-8")
    assert "# XROIQ Founder Intelligence Report" in content
    assert "## Storage Intelligence" in content
    assert "## Next Best Actions" in content


def test_action_generate_founder_intelligence_report_writes_action(monkeypatch, tmp_path):
    config_path, db_path = write_device_config(tmp_path)
    monkeypatch.setenv("XROIQ_CONFIG_PATH", str(config_path))

    response = TestClient(app).post("/api/actions/generate-founder-intelligence-report")

    assert response.status_code == 200
    body = response.json()
    assert body["action_type"] == "generate_founder_intelligence_report"
    actions = list_actions(db_path)
    assert actions[0]["action_type"] == "generate_founder_intelligence_report"
    assert actions[0]["status"] == "ok"
