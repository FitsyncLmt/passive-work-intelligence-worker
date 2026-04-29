import json

from fastapi.testclient import TestClient

from app.main import app
from xroiq_store import init_db, insert_event, insert_session


def write_config(tmp_path):
    db_path = tmp_path / "data" / "dashboard.db"
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"database_path": str(db_path)}), encoding="utf-8")
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
