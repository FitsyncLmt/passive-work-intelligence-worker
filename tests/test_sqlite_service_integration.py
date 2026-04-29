from datetime import datetime

from xroiq_store import list_events, list_sessions
from xroiq_work_intelligence_service import (
    DEFAULT_DATABASE_PATH,
    EventRecord,
    WorkIntelligenceService,
)

from .conftest import make_config


def test_config_accepts_database_path(tmp_path):
    database_path = tmp_path / "data" / "custom.db"
    config = make_config(tmp_path, database_path=str(database_path))

    paths = WorkIntelligenceService.validate_config(config)

    assert paths["database_path"] == database_path
    assert not database_path.parent.exists()


def test_default_database_path_fallback(tmp_path):
    config = make_config(tmp_path)
    del config["database_path"]

    merged = WorkIntelligenceService.apply_optional_defaults(config)

    assert merged["database_path"] == DEFAULT_DATABASE_PATH


def test_processed_event_writes_sqlite_event_and_session(service_factory, tmp_path):
    service = service_factory()
    service.validate_runtime()
    path = service.validated_paths["laptop_root"] / "sqlite-test.py"
    path.write_text("hello", encoding="utf-8")

    service.process_record(EventRecord("created", path, datetime.now()))

    events = list_events(service.database_path)
    sessions = list_sessions(service.database_path)
    assert events[0]["event_id"] == "EVT-000001"
    assert events[0]["event_type"] == "created"
    assert events[0]["file_name"] == "sqlite-test.py"
    assert sessions[0]["session_key"] == events[0]["session_key"]
    assert sessions[0]["category"] == "Build"


def test_sqlite_insert_failure_does_not_crash_service(service_factory, tmp_path, monkeypatch):
    service = service_factory()
    path = service.validated_paths["laptop_root"] / "sqlite-failure.py"
    path.write_text("hello", encoding="utf-8")

    def fail_insert(*_args, **_kwargs):
        raise RuntimeError("sqlite unavailable")

    monkeypatch.setattr("xroiq_work_intelligence_service.insert_event", fail_insert)

    service.process_record(EventRecord("created", path, datetime.now()))

    assert service.event_counter == 2
