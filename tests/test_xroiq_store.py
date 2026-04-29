import sqlite3

from xroiq_store import (
    init_db,
    insert_action,
    insert_event,
    insert_session,
    list_actions,
    list_devices,
    list_events,
    list_sessions,
    upsert_device,
)


def test_db_creation(tmp_path):
    db_path = tmp_path / "nested" / "xroiq.db"

    init_db(db_path)

    assert db_path.exists()


def test_required_tables_exist(tmp_path):
    db_path = tmp_path / "xroiq.db"

    init_db(db_path)

    with sqlite3.connect(db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert {"events", "sessions", "devices", "rules", "actions"}.issubset(tables)


def test_event_insertion(tmp_path):
    db_path = tmp_path / "xroiq.db"
    init_db(db_path)

    insert_event(db_path, {"event_id": "EVT-000001", "event_type": "created"})

    events = list_events(db_path)
    assert events[0]["event_id"] == "EVT-000001"
    assert events[0]["event_type"] == "created"


def test_session_insertion(tmp_path):
    db_path = tmp_path / "xroiq.db"
    init_db(db_path)

    insert_session(db_path, {"session_key": "SESSION-1", "category": "Build"})

    sessions = list_sessions(db_path)
    assert sessions[0]["session_key"] == "SESSION-1"
    assert sessions[0]["category"] == "Build"


def test_list_events_latest_first(tmp_path):
    db_path = tmp_path / "xroiq.db"
    init_db(db_path)

    insert_event(db_path, {"event_id": "EVT-000001"})
    insert_event(db_path, {"event_id": "EVT-000002"})

    assert [event["event_id"] for event in list_events(db_path)] == [
        "EVT-000002",
        "EVT-000001",
    ]


def test_list_sessions_latest_first(tmp_path):
    db_path = tmp_path / "xroiq.db"
    init_db(db_path)

    insert_session(db_path, {"session_key": "SESSION-1"})
    insert_session(db_path, {"session_key": "SESSION-2"})

    assert [session["session_key"] for session in list_sessions(db_path)] == [
        "SESSION-2",
        "SESSION-1",
    ]


def test_upsert_device_insert_update(tmp_path):
    db_path = tmp_path / "xroiq.db"
    init_db(db_path)

    upsert_device(db_path, {"device_id": "LAP-002", "name": "Laptop"})
    upsert_device(db_path, {"device_id": "LAP-002", "name": "Updated Laptop"})

    devices = list_devices(db_path)
    assert len(devices) == 1
    assert devices[0]["device_id"] == "LAP-002"
    assert devices[0]["name"] == "Updated Laptop"


def test_insert_action(tmp_path):
    db_path = tmp_path / "xroiq.db"
    init_db(db_path)

    insert_action(
        db_path,
        {
            "action_time": "2026-04-29T00:00:00",
            "action_type": "smoke_test",
            "status": "ok",
            "files_processed": 2,
        },
    )

    actions = list_actions(db_path)
    assert actions[0]["action_type"] == "smoke_test"
    assert actions[0]["files_processed"] == 2
