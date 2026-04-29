import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional


EVENT_FIELDS = [
    "event_id",
    "event_time",
    "event_type",
    "file_name",
    "full_path",
    "extension",
    "parent_folder",
    "category",
    "importance",
    "device",
    "source",
    "session_key",
    "handled_action",
    "backup_status",
    "notes",
    "project",
]

SESSION_FIELDS = [
    "session_key",
    "start_time",
    "end_time",
    "duration_minutes",
    "category",
    "project",
    "primary_file",
    "event_count",
    "device",
    "source",
    "notes",
]

DEVICE_FIELDS = [
    "device_id",
    "name",
    "device_type",
    "role",
    "drive_letter",
    "status",
    "free_space_gb",
    "last_seen",
    "notes",
    "updated_at",
]

ACTION_FIELDS = [
    "action_time",
    "action_type",
    "status",
    "files_processed",
    "errors",
    "notes",
]


def init_db(db_path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                event_time TEXT,
                event_type TEXT,
                file_name TEXT,
                full_path TEXT,
                extension TEXT,
                parent_folder TEXT,
                category TEXT,
                importance TEXT,
                device TEXT,
                source TEXT,
                session_key TEXT,
                handled_action TEXT,
                backup_status TEXT,
                notes TEXT,
                project TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_key TEXT,
                start_time TEXT,
                end_time TEXT,
                duration_minutes REAL,
                category TEXT,
                project TEXT,
                primary_file TEXT,
                event_count INTEGER,
                device TEXT,
                source TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT UNIQUE,
                name TEXT,
                device_type TEXT,
                role TEXT,
                drive_letter TEXT,
                status TEXT,
                free_space_gb REAL,
                last_seen TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_type TEXT,
                pattern TEXT,
                category TEXT,
                action TEXT,
                enabled INTEGER DEFAULT 1,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_time TEXT,
                action_type TEXT,
                status TEXT,
                files_processed INTEGER,
                errors TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def insert_event(db_path, record: Dict[str, Any]) -> int:
    return _insert_row(db_path, "events", EVENT_FIELDS, record)


def insert_session(db_path, session: Dict[str, Any]) -> int:
    return _insert_row(db_path, "sessions", SESSION_FIELDS, session)


def list_events(db_path, limit: int = 100) -> List[Dict[str, Any]]:
    return _list_rows(db_path, "events", limit)


def list_sessions(db_path, limit: int = 100) -> List[Dict[str, Any]]:
    return _list_rows(db_path, "sessions", limit)


def upsert_device(db_path, device: Dict[str, Any]) -> int:
    values = _values_for_fields(device, DEVICE_FIELDS)
    placeholders = ", ".join("?" for _ in DEVICE_FIELDS)
    assignments = ", ".join(
        f"{field}=excluded.{field}" for field in DEVICE_FIELDS if field != "device_id"
    )
    with sqlite3.connect(Path(db_path)) as connection:
        cursor = connection.execute(
            f"""
            INSERT INTO devices ({", ".join(DEVICE_FIELDS)})
            VALUES ({placeholders})
            ON CONFLICT(device_id) DO UPDATE SET {assignments}
            """,
            values,
        )
        return int(cursor.lastrowid or 0)


def list_devices(db_path) -> List[Dict[str, Any]]:
    return _list_rows(db_path, "devices", 1000)


def get_event_counts_by_category(db_path) -> Dict[str, int]:
    init_db(db_path)
    with sqlite3.connect(Path(db_path)) as connection:
        cursor = connection.execute(
            """
            SELECT COALESCE(NULLIF(category, ''), 'Uncategorized') AS category,
                   COUNT(*) AS total
            FROM events
            GROUP BY COALESCE(NULLIF(category, ''), 'Uncategorized')
            ORDER BY category
            """
        )
        return {str(row[0]): int(row[1]) for row in cursor.fetchall()}


def get_recent_activity_summary(db_path, hours: int = 24) -> Dict[str, Any]:
    init_db(db_path)
    cutoff = (datetime.now() - timedelta(hours=int(hours))).isoformat()
    with sqlite3.connect(Path(db_path)) as connection:
        cursor = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM events
            WHERE event_time IS NOT NULL
              AND event_time >= ?
            """,
            (cutoff,),
        )
        count = int(cursor.fetchone()[0])
    return {"hours": int(hours), "event_count": count}


def get_session_totals_by_category(db_path) -> Dict[str, float]:
    init_db(db_path)
    with sqlite3.connect(Path(db_path)) as connection:
        cursor = connection.execute(
            """
            SELECT COALESCE(NULLIF(category, ''), 'Uncategorized') AS category,
                   COALESCE(SUM(duration_minutes), 0) AS total_minutes
            FROM sessions
            GROUP BY COALESCE(NULLIF(category, ''), 'Uncategorized')
            ORDER BY category
            """
        )
        return {str(row[0]): float(row[1] or 0) for row in cursor.fetchall()}


def get_latest_event_time(db_path) -> Optional[str]:
    init_db(db_path)
    with sqlite3.connect(Path(db_path)) as connection:
        cursor = connection.execute(
            """
            SELECT event_time
            FROM events
            WHERE event_time IS NOT NULL
              AND event_time != ''
            ORDER BY event_time DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        return str(row[0]) if row and row[0] is not None else None


def insert_action(db_path, action: Dict[str, Any]) -> int:
    return _insert_row(db_path, "actions", ACTION_FIELDS, action)


def list_actions(db_path, limit: int = 100) -> List[Dict[str, Any]]:
    return _list_rows(db_path, "actions", limit)


def _insert_row(db_path, table: str, fields: List[str], record: Dict[str, Any]) -> int:
    values = _values_for_fields(record, fields)
    placeholders = ", ".join("?" for _ in fields)
    with sqlite3.connect(Path(db_path)) as connection:
        cursor = connection.execute(
            f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})",
            values,
        )
        return int(cursor.lastrowid)


def _list_rows(db_path, table: str, limit: int) -> List[Dict[str, Any]]:
    with sqlite3.connect(Path(db_path)) as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.execute(
            f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?",
            (int(limit),),
        )
        return [dict(row) for row in cursor.fetchall()]


def _values_for_fields(record: Dict[str, Any], fields: List[str]) -> List[Optional[Any]]:
    return [record.get(field) for field in fields]
