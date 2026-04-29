from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from xroiq_device_intelligence import collect_configured_device_health
from xroiq_storage_decisions import get_storage_decisions, top_recommendation
from xroiq_store import (
    get_event_counts_by_category,
    get_latest_event_time,
    get_recent_activity_summary,
    get_session_totals_by_category,
    init_db,
    list_events,
)


def generate_founder_intelligence_report(config: Dict[str, Any], db_path, output_root=None) -> Path:
    init_db(db_path)
    root = Path(output_root or config.get("backup_root")).expanduser()
    reports_dir = root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / f"xroiq_founder_intelligence_{datetime.now().strftime('%Y%m%d')}.md"
    content = build_founder_intelligence_markdown(config, db_path)
    report_path.write_text(content, encoding="utf-8")
    return report_path


def build_founder_intelligence_markdown(config: Dict[str, Any], db_path) -> str:
    events = list_events(db_path, limit=100)
    recent_events = events[:10]
    events_by_category = get_event_counts_by_category(db_path)
    session_minutes = get_session_totals_by_category(db_path)
    latest_event_time = get_latest_event_time(db_path)
    recent_summary = get_recent_activity_summary(db_path, hours=24)
    devices = collect_configured_device_health(config)
    decisions = get_storage_decisions(config, db_path)
    missing_devices = [device for device in devices if device.get("status") == "missing"]
    top = top_recommendation(decisions)

    lines = [
        "# XROIQ Founder Intelligence Report",
        "",
        f"Date: {datetime.now().date().isoformat()}",
        f"Database: {Path(db_path)}",
        "Generated locally: yes",
        "",
        "## Executive Summary",
        f"- Total events: {len(events)}",
        f"- Last 24h events: {recent_summary['event_count']}",
        f"- Latest event: {latest_event_time or '-'}",
        f"- Active categories: {', '.join(sorted(events_by_category)) if events_by_category else '-'}",
        f"- Missing devices: {_device_names(missing_devices)}",
        f"- Top recommendation: {top}",
        "",
        "## Work Pattern",
        "- Events by category:",
        *_mapping_lines(events_by_category, "No events recorded."),
        "- Session minutes by category:",
        *_mapping_lines(session_minutes, "No sessions recorded."),
        "- Recent files touched:",
        *_event_file_lines(recent_events),
        "",
        "## Storage Intelligence",
        "- Device status:",
        *_device_lines(devices),
        "- Storage recommendations:",
        *_decision_lines(decisions.get("items", [])),
        "- Warnings:",
        *_warning_lines(decisions.get("warnings", [])),
        "",
        "## R&D Evidence",
        *_category_evidence(events, session_minutes, "R&D"),
        "",
        "## Admin Evidence",
        *_category_evidence(events, session_minutes, "Admin"),
        "",
        "## Build Evidence",
        *_category_evidence(events, session_minutes, "Build"),
        "",
        "## Next Best Actions",
        *_next_best_actions(decisions, missing_devices),
    ]
    return "\n".join(lines) + "\n"


def _mapping_lines(values: Dict[str, Any], empty_message: str) -> List[str]:
    if not values:
        return [f"  - {empty_message}"]
    return [f"  - {key}: {value}" for key, value in sorted(values.items())]


def _event_file_lines(events: List[Dict[str, Any]]) -> List[str]:
    if not events:
        return ["  - No recent files recorded."]
    return [
        f"  - {event.get('event_time') or '-'} | {event.get('category') or '-'} | {event.get('file_name') or event.get('full_path') or '-'}"
        for event in events
    ]


def _device_lines(devices: List[Dict[str, Any]]) -> List[str]:
    if not devices:
        return ["  - No devices recorded."]
    return [
        f"  - {device.get('name') or device.get('device_id')}: {device.get('status') or '-'}"
        for device in devices
    ]


def _decision_lines(items: List[Dict[str, Any]]) -> List[str]:
    if not items:
        return ["  - No storage recommendations."]
    return [
        f"  - {item.get('recommendation')} | {item.get('target')} | {item.get('file_name') or item.get('full_path') or '-'} | {item.get('reason')}"
        for item in items[:10]
    ]


def _warning_lines(warnings: List[Dict[str, Any]]) -> List[str]:
    if not warnings:
        return ["  - No warnings."]
    return [
        f"  - {warning.get('recommendation')} | {warning.get('target')} | {warning.get('reason')}"
        for warning in warnings
    ]


def _category_evidence(
    events: List[Dict[str, Any]],
    session_minutes: Dict[str, float],
    category: str,
) -> List[str]:
    matching = [event for event in events if event.get("category") == category]
    lines = [f"- Session minutes classified as {category}: {session_minutes.get(category, 0)}"]
    if not matching:
        lines.append(f"- No {category} events recorded.")
        return lines
    lines.append(f"- {category} files/events:")
    lines.extend(
        f"  - {event.get('event_time') or '-'} | {event.get('event_type') or '-'} | {event.get('file_name') or event.get('full_path') or '-'}"
        for event in matching[:10]
    )
    return lines


def _next_best_actions(decisions: Dict[str, Any], missing_devices: List[Dict[str, Any]]) -> List[str]:
    actions = []
    for device in missing_devices:
        actions.append(f"- Reconnect {device.get('name') or device.get('device_id')} before relying on external storage.")
    for warning in decisions.get("warnings", []):
        actions.append(f"- Resolve storage warning: {warning.get('reason')}")
    for item in decisions.get("items", []):
        recommendation = item.get("recommendation")
        if recommendation in {"sync_to_microsd", "archive_to_wd", "keep_on_laptop"}:
            actions.append(f"- {recommendation}: {item.get('file_name') or item.get('full_path') or '-'}")
    if not actions:
        actions.append("- Continue current work capture pattern.")
    return actions[:7]


def _device_names(devices: List[Dict[str, Any]]) -> str:
    if not devices:
        return "none"
    return ", ".join(device.get("name") or device.get("device_id") or "-" for device in devices)
