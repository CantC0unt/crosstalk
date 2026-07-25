#!/usr/bin/env python3
"""Crosstalk: a dependency-free MCP server for AI-context group messaging.

Each group is stored in its own SQLite database inside ``~/.cache/crosstalk``
by default. Callers provide their own context_id on each read/send operation;
this keeps the server stateless and supports any number of groups per context.
"""

import json
import math
import os
import re
import sqlite3
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from urllib.parse import quote, unquote
from datetime import datetime, timedelta, timezone
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional


OUTPUT_LOCK = threading.Lock()
WAKEUP_REMINDER_SECONDS = 300
ACKNOWLEDGED_WAKEUP_RETENTION_DAYS = 7
SQLITE_LOCK_RETRY_SECONDS = 3
# Informational layout marker for databases created by this implementation.
# Crosstalk intentionally does not support automatic migration of older layouts.
GROUP_SCHEMA_VERSION = 5
MCP_PROTOCOL_VERSION = "2025-03-26"
DEFAULT_POLL_INTERVAL_SECONDS = 0.5
OBSERVABILITY_DATABASE_NAME = "observability.sqlite3"
# The initial observability schema. Future released schema changes must be
# designed separately; v1 intentionally has no migration path.
OBSERVABILITY_SCHEMA_VERSION = 1
RETENTION_CLEANUP_BATCH_SIZE = 1000


@dataclass(frozen=True)
class ObservabilityConfiguration:
    """Validated audit configuration for one MCP server process."""

    enabled: bool
    retention_days: Optional[int]


class ObservabilityConfigurationError(ValueError):
    """A configured audit setting is invalid for server startup."""


AUDIT_DETAILS_MAX_BYTES = 2048


@dataclass(frozen=True)
class AuditEvent:
    occurred_at: str
    audit_request_id: Optional[str]
    tool_name: str
    group_id: Optional[str]
    context_id: Optional[str]
    name: Optional[str]
    outcome: str
    duration_ms: int
    result_count: Optional[int]
    error_category: Optional[str]
    details_json: Optional[str]


def audit_identity(tool_name: str, arguments: Mapping[str, Any], result: Optional[Mapping[str, Any]] = None) -> Dict[str, Optional[str]]:
    """Extract audit identity semantically, never treating metadata as a caller name."""
    group_id = arguments.get("group_id") if isinstance(arguments.get("group_id"), str) else None
    context_id = arguments.get("context_id") if isinstance(arguments.get("context_id"), str) else None
    caller_name_tools = {
        "join_group", "leave_group", "get_all_messages", "get_latest_messages",
        "get_unread_messages", "get_messages_after", "search_messages", "send_message",
    }
    name = arguments.get("name") if tool_name in caller_name_tools and isinstance(arguments.get("name"), str) else None
    if tool_name == "create_group" and result is not None and isinstance(result.get("group_id"), str):
        group_id = result["group_id"]
    return {"group_id": group_id, "context_id": context_id, "name": name}


def safe_audit_details(tool_name: str, result: Mapping[str, Any]) -> Optional[str]:
    """Serialize only allowlisted, non-content result facts for an audit event."""
    details: Dict[str, Any] = {}
    if tool_name == "create_group" and isinstance(result.get("group_id"), str):
        details = {"created_group_id": result["group_id"]}
    elif tool_name == "list_groups" and isinstance(result.get("groups"), list):
        details = {"group_count": len(result["groups"])}
    elif tool_name == "update_group_metadata":
        details = {"updated_fields": sorted(field for field in ("name", "description") if field in result)}
    elif tool_name == "get_users" and isinstance(result.get("users"), list):
        details = {"member_count": len(result["users"])}
    elif tool_name in {"delete_group", "join_group", "leave_group"}:
        details = {"completed": True}
    elif tool_name in {"get_all_messages", "get_latest_messages", "get_unread_messages", "get_messages_after", "search_messages"} and isinstance(result.get("messages"), list):
        details = {"message_count": len(result["messages"])}
    elif tool_name == "send_message":
        details = {key: result[key] for key in ("message_id", "priority", "routing_reason") if key in result}
        if isinstance(result.get("wakeup_targets"), list):
            details["wakeup_target_count"] = len(result["wakeup_targets"])
    if not details:
        return None
    serialized = json.dumps(details, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return serialized if len(serialized.encode("utf-8")) <= AUDIT_DETAILS_MAX_BYTES else None


def audit_error_category(error: BaseException) -> str:
    """Map failures to the fixed analytics-safe category set."""
    message = str(error).lower()
    if "locked" in message or "busy" in message:
        return "sqlite_busy"
    if "not found" in message or "does not exist" in message:
        return "not_found"
    if "only the creator" in message or "not authorized" in message:
        return "authorization"
    if isinstance(error, (KeyError, TypeError, ValueError)):
        return "validation"
    return "internal"


def observability_configuration(environment: Optional[Mapping[str, str]] = None) -> ObservabilityConfiguration:
    """Read strict observability settings without creating any storage."""
    environment = os.environ if environment is None else environment
    retention = environment.get("CROSSTALK_OBSERVABILITY_RETENTION_DAYS")
    if retention is None:
        enabled = False
        retention_days = None
    elif retention == "inf":
        enabled = True
        retention_days = None
    elif re.fullmatch(r"[1-9][0-9]*", retention):
        enabled = True
        retention_days = int(retention)
    else:
        raise ObservabilityConfigurationError("CROSSTALK_OBSERVABILITY_RETENTION_DAYS must be 'inf' or a positive integer.")
    return ObservabilityConfiguration(enabled=enabled, retention_days=retention_days)


class ObservabilityStore:
    """The audit database, deliberately separate from group databases."""

    def __init__(self, groups_directory: str) -> None:
        self.groups_directory = Path(groups_directory)

    @property
    def database_path(self) -> Path:
        return self.groups_directory / OBSERVABILITY_DATABASE_NAME

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _connection(database_path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(str(database_path), timeout=0.1)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=100")
        return connection

    def initialize(self) -> None:
        """Create the initial audit schema without touching group databases."""
        self.groups_directory.mkdir(parents=True, exist_ok=True)
        is_new_database = not self.database_path.exists()
        database = self._connection(self.database_path)
        try:
            if is_new_database:
                # SQLite records this mode in the database header when set before tables.
                database.execute("PRAGMA auto_vacuum=INCREMENTAL")
            database.execute("PRAGMA journal_mode=WAL")
            schema_version = database.execute("PRAGMA user_version").fetchone()[0]
            if schema_version not in (0, OBSERVABILITY_SCHEMA_VERSION):
                raise ValueError("Unsupported observability schema version: " + str(schema_version))
            if schema_version == OBSERVABILITY_SCHEMA_VERSION:
                return
            database.executescript(
                """
                CREATE TABLE metadata (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    schema_version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    audit_enabled_at TEXT NOT NULL,
                    last_retention_cleanup_at TEXT,
                    retention_setting TEXT
                );
                CREATE TABLE tool_calls (
                    id INTEGER PRIMARY KEY,
                    occurred_at TEXT NOT NULL,
                    audit_request_id TEXT,
                    tool_name TEXT NOT NULL,
                    group_id TEXT,
                    context_id TEXT,
                    name TEXT,
                    outcome TEXT NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    result_count INTEGER,
                    error_category TEXT,
                    details_json TEXT
                );
                CREATE INDEX tool_calls_by_occurred_at ON tool_calls(occurred_at);
                CREATE INDEX tool_calls_by_tool_and_occurred_at ON tool_calls(tool_name, occurred_at);
                CREATE INDEX tool_calls_by_group_and_occurred_at ON tool_calls(group_id, occurred_at);
                CREATE INDEX tool_calls_by_context_and_occurred_at ON tool_calls(context_id, occurred_at);
                CREATE INDEX tool_calls_by_outcome_and_occurred_at ON tool_calls(outcome, occurred_at);
                """
            )
            now = self._now()
            database.execute(
                "INSERT INTO metadata(id, schema_version, created_at, audit_enabled_at, last_retention_cleanup_at, retention_setting) VALUES (1, ?, ?, ?, NULL, NULL)",
                (OBSERVABILITY_SCHEMA_VERSION, now, now),
            )
            database.execute("PRAGMA user_version=" + str(OBSERVABILITY_SCHEMA_VERSION))
            database.commit()
        finally:
            database.close()

    def record_event(self, event: AuditEvent, retention_setting: str = "inf") -> None:
        self.initialize()
        database = self._connection(self.database_path)
        try:
            database.execute("UPDATE metadata SET retention_setting = ? WHERE id = 1", (retention_setting,))
            database.execute(
                "INSERT INTO tool_calls(occurred_at, audit_request_id, tool_name, group_id, context_id, name, outcome, duration_ms, result_count, error_category, details_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (event.occurred_at, event.audit_request_id, event.tool_name, event.group_id, event.context_id, event.name, event.outcome, event.duration_ms, event.result_count, event.error_category, event.details_json),
            )
            database.commit()
        finally:
            database.close()

    def cleanup_retention(self, retention_days: Optional[int]) -> int:
        """Delete one bounded batch of expired rows, at most once per day."""
        if retention_days is None:
            return 0
        self.initialize()
        database = self._connection(self.database_path)
        try:
            now = datetime.now(timezone.utc)
            row = database.execute("SELECT last_retention_cleanup_at FROM metadata WHERE id = 1").fetchone()
            if row is not None and row[0]:
                last_cleanup = datetime.fromisoformat(row[0])
                if now - last_cleanup < timedelta(days=1):
                    return 0
            cutoff = (now - timedelta(days=retention_days)).isoformat()
            cursor = database.execute("DELETE FROM tool_calls WHERE id IN (SELECT id FROM tool_calls WHERE occurred_at < ? ORDER BY occurred_at LIMIT ?)", (cutoff, RETENTION_CLEANUP_BATCH_SIZE))
            database.execute("UPDATE metadata SET last_retention_cleanup_at = ? WHERE id = 1", (now.isoformat(),))
            database.commit()
            return cursor.rowcount
        finally:
            database.close()


def audit_tool_result(tool_name: str, arguments: Mapping[str, Any], result: Mapping[str, Any], request_id: Any, started_at: str, started_monotonic: float) -> AuditEvent:
    """Build one completed event from Crosstalk's normal tool result shape."""
    text = result.get("content", [{}])[0].get("text", "{}") if isinstance(result.get("content"), list) else "{}"
    try:
        payload = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        payload = {}
    is_error = bool(result.get("isError"))
    safe_arguments = arguments if isinstance(arguments, Mapping) else {}
    identity = audit_identity(tool_name, safe_arguments, payload if not is_error else None)
    messages = payload.get("messages") if isinstance(payload.get("messages"), list) else None
    return AuditEvent(
        occurred_at=started_at,
        audit_request_id=str(request_id) if request_id is not None else None,
        tool_name=tool_name,
        group_id=identity["group_id"], context_id=identity["context_id"], name=identity["name"],
        outcome="error" if is_error else "success",
        duration_ms=max(0, round((time.monotonic() - started_monotonic) * 1000)),
        result_count=len(messages) if messages is not None else None,
        error_category=audit_error_category(ValueError(str(payload.get("error", "")))) if is_error else None,
        details_json=None if is_error else safe_audit_details(tool_name, payload),
    )


def attempt_audit_write(store: "CrosstalkStore", event: AuditEvent, retention_days: Optional[int]) -> None:
    """Make one bounded audit attempt; audit storage never affects a tool call."""
    try:
        audit_store = ObservabilityStore(str(store.groups_directory))
        audit_store.record_event(event, "inf" if retention_days is None else str(retention_days))
        audit_store.cleanup_retention(retention_days)
    except (OSError, sqlite3.Error, ValueError):
        pass


def _server_version() -> str:
    try:
        return package_version("crosstalk-mcp-server")
    except PackageNotFoundError:
        return (Path(__file__).resolve().parents[1] / "VERSION").read_text(encoding="utf-8").strip()


SERVER_VERSION = _server_version()


class CrosstalkStore:
    """One SQLite database per group, shared safely by server processes."""

    def __init__(self, groups_directory: str) -> None:
        self.groups_directory = Path(groups_directory)
        self.groups_directory.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _validate_group_id(group_id: str) -> str:
        if not isinstance(group_id, str) or not group_id.startswith("grp_") or len(group_id) != 36:
            raise ValueError("Invalid group_id.")
        try:
            int(group_id[4:], 16)
        except ValueError as error:
            raise ValueError("Invalid group_id.") from error
        return group_id

    def _group_path(self, group_id: str) -> Path:
        return self.groups_directory / (self._validate_group_id(group_id) + ".sqlite3")

    def group_exists(self, group_id: str) -> bool:
        return self._group_path(group_id).is_file()

    @staticmethod
    def _connection(database_path: Path) -> sqlite3.Connection:
        # Keep each attempt short; call_tool performs bounded whole-operation retries.
        connection = sqlite3.connect(str(database_path), timeout=0.1)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=100")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    @staticmethod
    def _initialize_group(db: sqlite3.Connection, creator_context_id: str) -> None:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_context_id TEXT NOT NULL,
                sender_name TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL,
                priority TEXT NOT NULL DEFAULT 'normal',
                mentions TEXT NOT NULL DEFAULT '[]',
                wakeup_targets TEXT NOT NULL DEFAULT '[]',
                routing_reason TEXT NOT NULL DEFAULT 'fallback'
            );
            CREATE TABLE IF NOT EXISTS group_metadata (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                name TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                creator_context_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS group_members (
                context_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                joined_at TEXT NOT NULL,
                unread_message_ids TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE IF NOT EXISTS wakeup_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                context_id TEXT NOT NULL,
                relevance TEXT NOT NULL,
                priority TEXT NOT NULL DEFAULT 'normal',
                created_at TEXT NOT NULL,
                acknowledged_at TEXT,
                last_notified_at TEXT,
                UNIQUE(message_id, context_id)
            );
            CREATE INDEX IF NOT EXISTS wakeup_events_by_context ON wakeup_events(context_id, id);
            """
        )
        now = datetime.now(timezone.utc).isoformat()
        db.execute(
            "INSERT OR IGNORE INTO group_metadata(id, name, description, creator_context_id, created_at, updated_at) VALUES (1, '', '', ?, ?, ?)",
            (creator_context_id, now, now),
        )
        db.execute("PRAGMA user_version = " + str(GROUP_SCHEMA_VERSION))
        db.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def create_group(self, context_id: str, name: str = "", description: str = "") -> str:
        if not isinstance(context_id, str) or not context_id.strip():
            raise ValueError("context_id must be a non-empty string.")
        if not isinstance(name, str) or not isinstance(description, str):
            raise ValueError("name and description must be strings.")
        group_id = "grp_" + uuid.uuid4().hex
        db = self._connection(self._group_path(group_id))
        try:
            self._initialize_group(db, context_id)
            now = self._now()
            db.execute("UPDATE group_metadata SET name = ?, description = ?, updated_at = ? WHERE id = 1", (name, description, now))
            db.commit()
        finally:
            db.close()
        return group_id

    def list_groups(self) -> List[Dict[str, str]]:
        """List group databases without opening or exposing their message contents."""
        groups = []
        for path in self.groups_directory.glob("grp_*.sqlite3"):
            group_id = path.name[:-len(".sqlite3")]
            try:
                self._validate_group_id(group_id)
            except ValueError:
                continue
            try:
                groups.append(self.get_group_metadata(group_id))
            except (OSError, sqlite3.Error):
                groups.append({"group_id": group_id, "name": "", "description": ""})
        return sorted(groups, key=lambda group: group["group_id"])

    def get_group_metadata(self, group_id: str) -> Dict[str, str]:
        db = self._group_connection(group_id)
        try:
            row = db.execute("SELECT name, description, created_at, updated_at FROM group_metadata WHERE id = 1").fetchone()
        finally:
            db.close()
        if row is None:
            return {"group_id": group_id, "name": "", "description": ""}
        return {"group_id": group_id, "name": row["name"], "description": row["description"],
                "created_at": row["created_at"], "updated_at": row["updated_at"]}

    def update_group_metadata(self, group_id: str, context_id: str, name: str = None, description: str = None) -> Dict[str, str]:
        if not isinstance(context_id, str) or not context_id.strip():
            raise ValueError("context_id must be a non-empty string.")
        if name is None and description is None:
            raise ValueError("Provide name and/or description to update group metadata.")
        if name is not None and not isinstance(name, str):
            raise ValueError("name must be a string.")
        if description is not None and not isinstance(description, str):
            raise ValueError("description must be a string.")
        db = self._group_connection(group_id)
        try:
            with db:
                current = db.execute("SELECT name, description, creator_context_id FROM group_metadata WHERE id = 1").fetchone()
                if current is None or current["creator_context_id"] != context_id:
                    raise ValueError("Only the context that created this group can update its metadata.")
                current_name = current["name"] if current else ""
                current_description = current["description"] if current else ""
                creator_context_id = current["creator_context_id"] if current else ""
                db.execute(
                    """INSERT INTO group_metadata(id, name, description, creator_context_id, created_at, updated_at) VALUES (1, ?, ?, ?, ?, ?)
                       ON CONFLICT(id) DO UPDATE SET name = excluded.name, description = excluded.description, updated_at = excluded.updated_at""",
                    (name if name is not None else current_name, description if description is not None else current_description,
                     creator_context_id, self._now(), self._now()),
                )
        finally:
            db.close()
        return self.get_group_metadata(group_id)

    def group_update_snapshot(self, group_id: str) -> Dict[str, Any]:
        """Read lightweight resource metadata without changing unread state."""
        db = self._group_connection(group_id)
        try:
            row = db.execute("SELECT COUNT(*) AS count, MAX(id) AS latest_id FROM messages").fetchone()
        finally:
            db.close()
        return {"group_id": group_id, "message_count": row["count"], "latest_message_id": row["latest_id"]}

    def join_group(self, group_id: str, context_id: str, name: str) -> None:
        self._require_identity(context_id, name)
        db = self._group_connection(group_id)
        try:
            with db:
                if db.execute("SELECT 1 FROM group_members WHERE context_id = ?", (context_id,)).fetchone() is None:
                    message_ids = [row["id"] for row in db.execute("SELECT id FROM messages ORDER BY id ASC")]
                    db.execute(
                        "INSERT INTO group_members(context_id, name, joined_at, unread_message_ids) VALUES (?, ?, ?, ?)",
                        (context_id, name, self._now(), json.dumps(message_ids)),
                    )
                else:
                    db.execute("UPDATE group_members SET name = ? WHERE context_id = ?", (name, context_id))
        finally:
            db.close()

    def leave_group(self, group_id: str, context_id: str, name: str) -> None:
        self._require_identity(context_id, name)
        db = self._group_connection(group_id)
        try:
            with db:
                self._require_member(db, context_id, name)
                db.execute("DELETE FROM group_members WHERE context_id = ?", (context_id,))
                db.execute("DELETE FROM wakeup_events WHERE context_id = ?", (context_id,))
        finally:
            db.close()

    def leave_group_context(self, group_id: str, context_id: str) -> None:
        """Leave a group using the stored membership identity for MCP unsubscribe handling."""
        db = self._group_connection(group_id)
        try:
            with db:
                if db.execute("SELECT 1 FROM group_members WHERE context_id = ?", (context_id,)).fetchone() is None:
                    raise ValueError("Context is not a member of this group.")
                db.execute("DELETE FROM group_members WHERE context_id = ?", (context_id,))
                db.execute("DELETE FROM wakeup_events WHERE context_id = ?", (context_id,))
        finally:
            db.close()

    def _require_member(self, db: sqlite3.Connection, context_id: str, name: str) -> None:
        member = db.execute("SELECT name FROM group_members WHERE context_id = ?", (context_id,)).fetchone()
        if member is None:
            raise ValueError("Context has not joined this group. Call join_group first.")
        if member["name"] != name:
            raise ValueError("name does not match this context's joined group name. Call join_group to update it.")

    def wakeup_snapshot(self, group_id: str, context_id: str) -> Dict[str, Any]:
        db = self._group_connection(group_id)
        try:
            rows = db.execute(
                """SELECT wakeup_events.id, wakeup_events.message_id, wakeup_events.relevance, wakeup_events.priority, wakeup_events.created_at,
                          wakeup_events.acknowledged_at, wakeup_events.last_notified_at
                   FROM wakeup_events WHERE wakeup_events.context_id = ? ORDER BY wakeup_events.id ASC""",
                (context_id,),
            ).fetchall()
        finally:
            db.close()
        return {"group_id": group_id, "context_id": context_id,
                "wakeups": [{"event_id": row["id"], "message_id": row["message_id"], "relevance": row["relevance"], "priority": row["priority"], "created_at": row["created_at"], "acknowledged_at": row["acknowledged_at"], "last_notified_at": row["last_notified_at"]} for row in rows]}

    def next_wakeup_notification(self, group_id: str, context_id: str) -> Dict[str, Any]:
        """Return one due wakeup notification without marking it delivered."""
        cutoff = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() - WAKEUP_REMINDER_SECONDS, timezone.utc).isoformat()
        db = self._group_connection(group_id)
        try:
            row = db.execute(
                """SELECT id, priority, relevance FROM wakeup_events
                   WHERE context_id = ? AND acknowledged_at IS NULL
                     AND (last_notified_at IS NULL OR last_notified_at <= ?)
                   ORDER BY id ASC LIMIT 1""",
                (context_id, cutoff),
            ).fetchone()
            return {"id": row["id"], "priority": row["priority"], "relevance": row["relevance"]} if row else None
        finally:
            db.close()

    def mark_wakeup_notified(self, group_id: str, event_id: int) -> None:
        db = self._group_connection(group_id)
        try:
            with db:
                db.execute("UPDATE wakeup_events SET last_notified_at = ? WHERE id = ? AND acknowledged_at IS NULL", (self._now(), event_id))
        finally:
            db.close()

    def claim_wakeup_notification(self, group_id: str, context_id: str) -> Dict[str, Any]:
        """Compatibility helper that returns and immediately marks one due wakeup."""
        wakeup = self.next_wakeup_notification(group_id, context_id)
        if wakeup is not None:
            self.mark_wakeup_notified(group_id, wakeup["id"])
        return wakeup

    def _acknowledge_wakeups_in_transaction(self, db: sqlite3.Connection, context_id: str,
                                            message_ids: List[int]) -> None:
        if not message_ids:
            return
        placeholders = ",".join("?" for _ in message_ids)
        db.execute(
            "UPDATE wakeup_events SET acknowledged_at = ? WHERE context_id = ? AND message_id IN (" + placeholders + ") AND acknowledged_at IS NULL",
            [self._now(), context_id, *message_ids],
        )
        cutoff = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() - ACKNOWLEDGED_WAKEUP_RETENTION_DAYS * 86400, timezone.utc).isoformat()
        db.execute("DELETE FROM wakeup_events WHERE acknowledged_at IS NOT NULL AND acknowledged_at < ?", (cutoff,))

    def latest_wakeup_event_id(self, group_id: str, context_id: str) -> int:
        db = self._group_connection(group_id)
        try:
            row = db.execute("SELECT MAX(id) AS latest_id FROM wakeup_events WHERE context_id = ?", (context_id,)).fetchone()
        finally:
            db.close()
        return int(row["latest_id"] or 0)

    def latest_wakeup_event(self, group_id: str, context_id: str) -> Dict[str, Any]:
        db = self._group_connection(group_id)
        try:
            row = db.execute(
                "SELECT id, priority, relevance FROM wakeup_events WHERE context_id = ? ORDER BY id DESC LIMIT 1",
                (context_id,),
            ).fetchone()
        finally:
            db.close()
        return {"id": int(row["id"] or 0), "priority": row["priority"], "relevance": row["relevance"]} if row else {"id": 0, "priority": "normal", "relevance": "none"}

    def get_users(self, group_id: str) -> List[Dict[str, Any]]:
        """List current group members."""
        db = self._group_connection(group_id)
        try:
            members = db.execute("SELECT context_id, name, joined_at FROM group_members").fetchall()
            latest_messages = db.execute("SELECT sender_context_id AS context_id, MAX(id) AS latest_message_id FROM messages GROUP BY sender_context_id").fetchall()
        finally:
            db.close()
        latest_by_context = {row["context_id"]: row["latest_message_id"] for row in latest_messages}
        users: Dict[str, Dict[str, Any]] = {
            row["context_id"]: {"context_id": row["context_id"], "name": row["name"], "joined_at": row["joined_at"],
                                "latest_message_id": latest_by_context.get(row["context_id"])}
            for row in members
        }
        return sorted(users.values(), key=lambda user: user["context_id"])


    def delete_group(self, group_id: str, context_id: str) -> None:
        """Permanently remove one group's database and SQLite sidecar files."""
        if not isinstance(context_id, str) or not context_id.strip():
            raise ValueError("context_id must be a non-empty string.")
        path = self._group_path(group_id)
        if not path.is_file():
            raise ValueError("Group does not exist: " + group_id + ". It may already have been deleted.")
        db = self._connection(path)
        try:
            row = db.execute("SELECT creator_context_id FROM group_metadata WHERE id = 1").fetchone()
        finally:
            db.close()
        if row is None or row["creator_context_id"] != context_id:
            raise ValueError("Only the context that created this group can delete it.")
        # WAL mode can leave these adjacent temporary files. Their names are derived
        # exclusively from the validated group path above.
        for target in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
            try:
                target.unlink()
            except FileNotFoundError:
                pass

    def _group_connection(self, group_id: str) -> sqlite3.Connection:
        path = self._group_path(group_id)
        if not path.is_file():
            raise ValueError("Group does not exist: " + group_id + ". Create it first or check the group_id.")
        return self._connection(path)

    @staticmethod
    def _json_string_list(value: str) -> List[str]:
        try:
            readers = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            readers = []
        return [reader for reader in readers if isinstance(reader, str)]

    @staticmethod
    def _has_mention(message_lower: str, identifier: str) -> bool:
        """Match a complete mention token, including punctuation and end-of-message."""
        return re.search(r"@" + re.escape(identifier.lower()) + r"(?=$|[\s.,!?;:])", message_lower) is not None

    def _format_messages(self, rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
        return [{"id": row["id"], "context_id": row["sender_context_id"],
                 "name": row["sender_name"], "message": row["content"],
                 "priority": row["priority"], "mentions": self._json_string_list(row["mentions"]),
                 "wakeup_targets": self._json_string_list(row["wakeup_targets"]),
                 "routing_reason": row["routing_reason"], "sent_at": row["created_at"]}
                for row in rows]

    @staticmethod
    def _unread_ids(value: str) -> List[int]:
        try:
            ids = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            ids = []
        return [message_id for message_id in ids if isinstance(message_id, int) and not isinstance(message_id, bool) and message_id > 0]

    def _remove_unread(self, db: sqlite3.Connection, context_id: str, message_ids: List[int]) -> None:
        if not message_ids:
            return
        row = db.execute("SELECT unread_message_ids FROM group_members WHERE context_id = ?", (context_id,)).fetchone()
        if row is None:
            raise ValueError("Context has not joined this group. Call join_group first.")
        removed = set(message_ids)
        remaining = [message_id for message_id in self._unread_ids(row["unread_message_ids"]) if message_id not in removed]
        db.execute("UPDATE group_members SET unread_message_ids = ? WHERE context_id = ?", (json.dumps(remaining), context_id))

    def _messages_for_ids(self, db: sqlite3.Connection, message_ids: List[int]) -> List[sqlite3.Row]:
        rows = []
        for start in range(0, len(message_ids), 900):
            chunk = message_ids[start:start + 900]
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(db.execute(
                "SELECT id, sender_context_id, sender_name, content, created_at, priority, mentions, wakeup_targets, routing_reason "
                "FROM messages WHERE id IN (" + placeholders + ")", chunk
            ).fetchall())
        return sorted(rows, key=lambda row: row["id"])

    def _messages(self, group_id: str, context_id: str, name: str, after_id: int = 0) -> List[Dict[str, Any]]:
        db = self._group_connection(group_id)
        try:
            with db:
                self._require_member(db, context_id, name)
                rows = db.execute(
                    """SELECT id, sender_context_id, sender_name, content, created_at, priority, mentions, wakeup_targets, routing_reason
                       FROM messages WHERE id > ? ORDER BY id ASC""", (after_id,)
                ).fetchall()
                self._remove_unread(db, context_id, [row["id"] for row in rows])
                self._acknowledge_wakeups_in_transaction(db, context_id, [row["id"] for row in rows])
        finally:
            db.close()
        return self._format_messages(rows)

    def get_all_messages(self, group_id: str, context_id: str, name: str) -> List[Dict[str, Any]]:
        # Identity is deliberately accepted here for consistent caller-side audit logs.
        self._require_identity(context_id, name)
        return self._messages(group_id, context_id, name)

    def get_latest_messages(self, group_id: str, context_id: str, name: str) -> List[Dict[str, Any]]:
        self._require_identity(context_id, name)
        db = self._group_connection(group_id)
        try:
            with db:
                self._require_member(db, context_id, name)
                row = db.execute(
                    "SELECT MAX(id) AS last_id FROM messages WHERE sender_context_id = ?", (context_id,)
                ).fetchone()
                # Before this context's first send, every message in this group is unread.
                rows = db.execute(
                    """SELECT id, sender_context_id, sender_name, content, created_at, priority, mentions, wakeup_targets, routing_reason
                       FROM messages WHERE id > ? ORDER BY id ASC""", (int(row["last_id"] or 0),)
                ).fetchall()
                self._remove_unread(db, context_id, [message["id"] for message in rows])
                self._acknowledge_wakeups_in_transaction(db, context_id, [message["id"] for message in rows])
        finally:
            db.close()
        return self._format_messages(rows)

    def get_unread_messages(self, group_id: str, context_id: str, name: str) -> List[Dict[str, Any]]:
        self._require_identity(context_id, name)
        db = self._group_connection(group_id)
        try:
            with db:
                self._require_member(db, context_id, name)
                unread = db.execute("SELECT unread_message_ids FROM group_members WHERE context_id = ?", (context_id,)).fetchone()
                if unread is None:
                    raise ValueError("Context has not joined this group. Call join_group first.")
                unread_rows = self._messages_for_ids(db, self._unread_ids(unread["unread_message_ids"]))
                self._remove_unread(db, context_id, [row["id"] for row in unread_rows])
                self._acknowledge_wakeups_in_transaction(db, context_id, [row["id"] for row in unread_rows])
        finally:
            db.close()
        return self._format_messages(unread_rows)

    def get_messages_after(self, group_id: str, context_id: str, name: str, after_message_id: int) -> List[Dict[str, Any]]:
        self._require_identity(context_id, name)
        if not isinstance(after_message_id, int) or isinstance(after_message_id, bool) or after_message_id < 0:
            raise ValueError("after_message_id must be a non-negative integer.")
        return self._messages(group_id, context_id, name, after_message_id)

    def search_messages(self, group_id: str, context_id: str, name: str, query: str) -> List[Dict[str, Any]]:
        self._require_identity(context_id, name)
        if not isinstance(query, str) or not query.strip():
            raise ValueError("query must be a non-empty string.")
        db = self._group_connection(group_id)
        try:
            with db:
                self._require_member(db, context_id, name)
                rows = db.execute(
                    """SELECT id, sender_context_id, sender_name, content, created_at, priority, mentions, wakeup_targets, routing_reason
                       FROM messages WHERE content LIKE ? COLLATE NOCASE ORDER BY id ASC""",
                    ("%" + query + "%",),
                ).fetchall()
                self._remove_unread(db, context_id, [row["id"] for row in rows])
                self._acknowledge_wakeups_in_transaction(db, context_id, [row["id"] for row in rows])
        finally:
            db.close()
        return self._format_messages(rows)

    @staticmethod
    def _require_identity(context_id: str, name: str) -> None:
        if not isinstance(context_id, str) or not context_id.strip():
            raise ValueError("context_id must be a non-empty string.")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string.")

    def send_message(self, group_id: str, context_id: str, name: str, message: str,
        wake_context_ids: List[str] = None, priority: str = "normal") -> Dict[str, Any]:
        self._require_identity(context_id, name)
        if not isinstance(message, str) or not message.strip():
            raise ValueError("message must be a non-empty string.")
        if wake_context_ids is not None and (not isinstance(wake_context_ids, list) or not all(isinstance(item, str) and item.strip() for item in wake_context_ids)):
            raise ValueError("wake_context_ids must be an array of non-empty context IDs.")
        if priority not in {"low", "normal", "high", "urgent"}:
            raise ValueError("priority must be one of: low, normal, high, urgent.")
        db = self._group_connection(group_id)
        try:
            with db:
                self._require_member(db, context_id, name)
                cursor = db.execute(
                    """INSERT INTO messages(sender_context_id, sender_name, content, created_at, priority, mentions, wakeup_targets, routing_reason)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (context_id, name, message, self._now(), priority, "[]", "[]", "fallback")
                )
                members_with_unreads = db.execute(
                    "SELECT context_id, unread_message_ids FROM group_members WHERE context_id != ?", (context_id,)
                ).fetchall()
                for member in members_with_unreads:
                    message_ids = self._unread_ids(member["unread_message_ids"])
                    if cursor.lastrowid not in message_ids:
                        db.execute(
                            "UPDATE group_members SET unread_message_ids = ? WHERE context_id = ?",
                            (json.dumps(message_ids + [cursor.lastrowid]), member["context_id"]),
                        )
                members = db.execute(
                    "SELECT context_id, name FROM group_members WHERE context_id != ?", (context_id,)
                ).fetchall()
                requested_targets = set(wake_context_ids or [])
                explicit_targets = {row["context_id"] for row in members if row["context_id"] in requested_targets}
                message_lower = message.lower()
                mentioned_rows = members if self._has_mention(message_lower, "all") else [row for row in members if self._has_mention(message_lower, row["context_id"]) or self._has_mention(message_lower, row["name"])]
                parsed_mentions = [row["context_id"] for row in mentioned_rows]
                if explicit_targets:
                    targets = [(row["context_id"], "explicit") for row in members if row["context_id"] in explicit_targets]
                    routing_reason = "explicit"
                else:
                    if priority == "urgent":
                        targets = [(row["context_id"], "urgent") for row in members]
                        routing_reason = "urgent"
                    elif mentioned_rows:
                        targets = [(row["context_id"], "mentioned") for row in mentioned_rows]
                        routing_reason = "mentioned"
                    else:
                        # No unambiguous target: notify every other joined context.
                        targets = [(row["context_id"], "fallback") for row in members]
                        routing_reason = "fallback"
                for target_context_id, relevance in targets:
                    db.execute(
                        """INSERT OR IGNORE INTO wakeup_events(message_id, context_id, relevance, priority, created_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        (cursor.lastrowid, target_context_id, relevance, priority, self._now()),
                    )
                db.execute(
                    "UPDATE messages SET mentions = ?, wakeup_targets = ?, routing_reason = ? WHERE id = ?",
                    (json.dumps(parsed_mentions), json.dumps([target_context_id for target_context_id, _ in targets]), routing_reason, cursor.lastrowid),
                )
        finally:
            db.close()
        return {"message_id": cursor.lastrowid, "group_id": group_id, "priority": priority,
                "mentions": parsed_mentions, "routing_reason": routing_reason,
                "wakeup_targets": [target_context_id for target_context_id, _ in targets]}


class GroupSubscriptionMonitor:
    """Poll subscribed group databases and emit standard MCP resource updates."""

    URI_PREFIX = "crosstalk://groups/"
    URI_SUFFIX = "/messages"
    WAKEUP_SEGMENT = "/wakeups/"

    def __init__(self, store: CrosstalkStore, poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS) -> None:
        if not math.isfinite(poll_interval_seconds) or poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be a finite value greater than zero.")
        self.store = store
        self.poll_interval_seconds = poll_interval_seconds
        self._subscriptions: Dict[str, Dict[str, Any]] = {}
        # Targeted wakeup resources are scoped to contexts which joined through
        # this MCP server process.  A URI alone must not grant wakeup access.
        self._authorized_wakeup_uris: set[str] = set()
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._watch, name="crosstalk-subscriptions", daemon=True)
        self._thread.start()

    @classmethod
    def uri_for_group(cls, group_id: str) -> str:
        return cls.URI_PREFIX + group_id + cls.URI_SUFFIX

    @classmethod
    def group_id_from_uri(cls, uri: str) -> str:
        if not isinstance(uri, str) or not uri.startswith(cls.URI_PREFIX) or not uri.endswith(cls.URI_SUFFIX):
            raise ValueError("Invalid Crosstalk subscription URI.")
        group_id = uri[len(cls.URI_PREFIX):-len(cls.URI_SUFFIX)]
        CrosstalkStore._validate_group_id(group_id)
        return group_id

    @classmethod
    def wakeup_uri_for_group(cls, group_id: str, context_id: str) -> str:
        return cls.URI_PREFIX + group_id + cls.WAKEUP_SEGMENT + quote(context_id, safe="")

    @classmethod
    def wakeup_from_uri(cls, uri: str) -> Dict[str, str]:
        if not isinstance(uri, str) or not uri.startswith(cls.URI_PREFIX) or cls.WAKEUP_SEGMENT not in uri:
            raise ValueError("Invalid Crosstalk wakeup subscription URI.")
        remainder = uri[len(cls.URI_PREFIX):]
        group_id, context_id = remainder.split(cls.WAKEUP_SEGMENT, 1)
        CrosstalkStore._validate_group_id(group_id)
        context_id = unquote(context_id)
        if not context_id.strip() or "/" in context_id:
            raise ValueError("Invalid wakeup context ID.")
        return {"group_id": group_id, "context_id": context_id}

    def subscribe(self, uri: str) -> None:
        if self.WAKEUP_SEGMENT in uri:
            wakeup = self.wakeup_from_uri(uri)
            if not self.is_wakeup_authorized(uri):
                raise ValueError("Wakeup resource is not authorized for this MCP session. Call join_group first.")
            self.store.latest_wakeup_event_id(wakeup["group_id"], wakeup["context_id"])
            subscription = {"kind": "wakeup", **wakeup}
        else:
            group_id = self.group_id_from_uri(uri)
            latest_id = self.store.group_update_snapshot(group_id)["latest_message_id"] or 0
            subscription = {"kind": "group", "latest_id": latest_id, "group_id": group_id}
        with self._lock:
            self._subscriptions[uri] = subscription

    def authorize_wakeup(self, uri: str) -> None:
        """Allow this server session to access one joined context's wakeups."""
        self.wakeup_from_uri(uri)
        with self._lock:
            self._authorized_wakeup_uris.add(uri)

    def revoke_wakeup(self, uri: str) -> None:
        with self._lock:
            self._authorized_wakeup_uris.discard(uri)
            self._subscriptions.pop(uri, None)

    def is_wakeup_authorized(self, uri: str) -> bool:
        with self._lock:
            return uri in self._authorized_wakeup_uris

    def unsubscribe(self, uri: str) -> None:
        with self._lock:
            self._subscriptions.pop(uri, None)

    def stop(self) -> None:
        self._stopped.set()
        self._thread.join(timeout=1)

    @staticmethod
    def _emit_notification(method: str, params: Dict[str, Any]) -> bool:
        try:
            notify(method, params)
            return True
        except (OSError, ValueError):
            return False

    def _watch(self) -> None:
        while not self._stopped.wait(self.poll_interval_seconds):
            with self._lock:
                subscriptions = list(self._subscriptions.items())
            for uri, subscription in subscriptions:
                try:
                    if subscription["kind"] == "wakeup":
                        wakeup = self.store.next_wakeup_notification(subscription["group_id"], subscription["context_id"])
                        if wakeup is not None:
                            if self._emit_notification("notifications/resources/updated", {"uri": uri, "priority": wakeup["priority"], "relevance": wakeup["relevance"]}):
                                self.store.mark_wakeup_notified(subscription["group_id"], wakeup["id"])
                        continue
                    else:
                        latest_id = self.store.group_update_snapshot(subscription["group_id"])["latest_message_id"] or 0
                except sqlite3.OperationalError as error:
                    if "locked" in str(error).lower() or "busy" in str(error).lower():
                        # Concurrent writes are expected; retain the subscription and retry.
                        continue
                    if self.store.group_exists(subscription["group_id"]):
                        continue
                    with self._lock:
                        self._subscriptions.pop(uri, None)
                    self._emit_notification("notifications/resources/updated", {"uri": uri, "relevance": "deleted"})
                    continue
                except (OSError, ValueError, sqlite3.Error):
                    if self.store.group_exists(subscription["group_id"]):
                        continue
                    with self._lock:
                        self._subscriptions.pop(uri, None)
                    self._emit_notification("notifications/resources/updated", {"uri": uri, "relevance": "deleted"})
                    continue
                if latest_id > subscription["latest_id"]:
                    if not self._emit_notification("notifications/resources/updated", {"uri": uri, "priority": "normal", "relevance": "group_change"}):
                        continue
                    with self._lock:
                        if uri in self._subscriptions:
                            self._subscriptions[uri]["latest_id"] = latest_id


IDENTITY_PROPERTIES = {
    "group_id": {"type": "string", "description": "The target Crosstalk group ID."},
    "context_id": {"type": "string", "description": "Stable, unique identifier for the calling AI context/session."},
    "name": {"type": "string", "description": "Human-readable caller label or role, used for logging (for example, 'architect')."},
}
GROUP_METADATA_PROPERTIES = {
    "group_id": {"type": "string", "description": "The target Crosstalk group ID."},
    "name": {"type": "string", "description": "Human-readable group name."},
    "description": {"type": "string", "description": "Optional description of the group's purpose."},
}

TOOLS = [
    {"name": "create_group", "description": "Create a new Crosstalk group and return its ID. The creating context is the only context allowed to update its metadata or delete it. A name and description can be supplied as metadata.", "inputSchema": {"type": "object", "properties": {"context_id": IDENTITY_PROPERTIES["context_id"], "name": GROUP_METADATA_PROPERTIES["name"], "description": GROUP_METADATA_PROPERTIES["description"]}, "required": ["context_id"], "additionalProperties": False}},
    {"name": "list_groups", "description": "List all groups and their metadata stored in the shared cache directory.", "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False}},
    {"name": "get_group_metadata", "description": "Return a group's name, description, and timestamps.", "inputSchema": {"type": "object", "properties": {"group_id": GROUP_METADATA_PROPERTIES["group_id"]}, "required": ["group_id"], "additionalProperties": False}},
    {"name": "update_group_metadata", "description": "Update a group's name and/or description. Only the creating context may update metadata.", "inputSchema": {"type": "object", "properties": {**GROUP_METADATA_PROPERTIES, "context_id": IDENTITY_PROPERTIES["context_id"]}, "required": ["group_id", "context_id"], "additionalProperties": False}},
    {"name": "get_users", "description": "List current group members.", "inputSchema": {"type": "object", "properties": {"group_id": GROUP_METADATA_PROPERTIES["group_id"]}, "required": ["group_id"], "additionalProperties": False}},
    {"name": "delete_group", "description": "Permanently delete a group's SQLite database and all of its message history. Only the creating context may delete it.", "annotations": {"destructiveHint": True}, "inputSchema": {"type": "object", "properties": {"group_id": {"type": "string", "description": "The group ID to permanently delete."}, "context_id": IDENTITY_PROPERTIES["context_id"]}, "required": ["group_id", "context_id"], "additionalProperties": False}},
    {"name": "join_group", "description": "Join a group. A context must join before it can send, retrieve messages, or subscribe to wakeups.", "inputSchema": {"type": "object", "properties": IDENTITY_PROPERTIES, "required": ["group_id", "context_id", "name"], "additionalProperties": False}},
    {"name": "leave_group", "description": "Leave a group and remove this context's wakeup subscription and pending wake events.", "inputSchema": {"type": "object", "properties": IDENTITY_PROPERTIES, "required": ["group_id", "context_id", "name"], "additionalProperties": False}},
    {"name": "get_all_messages", "description": "Return the complete history of the supplied group.", "inputSchema": {"type": "object", "properties": IDENTITY_PROPERTIES, "required": ["group_id", "context_id", "name"], "additionalProperties": False}},
    {"name": "get_latest_messages", "description": "Return messages after the supplied context_id's latest sent message. Before it has sent a message, returns the full history.", "inputSchema": {"type": "object", "properties": IDENTITY_PROPERTIES, "required": ["group_id", "context_id", "name"], "additionalProperties": False}},
    {"name": "get_unread_messages", "description": "Return messages this context_id has not previously retrieved. All returned messages are marked as read by that context.", "inputSchema": {"type": "object", "properties": IDENTITY_PROPERTIES, "required": ["group_id", "context_id", "name"], "additionalProperties": False}},
    {"name": "get_messages_after", "description": "Return messages with an ID after after_message_id and mark them read for this context.", "inputSchema": {"type": "object", "properties": {**IDENTITY_PROPERTIES, "after_message_id": {"type": "integer", "minimum": 0, "description": "Return messages with IDs greater than this value."}}, "required": ["group_id", "context_id", "name", "after_message_id"], "additionalProperties": False}},
    {"name": "search_messages", "description": "Case-insensitively search message text in a group and mark matching messages read for this context. The query uses SQL LIKE wildcards: % matches any sequence and _ matches one character.", "inputSchema": {"type": "object", "properties": {**IDENTITY_PROPERTIES, "query": {"type": "string", "description": "Search pattern. SQL LIKE wildcards are supported: % matches any sequence and _ matches one character."}}, "required": ["group_id", "context_id", "name", "query"], "additionalProperties": False}},
    {"name": "send_message", "description": "Send a message to the supplied group as the supplied AI context. Use @context_id, @name, @all, or wake_context_ids for routing; otherwise every other joined context is targeted. Unknown wake_context_ids are ignored; if none are valid, normal routing is used.", "inputSchema": {"type": "object", "properties": {**IDENTITY_PROPERTIES, "message": {"type": "string", "description": "Message body to send."}, "priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"], "default": "normal", "description": "Message priority."}, "wake_context_ids": {"type": "array", "items": {"type": "string"}, "description": "Optional explicit context IDs to wake. Unknown IDs are ignored."}}, "required": ["group_id", "context_id", "name", "message"], "additionalProperties": False}},
]


def tool_result(value: Any, is_error: bool = False) -> Dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(value)}], "isError": is_error}


def call_tool(store: CrosstalkStore, tool_name: str, arguments: Dict[str, Any],
              subscriptions: GroupSubscriptionMonitor = None, retry_deadline: float = None,
              retry_delay: float = 0.025) -> Dict[str, Any]:
    if retry_deadline is None:
        retry_deadline = time.monotonic() + SQLITE_LOCK_RETRY_SECONDS
    try:
        if tool_name == "create_group":
            group_id = store.create_group(**arguments)
            return tool_result(store.get_group_metadata(group_id))
        if tool_name == "list_groups":
            return tool_result({"groups": store.list_groups()})
        if tool_name == "get_group_metadata":
            return tool_result(store.get_group_metadata(**arguments))
        if tool_name == "update_group_metadata":
            return tool_result(store.update_group_metadata(**arguments))
        if tool_name == "get_users":
            return tool_result({"users": store.get_users(**arguments)})
        if tool_name == "delete_group":
            store.delete_group(**arguments)
            return tool_result({"group_id": arguments["group_id"], "deleted": True})
        if tool_name == "join_group":
            store.join_group(**arguments)
            wakeup_uri = GroupSubscriptionMonitor.wakeup_uri_for_group(arguments["group_id"], arguments["context_id"])
            if subscriptions is not None:
                subscriptions.authorize_wakeup(wakeup_uri)
                subscriptions.subscribe(wakeup_uri)
            return tool_result({"group_id": arguments["group_id"], "joined": True, "wakeup_resource_uri": wakeup_uri})
        if tool_name == "leave_group":
            wakeup_uri = GroupSubscriptionMonitor.wakeup_uri_for_group(arguments["group_id"], arguments["context_id"])
            store.leave_group(**arguments)
            if subscriptions is not None:
                subscriptions.revoke_wakeup(wakeup_uri)
            return tool_result({"group_id": arguments["group_id"], "left": True})
        if tool_name == "get_all_messages":
            return tool_result({"messages": store.get_all_messages(**arguments)})
        if tool_name == "get_latest_messages":
            return tool_result({"messages": store.get_latest_messages(**arguments)})
        if tool_name == "get_unread_messages":
            return tool_result({"messages": store.get_unread_messages(**arguments)})
        if tool_name == "get_messages_after":
            return tool_result({"messages": store.get_messages_after(**arguments)})
        if tool_name == "search_messages":
            return tool_result({"messages": store.search_messages(**arguments)})
        if tool_name == "send_message":
            return tool_result(store.send_message(**arguments))
        return tool_result({"error": "Unknown tool: " + tool_name}, True)
    except KeyError as error:
        return tool_result({"error": "Missing required input: " + str(error).strip("'")}, True)
    except (TypeError, ValueError) as error:
        return tool_result({"error": str(error)}, True)
    except sqlite3.OperationalError as error:
        is_lock_error = "locked" in str(error).lower() or "busy" in str(error).lower()
        remaining = retry_deadline - time.monotonic()
        if is_lock_error and remaining > 0:
            time.sleep(min(retry_delay, remaining))
            return call_tool(store, tool_name, arguments, subscriptions, retry_deadline, min(retry_delay * 2, 0.4))
        if is_lock_error:
            return tool_result({"error": "Cannot complete operation because the group is busy. Please try again in a moment."}, True)
        return tool_result({"error": "Database error while processing " + tool_name + ": " + str(error)}, True)
    except sqlite3.Error as error:
        return tool_result({"error": "Database error while processing " + tool_name + ": " + str(error)}, True)
    except OSError as error:
        return tool_result({"error": "Storage error while processing " + tool_name + ": " + str(error)}, True)


def respond(request_id: Any, result: Dict[str, Any]) -> None:
    _write_json({"jsonrpc": "2.0", "id": request_id, "result": result})


def respond_error(request_id: Any, code: int, message: str) -> None:
    _write_json(_error_response(request_id, code, message))


def _error_response(request_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _write_json(value: Any) -> None:
    with OUTPUT_LOCK:
        sys.stdout.write(json.dumps(value) + "\n")
        sys.stdout.flush()


def notify(method: str, params: Dict[str, Any]) -> None:
    """Send a server-to-client JSON-RPC notification without a response ID."""
    with OUTPUT_LOCK:
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "method": method, "params": params}) + "\n")
        sys.stdout.flush()


def _handle_request(request: Any, store: CrosstalkStore, subscriptions: GroupSubscriptionMonitor,
                    state: Dict[str, bool]) -> Optional[Dict[str, Any]]:
    if not isinstance(request, dict) or request.get("jsonrpc") != "2.0":
        return _error_response(None, -32600, "Invalid Request")
    request_id = request.get("id")
    has_response_id = "id" in request and request_id is not None
    method = request.get("method")
    if not isinstance(method, str):
        return _error_response(request_id, -32600, "Invalid Request") if has_response_id else None
    try:
        if method == "initialize":
            if not has_response_id or state["initialized"]:
                return _error_response(request_id, -32600, "Invalid initialize request") if has_response_id else None
            params = request.get("params", {})
            if not isinstance(params, dict):
                raise ValueError("initialize params must be an object.")
            client_protocol_version = params.get("protocolVersion")
            if not isinstance(client_protocol_version, str) or not client_protocol_version.strip():
                raise ValueError("initialize protocolVersion must be a non-empty string.")
            state["initialized"] = True
            return {"jsonrpc": "2.0", "id": request_id, "result": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {}, "resources": {"subscribe": True, "listChanged": False}},
                "serverInfo": {"name": "crosstalk", "version": SERVER_VERSION},
            }}
        if method == "notifications/initialized":
            if state["initialized"]:
                state["client_ready"] = True
            return None
        if method == "ping":
            return {"jsonrpc": "2.0", "id": request_id, "result": {}} if has_response_id else None
        if not state["client_ready"]:
            return _error_response(request_id, -32002, "Server is not initialized") if has_response_id else None
        if not has_response_id:
            return None
        if method == "tools/list":
            result = {"tools": TOOLS}
        elif method == "tools/call":
            params = request.get("params", {})
            if not isinstance(params, dict):
                raise ValueError("tools/call params must be an object.")
            tool_name = params.get("name", "")
            arguments = params.get("arguments", {})
            started_at = datetime.now(timezone.utc).isoformat()
            started_monotonic = time.monotonic()
            result = call_tool(store, tool_name, arguments, subscriptions)
            audit_configuration = observability_configuration()
            if audit_configuration.enabled:
                event = audit_tool_result(tool_name, arguments, result, request_id, started_at, started_monotonic)
                attempt_audit_write(store, event, audit_configuration.retention_days)
        elif method == "resources/list":
            result = {"resources": [{"uri": GroupSubscriptionMonitor.uri_for_group(group["group_id"]), "name": "Crosstalk " + group["group_id"], "description": "Group-change signal. A change may not be relevant to this AI; call get_unread_messages to check.", "mimeType": "application/json"} for group in store.list_groups()]}
        elif method == "resources/read":
            params = request.get("params", {})
            if not isinstance(params, dict):
                raise ValueError("resources/read params must be an object.")
            uri = params.get("uri", "")
            if GroupSubscriptionMonitor.WAKEUP_SEGMENT in uri:
                wakeup = GroupSubscriptionMonitor.wakeup_from_uri(uri)
                if not subscriptions.is_wakeup_authorized(uri):
                    raise ValueError("Wakeup resource is not authorized for this MCP session. Call join_group first.")
                snapshot = store.wakeup_snapshot(wakeup["group_id"], wakeup["context_id"])
            else:
                group_id = GroupSubscriptionMonitor.group_id_from_uri(uri)
                snapshot = store.group_update_snapshot(group_id)
            result = {"contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(snapshot)}]}
        elif method == "resources/subscribe":
            params = request.get("params", {})
            if not isinstance(params, dict):
                raise ValueError("resources/subscribe params must be an object.")
            subscriptions.subscribe(params.get("uri", ""))
            result = {}
        elif method == "resources/unsubscribe":
            params = request.get("params", {})
            if not isinstance(params, dict):
                raise ValueError("resources/unsubscribe params must be an object.")
            uri = params.get("uri", "")
            if GroupSubscriptionMonitor.WAKEUP_SEGMENT in uri:
                wakeup = GroupSubscriptionMonitor.wakeup_from_uri(uri)
                if not subscriptions.is_wakeup_authorized(uri):
                    raise ValueError("Wakeup resource is not authorized for this MCP session. Call join_group first.")
                store.leave_group_context(wakeup["group_id"], wakeup["context_id"])
                subscriptions.revoke_wakeup(uri)
            else:
                subscriptions.unsubscribe(uri)
            result = {}
        else:
            return _error_response(request_id, -32601, "Method not found: " + method)
        return {"jsonrpc": "2.0", "id": request_id, "result": result}
    except (TypeError, ValueError) as error:
        return _error_response(request_id, -32602, "Invalid params: " + str(error)) if has_response_id else None
    except Exception:
        return _error_response(request_id, -32603, "Internal server error") if has_response_id else None


def serve() -> None:
    """Run the existing stdio MCP server."""
    # Validate before opening or creating any group storage.
    observability_configuration()
    default_groups_dir = Path.home() / ".cache" / "crosstalk"
    store = CrosstalkStore(os.environ.get("CROSSTALK_GROUPS_DIR", str(default_groups_dir)))
    try:
        poll_interval = float(os.environ.get("CROSSTALK_POLL_INTERVAL_SECONDS", str(DEFAULT_POLL_INTERVAL_SECONDS)))
        if not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ValueError
    except ValueError:
        poll_interval = DEFAULT_POLL_INTERVAL_SECONDS
    subscriptions = GroupSubscriptionMonitor(store, poll_interval)
    state = {"initialized": False, "client_ready": False}
    try:
        for line in sys.stdin:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                respond_error(None, -32700, "Parse error")
                continue
            if isinstance(payload, list):
                if not payload:
                    respond_error(None, -32600, "Invalid Request")
                    continue
                if any(isinstance(item, dict) and item.get("method") == "initialize" for item in payload):
                    respond_error(None, -32600, "initialize must not be sent in a batch")
                    continue
                responses = [response for response in (_handle_request(item, store, subscriptions, state) for item in payload) if response is not None]
                if responses:
                    _write_json(responses)
                continue
            response = _handle_request(payload, store, subscriptions, state)
            if response is not None:
                _write_json(response)
    finally:
        subscriptions.stop()


if __name__ == "__main__":
    serve()
