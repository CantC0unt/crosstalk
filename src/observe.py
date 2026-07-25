"""Observer dashboard implementation boundary."""

import argparse
import html
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
import os
import sqlite3
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import sys
import webbrowser
import secrets
import queue
import threading
import time
from typing import List, Optional
from pathlib import Path
from urllib.parse import parse_qs, urlparse


DEFAULT_POLL_INTERVAL_SECONDS = 0.5


@dataclass(frozen=True)
class ObserverOptions:
    """Validated command-line configuration for the local observer."""

    silent: bool
    port: Optional[int]
    poll_interval: float
    groups_dir: Optional[str]


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("port must be an integer between 1 and 65535") from error
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be an integer between 1 and 65535")
    return port


def _poll_interval(value: str) -> float:
    try:
        interval = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("poll interval must be a finite positive number of seconds") from error
    if not math.isfinite(interval) or interval <= 0:
        raise argparse.ArgumentTypeError("poll interval must be a finite positive number of seconds")
    return interval


def parse_arguments(arguments: Optional[List[str]] = None) -> ObserverOptions:
    """Parse observer-only command-line options without starting the server."""
    parser = argparse.ArgumentParser(
        prog="crosstalk-mcp observe",
        description="Open Crosstalk's local observability dashboard.",
    )
    parser.add_argument("--silent", action="store_true", help="do not open a browser automatically")
    parser.add_argument("--port", type=_port, metavar="PORT", help="bind exactly this loopback port")
    parser.add_argument(
        "--poll-interval",
        type=_poll_interval,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        metavar="SECONDS",
        help="observer refresh interval in seconds (default: %(default)s)",
    )
    parser.add_argument("--groups-dir", metavar="PATH", help="directory containing Crosstalk group databases")
    parsed = parser.parse_args(arguments)
    return ObserverOptions(
        silent=parsed.silent,
        port=parsed.port,
        poll_interval=parsed.poll_interval,
        groups_dir=parsed.groups_dir,
    )


def resolve_groups_directory(options: ObserverOptions, environment: Optional[dict] = None) -> Path:
    """Resolve the observer's read-only groups directory without creating it."""
    environment = os.environ if environment is None else environment
    if options.groups_dir is not None:
        return Path(options.groups_dir)
    if environment.get("CROSSTALK_GROUPS_DIR"):
        return Path(environment["CROSSTALK_GROUPS_DIR"])
    return Path.home() / ".cache" / "crosstalk"


def open_read_only_database(database_path: Path) -> sqlite3.Connection:
    """Open an existing SQLite database without any ability to mutate it."""
    uri = database_path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=0.1)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


GROUP_DATABASE_PATTERN = re.compile(r"grp_[0-9a-f]{32}\.sqlite3$")
DEFAULT_MESSAGE_PAGE_SIZE = 100
MAX_MESSAGE_PAGE_SIZE = 200
OBSERVER_STATIC_ASSETS = {
    "/static/observer.css": (
        "text/css; charset=utf-8",
        b"button.danger{background:#7d3030}.danger:hover{background:#a33d3d}",
    ),
}


class _ObserverHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        request = urlparse(self.path)
        asset = OBSERVER_STATIC_ASSETS.get(request.path)
        if asset is not None:
            content_type, body = asset
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if request.path == "/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            subscriber = self.server.event_hub.subscribe()
            try:
                self.wfile.write(("event: snapshot\ndata: " + json.dumps(self.server.snapshot_provider(), separators=(",", ":")) + "\n\n").encode())
                self.wfile.flush()
                while True:
                    item = subscriber.get()
                    if item is None:
                        break
                    event, payload = item
                    self.wfile.write(("event: " + event + "\ndata: " + json.dumps(payload, separators=(",", ":")) + "\n\n").encode())
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                self.server.event_hub.unsubscribe(subscriber)
            return
        if request.path == "/":
            self._send_html(render_dashboard(self.server.groups_directory, self.server.csrf_token))
            return
        if request.path == "/api/health":
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        query = parse_qs(request.query)
        group_id = query.get("group_id", [None])[0]
        if request.path == "/fragments/chat":
            self._send_html(render_chat_panel(self.server.groups_directory, group_id))
            return
        if request.path == "/fragments/overview":
            self._send_html(render_overview(self.server.groups_directory))
            return
        if request.path == "/fragments/analytics":
            self._send_html(render_tool_analytics(self.server.groups_directory, {key: value[0] for key, value in query.items()}))
            return
        if request.path == "/fragments/storage":
            self._send_html(render_storage(self.server.groups_directory, self.server.csrf_token))
            return
        if request.path == "/fragments/messages":
            older_than = query.get("older_than_message_id", [None])[0]
            try:
                cursor = int(older_than) if older_than is not None else None
            except ValueError:
                self._send_html("<p class=\"notice error\">Invalid message cursor.</p>", 400)
                return
            self._send_html(render_message_page(self.server.groups_directory, group_id, cursor))
            return
        if request.path == "/fragments/message":
            message_id = query.get("message_id", [None])[0]
            try:
                message_id_value = int(message_id) if message_id is not None else 0
            except ValueError:
                message_id_value = 0
            self._send_html(render_visible_message(self.server.groups_directory, group_id, message_id_value), 200 if message_id_value else 400)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:
        endpoint = urlparse(self.path).path
        if endpoint not in {"/api/storage/reclaim", "/api/storage/delete-history"}:
            self.send_response(404)
            self.end_headers()
            return
        if not self.server.owner.valid_csrf_token(self.headers.get("X-CSRF-Token")):
            self._send_html('<p class="notice error">Maintenance request rejected: invalid CSRF token.</p>', 403)
            return
        if endpoint == "/api/storage/delete-history":
            if self.headers.get("X-Crosstalk-Confirm") != "DELETE AUDIT HISTORY":
                self._send_html('<p class="notice error">Audit-history deletion requires explicit confirmation.</p>', 400)
                return
            result = delete_audit_history(self.server.groups_directory)
        else:
            result = reclaim_audit_free_space(self.server.groups_directory)
        status = 200 if result["ok"] else 409
        self._send_html('<p class="notice{}">{}</p>'.format("" if result["ok"] else " error", html.escape(result["message"])), status)

    def _send_html(self, page: str, status: int = 200) -> None:
        body = page.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


class ObserverHTTPServer:
    """Loopback-only HTTP server with graceful shutdown."""

    def __init__(self, port: int, poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS, groups_directory: Optional[Path] = None) -> None:
        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), _ObserverHandler)
        self.httpd.daemon_threads = True
        self.csrf_token = secrets.token_urlsafe(32)
        self.httpd.csrf_token = self.csrf_token
        self.httpd.owner = self
        self.event_hub = EventHub()
        self.httpd.event_hub = self.event_hub
        self.httpd.snapshot_provider = lambda: observer_snapshot(groups_directory) if groups_directory else {"groups": {}, "latest_audit_id": None}
        self.httpd.groups_directory = groups_directory
        self.poll_interval = poll_interval
        self.poller = ObserverPoller(groups_directory, poll_interval, self.event_hub) if groups_directory else None
        self._poller_thread: Optional[threading.Thread] = None

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    def serve_forever(self) -> None:
        try:
            if self.poller is not None:
                # Establish a baseline before clients can subscribe, so existing
                # records are represented by their snapshot rather than live events.
                self.poller.poll_once()
                self._poller_thread = threading.Thread(target=self.poller.run, daemon=True)
                self._poller_thread.start()
            self.httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            if self.poller is not None:
                self.poller.stop()
            if self._poller_thread is not None:
                self._poller_thread.join(timeout=max(1.0, self.poll_interval * 2))
            self.event_hub.close()
            self.httpd.server_close()

    def valid_csrf_token(self, token: Optional[str]) -> bool:
        return isinstance(token, str) and secrets.compare_digest(token, self.csrf_token)


class EventHub:
    def __init__(self) -> None:
        self.subscribers: List[queue.Queue] = []
        self._lock = threading.Lock()
        self._closed = False

    def subscribe(self) -> queue.Queue:
        subscriber: queue.Queue = queue.Queue(maxsize=32)
        with self._lock:
            if self._closed:
                subscriber.put_nowait(None)
            else:
                self.subscribers.append(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: queue.Queue) -> None:
        with self._lock:
            if subscriber in self.subscribers:
                self.subscribers.remove(subscriber)

    def publish(self, event: str, payload: dict) -> None:
        with self._lock:
            if self._closed:
                return
            subscribers = list(self.subscribers)
        for subscriber in subscribers:
            try:
                subscriber.put_nowait((event, payload))
            except queue.Full:
                self.unsubscribe(subscriber)

    def close(self) -> None:
        """Wake SSE handlers so server shutdown does not wait for browsers."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            subscribers = list(self.subscribers)
            self.subscribers.clear()
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(None)
            except queue.Full:
                try:
                    subscriber.get_nowait()
                except queue.Empty:
                    pass
                try:
                    subscriber.put_nowait(None)
                except queue.Full:
                    pass


class ObserverPoller:
    """Poll compact group markers and publish only changed state."""
    def __init__(self, groups_directory: Path, interval: float, event_hub: EventHub) -> None:
        self.groups_directory, self.interval, self.event_hub = groups_directory, interval, event_hub
        self._markers: dict = {}
        self._latest_audit_id: Optional[int] = None
        self._audit_marker_known = False
        self._stopped = threading.Event()

    def poll_once(self) -> None:
        group_ids = discover_groups(self.groups_directory)
        for group_id in group_ids:
            try:
                marker = group_fingerprint(self.groups_directory, group_id)
            except (OSError, sqlite3.Error, ValueError):
                continue
            previous = self._markers.get(group_id)
            self._markers[group_id] = marker
            if previous is not None and marker[0] != previous[0]:
                self.event_hub.publish("message.created", {"group_id": group_id, "message_id": marker[0]})
            if previous is not None and marker[1] != previous[1]:
                self.event_hub.publish("group.changed", {"group_id": group_id})
            if previous is not None and marker[2] != previous[2]:
                self.event_hub.publish("member.changed", {"group_id": group_id})
            if previous is not None and marker[3] != previous[3]:
                self.event_hub.publish("wakeup.changed", {"group_id": group_id})
        # A missing directory can be transient while storage is mounted or
        # replaced. Only treat a missing individual file as group deletion.
        if self.groups_directory.is_dir():
            for group_id in set(self._markers) - set(group_ids):
                del self._markers[group_id]
                self.event_hub.publish("group.deleted", {"group_id": group_id})
        try:
            latest_audit_id = latest_audit_id_for_directory(self.groups_directory)
        except (OSError, sqlite3.Error, ValueError):
            return
        if self._audit_marker_known and latest_audit_id is not None and latest_audit_id != self._latest_audit_id:
            self.event_hub.publish("tool_call.completed", {"tool_call_id": latest_audit_id})
        self._latest_audit_id = latest_audit_id
        self._audit_marker_known = True

    def run(self) -> None:
        while not self._stopped.wait(self.interval):
            self.poll_once()

    def stop(self) -> None:
        self._stopped.set()


def create_observer_server(port: Optional[int], poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS, groups_directory: Optional[Path] = None) -> ObserverHTTPServer:
    """Bind an explicit port, or prefer 8787 before an OS-selected fallback."""
    if port is not None:
        server = ObserverHTTPServer(port, poll_interval, groups_directory)
        server.used_default_port_fallback = False
        return server
    try:
        server = ObserverHTTPServer(8787, poll_interval, groups_directory)
        server.used_default_port_fallback = False
        return server
    except OSError:
        server = ObserverHTTPServer(0, poll_interval, groups_directory)
        server.used_default_port_fallback = True
        return server


def discover_groups(groups_directory: Path) -> List[str]:
    """Return valid group IDs without creating a missing directory."""
    if not groups_directory.is_dir():
        return []
    return sorted(path.name[:-8] for path in groups_directory.iterdir() if path.is_file() and GROUP_DATABASE_PATTERN.fullmatch(path.name))


def _valid_group_id(group_id: Optional[str]) -> bool:
    return isinstance(group_id, str) and GROUP_DATABASE_PATTERN.fullmatch(group_id + ".sqlite3") is not None


def _group_database_path(groups_directory: Path, group_id: Optional[str]) -> Path:
    if not _valid_group_id(group_id):
        raise ValueError("Invalid group ID.")
    return groups_directory / (group_id + ".sqlite3")


def read_group_snapshot(groups_directory: Path, group_id: str) -> dict:
    """Read one group database without changing unread or wakeup state."""
    path = _group_database_path(groups_directory, group_id)
    connection = open_read_only_database(path)
    try:
        metadata = connection.execute("SELECT name, description, creator_context_id, created_at, updated_at FROM group_metadata WHERE id = 1").fetchone()
        members = [dict(row) | {"unread_count": len(json.loads(row["unread_message_ids"]))} for row in connection.execute("SELECT context_id, name, joined_at, unread_message_ids FROM group_members ORDER BY joined_at")]
        latest = connection.execute("SELECT id, created_at FROM messages ORDER BY id DESC LIMIT 1").fetchone()
        wakeups = [dict(row) for row in connection.execute("SELECT message_id, context_id, relevance, priority, created_at, acknowledged_at, last_notified_at FROM wakeup_events ORDER BY id")]
        return {"group_id": group_id, "metadata": dict(metadata) if metadata else None, "members": members, "latest_message_id": latest["id"] if latest else None, "latest_activity_at": latest["created_at"] if latest else None, "wakeups": wakeups}
    finally:
        connection.close()


def read_message_page(groups_directory: Path, group_id: str, older_than_message_id: Optional[int] = None, limit: int = DEFAULT_MESSAGE_PAGE_SIZE) -> dict:
    """Return a bounded chronological page, optionally before a message cursor."""
    if not isinstance(limit, int) or limit < 1 or limit > MAX_MESSAGE_PAGE_SIZE:
        raise ValueError("message page limit must be between 1 and " + str(MAX_MESSAGE_PAGE_SIZE))
    if older_than_message_id is not None and (not isinstance(older_than_message_id, int) or older_than_message_id < 1):
        raise ValueError("older message cursor must be a positive integer")
    connection = open_read_only_database(_group_database_path(groups_directory, group_id))
    try:
        if older_than_message_id is None:
            rows = connection.execute("SELECT id, sender_context_id, sender_name, content, created_at, priority, mentions, wakeup_targets, routing_reason FROM messages ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        else:
            rows = connection.execute("SELECT id, sender_context_id, sender_name, content, created_at, priority, mentions, wakeup_targets, routing_reason FROM messages WHERE id < ? ORDER BY id DESC LIMIT ?", (older_than_message_id, limit)).fetchall()
        messages = [dict(row) for row in reversed(rows)]
        return {"messages": messages, "next_older_message_id": messages[0]["id"] if len(rows) == limit else None}
    finally:
        connection.close()


def read_audit_metadata(groups_directory: Path) -> Optional[dict]:
    path = groups_directory / "observability.sqlite3"
    if not path.is_file():
        return None
    connection = open_read_only_database(path)
    try:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(metadata)")}
        selected = ["schema_version", "created_at", "audit_enabled_at", "last_retention_cleanup_at"]
        if "retention_setting" in columns:
            selected.append("retention_setting")
        row = connection.execute("SELECT " + ", ".join(selected) + " FROM metadata WHERE id = 1").fetchone()
        metadata = dict(row) if row else None
        if metadata is not None:
            metadata.setdefault("retention_setting", None)
        return metadata
    finally:
        connection.close()


def read_tool_calls(groups_directory: Path, limit: int = 100) -> List[dict]:
    """Read raw recent audit rows; analytics remain derived from raw events."""
    if not 1 <= limit <= MAX_MESSAGE_PAGE_SIZE:
        raise ValueError("tool call limit must be between 1 and " + str(MAX_MESSAGE_PAGE_SIZE))
    connection = open_read_only_database(groups_directory / "observability.sqlite3")
    try:
        rows = [dict(row) for row in connection.execute("SELECT * FROM tool_calls ORDER BY id DESC LIMIT ?", (limit,))]
        for row in rows:
            group_id = row.get("group_id")
            row["group_deleted"] = bool(group_id) and not (groups_directory / (group_id + ".sqlite3")).is_file()
        return rows
    finally:
        connection.close()


def read_tool_duration_values(groups_directory: Path) -> List[int]:
    connection = open_read_only_database(groups_directory / "observability.sqlite3")
    try:
        return [row[0] for row in connection.execute("SELECT duration_ms FROM tool_calls ORDER BY occurred_at")]
    finally:
        connection.close()


def latest_audit_id_for_directory(groups_directory: Path) -> Optional[int]:
    """Return the latest completed audit row without creating audit storage."""
    path = groups_directory / "observability.sqlite3"
    if not path.is_file():
        return None
    connection = open_read_only_database(path)
    try:
        row = connection.execute("SELECT MAX(id) FROM tool_calls").fetchone()
        return row[0] if row is not None else None
    finally:
        connection.close()


def group_fingerprint(groups_directory: Path, group_id: str) -> tuple:
    """Compact deterministic state marker for observer polling."""
    snapshot = read_group_snapshot(groups_directory, group_id)
    members = tuple((member["context_id"], member["unread_count"]) for member in snapshot["members"])
    wakeups = tuple((item["message_id"], item["context_id"], item["acknowledged_at"], item["last_notified_at"]) for item in snapshot["wakeups"])
    metadata = snapshot["metadata"] or {}
    return (snapshot["latest_message_id"], metadata.get("updated_at"), members, wakeups)


def observer_snapshot(groups_directory: Path) -> dict:
    groups = {}
    for group_id in discover_groups(groups_directory):
        try:
            marker = group_fingerprint(groups_directory, group_id)
            groups[group_id] = {"latest_message_id": marker[0], "member_fingerprint": repr(marker[2]), "wakeup_fingerprint": repr(marker[3])}
        except (OSError, sqlite3.Error, ValueError):
            pass
    try:
        latest_audit_id = latest_audit_id_for_directory(groups_directory)
    except (OSError, sqlite3.Error, ValueError):
        latest_audit_id = None
    return {"groups": groups, "latest_audit_id": latest_audit_id}


def _message_html(message: dict) -> str:
    """Render a single message without trusting any group database text as HTML."""
    return (
        '<article class="message" data-message-id="{id}"><header><strong>{sender}</strong>'
        '<time>{created}</time></header><p>{content}</p><footer>priority: {priority} · routing: {routing}</footer></article>'
    ).format(
        id=message["id"],
        sender=html.escape(str(message.get("sender_name") or message.get("sender_context_id") or "Unknown")),
        created=html.escape(format_timestamp(message.get("created_at"), "")),
        content=html.escape(str(message.get("content") or "")),
        priority=html.escape(str(message.get("priority") or "normal")),
        routing=html.escape(str(message.get("routing_reason") or "normal")),
    )


def format_timestamp(value: object, empty: str = "—", local_timezone: Optional[timezone] = None) -> str:
    """Render an ISO timestamp in the observer machine's local time zone."""
    if not value:
        return empty
    try:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return str(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    local = timestamp.astimezone(local_timezone) if local_timezone is not None else timestamp.astimezone()
    return local.strftime("%Y-%m-%d %H:%M:%S %Z (%z)")


def group_display_name(groups_directory: Optional[Path], group_id: Optional[str]) -> str:
    """Return a human-readable group name while preserving the ID for routing."""
    if groups_directory is None or not _valid_group_id(group_id):
        return str(group_id or "—")
    try:
        metadata = read_group_snapshot(groups_directory, group_id)["metadata"] or {}
    except (OSError, sqlite3.Error, ValueError):
        return group_id
    return str(metadata.get("name") or group_id)


def render_message_page(groups_directory: Optional[Path], group_id: Optional[str], older_than_message_id: Optional[int] = None) -> str:
    if groups_directory is None or not _valid_group_id(group_id):
        return '<p class="notice error">Choose a valid group.</p>'
    try:
        page = read_message_page(groups_directory, group_id, older_than_message_id)
    except (OSError, sqlite3.Error, ValueError):
        return '<p class="notice error">Messages are temporarily unavailable; retry shortly.</p>'
    messages = "".join(_message_html(message) for message in page["messages"])
    older = page["next_older_message_id"]
    load_older = ""
    if older is not None:
        load_older = (
            '<button class="load-older" hx-get="/fragments/messages?group_id={group}&amp;older_than_message_id={cursor}" '
            'hx-target="this" hx-swap="outerHTML">Load older messages</button>'
        ).format(group=html.escape(group_id), cursor=older)
    return load_older + messages or '<p class="notice">No messages in this group yet.</p>'


def render_visible_message(groups_directory: Optional[Path], group_id: Optional[str], message_id: int) -> str:
    if groups_directory is None or not _valid_group_id(group_id) or message_id < 1:
        return '<p class="notice error">Message is unavailable.</p>'
    try:
        connection = open_read_only_database(_group_database_path(groups_directory, group_id))
        try:
            row = connection.execute(
                "SELECT id, sender_context_id, sender_name, content, created_at, priority, routing_reason FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
        finally:
            connection.close()
    except (OSError, sqlite3.Error, ValueError):
        return '<p class="notice error">Message is temporarily unavailable.</p>'
    return _message_html(dict(row)) if row is not None else '<p class="notice">Message is no longer available.</p>'


def render_chat_panel(groups_directory: Optional[Path], group_id: Optional[str]) -> str:
    if groups_directory is None or not _valid_group_id(group_id):
        return '<section class="empty"><h2>Choose a chat</h2><p>Select a group to view its read-only history.</p></section>'
    try:
        snapshot = read_group_snapshot(groups_directory, group_id)
    except (OSError, sqlite3.Error, ValueError):
        return '<section class="empty"><h2>Chat unavailable</h2><p>The group may be deleted or temporarily locked.</p></section>'
    metadata = snapshot["metadata"] or {}
    members = "".join(
        '<li>{name}<small>{unread} unread</small></li>'.format(
            name=html.escape(str(member.get("name") or member["context_id"])), unread=member["unread_count"]
        ) for member in snapshot["members"]
    ) or "<li>No members</li>"
    wakeups = "".join(
        '<li>message #{message} → {context}<small>{priority}/{relevance} · created {created} · acknowledged {acknowledged} · last notified {notified}</small></li>'.format(
            message=html.escape(str(wakeup.get("message_id") or "—")),
            context=html.escape(str(wakeup.get("context_id") or "—")),
            priority=html.escape(str(wakeup.get("priority") or "normal")),
            relevance=html.escape(str(wakeup.get("relevance") or "")),
            created=html.escape(format_timestamp(wakeup.get("created_at"))),
            acknowledged=html.escape(format_timestamp(wakeup.get("acknowledged_at"), "pending")),
            notified=html.escape(format_timestamp(wakeup.get("last_notified_at"), "never")),
        ) for wakeup in snapshot["wakeups"]
    ) or "<li>No wakeup events.</li>"
    return (
        '<section class="chat" data-group-id="{group}"><header><h2>{name}</h2><p>{description}</p></header>'
        '<aside><h3>Members</h3><ul>{members}</ul><h3>Wakeups</h3><ul>{wakeups}</ul></aside><div id="message-list" class="messages">{messages}</div></section>'
    ).format(
        group=html.escape(group_id), name=html.escape(str(metadata.get("name") or group_id)),
        description=html.escape(str(metadata.get("description") or "")), members=members,
        wakeups=wakeups, messages=render_message_page(groups_directory, group_id),
    )


def overview_data(groups_directory: Optional[Path]) -> dict:
    """Derive small, current overview metrics directly from read-only databases."""
    totals = {"groups": 0, "messages": 0, "members": 0, "unread": 0, "pending_wakeups": 0, "wakeup_response": "—", "acknowledged_wakeups": 0, "tool_calls": "—", "error_rate": "—"}
    priorities: dict = {}
    routing: dict = {}
    activity: List[dict] = []
    contexts = set()
    wakeup_response_seconds: List[float] = []
    if groups_directory is None:
        return {"totals": totals, "priorities": priorities, "routing": routing, "activity": activity, "audit": None}
    for group_id in discover_groups(groups_directory):
        try:
            snapshot = read_group_snapshot(groups_directory, group_id)
            totals["groups"] += 1
            totals["members"] += len(snapshot["members"])
            totals["unread"] += sum(member["unread_count"] for member in snapshot["members"])
            totals["pending_wakeups"] += sum(1 for wakeup in snapshot["wakeups"] if wakeup.get("acknowledged_at") is None)
            for wakeup in snapshot["wakeups"]:
                if not wakeup.get("created_at") or not wakeup.get("acknowledged_at"):
                    continue
                try:
                    response = (datetime.fromisoformat(wakeup["acknowledged_at"]) - datetime.fromisoformat(wakeup["created_at"])).total_seconds()
                except ValueError:
                    continue
                wakeup_response_seconds.append(max(0, response))
            contexts.update(member["context_id"] for member in snapshot["members"])
            if snapshot["latest_message_id"] is not None:
                activity.append({"group_id": group_id, "group_name": str((snapshot["metadata"] or {}).get("name") or group_id), "created_at": snapshot["latest_activity_at"], "message_id": snapshot["latest_message_id"]})
            connection = open_read_only_database(_group_database_path(groups_directory, group_id))
            try:
                for priority, count in connection.execute("SELECT COALESCE(priority, 'normal'), COUNT(*) FROM messages GROUP BY priority"):
                    priorities[priority] = priorities.get(priority, 0) + count
                    totals["messages"] += count
                for reason, count in connection.execute("SELECT COALESCE(routing_reason, 'normal'), COUNT(*) FROM messages GROUP BY routing_reason"):
                    routing[reason] = routing.get(reason, 0) + count
            finally:
                connection.close()
        except (OSError, sqlite3.Error, ValueError):
            continue
    totals["contexts"] = len(contexts)
    totals["wakeup_response"] = "—" if not wakeup_response_seconds else "{:.1f}s avg".format(sum(wakeup_response_seconds) / len(wakeup_response_seconds))
    totals["acknowledged_wakeups"] = len(wakeup_response_seconds)
    activity.sort(key=lambda item: item["created_at"] or "", reverse=True)
    try:
        audit = read_audit_metadata(groups_directory)
    except (OSError, sqlite3.Error, ValueError):
        audit = None
    if audit is not None:
        try:
            connection = open_read_only_database(groups_directory / "observability.sqlite3")
            try:
                call_count, error_count = connection.execute("SELECT COUNT(*), COALESCE(SUM(outcome = 'error'), 0) FROM tool_calls").fetchone()
            finally:
                connection.close()
            totals["tool_calls"] = call_count
            totals["error_rate"] = "{:.1f}%".format(error_count * 100 / call_count) if call_count else "0.0%"
        except (OSError, sqlite3.Error, ValueError):
            pass
    return {"totals": totals, "priorities": priorities, "routing": routing, "activity": activity[:10], "audit": audit}


def render_overview(groups_directory: Optional[Path]) -> str:
    data = overview_data(groups_directory)
    totals = data["totals"]
    metric_labels = (("Groups", "groups"), ("Messages", "messages"), ("Tool calls", "tool_calls"), ("Error rate", "error_rate"), ("Active contexts", "contexts"), ("Unread", "unread"), ("Pending wakeups", "pending_wakeups"), ("Wakeup response", "wakeup_response"))
    metrics = "".join('<article class="metric"><strong>{}</strong><span>{}</span></article>'.format(html.escape(label), totals[key]) for label, key in metric_labels)
    def breakdown(values: dict) -> str:
        return "".join('<li>{}: <strong>{}</strong></li>'.format(html.escape(str(key)), value) for key, value in sorted(values.items())) or "<li>None yet</li>"
    activity = "".join('<li><strong>{}</strong> · message #{} <small>{}</small></li>'.format(html.escape(item["group_name"]), item["message_id"], html.escape(format_timestamp(item["created_at"], ""))) for item in data["activity"]) or "<li>No recent activity.</li>"
    audit_notice = '<p class="notice error">Audit analytics are disabled. Set <code>CROSSTALK_OBSERVABILITY_RETENTION_DAYS=inf</code> (or a positive number of days) before starting Crosstalk.</p>' if data["audit"] is None else '<p class="notice">Audit active since {}.</p>'.format(html.escape(format_timestamp(data["audit"].get("audit_enabled_at"), "")))
    responsiveness = "No acknowledged wakeups yet." if not totals["acknowledged_wakeups"] else "{} acknowledged wakeup{} included in the average.".format(totals["acknowledged_wakeups"], "" if totals["acknowledged_wakeups"] == 1 else "s")
    return '<section class="overview" data-overview="true"><header><h2>Overview</h2>{}</header><div class="metrics">{}</div><div class="overview-columns"><section><h3>Recent activity</h3><ul>{}</ul></section><section><h3>Message priority</h3><ul>{}</ul><h3>Routing</h3><ul>{}</ul><h3>Wakeup responsiveness</h3><p class="notice">{}</p></section></div></section>'.format(audit_notice, metrics, activity, breakdown(data["priorities"]), breakdown(data["routing"]), responsiveness)


ANALYTICS_FILTERS = ("from", "to", "group_id", "context_id", "name", "tool_name", "outcome")


def read_tool_analytics(groups_directory: Optional[Path], filters: Optional[dict] = None) -> dict:
    """Filter raw audit calls and derive dashboard metrics without aggregate tables."""
    filters = filters or {}
    if groups_directory is None or not (groups_directory / "observability.sqlite3").is_file():
        return {"available": False, "rows": [], "by_tool": {}, "by_time": {}, "durations": []}
    clauses, values = [], []
    for field in ANALYTICS_FILTERS:
        value = filters.get(field)
        if not value:
            continue
        column = "occurred_at" if field in {"from", "to"} else field
        operator = ">=" if field == "from" else "<=" if field == "to" else "="
        clauses.append(column + " " + operator + " ?")
        values.append(value)
    query = "SELECT id, occurred_at, tool_name, group_id, context_id, name, outcome, duration_ms, error_category FROM tool_calls"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY occurred_at DESC LIMIT 1000"
    try:
        connection = open_read_only_database(groups_directory / "observability.sqlite3")
        try:
            rows = [dict(row) for row in connection.execute(query, values)]
        finally:
            connection.close()
    except (OSError, sqlite3.Error, ValueError):
        return {"available": False, "rows": [], "by_tool": {}, "by_time": {}, "durations": []}
    by_tool: dict = {}
    by_time: dict = {}
    for row in rows:
        group_id = row.get("group_id")
        row["group_deleted"] = bool(group_id) and not (groups_directory / (group_id + ".sqlite3")).is_file()
        item = by_tool.setdefault(row["tool_name"], {"count": 0, "errors": 0})
        item["count"] += 1
        item["errors"] += row["outcome"] == "error"
        bucket = row["occurred_at"][:13] + ":00Z"
        by_time[bucket] = by_time.get(bucket, 0) + 1
    return {"available": True, "rows": rows, "by_tool": by_tool, "by_time": by_time, "durations": [row["duration_ms"] for row in rows]}


def _percentile(values: List[int], percentile: float) -> Optional[int]:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * percentile) - 1))
    return ordered[index]


def _analytics_chart(values: dict, label: str) -> str:
    if not values:
        return '<p class="notice">No calls match this range.</p>'
    maximum = max(item["count"] if isinstance(item, dict) else item for item in values.values())
    bars, labels = [], []
    for index, (tool, item) in enumerate(sorted(values.items())):
        count = item["count"] if isinstance(item, dict) else item
        height = max(2, round(count / maximum * 100))
        x = 20 + index * 65
        bars.append('<rect x="{}" y="{}" width="38" height="{}" class="bar"><title>{}: {} calls</title></rect>'.format(x, 120 - height, height, html.escape(tool), count))
        labels.append('<text x="{}" y="138" text-anchor="middle">{}</text>'.format(x + 19, html.escape(tool[:9])))
    width = max(180, 25 + len(values) * 65)
    return '<svg class="chart" viewBox="0 0 {} 150" role="img" aria-label="{}">{}{}<line x1="12" y1="120" x2="{}" y2="120"/></svg>'.format(width, html.escape(label), "".join(bars), "".join(labels), width - 8)


def render_tool_analytics(groups_directory: Optional[Path], filters: Optional[dict] = None) -> str:
    filters = dict(filters or {})
    if not filters.get("from"):
        filters["from"] = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    data = read_tool_analytics(groups_directory, filters)
    fields = "".join('<label>{}<input name="{}" value="{}"></label>'.format(html.escape(field), html.escape(field), html.escape(str(filters.get(field, "")))) for field in ANALYTICS_FILTERS)
    form = '<form hx-get="/fragments/analytics" hx-target="#chat-panel" hx-swap="innerHTML"><h2>Tool analytics</h2><div class="filters">{}<button type="submit">Apply filters</button></div></form>'.format(fields)
    if not data["available"]:
        return '<section class="analytics" data-analytics="true">{}<p class="notice error">Audit data is unavailable. Enable auditing to begin collecting tool analytics.</p></section>'.format(form)
    p50, p95 = _percentile(data["durations"], .50), _percentile(data["durations"], .95)
    summary = '<div class="metrics"><article class="metric"><strong>Calls</strong><span>{}</span></article><article class="metric"><strong>p50 latency</strong><span>{} ms</span></article><article class="metric"><strong>p95 latency</strong><span>{} ms</span></article></div>'.format(len(data["rows"]), p50 if p50 is not None else "—", p95 if p95 is not None else "—")
    rows = "".join('<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{} ms</td></tr>'.format(html.escape(format_timestamp(row["occurred_at"])), html.escape(row["tool_name"]), html.escape(str(row["name"] or "—")), html.escape(row["outcome"]), html.escape((str(row["group_id"]) + " (deleted)") if row["group_deleted"] else group_display_name(groups_directory, row["group_id"])), row["duration_ms"]) for row in data["rows"][:100]) or "<tr><td colspan=\"6\">No calls match this range.</td></tr>"
    displayed_by_time = {format_timestamp(bucket, ""): count for bucket, count in data["by_time"].items()}
    return '<section class="analytics" data-analytics="true">{}{}<h3>Calls by tool</h3>{}<h3>Calls over time</h3>{}<table><thead><tr><th>When</th><th>Tool</th><th>Caller</th><th>Outcome</th><th>Group</th><th>Duration</th></tr></thead><tbody>{}</tbody></table></section>'.format(form, summary, _analytics_chart(data["by_tool"], "Calls by tool"), _analytics_chart(displayed_by_time, "Calls over time"), rows)


def audit_storage_status(groups_directory: Optional[Path], environment: Optional[dict] = None) -> dict:
    """Read audit storage state without creating or modifying the database."""
    status = {"available": False, "retention": "unavailable", "size_bytes": 0, "row_count": 0, "reclaimable_bytes": 0, "metadata": None}
    if groups_directory is None:
        return status
    path = groups_directory / "observability.sqlite3"
    if not path.is_file():
        return status
    try:
        connection = open_read_only_database(path)
        try:
            status["row_count"] = connection.execute("SELECT COUNT(*) FROM tool_calls").fetchone()[0]
            page_size = connection.execute("PRAGMA page_size").fetchone()[0]
            free_pages = connection.execute("PRAGMA freelist_count").fetchone()[0]
            status["reclaimable_bytes"] = page_size * free_pages
        finally:
            connection.close()
        status["metadata"] = read_audit_metadata(groups_directory)
        status["retention"] = (status["metadata"] or {}).get("retention_setting") or "unknown (legacy audit database)"
        status["size_bytes"] = path.stat().st_size
        status["available"] = True
    except (OSError, sqlite3.Error, ValueError):
        status["error"] = "Audit storage is temporarily unavailable; retry shortly."
    return status


def _bytes(value: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            return "{} {}".format(value if unit == "B" else round(value, 1), unit)
        value /= 1024
    return str(value)


def reclaim_audit_free_space(groups_directory: Optional[Path], pages: int = 100) -> dict:
    """Run one bounded incremental-vacuum step against audit storage only."""
    if groups_directory is None or not isinstance(pages, int) or not 1 <= pages <= 1000:
        return {"ok": False, "message": "Audit storage is unavailable."}
    path = groups_directory / "observability.sqlite3"
    if not path.is_file():
        return {"ok": False, "message": "No audit database exists yet."}
    try:
        connection = sqlite3.connect(str(path), timeout=0.1)
        try:
            connection.execute("PRAGMA busy_timeout=100")
            # This is deliberately bounded and does not delete retained history.
            connection.execute("PRAGMA incremental_vacuum(" + str(pages) + ")")
            connection.commit()
        finally:
            connection.close()
    except (OSError, sqlite3.Error):
        return {"ok": False, "message": "Audit storage is busy; retry later."}
    return {"ok": True, "message": "Reclaimed available audit free pages."}


def delete_audit_history(groups_directory: Optional[Path]) -> dict:
    """Immediately delete audit rows only; reclaiming freed pages remains separate."""
    if groups_directory is None:
        return {"ok": False, "message": "Audit storage is unavailable."}
    path = groups_directory / "observability.sqlite3"
    if not path.is_file():
        return {"ok": False, "message": "No audit database exists yet."}
    try:
        connection = sqlite3.connect(str(path), timeout=0.1)
        try:
            connection.execute("PRAGMA busy_timeout=100")
            deleted = connection.execute("DELETE FROM tool_calls").rowcount
            connection.commit()
        finally:
            connection.close()
    except (OSError, sqlite3.Error):
        return {"ok": False, "message": "Audit storage is busy; retry later."}
    return {"ok": True, "message": "Deleted {} audit-history rows. Reclaim free space separately if needed.".format(deleted)}


def render_storage(groups_directory: Optional[Path], csrf_token: Optional[str] = None) -> str:
    status = audit_storage_status(groups_directory)
    if not status["available"]:
        message = status.get("error") or "No audit database exists yet. Enable auditing and make a tool call to create it."
        return '<section class="storage" data-storage="true"><h2>Storage</h2><p class="notice error">{}</p></section>'.format(html.escape(message))
    metadata = status["metadata"] or {}
    values = (("Audit file", _bytes(status["size_bytes"])), ("Audit rows", str(status["row_count"])), ("Reclaimable", _bytes(status["reclaimable_bytes"])), ("Retention", str(status["retention"])), ("Activated", format_timestamp(metadata.get("audit_enabled_at"))), ("Last cleanup", format_timestamp(metadata.get("last_retention_cleanup_at"), "Never")))
    details = "".join('<dt>{}</dt><dd>{}</dd>'.format(html.escape(label), html.escape(value)) for label, value in values)
    control = '<p class="notice">Maintenance controls are available after the next update.</p>'
    if csrf_token is not None:
        token = html.escape(csrf_token, quote=True)
        control = '<button hx-post="/api/storage/reclaim" hx-target="#maintenance-status" hx-swap="innerHTML" hx-headers=\'{"X-CSRF-Token":"%s"}\'>Reclaim free space</button><button class="danger" hx-post="/api/storage/delete-history" hx-target="#maintenance-status" hx-swap="innerHTML" hx-confirm="Permanently delete all audit history? This cannot be undone." hx-headers=\'{"X-CSRF-Token":"%s","X-Crosstalk-Confirm":"DELETE AUDIT HISTORY"}\'>Delete audit history</button><div id="maintenance-status"></div>' % (token, token)
    return '<section class="storage" data-storage="true"><h2>Storage</h2><p class="notice">Audit storage is separate from group databases. Reclaiming free pages never deletes retained audit history.</p><dl class="storage-details">{}</dl>{}</section>'.format(details, control)


def render_dashboard(groups_directory: Optional[Path], csrf_token: Optional[str] = None) -> str:
    groups = discover_groups(groups_directory) if groups_directory is not None else []
    selected = groups[0] if groups else None
    picker = "".join(
        '<button hx-get="/fragments/chat?group_id={id}" hx-target="#chat-panel" hx-swap="innerHTML" data-group-id="{id}" title="{id}">{name}</button>'.format(id=html.escape(group_id), name=html.escape(group_display_name(groups_directory, group_id)))
        for group_id in groups
    ) or '<p class="notice">No groups found. This page will update when a group database appears.</p>'
    return """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crosstalk observer</title>
<link rel="stylesheet" href="/static/observer.css">
<script src="https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js" integrity="sha384-HGfztofotfshcF7+8n44JQL2oJmowVChPTg48S+jvZoztPfvwD79OC/LTtG6dMp+" crossorigin="anonymous"></script>
<script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.14.8/dist/cdn.min.js" integrity="sha384-X9kJyAubVxnP0hcA+AMMs21U445qsnqhnUF8EBlEpP3a42Kh/JwWjlv2ZcvGfphb" crossorigin="anonymous"></script>
<style>body{margin:0;background:#10151f;color:#e7edf7;font:16px system-ui,sans-serif}main{max-width:1200px;margin:auto;padding:24px}.layout{display:grid;grid-template-columns:260px 1fr;gap:24px}button{display:block;width:100%;margin:6px 0;padding:9px;border:0;border-radius:6px;background:#26364d;color:inherit;text-align:left}.message{border-left:3px solid #4ba3ff;padding:10px 14px;margin:10px 0;background:#172130;border-radius:5px}.message header,.message footer{display:flex;justify-content:space-between;color:#aebdce;font-size:.84em}.message p{white-space:pre-wrap}.notice{color:#aebdce}.error{color:#ffaba0}.chat{position:relative}.chat aside{float:right;width:180px;background:#172130;padding:12px}.chat aside ul,.overview ul{padding:0;list-style:none}.chat aside small,.overview small{display:block;color:#aebdce}.metrics{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}.metric{padding:14px;background:#172130;border-radius:6px}.metric strong,.metric span{display:block}.metric span{font-size:1.5em}.overview-columns{display:grid;grid-template-columns:1fr 1fr;gap:24px}.filters{display:grid;grid-template-columns:repeat(3,1fr);gap:9px}.filters input{display:block;width:95%;background:#172130;color:inherit;border:1px solid #52647d;padding:6px}.chart{max-width:100%;background:#172130}.chart .bar{fill:#4ba3ff}.chart text{fill:#e7edf7;font-size:8px}.chart line{stroke:#aebdce}.analytics table{width:100%;border-collapse:collapse}.analytics td,.analytics th{padding:6px;text-align:left;border-bottom:1px solid #26364d}.storage-details{display:grid;grid-template-columns:160px 1fr;gap:10px}.storage-details dt{color:#aebdce}.storage-details dd{margin:0}@media(max-width:700px){.layout,.overview-columns,.filters{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(2,1fr)}.chat aside{float:none;width:auto}}</style></head>
<body><main x-data><header><h1>Crosstalk observer</h1><p>Local, read-only chat monitoring <span id="activity-indicator"></span></p></header><div class="layout"><nav aria-label="Groups"><button hx-get="/fragments/overview" hx-target="#chat-panel" hx-swap="innerHTML">Overview</button><button hx-get="/fragments/analytics" hx-target="#chat-panel" hx-swap="innerHTML">Tool analytics</button><button hx-get="/fragments/storage" hx-target="#chat-panel" hx-swap="innerHTML">Storage</button><h2>Chats</h2>__PICKER__</nav><div id="chat-panel">__PANEL__</div></div></main>
<script>(function(){function prune(list,fromStart){while(list&&list.querySelectorAll('.message').length>200){var messages=list.querySelectorAll('.message');messages[fromStart?0:messages.length-1].remove()}}function refreshOverview(){var panel=document.getElementById('chat-panel');if(panel.querySelector('[data-overview]'))fetch('/fragments/overview').then(function(r){return r.text()}).then(function(v){panel.innerHTML=v})}function refreshAnalytics(){var panel=document.getElementById('chat-panel');if(panel.querySelector('[data-analytics]'))fetch('/fragments/analytics').then(function(r){return r.text()}).then(function(v){panel.innerHTML=v})}function refreshOpenChat(group){var panel=document.getElementById('chat-panel'),chat=panel.querySelector('[data-group-id]');if(chat&&chat.dataset.groupId===group)fetch('/fragments/chat?group_id='+encodeURIComponent(group)).then(function(r){return r.text()}).then(function(v){panel.innerHTML=v})}document.body.addEventListener('htmx:afterSettle',function(){prune(document.getElementById('message-list'),false)});var source=new EventSource('/events');source.addEventListener('snapshot',function(e){var d=JSON.parse(e.data),chat=document.querySelector('[data-group-id]');if(chat)refreshOpenChat(chat.dataset.groupId);refreshOverview();refreshAnalytics()});source.addEventListener('message.created',function(e){var d=JSON.parse(e.data),chat=document.querySelector('[data-group-id]');if(!chat||chat.dataset.groupId!==d.group_id){document.getElementById('activity-indicator').textContent='New activity';refreshOverview();return}fetch('/fragments/message?group_id='+encodeURIComponent(d.group_id)+'&message_id='+d.message_id).then(function(r){return r.text()}).then(function(fragment){var list=document.getElementById('message-list');if(!list||list.querySelector('[data-message-id="'+d.message_id+'"]'))return;list.insertAdjacentHTML('beforeend',fragment);prune(list,true)})});source.addEventListener('group.changed',function(e){var d=JSON.parse(e.data);document.getElementById('activity-indicator').textContent='Group metadata changed';refreshOpenChat(d.group_id);refreshOverview()});source.addEventListener('group.deleted',function(e){var d=JSON.parse(e.data);document.getElementById('activity-indicator').textContent='Group deleted';refreshOpenChat(d.group_id);refreshOverview()});source.addEventListener('member.changed',function(){document.getElementById('activity-indicator').textContent='Group state changed';refreshOverview()});source.addEventListener('wakeup.changed',function(){document.getElementById('activity-indicator').textContent='Wakeup state changed';refreshOverview()});source.addEventListener('tool_call.completed',function(){refreshOverview();refreshAnalytics()});})();</script></body></html>""".replace("__PICKER__", picker).replace("__PANEL__", render_overview(groups_directory))


def serve(arguments: Optional[List[str]] = None) -> int:
    """Start the local observer server."""
    options = parse_arguments(arguments)
    server = create_observer_server(options.port, options.poll_interval, resolve_groups_directory(options))
    url = "http://127.0.0.1:" + str(server.port) + "/"
    sys.stdout.write(url + "\n")
    if server.used_default_port_fallback:
        sys.stderr.write("Port 8787 is unavailable; using " + url + "\n")
    if not options.silent:
        try:
            if not webbrowser.open(url):
                sys.stderr.write("Could not open a browser; visit " + url + "\n")
        except Exception:
            sys.stderr.write("Could not open a browser; visit " + url + "\n")
    server.serve_forever()
    return 0
