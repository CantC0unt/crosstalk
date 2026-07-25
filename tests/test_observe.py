from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import observe
import mcp


class ObserveModuleTests(unittest.TestCase):
    def test_observer_module_is_importable(self):
        self.assertIsNotNone(observe)

    def test_observer_options_defaults_and_values(self):
        self.assertEqual(
            observe.parse_arguments([]),
            observe.ObserverOptions(False, None, observe.DEFAULT_POLL_INTERVAL_SECONDS, None),
        )
        self.assertEqual(
            observe.parse_arguments(["--silent", "--port", "8788", "--poll-interval", "1.25", "--groups-dir", "/tmp/groups"]),
            observe.ObserverOptions(True, 8788, 1.25, "/tmp/groups"),
        )

    def test_observer_options_reject_invalid_values(self):
        for arguments in (
            ["--port", "0"],
            ["--port", "65536"],
            ["--port", "not-a-port"],
            ["--poll-interval", "0"],
            ["--poll-interval", "nan"],
            ["--poll-interval", "inf"],
            ["--poll-interval", "not-a-number"],
        ):
            with redirect_stderr(StringIO()):
                with self.assertRaises(SystemExit):
                    observe.parse_arguments(arguments)

    def test_groups_directory_resolution_is_ordered_and_read_only(self):
        missing = Path(self.id().replace(".", "-"))
        options = observe.parse_arguments(["--groups-dir", str(missing)])
        self.assertEqual(observe.resolve_groups_directory(options, {"CROSSTALK_GROUPS_DIR": "/env/groups"}), missing)
        self.assertFalse(missing.exists())
        self.assertEqual(observe.resolve_groups_directory(observe.parse_arguments([]), {"CROSSTALK_GROUPS_DIR": "/env/groups"}), Path("/env/groups"))

    def test_read_only_database_helper_cannot_mutate_or_create(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "group.sqlite3"
            writable = observe.sqlite3.connect(str(database_path))
            try:
                writable.execute("CREATE TABLE entries(value TEXT)")
                writable.execute("INSERT INTO entries VALUES ('kept')")
                writable.commit()
            finally:
                writable.close()
            connection = observe.open_read_only_database(database_path)
            try:
                self.assertEqual(connection.execute("SELECT value FROM entries").fetchone()[0], "kept")
                with self.assertRaises(observe.sqlite3.OperationalError):
                    connection.execute("INSERT INTO entries VALUES ('blocked')")
            finally:
                connection.close()
            self.assertFalse((Path(directory) / "missing.sqlite3").exists())

    def test_discovery_tolerates_missing_directories(self):
        self.assertEqual(observe.discover_groups(Path("definitely-missing-crosstalk-groups")), [])

    def test_loopback_server_and_csrf_token(self):
        try:
            server = observe.create_observer_server(0)
        except PermissionError:
            self.skipTest("loopback sockets are unavailable in this sandbox")
        try:
            self.assertEqual(server.httpd.server_address[0], "127.0.0.1")
            self.assertTrue(server.valid_csrf_token(server.csrf_token))
            self.assertFalse(server.valid_csrf_token("wrong"))
        finally:
            server.httpd.server_close()

    def test_default_port_fallback_is_marked_and_warned(self):
        fallback = Mock()
        fallback.port = 43210
        fallback.used_default_port_fallback = True
        fallback.serve_forever = Mock()
        with patch.object(observe, "ObserverHTTPServer", side_effect=[OSError("busy"), fallback]):
            self.assertIs(observe.create_observer_server(None), fallback)
        with patch.object(observe, "create_observer_server", return_value=fallback), \
             redirect_stdout(StringIO()), redirect_stderr(StringIO()) as stderr:
            self.assertEqual(observe.serve(["--silent"]), 0)
        self.assertIn("Port 8787 is unavailable; using http://127.0.0.1:43210/", stderr.getvalue())

    def test_snapshot_for_missing_directory_is_compact_and_stable(self):
        self.assertEqual(observe.observer_snapshot(Path("missing-observer-state")), {"groups": {}, "latest_audit_id": None})


class ObserverPollingTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.groups_directory = Path(self.temporary_directory.name) / "groups"
        self.store = mcp.CrosstalkStore(str(self.groups_directory))
        self.group_id = self.store.create_group("writer")
        self.store.join_group(self.group_id, "writer", "Writer")

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_polling_interval_delivers_a_new_message_promptly(self):
        hub = observe.EventHub()
        poller = observe.ObserverPoller(self.groups_directory, 0.02, hub)
        poller.poll_once()
        subscriber = hub.subscribe()
        thread = threading.Thread(target=poller.run, daemon=True)
        thread.start()
        try:
            self.store.send_message(self.group_id, "writer", "Writer", "arrived")
            event, payload = subscriber.get(timeout=0.5)
            self.assertEqual((event, payload), ("message.created", {"group_id": self.group_id, "message_id": 1}))
        finally:
            poller.stop()
            thread.join(timeout=0.5)
            hub.unsubscribe(subscriber)

    def test_concurrent_writers_produce_one_latest_live_message_without_duplicates(self):
        hub = observe.EventHub()
        poller = observe.ObserverPoller(self.groups_directory, 0.5, hub)
        poller.poll_once()
        subscriber = hub.subscribe()
        writers = [threading.Thread(target=self.store.send_message, args=(self.group_id, "writer", "Writer", "message " + str(index))) for index in range(4)]
        for writer in writers:
            writer.start()
        for writer in writers:
            writer.join(timeout=2)
            self.assertFalse(writer.is_alive())
        poller.poll_once()
        self.assertEqual(subscriber.get(timeout=0.2), ("message.created", {"group_id": self.group_id, "message_id": 4}))
        poller.poll_once()
        with self.assertRaises(observe.queue.Empty):
            subscriber.get(timeout=0.05)
        hub.unsubscribe(subscriber)

    def test_event_hub_close_wakes_current_and_later_sse_subscribers(self):
        hub = observe.EventHub()
        subscriber = hub.subscribe()
        hub.close()
        self.assertIsNone(subscriber.get(timeout=0.2))
        self.assertIsNone(hub.subscribe().get(timeout=0.2))

    def test_temporary_database_lock_is_skipped_and_detected_after_release(self):
        hub = observe.EventHub()
        poller = observe.ObserverPoller(self.groups_directory, 0.5, hub)
        poller.poll_once()
        subscriber = hub.subscribe()
        self.store.send_message(self.group_id, "writer", "Writer", "locked briefly")
        connection = mcp.sqlite3.connect(str(self.groups_directory / (self.group_id + ".sqlite3")))
        try:
            connection.execute("BEGIN EXCLUSIVE")
            poller.poll_once()
        finally:
            connection.rollback()
            connection.close()
        poller.poll_once()
        self.assertEqual(subscriber.get(timeout=0.2), ("message.created", {"group_id": self.group_id, "message_id": 1}))
        hub.unsubscribe(subscriber)

    def test_metadata_only_change_publishes_compact_group_changed_event(self):
        hub = observe.EventHub()
        poller = observe.ObserverPoller(self.groups_directory, 0.5, hub)
        poller.poll_once()
        subscriber = hub.subscribe()
        self.store.update_group_metadata(self.group_id, "writer", name="Renamed")
        poller.poll_once()
        self.assertEqual(subscriber.get(timeout=0.2), ("group.changed", {"group_id": self.group_id}))
        hub.unsubscribe(subscriber)

    def test_simultaneous_marker_changes_fan_out_as_individual_events(self):
        hub = observe.EventHub()
        poller = observe.ObserverPoller(self.groups_directory, 0.5, hub)
        before = (None, "before", (("writer", 0),), ())
        after = (1, "after", (("writer", 1),), ((1, "writer", None, None),))
        with patch.object(observe, "group_fingerprint", side_effect=[before, after]):
            poller.poll_once()
            subscriber = hub.subscribe()
            poller.poll_once()
        events = {subscriber.get(timeout=0.2)[0] for _ in range(4)}
        self.assertEqual(events, {"message.created", "group.changed", "member.changed", "wakeup.changed"})
        hub.unsubscribe(subscriber)

    def test_first_audit_row_after_empty_baseline_publishes_completion_event(self):
        hub = observe.EventHub()
        poller = observe.ObserverPoller(self.groups_directory, 0.5, hub)
        poller.poll_once()
        subscriber = hub.subscribe()
        audit_store = mcp.ObservabilityStore(str(self.groups_directory))
        audit_store.record_event(mcp.AuditEvent("2026-01-01T12:00:00+00:00", "1", "list_groups", None, None, None, "success", 1, None, None, None))
        poller.poll_once()
        self.assertEqual(subscriber.get(timeout=0.2), ("tool_call.completed", {"tool_call_id": 1}))
        hub.unsubscribe(subscriber)

    def test_deleted_group_publishes_live_deletion_event(self):
        hub = observe.EventHub()
        poller = observe.ObserverPoller(self.groups_directory, 0.5, hub)
        poller.poll_once()
        subscriber = hub.subscribe()
        self.store.delete_group(self.group_id, "writer")
        poller.poll_once()
        self.assertEqual(subscriber.get(timeout=0.2), ("group.deleted", {"group_id": self.group_id}))
        hub.unsubscribe(subscriber)

    def test_reconnect_snapshot_is_current_and_live_messages_are_not_replayed(self):
        hub = observe.EventHub()
        poller = observe.ObserverPoller(self.groups_directory, 0.5, hub)
        poller.poll_once()
        first_connection = hub.subscribe()
        self.assertEqual(observe.observer_snapshot(self.groups_directory)["groups"][self.group_id]["latest_message_id"], None)
        self.store.send_message(self.group_id, "writer", "Writer", "first")
        poller.poll_once()
        self.assertEqual(first_connection.get(timeout=0.2)[0], "message.created")
        hub.unsubscribe(first_connection)

        second_connection = hub.subscribe()
        snapshot = observe.observer_snapshot(self.groups_directory)
        self.assertEqual(snapshot["groups"][self.group_id]["latest_message_id"], 1)
        poller.poll_once()
        with self.assertRaises(observe.queue.Empty):
            second_connection.get(timeout=0.05)
        hub.unsubscribe(second_connection)

    def test_chat_html_escapes_content_and_uses_cursor_pagination(self):
        self.store.send_message(self.group_id, "writer", "Writer", "<script>alert('no')</script>")
        rendered = observe.render_chat_panel(self.groups_directory, self.group_id)
        self.assertIn("&lt;script&gt;alert(&#x27;no&#x27;)&lt;/script&gt;", rendered)
        self.assertNotIn("<script>alert", rendered)
        self.assertIn('data-message-id="1"', rendered)

    def test_dashboard_uses_group_names_and_local_timestamps(self):
        self.store.update_group_metadata(self.group_id, "writer", name="Project launch")
        self.store.send_message(self.group_id, "writer", "Writer", "first")
        dashboard = observe.render_dashboard(self.groups_directory)
        chat = observe.render_chat_panel(self.groups_directory, self.group_id)
        self.assertIn(">Project launch</button>", dashboard)
        self.assertIn('title="{}"'.format(self.group_id), dashboard)
        displayed_time = chat.split("<time>", 1)[1].split("</time>", 1)[0]
        self.assertRegex(displayed_time, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
        self.assertNotIn("T", displayed_time[:19])
        self.assertEqual(
            observe.format_timestamp("2026-01-01T00:00:00+00:00", local_timezone=observe.timezone(observe.timedelta(hours=5, minutes=30))),
            "2026-01-01 05:30:00 UTC+05:30 (+0530)",
        )

    def test_chat_renders_wakeup_creation_acknowledgement_and_notification_state(self):
        self.store.join_group(self.group_id, "reader", "Reader")
        self.store.send_message(self.group_id, "writer", "Writer", "please read", priority="urgent")
        rendered = observe.render_chat_panel(self.groups_directory, self.group_id)
        self.assertIn("Wakeups", rendered)
        self.assertIn("message #1 → reader", rendered)
        self.assertIn("acknowledged pending", rendered)
        self.assertIn("last notified", rendered)

    def test_dashboard_uses_pinned_sri_assets_and_native_live_client(self):
        dashboard = observe.render_dashboard(self.groups_directory)
        self.assertIn('href="/static/observer.css"', dashboard)
        self.assertEqual(observe.OBSERVER_STATIC_ASSETS["/static/observer.css"][0], "text/css; charset=utf-8")
        self.assertIn("htmx.org@2.0.4", dashboard)
        self.assertIn("alpinejs@3.14.8", dashboard)
        self.assertIn("sha384-HGfztofotfshcF7+8n44JQL2oJmowVChPTg48S+jvZoztPfvwD79OC/LTtG6dMp+", dashboard)
        self.assertIn("sha384-X9kJyAubVxnP0hcA+AMMs21U445qsnqhnUF8EBlEpP3a42Kh/JwWjlv2ZcvGfphb", dashboard)
        self.assertEqual(dashboard.count('integrity="sha384-'), 2)
        self.assertIn("new EventSource('/events')", dashboard)
        self.assertIn("source.addEventListener('snapshot'", dashboard)
        self.assertIn("source.addEventListener('group.deleted'", dashboard)
        self.assertIn("htmx:afterSettle", dashboard)
        self.assertIn("hx-get=\"/fragments/chat?group_id=", dashboard)

    def test_overview_derives_chat_metrics_and_shows_disabled_audit_notice(self):
        self.store.send_message(self.group_id, "writer", "Writer", "urgent", priority="high")
        overview = observe.render_overview(self.groups_directory)
        self.assertIn("Overview", overview)
        self.assertIn("Messages</strong><span>1", overview)
        self.assertIn("high: <strong>1</strong>", overview)
        self.assertIn("Audit analytics are disabled", overview)

    def test_overview_reports_wakeup_responsiveness_after_acknowledgement(self):
        self.store.join_group(self.group_id, "reader", "Reader")
        self.store.send_message(self.group_id, "writer", "Writer", "urgent", priority="urgent")
        self.store.get_unread_messages(self.group_id, "reader", "Reader")
        overview = observe.render_overview(self.groups_directory)
        self.assertIn("Wakeup response", overview)
        self.assertIn("1 acknowledged wakeup included", overview)

    def test_overview_reports_audit_volume_and_error_rate(self):
        audit_store = mcp.ObservabilityStore(str(self.groups_directory))
        audit_store.record_event(mcp.AuditEvent("2026-01-01T12:00:00+00:00", "1", "list_groups", None, None, None, "success", 1, None, None, None))
        audit_store.record_event(mcp.AuditEvent("2026-01-01T12:01:00+00:00", "2", "list_groups", None, None, None, "error", 1, None, "validation", None))
        overview = observe.render_overview(self.groups_directory)
        self.assertIn("Tool calls</strong><span>2", overview)
        self.assertIn("Error rate</strong><span>50.0%", overview)

    def test_tool_analytics_filters_raw_audit_events_and_calculates_percentiles(self):
        audit_store = mcp.ObservabilityStore(str(self.groups_directory))
        audit_store.record_event(mcp.AuditEvent("2026-01-01T12:00:00+00:00", "1", "send_message", self.group_id, "writer", "Writer", "success", 12, None, None, None))
        audit_store.record_event(mcp.AuditEvent("2026-01-01T13:00:00+00:00", "2", "send_message", self.group_id, "writer", "Writer", "error", 40, None, "validation", None))
        data = observe.read_tool_analytics(self.groups_directory, {"tool_name": "send_message", "name": "Writer", "from": "2026-01-01T00:00:00+00:00"})
        self.assertEqual(len(data["rows"]), 2)
        self.assertEqual((observe._percentile(data["durations"], .5), observe._percentile(data["durations"], .95)), (12, 40))
        self.assertEqual(data["by_time"], {"2026-01-01T12:00Z": 1, "2026-01-01T13:00Z": 1})
        rendered = observe.render_tool_analytics(self.groups_directory, {"from": "2026-01-01T00:00:00+00:00"})
        self.assertIn("Calls over time", rendered)
        self.assertIn("p95 latency", rendered)
        self.assertIn("<th>Caller</th>", rendered)
        self.assertIn('name="name"', rendered)

    def test_storage_view_reports_only_observability_database_state(self):
        audit_store = mcp.ObservabilityStore(str(self.groups_directory))
        audit_store.record_event(mcp.AuditEvent("2026-01-01T12:00:00+00:00", "1", "list_groups", None, None, None, "success", 2, None, None, None))
        status = observe.audit_storage_status(self.groups_directory, {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "30"})
        self.assertTrue(status["available"])
        self.assertEqual((status["row_count"], status["retention"]), (1, "inf"))
        self.assertGreater(status["size_bytes"], 0)
        rendered = observe.render_storage(self.groups_directory)
        self.assertIn("Audit rows", rendered)
        self.assertIn("separate from group databases", rendered)

    def test_bounded_audit_reclaim_never_targets_group_databases(self):
        audit_store = mcp.ObservabilityStore(str(self.groups_directory))
        audit_store.initialize()
        group_path = self.groups_directory / (self.group_id + ".sqlite3")
        group_size = group_path.stat().st_size
        result = observe.reclaim_audit_free_space(self.groups_directory, pages=1)
        self.assertTrue(result["ok"])
        self.assertEqual(group_path.stat().st_size, group_size)
        self.assertIn("hx-post=\"/api/storage/reclaim\"", observe.render_storage(self.groups_directory, "csrf-token"))

    def test_immediate_audit_deletion_is_separate_from_reclaim_and_never_touches_groups(self):
        audit_store = mcp.ObservabilityStore(str(self.groups_directory))
        audit_store.record_event(mcp.AuditEvent("2026-01-01T12:00:00+00:00", "1", "list_groups", None, None, None, "success", 2, None, None, None))
        group_path = self.groups_directory / (self.group_id + ".sqlite3")
        group_size = group_path.stat().st_size
        result = observe.delete_audit_history(self.groups_directory)
        self.assertTrue(result["ok"])
        self.assertEqual(observe.read_tool_calls(self.groups_directory), [])
        self.assertTrue((self.groups_directory / "observability.sqlite3").is_file())
        self.assertEqual(group_path.stat().st_size, group_size)
        controls = observe.render_storage(self.groups_directory, "csrf-token")
        self.assertIn("/api/storage/delete-history", controls)
        self.assertIn("hx-confirm", controls)

    def test_chat_pagination_has_no_omissions_or_duplicates(self):
        for index in range(105):
            self.store.send_message(self.group_id, "writer", "Writer", "message " + str(index))
        newest = observe.read_message_page(self.groups_directory, self.group_id)
        older = observe.read_message_page(self.groups_directory, self.group_id, newest["next_older_message_id"])
        identifiers = [message["id"] for message in older["messages"] + newest["messages"]]
        self.assertEqual(identifiers, list(range(1, 106)))
        fragment = observe.render_message_page(self.groups_directory, self.group_id)
        self.assertIn("Load older messages", fragment)
        self.assertIn('hx-target="this" hx-swap="outerHTML"', fragment)

    def test_ui_states_cover_disabled_audit_and_deleted_group_analytics(self):
        self.assertIn("Audit data is unavailable", observe.render_tool_analytics(self.groups_directory))
        audit_store = mcp.ObservabilityStore(str(self.groups_directory))
        audit_store.record_event(mcp.AuditEvent("2026-01-01T12:00:00+00:00", "1", "send_message", self.group_id, "writer", "Writer", "success", 1, None, None, None))
        self.store.delete_group(self.group_id, "writer")
        calls = observe.read_tool_calls(self.groups_directory)
        self.assertTrue(calls[0]["group_deleted"])
        self.assertIn("Chat unavailable", observe.render_chat_panel(self.groups_directory, self.group_id))
        self.assertIn("(deleted)", observe.render_tool_analytics(self.groups_directory, {"from": "2026-01-01T00:00:00+00:00"}))

    def test_dashboard_output_escapes_group_metadata_and_exposes_csrf_control(self):
        database = self.groups_directory / (self.group_id + ".sqlite3")
        connection = mcp.sqlite3.connect(str(database))
        try:
            connection.execute("UPDATE group_metadata SET name = '<img src=x onerror=alert(1)>' WHERE id = 1")
            connection.commit()
        finally:
            connection.close()
        chat = observe.render_chat_panel(self.groups_directory, self.group_id)
        self.assertIn("&lt;img src=x onerror=alert(1)&gt;", chat)
        self.assertNotIn("<img src=x", chat)
        mcp.ObservabilityStore(str(self.groups_directory)).initialize()
        storage = observe.render_storage(self.groups_directory, "csrf-token")
        self.assertIn("X-CSRF-Token", storage)


if __name__ == "__main__":
    unittest.main()
