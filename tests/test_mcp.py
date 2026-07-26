import tempfile
import threading
import time
import unittest
from io import StringIO
from pathlib import Path
import sys
from unittest.mock import patch
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import mcp as main


class CrosstalkStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.groups_directory = Path(self.temporary_directory.name) / "groups"
        self.store = main.CrosstalkStore(str(self.groups_directory))
        self.group_id = self.store.create_group("developer-1")
        for context_id, name in [
            ("developer-1", "developer"),
            ("architect-1", "architect"),
            ("reviewer-1", "reviewer"),
            ("searcher-1", "researcher"),
            ("reader-1", "reader"),
        ]:
            self.store.join_group(self.group_id, context_id, name)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_read_tracking_latest_search_and_cursor(self):
        self.store.send_message(self.group_id, "developer-1", "developer", "Implement cache eviction")
        self.store.send_message(self.group_id, "architect-1", "architect", "Review cache design")

        unread = self.store.get_unread_messages(self.group_id, "reviewer-1", "reviewer")
        self.assertEqual([message["id"] for message in unread], [1, 2])
        self.assertEqual(self.store.get_unread_messages(self.group_id, "reviewer-1", "reviewer"), [])

        latest = self.store.get_latest_messages(self.group_id, "developer-1", "developer")
        self.assertEqual([message["message"] for message in latest], ["Review cache design"])
        search = self.store.search_messages(self.group_id, "searcher-1", "researcher", "cache")
        self.assertEqual(len(search), 2)
        after = self.store.get_messages_after(self.group_id, "reader-1", "reader", 1)
        self.assertEqual([message["id"] for message in after], [2])

    def test_search_supports_sql_like_wildcards(self):
        self.store.send_message(self.group_id, "developer-1", "developer", "cache eviction")
        self.store.send_message(self.group_id, "architect-1", "architect", "cache design")
        matches = self.store.search_messages(self.group_id, "searcher-1", "researcher", "cache %")
        self.assertEqual([message["id"] for message in matches], [1, 2])

    def test_sender_messages_are_not_unread_for_the_sender(self):
        self.store.send_message(self.group_id, "developer-1", "developer", "My message")
        self.assertEqual(self.store.get_unread_messages(self.group_id, "developer-1", "developer"), [])
        self.store.send_message(self.group_id, "architect-1", "architect", "A reply")
        unread = self.store.get_unread_messages(self.group_id, "developer-1", "developer")
        self.assertEqual([message["message"] for message in unread], ["A reply"])

    def test_tool_call_example_collaboration_flow(self):
        monitor = main.GroupSubscriptionMonitor(self.store, 0.1)
        try:
            created = main.call_tool(self.store, "create_group", {
                "context_id": "developer-example", "name": "Cache redesign", "description": "Coordinate the cache work",
            }, monitor)
            group_id = json.loads(created["content"][0]["text"])["group_id"]
            for context_id, name in [("developer-example", "developer"), ("architect-example", "architect")]:
                joined = main.call_tool(self.store, "join_group", {
                    "group_id": group_id, "context_id": context_id, "name": name,
                }, monitor)
                self.assertFalse(joined["isError"])
            sent = main.call_tool(self.store, "send_message", {
                "group_id": group_id, "context_id": "developer-example", "name": "developer",
                "message": "@architect Please review the cache invalidation design.", "priority": "high",
            }, monitor)
            sent_payload = json.loads(sent["content"][0]["text"])
            self.assertEqual((sent_payload["routing_reason"], sent_payload["wakeup_targets"]), ("mentioned", ["architect-example"]))
            unread = main.call_tool(self.store, "get_unread_messages", {
                "group_id": group_id, "context_id": "architect-example", "name": "architect",
            }, monitor)
            messages = json.loads(unread["content"][0]["text"])["messages"]
            self.assertEqual([message["message"] for message in messages], ["@architect Please review the cache invalidation design."])
        finally:
            monitor.stop()

    def test_delete_removes_only_target_group(self):
        other_group = self.store.create_group("developer-1")
        self.assertEqual(
            [group["group_id"] for group in self.store.list_groups()],
            sorted([self.group_id, other_group]),
        )
        self.store.delete_group(self.group_id, "developer-1")
        self.assertFalse((self.groups_directory / (self.group_id + ".sqlite3")).exists())
        self.assertTrue((self.groups_directory / (other_group + ".sqlite3")).exists())

    def test_only_creator_can_delete_group(self):
        with self.assertRaisesRegex(main.AuthorizationError, "created this group"):
            self.store.delete_group(self.group_id, "architect-1")
        result = main.call_tool(self.store, "delete_group", {"group_id": self.group_id, "context_id": "architect-1"})
        event = main.audit_tool_result("delete_group", {"group_id": self.group_id, "context_id": "architect-1"}, result, 1, main.datetime.now(main.timezone.utc).isoformat(), main.time.monotonic())
        self.assertEqual((result["isError"], event.error_category), (True, "authorization"))
        self.assertTrue((self.groups_directory / (self.group_id + ".sqlite3")).exists())
        self.store.delete_group(self.group_id, "developer-1")
        self.assertFalse((self.groups_directory / (self.group_id + ".sqlite3")).exists())

    def test_group_resource_uri_and_snapshot(self):
        uri = main.GroupSubscriptionMonitor.uri_for_group(self.group_id)
        self.assertEqual(main.GroupSubscriptionMonitor.group_id_from_uri(uri), self.group_id)
        self.assertEqual(self.store.group_update_snapshot(self.group_id)["message_count"], 0)
        self.store.send_message(self.group_id, "developer-1", "developer", "A message")
        self.assertEqual(self.store.group_update_snapshot(self.group_id)["latest_message_id"], 1)

    def test_current_schema_version_is_recorded(self):
        database = self.groups_directory / (self.group_id + ".sqlite3")
        connection = main.sqlite3.connect(str(database))
        try:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], main.GROUP_SCHEMA_VERSION)
        finally:
            connection.close()

    def test_subscription_poll_interval_is_configurable(self):
        monitor = main.GroupSubscriptionMonitor(self.store, 0.1)
        try:
            self.assertEqual(monitor.poll_interval_seconds, 0.1)
        finally:
            monitor.stop()

    def test_observability_configuration_defaults_and_opt_in(self):
        self.assertEqual(
            main.observability_configuration({}),
            main.ObservabilityConfiguration(enabled=False, retention_days=None),
        )
        self.assertEqual(
            main.observability_configuration({"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "inf"}),
            main.ObservabilityConfiguration(enabled=True, retention_days=None),
        )
        self.assertEqual(
            main.observability_configuration({"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "30"}),
            main.ObservabilityConfiguration(enabled=True, retention_days=30),
        )

    def test_observability_configuration_rejects_invalid_values_before_startup(self):
        invalid_environments = (
            {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "0"},
            {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "-1"},
            {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "1.5"},
            {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": " 1"},
            {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "INF"},
            {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": ""},
        )
        for environment in invalid_environments:
            with self.assertRaises(ValueError):
                main.observability_configuration(environment)
        with patch.dict("os.environ", {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "invalid"}, clear=True), \
             patch.object(main, "CrosstalkStore") as store:
            with self.assertRaises(ValueError):
                main.serve()
        store.assert_not_called()

    def test_observability_store_creates_the_independent_initial_schema(self):
        group_database = self.groups_directory / (self.group_id + ".sqlite3")
        group_connection = main.sqlite3.connect(str(group_database))
        try:
            group_tables_before = group_connection.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").fetchall()
        finally:
            group_connection.close()

        store = main.ObservabilityStore(str(self.groups_directory))
        store.initialize()
        self.assertEqual(store.database_path, self.groups_directory / "observability.sqlite3")
        connection = main.sqlite3.connect(str(store.database_path))
        try:
            self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(connection.execute("PRAGMA auto_vacuum").fetchone()[0], 2)
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], main.OBSERVABILITY_SCHEMA_VERSION)
            metadata = connection.execute("SELECT schema_version, created_at, audit_enabled_at, last_retention_cleanup_at FROM metadata WHERE id = 1").fetchone()
            self.assertEqual(metadata[0], main.OBSERVABILITY_SCHEMA_VERSION)
            self.assertTrue(metadata[1])
            self.assertTrue(metadata[2])
            self.assertIsNone(metadata[3])
            self.assertIn("retention_setting", {row[1] for row in connection.execute("PRAGMA table_info(metadata)")})
            columns = {row[1] for row in connection.execute("PRAGMA table_info(tool_calls)")}
            self.assertEqual(columns, {"id", "occurred_at", "audit_request_id", "tool_name", "group_id", "context_id", "name", "outcome", "duration_ms", "result_count", "error_category", "details_json"})
            indexes = {row[1] for row in connection.execute("PRAGMA index_list(tool_calls)")}
            self.assertEqual(indexes, {"tool_calls_by_occurred_at", "tool_calls_by_tool_and_occurred_at", "tool_calls_by_group_and_occurred_at", "tool_calls_by_context_and_occurred_at", "tool_calls_by_outcome_and_occurred_at"})
        finally:
            connection.close()
        group_connection = main.sqlite3.connect(str(group_database))
        try:
            self.assertEqual(group_connection.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").fetchall(), group_tables_before)
        finally:
            group_connection.close()

    def test_audit_identity_does_not_confuse_metadata_with_caller_name(self):
        self.assertEqual(main.audit_identity("create_group", {"context_id": "creator", "name": "Group title"}, {"group_id": self.group_id}), {"group_id": self.group_id, "context_id": "creator", "name": None})
        self.assertEqual(main.audit_identity("update_group_metadata", {"group_id": self.group_id, "context_id": "creator", "name": "New title"}), {"group_id": self.group_id, "context_id": "creator", "name": None})
        self.assertEqual(main.audit_identity("send_message", {"group_id": self.group_id, "context_id": "sender", "name": "developer"}), {"group_id": self.group_id, "context_id": "sender", "name": "developer"})

    def test_audit_identity_prefers_explicit_caller_name(self):
        self.assertEqual(
            main.audit_identity("update_group_metadata", {"group_id": self.group_id, "context_id": "creator", "name": "New title", "caller_name": "operator"}),
            {"group_id": self.group_id, "context_id": "creator", "name": "operator"},
        )

    def test_safe_audit_details_exclude_content_and_are_bounded(self):
        details = main.safe_audit_details("send_message", {"message_id": 7, "priority": "high", "routing_reason": "mentioned", "wakeup_targets": ["a", "b"], "message": "secret"})
        self.assertEqual(json.loads(details), {"message_id": 7, "priority": "high", "routing_reason": "mentioned", "wakeup_target_count": 2})
        self.assertIsNone(main.safe_audit_details("get_group_metadata", {"name": "secret", "description": "secret"}))
        self.assertIsNone(main.safe_audit_details("create_group", {"group_id": "x" * 3000}))

    def test_audit_error_categories_are_fixed(self):
        self.assertEqual(main.audit_error_category(main.DatabaseBusyError("database is locked")), "sqlite_busy")
        self.assertEqual(main.audit_error_category(main.NotFoundError("group does not exist")), "not_found")
        self.assertEqual(main.audit_error_category(main.AuthorizationError("only the creator may delete")), "authorization")
        self.assertEqual(main.audit_error_category(main.AuthorizationError("Only the context that created this group can delete it.")), "authorization")
        self.assertEqual(main.audit_error_category(ValueError("bad input")), "validation")
        self.assertEqual(main.audit_error_category(RuntimeError("secret error")), "internal")

    def test_completed_tool_error_preserves_sqlite_busy_audit_category(self):
        result = main.tool_result({"error": "database is locked"}, is_error=True, error_category="sqlite_busy")
        event = main.audit_tool_result("send_message", {"group_id": self.group_id, "context_id": "developer-1", "name": "developer"}, result, 1, main.datetime.now(main.timezone.utc).isoformat(), main.time.monotonic())
        self.assertEqual((event.outcome, event.error_category), ("error", "sqlite_busy"))

    def test_audit_write_failure_does_not_change_a_tool_result(self):
        monitor = main.GroupSubscriptionMonitor(self.store, 0.1)
        try:
            request = {"jsonrpc": "2.0", "id": 9, "method": "tools/call", "params": {"name": "list_groups", "arguments": {}}}
            with patch.dict("os.environ", {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "inf"}, clear=True), \
                 patch.object(main.ObservabilityStore, "record_event", side_effect=main.sqlite3.OperationalError("database is locked")) as record_event:
                response = main._handle_request(request, self.store, monitor, {"initialized": True, "client_ready": True})
            self.assertEqual(response["result"]["isError"], False)
            record_event.assert_called_once()
        finally:
            monitor.stop()

    def test_retention_cleanup_is_bounded_and_skips_unlimited_retention(self):
        audit_store = main.ObservabilityStore(str(self.groups_directory))
        audit_store.initialize()
        database = main.sqlite3.connect(str(audit_store.database_path))
        try:
            database.execute("INSERT INTO tool_calls(occurred_at, tool_name, outcome, duration_ms) VALUES (?, 'list_groups', 'success', 1)", ((main.datetime.now(main.timezone.utc) - main.timedelta(days=2)).isoformat(),))
            database.execute("INSERT INTO tool_calls(occurred_at, tool_name, outcome, duration_ms) VALUES (?, 'list_groups', 'success', 1)", (main.datetime.now(main.timezone.utc).isoformat(),))
            database.execute("INSERT INTO tool_call_group_names(tool_call_id, group_name) VALUES (1, 'Expired group')")
            database.execute("INSERT INTO tool_call_group_names(tool_call_id, group_name) VALUES (2, 'Current group')")
            database.commit()
        finally:
            database.close()
        self.assertEqual(audit_store.cleanup_retention(None), 0)
        self.assertEqual(audit_store.cleanup_retention(1), 1)
        self.assertEqual(audit_store.cleanup_retention(1), 0)
        database = main.sqlite3.connect(str(audit_store.database_path))
        try:
            self.assertEqual(database.execute("SELECT tool_call_id, group_name FROM tool_call_group_names ORDER BY tool_call_id").fetchall(), [(2, "Current group")])
        finally:
            database.close()

    def test_enabled_auditing_records_each_completed_tool_call_not_protocol_requests(self):
        monitor = main.GroupSubscriptionMonitor(self.store, 0.1)
        try:
            state = {"initialized": True, "client_ready": True}
            with patch.dict("os.environ", {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "inf"}, clear=True):
                self.assertIsNotNone(main._handle_request({"jsonrpc": "2.0", "id": 1, "method": "ping"}, self.store, monitor, state))
                success = main._handle_request({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "list_groups", "arguments": {}}}, self.store, monitor, state)
                failure = main._handle_request({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "unknown", "arguments": {}}}, self.store, monitor, state)
            self.assertFalse(success["result"]["isError"])
            self.assertTrue(failure["result"]["isError"])
            audit = main.sqlite3.connect(str(self.groups_directory / "observability.sqlite3"))
            try:
                rows = audit.execute("SELECT audit_request_id, tool_name, outcome, error_category FROM tool_calls ORDER BY id").fetchall()
            finally:
                audit.close()
            self.assertEqual(rows, [("2", "list_groups", "success", None), ("3", "unknown", "error", "validation")])
        finally:
            monitor.stop()

    def test_invalid_tool_arguments_return_validation_errors_and_are_audited(self):
        monitor = main.GroupSubscriptionMonitor(self.store, 0.1)
        try:
            state = {"initialized": True, "client_ready": True}
            with patch.dict("os.environ", {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "inf"}, clear=True):
                responses = [main._handle_request(
                    {"jsonrpc": "2.0", "id": index, "method": "tools/call", "params": {"name": "list_groups", "arguments": arguments}},
                    self.store, monitor, state,
                ) for index, arguments in enumerate((None, [], "not-an-object"), start=1)]
            self.assertEqual([response["error"]["code"] for response in responses], [-32602, -32602, -32602])
            audit = main.sqlite3.connect(str(self.groups_directory / "observability.sqlite3"))
            try:
                rows = audit.execute("SELECT audit_request_id, tool_name, outcome, error_category FROM tool_calls ORDER BY id").fetchall()
            finally:
                audit.close()
            self.assertEqual(rows, [("1", "list_groups", "error", "validation"), ("2", "list_groups", "error", "validation"), ("3", "list_groups", "error", "validation")])
        finally:
            monitor.stop()

    def test_auditing_records_an_explicit_caller_name_for_any_tool(self):
        monitor = main.GroupSubscriptionMonitor(self.store, 0.1)
        try:
            state = {"initialized": True, "client_ready": True}
            with patch.dict("os.environ", {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "inf"}, clear=True):
                response = main._handle_request(
                    {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "list_groups", "arguments": {"caller_name": "operations"}}},
                    self.store, monitor, state,
                )
            self.assertFalse(response["result"]["isError"])
            audit = main.sqlite3.connect(str(self.groups_directory / "observability.sqlite3"))
            try:
                self.assertEqual(audit.execute("SELECT name FROM tool_calls").fetchone()[0], "operations")
            finally:
                audit.close()
        finally:
            monitor.stop()

    def test_auditing_preserves_deleted_group_name_snapshot(self):
        self.store.update_group_metadata(self.group_id, "developer-1", name="Release coordination")
        monitor = main.GroupSubscriptionMonitor(self.store, 0.1)
        try:
            state = {"initialized": True, "client_ready": True}
            request = {
                "jsonrpc": "2.0", "id": 8, "method": "tools/call",
                "params": {"name": "delete_group", "arguments": {
                    "group_id": self.group_id, "context_id": "developer-1",
                }},
            }
            with patch.dict("os.environ", {"CROSSTALK_OBSERVABILITY_RETENTION_DAYS": "inf"}, clear=True):
                response = main._handle_request(request, self.store, monitor, state)
            self.assertFalse(response["result"]["isError"])
            self.assertFalse((self.groups_directory / (self.group_id + ".sqlite3")).exists())
            audit = main.sqlite3.connect(str(self.groups_directory / "observability.sqlite3"))
            try:
                self.assertEqual(
                    audit.execute(
                        "SELECT group_name FROM tool_call_group_names "
                        "JOIN tool_calls ON tool_calls.id = tool_call_group_names.tool_call_id "
                        "WHERE tool_calls.tool_name = 'delete_group'"
                    ).fetchone()[0],
                    "Release coordination",
                )
            finally:
                audit.close()
        finally:
            monitor.stop()

    def test_subscription_poll_interval_rejects_nonfinite_values(self):
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaisesRegex(ValueError, "finite"):
                main.GroupSubscriptionMonitor(self.store, value)

    def test_invalid_environment_poll_interval_uses_default(self):
        with patch.dict("os.environ", {"CROSSTALK_GROUPS_DIR": str(self.groups_directory), "CROSSTALK_POLL_INTERVAL_SECONDS": "inf"}), \
             patch("sys.stdin", StringIO("")), \
             patch("sys.argv", ["crosstalk-mcp"]), \
             patch.object(main, "GroupSubscriptionMonitor", wraps=main.GroupSubscriptionMonitor) as monitor:
            main.serve()
        self.assertEqual(monitor.call_args[0][1], main.DEFAULT_POLL_INTERVAL_SECONDS)

    def test_coordinator_routes_and_deduplicates_wakeups(self):
        fallback = self.store.send_message(self.group_id, "developer-1", "developer", "Please consider this")
        self.assertEqual(set(fallback["wakeup_targets"]), {"architect-1", "reader-1", "reviewer-1", "searcher-1"})
        self.assertEqual(len(self.store.wakeup_snapshot(self.group_id, "architect-1")["wakeups"]), 1)
        self.assertEqual(len(self.store.wakeup_snapshot(self.group_id, "reviewer-1")["wakeups"]), 1)

        mention = self.store.send_message(self.group_id, "developer-1", "developer", "@architect please review")
        self.assertEqual(mention["wakeup_targets"], ["architect-1"])
        explicit = self.store.send_message(self.group_id, "developer-1", "developer", "Review now", ["reviewer-1"])
        self.assertEqual(explicit["wakeup_targets"], ["reviewer-1"])
        self.assertEqual(len(self.store.wakeup_snapshot(self.group_id, "architect-1")["wakeups"]), 2)
        self.assertEqual(len(self.store.wakeup_snapshot(self.group_id, "reviewer-1")["wakeups"]), 2)
        partial = self.store.send_message(self.group_id, "developer-1", "developer", "@arch check this")
        self.assertEqual(partial["routing_reason"], "fallback")
        self.assertEqual(set(partial["wakeup_targets"]), {"architect-1", "reader-1", "reviewer-1", "searcher-1"})
        punctuation = self.store.send_message(self.group_id, "developer-1", "developer", "@architect!")
        self.assertEqual(punctuation["wakeup_targets"], ["architect-1"])
        end_of_message = self.store.send_message(self.group_id, "developer-1", "developer", "Please review @reviewer")
        self.assertEqual(end_of_message["wakeup_targets"], ["reviewer-1"])

    def test_invalid_explicit_wakeup_targets_use_normal_routing(self):
        fallback = self.store.send_message(
            self.group_id, "developer-1", "developer", "Please review this", ["unknown-context"]
        )
        self.assertEqual(fallback["routing_reason"], "fallback")
        self.assertEqual(
            set(fallback["wakeup_targets"]), {"architect-1", "reader-1", "reviewer-1", "searcher-1"}
        )
        mentioned = self.store.send_message(
            self.group_id, "developer-1", "developer", "@architect Please review this", ["unknown-context"]
        )
        self.assertEqual((mentioned["routing_reason"], mentioned["wakeup_targets"]), ("mentioned", ["architect-1"]))

    def test_retrieving_message_acknowledges_matching_wakeup(self):
        self.store.send_message(self.group_id, "developer-1", "developer", "@architect review this")
        first_notification = self.store.claim_wakeup_notification(self.group_id, "architect-1")
        self.assertEqual(first_notification["relevance"], "mentioned")
        self.store.get_unread_messages(self.group_id, "architect-1", "architect")
        wakeup = self.store.wakeup_snapshot(self.group_id, "architect-1")["wakeups"][0]
        self.assertIsNotNone(wakeup["acknowledged_at"])
        self.assertIsNone(self.store.claim_wakeup_notification(self.group_id, "architect-1"))

    def test_metadata_priority_and_mentions(self):
        self.store.update_group_metadata(self.group_id, "developer-1", name="Architecture", description="Design discussions")
        metadata = self.store.get_group_metadata(self.group_id)
        self.assertEqual((metadata["name"], metadata["description"]), ("Architecture", "Design discussions"))
        result = self.store.send_message(self.group_id, "developer-1", "developer", "urgent review", priority="urgent")
        self.assertEqual(result["mentions"], [])
        self.assertEqual(result["routing_reason"], "urgent")
        self.assertEqual(set(result["wakeup_targets"]), {"architect-1", "reader-1", "reviewer-1", "searcher-1"})
        message = self.store.get_all_messages(self.group_id, "reader-1", "reader")[0]
        self.assertEqual(message["priority"], "urgent")
        self.assertEqual(message["mentions"], [])
        self.assertEqual(set(message["wakeup_targets"]), {"architect-1", "reader-1", "reviewer-1", "searcher-1"})
        users = self.store.get_users(self.group_id)
        self.assertEqual([user["context_id"] for user in users], ["architect-1", "developer-1", "reader-1", "reviewer-1", "searcher-1"])

    def test_only_creator_can_update_group_metadata(self):
        with self.assertRaisesRegex(ValueError, "created this group"):
            self.store.update_group_metadata(self.group_id, "architect-1", name="Unauthorized")
        metadata = self.store.update_group_metadata(self.group_id, "developer-1", name="Authorized")
        self.assertEqual(metadata["name"], "Authorized")

    def test_join_and_leave_enforce_membership(self):
        with self.assertRaisesRegex(ValueError, "has not joined"):
            self.store.send_message(self.group_id, "outsider-1", "outsider", "Hello")
        self.store.join_group(self.group_id, "outsider-1", "outsider")
        self.store.send_message(self.group_id, "outsider-1", "outsider", "Hello")
        self.store.leave_group(self.group_id, "outsider-1", "outsider")
        database = self.groups_directory / (self.group_id + ".sqlite3")
        connection = main.sqlite3.connect(str(database))
        try:
            self.assertIsNone(connection.execute("SELECT 1 FROM group_members WHERE context_id = ?", ("outsider-1",)).fetchone())
        finally:
            connection.close()
        with self.assertRaisesRegex(ValueError, "has not joined"):
            self.store.get_all_messages(self.group_id, "outsider-1", "outsider")

    def test_member_actions_require_the_joined_name(self):
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.store.send_message(self.group_id, "developer-1", "architect", "Incorrect label")
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.store.get_all_messages(self.group_id, "developer-1", "architect")
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.store.leave_group(self.group_id, "developer-1", "architect")

    def test_malformed_string_inputs_return_validation_errors(self):
        cases = [
            ({"group_id": self.group_id, "context_id": None, "name": "developer", "message": "Hello"}, "context_id must be a non-empty string"),
            ({"group_id": self.group_id, "context_id": "developer-1", "name": 1, "message": "Hello"}, "name must be a non-empty string"),
            ({"group_id": self.group_id, "context_id": "developer-1", "name": "developer", "message": None}, "message must be a non-empty string"),
        ]
        for arguments, error in cases:
            result = main.call_tool(self.store, "send_message", arguments)
            self.assertTrue(result["isError"])
            self.assertIn(error, json.loads(result["content"][0]["text"])["error"])
        result = main.call_tool(self.store, "search_messages", {
            "group_id": self.group_id, "context_id": "developer-1", "name": "developer", "query": False,
        })
        self.assertTrue(result["isError"])
        self.assertIn("query must be a non-empty string", json.loads(result["content"][0]["text"])["error"])

    def test_json_rpc_error_shape(self):
        with patch("sys.stdout", new_callable=StringIO) as output:
            main.respond_error(7, -32601, "Method not found")
        response = json.loads(output.getvalue())
        self.assertEqual(response["id"], 7)
        self.assertEqual(response["error"], {"code": -32601, "message": "Method not found"})

    def test_stdio_enforces_lifecycle_and_receives_batches(self):
        requests = [
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            {"jsonrpc": "2.0", "id": 2, "method": "initialize", "params": {"protocolVersion": "unsupported-client-version"}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            [{"jsonrpc": "2.0", "id": 3, "method": "tools/list"}, {"jsonrpc": "2.0", "method": "notifications/initialized"}],
        ]
        input_text = "\n".join(json.dumps(request) for request in requests) + "\n"
        with patch.dict("os.environ", {"CROSSTALK_GROUPS_DIR": str(self.groups_directory)}), \
             patch("sys.stdin", StringIO(input_text)), patch("sys.stdout", new_callable=StringIO) as output, \
             patch("sys.argv", ["crosstalk-mcp"]):
            main.serve()
        responses = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(responses[0]["error"]["code"], -32002)
        self.assertEqual(responses[1]["id"], 2)
        self.assertEqual(responses[1]["result"]["protocolVersion"], main.MCP_PROTOCOL_VERSION)
        self.assertIsInstance(responses[2], list)
        self.assertEqual(responses[2][0]["id"], 3)
        self.assertIn("tools", responses[2][0]["result"])

    def test_stdio_rejects_batched_initialize_without_initializing(self):
        requests = [
            [{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": main.MCP_PROTOCOL_VERSION}}],
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            {"jsonrpc": "2.0", "id": 3, "method": "initialize", "params": {"protocolVersion": main.MCP_PROTOCOL_VERSION}},
        ]
        input_text = "\n".join(json.dumps(request) for request in requests) + "\n"
        with patch.dict("os.environ", {"CROSSTALK_GROUPS_DIR": str(self.groups_directory)}), \
             patch("sys.stdin", StringIO(input_text)), patch("sys.stdout", new_callable=StringIO) as output, \
             patch("sys.argv", ["crosstalk-mcp"]):
            main.serve()
        responses = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(responses[0]["error"]["code"], -32600)
        self.assertEqual(responses[1]["error"]["code"], -32002)
        self.assertEqual(responses[2]["id"], 3)

    def test_runtime_version_matches_version_file(self):
        self.assertEqual(main.SERVER_VERSION, (Path(__file__).resolve().parents[1] / "VERSION").read_text().strip())

    def test_transient_lock_retries_before_responding(self):
        database = self.groups_directory / (self.group_id + ".sqlite3")
        lock_connection = main.sqlite3.connect(str(database))
        lock_connection.execute("BEGIN IMMEDIATE")
        result = []
        worker = threading.Thread(target=lambda: result.append(main.call_tool(
            self.store, "send_message", {"group_id": self.group_id, "context_id": "developer-1", "name": "developer", "message": "Retry after lock"}
        )))
        worker.start()
        time.sleep(0.2)
        lock_connection.commit()
        lock_connection.close()
        worker.join(timeout=3)
        self.assertFalse(worker.is_alive())
        self.assertFalse(result[0]["isError"])

    def test_persistent_lock_returns_retryable_error(self):
        database = self.groups_directory / (self.group_id + ".sqlite3")
        lock_connection = main.sqlite3.connect(str(database))
        lock_connection.execute("BEGIN IMMEDIATE")
        original_timeout = main.SQLITE_LOCK_RETRY_SECONDS
        main.SQLITE_LOCK_RETRY_SECONDS = 0.05
        try:
            result = main.call_tool(
                self.store, "send_message", {"group_id": self.group_id, "context_id": "developer-1", "name": "developer", "message": "Locked"}
            )
        finally:
            main.SQLITE_LOCK_RETRY_SECONDS = original_timeout
            lock_connection.rollback()
            lock_connection.close()
        self.assertTrue(result["isError"])
        self.assertIn("group is busy", json.loads(result["content"][0]["text"])["error"])

    def test_joined_context_receives_wakeup_notification(self):
        notifications = []
        uri = main.GroupSubscriptionMonitor.wakeup_uri_for_group(self.group_id, "architect-1")
        with patch.object(main, "notify", side_effect=lambda method, params: notifications.append((method, params))):
            monitor = main.GroupSubscriptionMonitor(self.store, 0.01)
            try:
                monitor.authorize_wakeup(uri)
                monitor.subscribe(uri)
                self.store.send_message(self.group_id, "developer-1", "developer", "@architect Please review")
                deadline = time.monotonic() + 1
                while not notifications and time.monotonic() < deadline:
                    time.sleep(0.01)
            finally:
                monitor.stop()
        self.assertEqual(notifications[0][0], "notifications/resources/updated")
        self.assertEqual(notifications[0][1]["uri"], uri)
        deadline = time.monotonic() + 1
        last_notified_at = None
        while last_notified_at is None and time.monotonic() < deadline:
            last_notified_at = self.store.wakeup_snapshot(self.group_id, "architect-1")["wakeups"][0]["last_notified_at"]
            if last_notified_at is None:
                time.sleep(0.01)
        self.assertIsNotNone(last_notified_at)

    def test_join_and_leave_control_targeted_wakeup_authorization(self):
        context_id = "new-context-1"
        name = "new-context"
        uri = main.GroupSubscriptionMonitor.wakeup_uri_for_group(self.group_id, context_id)
        monitor = main.GroupSubscriptionMonitor(self.store, 0.1)
        ready_state = {"initialized": True, "client_ready": True}
        try:
            with self.assertRaisesRegex(ValueError, "not authorized"):
                monitor.subscribe(uri)
            before_join_read = main._handle_request({
                "jsonrpc": "2.0", "id": 1, "method": "resources/read", "params": {"uri": uri},
            }, self.store, monitor, ready_state)
            self.assertEqual(before_join_read["error"]["code"], -32602)
            joined = main.call_tool(self.store, "join_group", {
                "group_id": self.group_id, "context_id": context_id, "name": name,
            }, monitor)
            self.assertFalse(joined["isError"])
            self.assertTrue(monitor.is_wakeup_authorized(uri))
            after_join_read = main._handle_request({
                "jsonrpc": "2.0", "id": 1, "method": "resources/read", "params": {"uri": uri},
            }, self.store, monitor, ready_state)
            self.assertEqual(after_join_read["result"]["contents"][0]["uri"], uri)
            unsubscribe = main._handle_request({
                "jsonrpc": "2.0", "id": 1, "method": "resources/unsubscribe", "params": {"uri": uri},
            }, self.store, monitor, ready_state)
            self.assertEqual(unsubscribe["result"], {})
            self.assertFalse(monitor.is_wakeup_authorized(uri))
            connection = self.store._group_connection(self.group_id)
            try:
                self.assertIsNone(connection.execute(
                    "SELECT 1 FROM group_members WHERE context_id = ?", (context_id,)
                ).fetchone())
            finally:
                connection.close()
            with self.assertRaisesRegex(ValueError, "not authorized"):
                monitor.subscribe(uri)
            after_leave_read = main._handle_request({
                "jsonrpc": "2.0", "id": 1, "method": "resources/read", "params": {"uri": uri},
            }, self.store, monitor, ready_state)
            self.assertEqual(after_leave_read["error"]["code"], -32602)
        finally:
            monitor.stop()

    def test_join_supports_a_context_id_with_a_slash(self):
        context_id = "/root"
        uri = main.GroupSubscriptionMonitor.wakeup_uri_for_group(self.group_id, context_id)
        monitor = main.GroupSubscriptionMonitor(self.store, 0.1)
        try:
            joined = main.call_tool(self.store, "join_group", {
                "group_id": self.group_id, "context_id": context_id, "name": "Frrank",
            }, monitor)
            self.assertFalse(joined["isError"])
            self.assertEqual(json.loads(joined["content"][0]["text"])["wakeup_resource_uri"], uri)
            self.assertTrue(monitor.is_wakeup_authorized(uri))
            self.assertEqual(
                main.GroupSubscriptionMonitor.wakeup_from_uri(uri),
                {"group_id": self.group_id, "context_id": context_id},
            )
            self.assertEqual(
                [user["context_id"] for user in self.store.get_users(self.group_id) if user["context_id"] == context_id],
                [context_id],
            )
        finally:
            monitor.stop()

    def test_leaving_group_stops_wakeup_delivery(self):
        notifications = []
        uri = main.GroupSubscriptionMonitor.wakeup_uri_for_group(self.group_id, "architect-1")
        with patch.object(main, "notify", side_effect=lambda method, params: notifications.append((method, params))):
            monitor = main.GroupSubscriptionMonitor(self.store, 0.01)
            try:
                monitor.authorize_wakeup(uri)
                monitor.subscribe(uri)
                self.store.leave_group(self.group_id, "architect-1", "architect")
                monitor.unsubscribe(uri)
                self.store.send_message(self.group_id, "developer-1", "developer", "Message after leave")
                time.sleep(0.05)
            finally:
                monitor.stop()
        self.assertEqual(notifications, [])

    def test_monitor_retains_group_subscription_during_transient_lock(self):
        uri = main.GroupSubscriptionMonitor.uri_for_group(self.group_id)
        monitor = main.GroupSubscriptionMonitor(self.store, 0.01)
        try:
            monitor.subscribe(uri)
            with patch.object(self.store, "group_update_snapshot", side_effect=main.sqlite3.OperationalError("database is locked")):
                time.sleep(0.05)
            with monitor._lock:
                self.assertIn(uri, monitor._subscriptions)
        finally:
            monitor.stop()

    def test_monitor_removes_subscription_after_group_deletion(self):
        notifications = []
        uri = main.GroupSubscriptionMonitor.uri_for_group(self.group_id)
        with patch.object(main, "notify", side_effect=lambda method, params: notifications.append((method, params))):
            monitor = main.GroupSubscriptionMonitor(self.store, 0.01)
            try:
                monitor.subscribe(uri)
                self.store.delete_group(self.group_id, "developer-1")
                deadline = time.monotonic() + 1
                while not notifications and time.monotonic() < deadline:
                    time.sleep(0.01)
                with monitor._lock:
                    self.assertNotIn(uri, monitor._subscriptions)
            finally:
                monitor.stop()
        self.assertEqual(notifications, [("notifications/resources/updated", {"uri": uri, "relevance": "deleted"})])

    def test_wakeup_write_failure_does_not_mark_notification_delivered(self):
        uri = main.GroupSubscriptionMonitor.wakeup_uri_for_group(self.group_id, "architect-1")
        monitor = main.GroupSubscriptionMonitor(self.store, 0.01)
        try:
            monitor.authorize_wakeup(uri)
            monitor.subscribe(uri)
            self.store.send_message(self.group_id, "developer-1", "developer", "@architect review")
            with patch.object(main, "notify", side_effect=OSError("stdout unavailable")):
                time.sleep(0.05)
                wakeup = self.store.wakeup_snapshot(self.group_id, "architect-1")["wakeups"][0]
                self.assertIsNone(wakeup["last_notified_at"])
        finally:
            monitor.stop()

    def test_group_change_write_failure_preserves_subscription_cursor(self):
        uri = main.GroupSubscriptionMonitor.uri_for_group(self.group_id)
        monitor = main.GroupSubscriptionMonitor(self.store, 0.01)
        try:
            monitor.subscribe(uri)
            self.store.send_message(self.group_id, "developer-1", "developer", "A group update")
            with patch.object(main, "notify", side_effect=OSError("stdout unavailable")):
                time.sleep(0.05)
            with monitor._lock:
                self.assertEqual(monitor._subscriptions[uri]["latest_id"], 0)
            notifications = []
            with patch.object(main, "notify", side_effect=lambda method, params: notifications.append((method, params))):
                deadline = time.monotonic() + 1
                while not notifications and time.monotonic() < deadline:
                    time.sleep(0.01)
            self.assertEqual(notifications[0][1]["relevance"], "group_change")
            with monitor._lock:
                self.assertEqual(monitor._subscriptions[uri]["latest_id"], 1)
        finally:
            monitor.stop()


if __name__ == "__main__":
    unittest.main()
