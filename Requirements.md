# Crosstalk Observability Dashboard Requirements

## 1. Purpose and scope

Crosstalk shall provide a local, single-user observability dashboard for real-time human monitoring of group chats and analytics of MCP tool usage.

It is an extension of the existing package, not a replacement for the stdio MCP server.
The dashboard is a separate local process: it does not attach to or communicate with a running stdio process. Crosstalk and the dashboard coordinate solely through the shared groups directory and SQLite files.

In scope:

- Live, read-only visibility of group chats, members, unread state, and wakeup state.
- Exact, opt-in auditing of every MCP `tools/call` invocation.
- Local analytics from audit data.
- A browser dashboard started with `crosstalk-mcp observe`.
- Configurable audit retention and an explicit free-space reclamation control.

Out of scope for v1:

- Remote/network serving, accounts, roles, SSO, public access, or multi-user collaboration.
- Changing MCP transport or client protocol.
- External telemetry or copying message content into audit data.
- Reconstructing tool calls from before auditing was enabled.
- A third-party charting library.

## 2. Repository and packaging

The project shall be reorganized as:

```text
crosstalk/
  pyproject.toml
  VERSION
  LICENSE
  README.md
  src/
    main.py
    mcp.py
    observe.py
  tests/
    test_main.py
    test_mcp.py
    test_observe.py
```

- `src/mcp.py` contains existing MCP code plus only the minimal audit/dispatch hooks it requires.
- `src/observe.py` contains all dashboard HTTP, read-model, SSE, templates, styles, and browser code.
- Root metadata describes the whole project.
- One dependency-free Python distribution is published. There is no observer add-on and no `[observe]` extra.
- Observer-specific code/assets target less than 250 KB installed and roughly 40--120 KB compressed contribution to the wheel.

## 3. Commands and configuration

### Commands

Existing behavior is preserved:

```sh
crosstalk-mcp
```

starts the MCP stdio server.

The dashboard command is:

```sh
crosstalk-mcp observe [--silent] [--port PORT] [--poll-interval SECONDS] [--groups-dir PATH]
```

- No options after `observe` starts the dashboard and opens the browser.
- `--silent` suppresses browser launch only; the URL and warnings still print.
- `--port PORT` binds exactly that loopback port and fails clearly if it is unavailable.
- Without `--port`, try `127.0.0.1:8787`. If busy, bind `127.0.0.1:0`, let the OS choose an available port, warn with the chosen URL, and open it.
- `--poll-interval` is a finite positive number of seconds, defaults to `0.5`, and affects observer refresh only.
- `--groups-dir` takes precedence over environment/default directory.
- If browser launch fails (for example, headless use), leave the server running and print the URL and warning.

### Groups-directory resolution

Resolve in this exact order:

1. `--groups-dir PATH`
2. `CROSSTALK_GROUPS_DIR`
3. Existing default: `~/.cache/crosstalk`

This is necessary because an MCP client's configured environment may not be present in the terminal that starts the dashboard.

### Audit configuration

Auditing is disabled by default. Enable it with one of:

```text
CROSSTALK_OBSERVABILITY_RETENTION_DAYS=inf
CROSSTALK_OBSERVABILITY_RETENTION_DAYS=<positive integer>
```

When enabled, Crosstalk writes to:

```text
<CROSSTALK_GROUPS_DIR>/observability.sqlite3
```

- Unset retention disables auditing.
- `inf` enables auditing with unlimited retention; no cleanup query runs.
- A positive integer enables auditing and removes rows older than that many days.
- Invalid observability settings fail server startup clearly; they never silently enable/disable auditing or delete data.
- The dashboard starts normally with auditing disabled, displays chat/state data, and shows a non-blocking instruction for enabling analytics. It never tries to enable auditing itself.

## 4. Storage and audit data

### Group databases

The observer treats every `grp_*.sqlite3` file as strictly read-only. It must not call Crosstalk read tools because those alter unread and wakeup state.

Direct reads support display of group metadata, membership, messages, routing/wakeup information, unread counts, latest member activity, and pending/acknowledged wakeups.

### Observability database

`observability.sqlite3` is a global database shared by MCP processes using the same groups directory. It is separate from group databases.

- Crosstalk writes audit events; the observer normally opens it read-only.
- The only observer write exception is explicit maintenance of this database.
- Use SQLite WAL mode, `PRAGMA user_version`, and incremental auto-vacuum.
- Maintain a one-row `metadata` table with at least `schema_version`, `created_at`, `audit_enabled_at`, and `last_retention_cleanup_at`.

The `tool_calls` table shall include:

```text
id INTEGER PRIMARY KEY
occurred_at TEXT NOT NULL
audit_request_id TEXT NULL
tool_name TEXT NOT NULL
group_id TEXT NULL
context_id TEXT NULL
name TEXT NULL
outcome TEXT NOT NULL
duration_ms INTEGER NOT NULL
result_count INTEGER NULL
error_category TEXT NULL
details_json TEXT NULL
```

`name` is the caller label at call time; `context_id` remains the stable identity. Index normal time/filter access: time, tool/time, group/time, context/time, and outcome/time.

For v1, `tool_calls` is the sole analytics source of truth. Do not create precomputed aggregate/cache tables; add them only later if measured all-time queries require them.

Schema changes must be additive where possible. Observers inspect available columns, ignore unknown optional fields, and show a clear metric-unavailable state if a required older field is absent. Required field removal/rename is a deliberate breaking schema change.
Only explicit forward observability migrations are permitted. V1 has no destructive automatic migration; if a future destructive migration is ever required, it must be separately designed with a visible warning and timestamped local backup.

### Tool-call events

Write exactly one completed audit row for every MCP `tools/call` request, successful or failed. Do not audit protocol housekeeping such as initialization, tool/resource listing, subscriptions, or notifications.

Fields not applicable to a tool (for example, `group_id`, `context_id`, or `name` on `list_groups`) remain `NULL`; analytics must handle those rows rather than inventing a caller identity.
Identity extraction is semantic, not a naïve argument-name copy: the audit `name` field is populated only when a tool's `name` argument is the caller identity. For example, `create_group(name=...)` and `update_group_metadata(name=...)` use `name` as group metadata, so they must not populate the actor-name field. A newly created group's returned ID may populate that event's `group_id`/safe details after success.

- `occurred_at`: UTC ISO-8601 timestamp captured at tool-handling start.
- `duration_ms`: monotonic-clock elapsed time through validation, SQLite work, and normal retry.
- `audit_request_id`: JSON-RPC request ID when available.
- `outcome`: `success` or `error`.
- Successful rows have `error_category = NULL`; errors use only `validation`, `not_found`, `authorization`, `sqlite_busy`, or `internal`.
- Audit after tool handling completes and before returning the normal result.
- Auditing is best effort: a busy/failed audit write must not change tool behavior, materially delay it, or prevent messaging.
- After the bounded attempt fails, drop that audit event; do not create an in-memory retry queue or backlog in the MCP request path.

`details_json` is a documented per-tool allowlist of small result facts, for example `message_id`, priority, routing reason, and returned counts. It must never include message bodies, search queries, descriptions, arbitrary inputs, raw errors, or stack traces. Cap serialized details at 2 KB per row.

The v1 allowlist is: created group ID; group/member/message counts; changed metadata field names; completion booleans; and a sent message's ID, priority, routing reason, and wakeup-target count. It never records metadata values, names, context IDs, recipient IDs, or query/message content.

### Retention and lifecycle

When retention is configured, each MCP process checks at most once a day and removes expired rows in bounded batches. No cleanup query runs under unlimited retention.
Retention cleanup frees SQLite pages for reuse but does not automatically compact the file; disk-space reclamation remains an explicit dashboard maintenance action.

The dashboard displays file size, row count, retention setting, activation time, last cleanup time, and reclaimable space when available. Existing chat history is immediately browseable through pagination, but tool analytics begin only at audit activation time. If a group is deleted, retained analytics remain visible and are labelled deleted; its chat data is unavailable.

## 5. Observer backend and live updates

The backend uses Python standard library only: `http.server`/`ThreadingHTTPServer`, `sqlite3`, `threading`, `json`, `pathlib`, and related modules. There are no Python runtime dependencies.

- Bind only to `127.0.0.1`; v1 has no network-facing `--host`.
- Ordinary observer database connections use SQLite URI read-only mode plus `PRAGMA query_only = ON` and never reuse MCP writer helpers.
- Multiple MCP processes may share the audit database; WAL serialization is sufficient and no coordination service is required.
- On temporary read locks/errors, show a warning/retry rather than crash.
- If the resolved groups directory does not exist yet, do not create it; start with an empty-state dashboard and continue checking for it on subsequent polls.

The observer polls at the configured interval. Each cycle only discovers group files and checks lightweight state markers: latest message ID, audit-row ID, metadata update timestamp, and deterministic compact fingerprints for member/unread state and wakeup state. The fingerprints are necessary because joining/leaving, changing a member name, reading a message, or acknowledging a wakeup can change state without creating a new message or wakeup-event ID. It fetches full rows only after a relevant marker changes.

A same-origin SSE endpoint publishes compact JSON events, for example:

```json
{"type":"message.created","group_id":"grp_...","message_id":42}
```

On reconnect/restart, the browser loads a normal snapshot first. Events must be duplicate-safe.
The event vocabulary includes at least `message.created`, `member.changed`, `wakeup.changed`, and `tool_call.completed`.
Live event payloads carry identifiers/state hints, not arbitrary message content. For a visible new message, client code obtains a server-rendered, HTML-escaped message fragment before appending it to the chat timeline.

## 6. Browser UI

The local server provides server-rendered HTML.

- HTMX and Alpine.js load from exact pinned CDN URLs with Subresource Integrity hashes.
- They are not vendored in the package; normal browser caching is used.
- Because no third-party browser-library copy is distributed in the package, no separate bundled-library license file is required for HTMX/Alpine. Crosstalk's own MIT license remains the project license.
- No service worker/offline layer is required. An uncached offline browser may fail to load these UI libraries; that is accepted for v1.
- The initial uncached asset request reveals ordinary browser/network metadata to the selected CDN, but the observer sends no group messages, audit rows, or analytics payloads to it.
- Small project JavaScript handles live chat behavior and native SVG/canvas charts.
- Do not include React, FastAPI, Uvicorn, a chart library, or any Node.js/npm/Vite build-time or runtime requirement.

Views:

1. **Overview**: live activity, active groups, message/tool volume, error rate, active contexts, message priority/routing breakdowns, and wakeup responsiveness.
2. **Chats**: group picker, group/member data, chronological messages, routing/wakeup details, and live indicators.
3. **Tool analytics**: calls by tool/group/context/name, outcomes, latency, and time filters.
4. **Storage**: audit storage/retention status and maintenance control.

These are the initial v1 views. Their layout and exact endpoint/fragment shape are not a stable public API and may evolve without changing the audit-storage contract.

Analytics filter names are `from`, `to`, `group_id`, `context_id`, `tool_name`, and `outcome`. Analytics default to the most recent 24 hours; users may widen the range deliberately.

Chats initially load the newest 100 messages. Older content uses a message-ID cursor, not offsets. The DOM must remain bounded even after extensive scrolling: use a windowed/virtualized list or prune off-screen rows while retaining cursor position. Append relevant live messages only if their group is open; otherwise update an activity indicator.

HTMX owns server-rendered page fragments, filters, and older-history loading. Alpine.js owns small local UI state such as menus, tabs, and time filters. Native `EventSource` plus project JavaScript owns live event handling; HTMX's SSE extension is not required.

Use SQLite for counts, grouping, and time buckets. Compute portable `p50`/`p95` latency in Python from the selected bounded range. Use SVG first; canvas/downsampling can be added for dense future series.

Chat analytics are derived directly from group databases and include message volume over time, activity by sender/context, priority and routing-reason distribution, pending/acknowledged wakeups, and wakeup response time when both event timestamps are available. The chat view exposes wakeup creation, acknowledgement, and last-notified state where available.

## 7. Security and storage maintenance

This is a no-login, single-user local tool, but it must be safe against browser-origin attacks.

It does not create a new access-control boundary: anyone/process with equivalent local-account, groups-directory, or loopback access is within the existing trusted-local boundary and may be able to inspect the same plaintext information.

- Emit no permissive CORS headers; pages, API, and SSE are same-origin only.
- Escape all user-generated message content in HTML fragments.
- Do not expose arbitrary database-query endpoints.
- Issue a random per-start CSRF token in dashboard HTML and require it in a custom header for maintenance writes.

The Storage view has a **Reclaim free space** operation for `observability.sqlite3` only.

- It does not delete retained audit history.
- It runs bounded `PRAGMA incremental_vacuum` work only; never full `VACUUM` in v1.
- It never touches a group database.
- If busy, report a retry-later message and do not aggressively retry or disrupt MCP traffic.
- Any future immediate audit-history deletion is a separate, confirmed operation.

## 8. Documentation requirements

The root README shall document: the local-only trust boundary; enabling/disabling auditing; unlimited default retention; custom groups directory resolution; dashboard invocation/options; default browser behavior; port fallback; audit privacy; historical analytics cutoff; storage maintenance; and CDN/caching/offline behavior.

## 9. Acceptance criteria

1. `crosstalk-mcp` without arguments still starts the stdio MCP server.
2. Disabled auditing creates no observability database and leaves tool behavior unchanged.
3. Enabled auditing records one safe completed row for every successful/failed `tools/call`.
4. Audit write failure cannot turn a successful tool call into an error.
5. Audit rows never contain raw message/search/argument/error/stack-trace data.
6. Unlimited retention never removes rows; configured retention removes only expired rows and runs at most once daily per process.
7. `crosstalk-mcp observe` is loopback-only, opens a browser by default, and `--silent` suppresses only that launch.
8. An occupied explicit port fails; an occupied default port selects an OS port and prints a warning/actual URL.
9. Directory resolution follows flag, environment, then default precedence.
10. Observer reads never mutate group messages, unread IDs, wakeups, metadata, or schema.
11. New messages/audit rows and membership, unread, wakeup, and metadata-only state changes appear within one poll interval plus normal local rendering.
12. Chat pagination has no omissions/duplicates and avoids unbounded first-load/DOM work.
13. Empty, auditing-disabled, missing/deleted-group, and temporarily locked-database states are visible and do not crash the observer.
14. Maintenance rejects invalid CSRF tokens, never changes group files, and handles a busy audit database gracefully.
15. Cross-origin pages cannot read dashboard data or invoke maintenance through CORS.
16. The distribution has no non-standard-library Python runtime dependency and stays within the observer size budget.
17. Audit actor identity is not incorrectly populated from group-metadata `name` arguments, and message analytics include priority/routing and wakeup-state/responsiveness data.
18. MCP protocol housekeeping requests create no audit rows; every completed `tools/call` does.
19. `details_json` follows the tool allowlist, remains valid JSON within its 2 KB cap, and contains no suppressed fields.
20. Invalid observer options/settings fail clearly, and observing a missing groups directory neither creates it nor prevents a later-created directory from appearing.
21. Dashboard HTML uses exact CDN versions with valid SRI attributes; the wheel contains no HTMX/Alpine copies, Node build output, or non-standard-library Python dependency.
