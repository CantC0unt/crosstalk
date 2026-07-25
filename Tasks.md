# Crosstalk Observability Implementation Tasks

Priorities: **P0** = required for a safe/useful first release; **P1** = required for a complete polished v1, after the core vertical slice; **P2** = intentional later enhancement.

## A. Repository, packaging, and command surface

| ID | Priority | Task | Blocked by / depends on |
|---|---|---|---|
| A1 | P0 | Restructure the repository into root metadata plus `src/main.py`, `src/mcp.py`, `src/observe.py`, and root `tests/test_main.py`, `tests/test_mcp.py`, and `tests/test_observe.py`, preserving package name and existing install behavior. | None |
| A2 | P0 | Update root packaging configuration to include the MCP and observer modules in one dependency-free distribution. | A1 |
| A3 | P0 | Preserve no-argument `crosstalk-mcp` as stdio MCP startup. Add `observe` dispatch without affecting existing MCP clients. | A2 |
| A4 | P0 | Parse and validate `observe --silent --port --poll-interval --groups-dir`; add useful help/error text. | A3 |
| A5 | P1 | Add package/CLI regression tests: imports, installed entry point, no-argument server path, observer dispatch, and invalid options. | A2, A3, A4 |

## B. Observability configuration and storage

| ID | Priority | Task | Blocked by / depends on |
|---|---|---|---|
| B1 | P0 | Implement strict parsing and startup validation for `CROSSTALK_OBSERVABILITY_RETENTION_DAYS`: unset disables auditing, `inf` enables unlimited retention, and a positive integer enables bounded retention. | A1 |
| B2 | P0 | Implement `<groups_dir>/observability.sqlite3`: WAL, schema version, metadata table, `tool_calls`, required indexes, and incremental auto-vacuum setup. | B1 |
| B3 | P0 | Treat the v1 observability schema as the initial implementation: no migration exists yet. Any future observability schema change must be explicit, additive, and never alter `grp_*.sqlite3` or perform destructive automatic migration. | B2 |
| B4 | P0 | Define the audit event model: all required fields, semantic per-tool actor extraction (do not confuse group-metadata `name` with caller name), documented safe `details_json` allowlists/2 KB cap, and fixed error-category mapping. | B2 |
| B5 | P0 | Instrument the MCP `tools/call` path to attempt exactly one completed audit event for every success/failure, including monotonic duration and JSON-RPC request ID when available. | B4 |
| B6 | P0 | Make audit writes best effort: bounded lock handling, drop-on-failure with no retry backlog, and complete isolation from tool result/delivery behavior. | B5 |
| B7 | P1 | Implement once-per-day, bounded-batch retention cleanup and update cleanup metadata. Ensure unlimited retention runs no cleanup query. | B1, B2 |
| B8 | P1 | Test schema creation/migration, every-tool-call versus no-protocol-housekeeping records, success/error records, semantic identity/name extraction (including group-metadata name collisions), safe/capped details, audit-write failure isolation, concurrent writers, and retention. | B3, B4, B5, B6, B7 |

## C. Read-only observer data layer

| ID | Priority | Task | Blocked by / depends on |
|---|---|---|---|
| C1 | P0 | Implement groups-directory resolution: CLI flag, then environment, then existing default; tolerate a missing directory as an empty read-only state without creating it. | A4 |
| C2 | P0 | Implement observer SQLite helpers using URI read-only mode and `PRAGMA query_only = ON`, separate from MCP writer helpers. | C1 |
| C3 | P0 | Implement group discovery/read models for metadata, members, messages, unread counts, latest activity, and wakeup state including creation/acknowledgement/last-notified timestamps. | C2 |
| C4 | P0 | Implement cursor-based chat history: newest page and older-than-message-ID page with bounded limits. | C3 |
| C5 | P0 | Implement raw-audit read models only: activation metadata, filtered tool calls, grouped/time-bucket analytics, and duration values for percentiles; do not add aggregate/cache tables. | C2, B2 |
| C6 | P1 | Implement deleted-group behavior: keep analytics, label missing group data deleted, never recreate files. | C3, C5 |
| C7 | P1 | Test that observer reads do not mutate state; test pagination, locked/missing files, and graceful additive-schema degradation. | C2, C3, C4, C5, C6 |

## D. Local HTTP server and safety

| ID | Priority | Task | Blocked by / depends on |
|---|---|---|---|
| D1 | P0 | Implement standard-library `ThreadingHTTPServer`, loopback-only binding, lifecycle, and graceful Ctrl-C shutdown. | A3 |
| D2 | P0 | Implement default-port behavior: 8787 first, OS-selected port fallback when unspecified, and explicit-port failure. | D1 |
| D3 | P0 | Implement default browser launch, `--silent`, and headless/browser-launch fallback. | D2, A4 |
| D4 | P0 | Serve pages, project-owned static assets, and same-origin API/HTML-fragment endpoints with no CORS headers. | D1 |
| D5 | P1 | Implement per-process CSRF token issuance/validation for all maintenance write endpoints. | D4 |
| D6 | P1 | Test loopback binding, ports, browser fallback, no-CORS behavior, and CSRF rejection. | D1, D2, D3, D4, D5 |

## E. Polling and real-time transport

| ID | Priority | Task | Blocked by / depends on |
|---|---|---|---|
| E1 | P0 | Implement finite-positive observer polling configuration with 0.5-second default. | C1, D1 |
| E2 | P0 | Implement lightweight change detection: group-file discovery, latest message/audit markers, metadata timestamp, and deterministic compact member/unread and wakeup-state fingerprints. | C3, C5, E1 |
| E3 | P0 | Implement same-origin SSE and fan-out for compact `message.created`, `member.changed`, `wakeup.changed`, and `tool_call.completed` events. | D4, E2 |
| E4 | P1 | Define/implement snapshot-on-reconnect and duplicate-safe browser event behavior. | E3, C4, C5 |
| E5 | P1 | Test timing, concurrent writers, temporary locks, reconnects, and no duplicate live messages. | E2, E3, E4, B8 |

## F. Dashboard UI

| ID | Priority | Task | Blocked by / depends on |
|---|---|---|---|
| F1 | P0 | Create server-rendered base layout, styling, and bounded empty/error states within package-size budget. | D4 |
| F2 | P0 | Add exact pinned HTMX/Alpine CDN URLs with Subresource Integrity hashes. Do not vendor them or add a service worker. | F1 |
| F3 | P0 | Build Chats: group picker, newest 100 messages, HTMX cursor pagination, group/member details, escaped content, indicators, and a windowed/virtualized or pruned history DOM. Keep HTMX to fragments/pagination rather than live-stream DOM replacement. | C3, C4, F1, F2 |
| F4 | P0 | Build native `EventSource` live-chat client that uses compact event identifiers to obtain escaped visible-message fragments and append them without replacing an unbounded container. | E3, F3 |
| F5 | P1 | Build Overview: current groups, activity feed, key counts, audit-disabled notice, message priority/routing breakdowns, and wakeup responsiveness. | C3, C5, F1 |
| F6 | P1 | Build Tool Analytics: common filters, a default recent-24-hour range, grouped/time data, Python p50/p95, and native SVG charts. | C5, F1 |
| F7 | P1 | Build Storage view: size, rows, retention/activation/cleanup status, reclaimable space, and maintenance control. | B2, B7, D5, F1 |
| F8 | P1 | Implement bounded incremental-vacuum operation against `observability.sqlite3` only, with busy handling and UI status. Never use full `VACUUM`. | F7 |
| F9 | P1 | Add browser/UI tests for pagination, live append, filters, disabled audit, empty/deleted groups, escaping, and storage/CSRF. | F3, F4, F5, F6, F7, F8 |

## G. Documentation, release, and quality gates

| ID | Priority | Task | Blocked by / depends on |
|---|---|---|---|
| G1 | P0 | Update root README: local trust boundary, command usage, browser behavior, port fallback, and groups-directory resolution. | A1, A3, A4, D2, D3 |
| G2 | P0 | Document audit opt-in, database location, unlimited default retention, privacy rules, and historical analytics start time. | B1, B4, B7 |
| G3 | P1 | Document CDN details: pinned/SRI assets, ordinary asset-request privacy boundary, normal caching, no service worker, and first-load/offline limitation. | F2 |
| G4 | P1 | Document observability schema as an additive public read contract and older-observer degradation. | B3, C7 |
| G5 | P1 | Run full regressions and package-size/content checks before release. | A5, B8, C7, D6, E5, F9, G1, G2, G3, G4 |

## H. Deferred work

| ID | Priority | Task | Blocked by / depends on |
|---|---|---|---|
| H1 | P2 | Add cached daily aggregates if all-time raw analytics become too slow. | C5, F6 |
| H2 | P2 | Add canvas/downsampling for very dense time-series charts. | F6 |
| H3 | P2 | Add explicit, confirmed immediate audit-history deletion, separate from retention and reclaiming free pages. | F7, F8 |
| H4 | P2 | Revisit remote/team access, authentication, authorization, and an async backend only if scope expands beyond single-user localhost use. | D1, F9 |
