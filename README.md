<div align="center">

# Crosstalk MCP Server

A dependency-free [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for exchanging messages between separate AI contexts.

[![CI](https://github.com/CantC0unt/crosstalk/actions/workflows/ci.yml/badge.svg)](https://github.com/CantC0unt/crosstalk/actions/workflows/ci.yml)
[![CodeQL](https://github.com/CantC0unt/crosstalk/actions/workflows/codeql.yml/badge.svg)](https://github.com/CantC0unt/crosstalk/actions/workflows/codeql.yml)
[![PyPI](https://img.shields.io/pypi/v/crosstalk-mcp-server.svg)](https://pypi.org/project/crosstalk-mcp-server/)
[![Python](https://img.shields.io/pypi/pyversions/crosstalk-mcp-server.svg)](https://pypi.org/project/crosstalk-mcp-server/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

</div>

Crosstalk stores each group in its own SQLite database, making it useful for coordinating agents, sessions, or tools that share a filesystem.

> **Trust boundary:** Crosstalk is for trusted collaborators only. Its SQLite data is plaintext, and `context_id` values are caller-provided rather than authenticated. See [Security](#security) before using it with sensitive or untrusted workloads.

## Highlights

- Dependency-free Python 3.9+ server using MCP JSON-RPC over standard input/output
- Persistent, shared group message history backed by SQLite
- Per-context unread tracking, message search, cursors, and full-history retrieval
- Targeted wakeup hints through explicit recipients, `@context_id`, `@name`, or `@all` mentions
- MCP resource subscriptions for group changes and targeted wakeup notifications

## Quick start

Install from PyPI:

```sh
python3 -m pip install --user --upgrade crosstalk-mcp-server
```

To install from a checkout instead:

```sh
python3 -m pip install --user --upgrade .
```

Confirm that the command is available:

```sh
command -v crosstalk-mcp
```

Then add the server to your MCP client. For Codex, put this in `~/.codex/config.toml`:

```toml
[mcp_servers.crosstalk]
command = "crosstalk-mcp"

[mcp_servers.crosstalk.env]
# Omit this table to use ~/.cache/crosstalk.
CROSSTALK_GROUPS_DIR = "/absolute/path/to/shared/crosstalk-groups"
# Optional; defaults to 0.5 seconds.
CROSSTALK_POLL_INTERVAL_SECONDS = "0.5"
```

Restart Codex after changing its configuration. To enable Crosstalk for one trusted repository only, place the same configuration in that repository’s `.codex/config.toml`.

For JSON-based MCP clients, use [examples/mcp.example.json](examples/mcp.example.json). A standalone Codex configuration is available at [examples/mcp.example.toml](examples/mcp.example.toml).

## Installation

### macOS and Linux

The portable installation command is:

```sh
python3 -m pip install --user --upgrade .
```

Alternatively, `make install` installs the package and, for zsh, bash, or fish, adds Python’s user-script directory to the relevant shell startup file when necessary. It requires `rsync`. Restart your shell and MCP client after installing.

If `crosstalk-mcp` is not on `PATH`, use absolute paths to run the checkout directly:

```toml
[mcp_servers.crosstalk]
command = "/absolute/path/to/python3"
args = ["/absolute/path/to/crosstalk/src/main.py"]
```

### Windows PowerShell

```powershell
py -m pip install --user --upgrade .
Get-Command crosstalk-mcp
```

If PowerShell cannot find the command after opening a new window, add Python’s user scripts directory to your user `Path`:

```powershell
$scriptsDir = "$(py -m site --user-base)\Scripts"
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if (($userPath -split ';') -notcontains $scriptsDir) {
    [Environment]::SetEnvironmentVariable("Path", "$userPath;$scriptsDir", "User")
}
```

To run directly from a checkout instead, configure absolute Windows paths:

```toml
[mcp_servers.crosstalk]
command = 'C:\absolute\path\to\python.exe'
args = ['C:\absolute\path\to\crosstalk\src\main.py']
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `CROSSTALK_GROUPS_DIR` | `~/.cache/crosstalk` | Shared directory that holds one `<group_id>.sqlite3` database per group. |
| `CROSSTALK_POLL_INTERVAL_SECONDS` | `0.5` | Subscription polling interval. Use a larger value such as `2` to reduce database activity. Invalid, non-positive, and non-finite values use the default. |

All participants must use the same accessible `CROSSTALK_GROUPS_DIR`. The default works for server processes under the same user account; configure a shared writable directory for containers, separate users, or machines.

The server creates its default directory automatically. Each database records its schema version for inspection; automatic migration of old layouts is intentionally not supported.

## How it works

Create a group, then have every participating context join before it sends or retrieves messages:

```text
create_group(context_id="developer-1", name="Cache redesign", description="Coordinate the cache work")
→ {"group_id": "grp_<id>", "name": "Cache redesign", ...}

join_group(group_id="grp_<id>", context_id="developer-1", name="developer")
join_group(group_id="grp_<id>", context_id="architect-1", name="architect")

send_message(
  group_id="grp_<id>",
  context_id="developer-1",
  name="developer",
  message="@architect Please review the cache invalidation design.",
  priority="high"
)
→ {"message_id": 1, "routing_reason": "mentioned", "wakeup_targets": ["architect-1"], ...}

get_unread_messages(group_id="grp_<id>", context_id="architect-1", name="architect")
```

`context_id` is the stable, unique identity for an AI or session; `name` is its human-readable role or label. For Codex, use the thread ID exposed in `CODEX_THREAD_ID` as `context_id`, so the identity remains stable when the thread resumes. Do not use a display label such as `architect` as the ID.

Creating a group does **not** join its creator. `join_group` is idempotent and should be called whenever a context connects or reconnects to a server process. Leaving a group removes the membership, unread state, and pending wakeups for that context.

## Tools

| Tool | Description |
| --- | --- |
| `create_group(context_id, name?, description?)` | Create a group. The creator owns metadata updates and deletion. |
| `list_groups` / `get_group_metadata(group_id)` | Discover groups and read their metadata. |
| `update_group_metadata(group_id, context_id, name?, description?)` | Update metadata as the creator. |
| `get_users(group_id)` | List current members. |
| `join_group(group_id, context_id, name)` / `leave_group(...)` | Manage membership and wakeup registration. |
| `send_message(group_id, context_id, name, message, priority?, wake_context_ids?)` | Send a message and optionally route a wakeup hint. |
| `get_unread_messages(...)` | Retrieve messages not previously retrieved by the caller. |
| `get_all_messages(...)` / `get_latest_messages(...)` | Retrieve full history or messages after the caller’s latest sent message. |
| `get_messages_after(..., after_message_id)` | Retrieve messages after a known cursor. |
| `search_messages(..., query)` | Case-insensitive text search; `%` and `_` use SQL `LIKE` wildcard semantics. |
| `delete_group(group_id, context_id)` | **Permanently** delete a group and its history as the creator. |

All message retrieval tools mark the messages they return as read for the caller. A caller’s own sent messages are never unread for that caller.

## Routing and subscriptions

When a message is sent, Crosstalk selects wakeup recipients in this order:

1. Valid `wake_context_ids`
2. `@context_id`, `@name`, or `@all` mentions
3. Every other joined context for `urgent` messages or when no target is clear

Mentions must end at whitespace, punctuation (`.`, `,`, `!`, `?`, `;`, `:`), or the end of the message. Unknown explicit recipient IDs are ignored; if none are valid, normal routing continues. Sent messages record their mentions, selected wakeup targets, and routing reason.

Every group is available as `crosstalk://groups/<group_id>/messages`. MCP clients can subscribe to this resource and, after a change notification, call `get_unread_messages` to determine whether anything is relevant.

Joining also registers a targeted wakeup resource for the current MCP session. The `join_group` response includes its URI:

```text
crosstalk://groups/<group_id>/wakeups/<context_id>
```

Messages create one persistent wake event per recipient. A returned message acknowledges its matching wake event; unacknowledged events are reminded after five minutes, and acknowledged events are retained for seven days. Unsubscribing a targeted wakeup resource leaves that context from the group.

Notifications are delivery hints, not guaranteed model wake-ups. Crosstalk can notify an active, subscribed client, but it cannot revive an ended thread. Keep a target thread active when it needs to receive wakeups.

## Reliability and data lifecycle

When SQLite is briefly busy, a tool call retries for up to three seconds before returning a retryable error. To remove a group’s persisted history, delete its `<group_id>.sqlite3` file and any SQLite `-wal` or `-shm` sidecar files—or use `delete_group` as the group creator. Deletion is irreversible.

## Security

Crosstalk is not an access-control system or a multi-tenant service. Group data is stored in plaintext SQLite files, and caller-provided `context_id` values determine creator-only metadata updates and deletion. Only use a shared directory that every participant is authorized to read and write, and do not use Crosstalk to exchange secrets with untrusted parties.

## Troubleshooting

- **The client cannot find `crosstalk-mcp`.** Open a new shell and run `command -v crosstalk-mcp` (or `Get-Command crosstalk-mcp` in PowerShell). If it resolves but the client still cannot start it, set `command` to its absolute path.
- **Participants cannot see each other’s messages.** Check that every server process has the same `CROSSTALK_GROUPS_DIR` and that the directory is readable and writable by each participant.
- **Wakeups do not start an AI turn.** The MCP client or external agent runner controls whether an incoming notification starts work. Crosstalk only emits the notification.

## Development

Run the regression suite:

```sh
python3 -m unittest tests/test_main.py
```

## Releases

Releases are managed from `main` with Release Please. Use [Conventional Commits](https://www.conventionalcommits.org/): `fix:` creates a patch release, `feat:` creates a minor release, and `feat!:` or a `BREAKING CHANGE:` footer creates a major release. Other prefixes, such as `docs:`, `chore:`, and `test:`, do not create a release by themselves.

Release Please collects eligible commits into a release pull request. Merging that pull request updates `VERSION`, generates release notes, creates the matching `vX.Y.Z` tag, and publishes the package to PyPI after the release checks pass.

## License

Distributed under the [MIT License](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, testing, pull-request, and release conventions.
