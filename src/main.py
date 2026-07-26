"""Command-line dispatcher for Crosstalk."""

import sys
from typing import List, Optional

import mcp
import observe


USAGE = """Usage: crosstalk-mcp [COMMAND]

Run without a command to start the stdio MCP server.

Commands:
  observe [OPTIONS]            Start the local read-only observer dashboard.
    --silent                   Do not open a browser automatically.
    --port PORT                Bind exactly this loopback port.
    --poll-interval SECONDS    Set the observer refresh interval (default: 0.5).
    --groups-dir PATH          Read group databases from this directory.

Options:
  -h, --help                   Show this help message.
  --version                    Show the installed Crosstalk version.

Run 'crosstalk-mcp observe --help' for observer option details.
"""


def serve(arguments: Optional[List[str]] = None) -> int:
    """Run the stdio MCP server or the observer subcommand."""
    if arguments is None:
        arguments = sys.argv[1:]
    if not arguments:
        try:
            mcp.serve()
        except mcp.ObservabilityConfigurationError as error:
            sys.stderr.write(str(error) + "\n")
            return 2
        return 0
    if arguments[0] in {"-h", "--help"}:
        sys.stdout.write(USAGE)
        return 0
    if arguments[0] == "--version":
        sys.stdout.write(mcp.SERVER_VERSION + "\n")
        return 0
    if arguments[0] == "observe":
        return observe.serve(arguments[1:])
    sys.stderr.write("Unknown command: " + arguments[0] + "\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(serve())
