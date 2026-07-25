"""Command-line dispatcher for Crosstalk."""

import sys
from typing import List, Optional

import mcp
import observe


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
    if arguments[0] == "observe":
        return observe.serve(arguments[1:])
    sys.stderr.write("Unknown command: " + arguments[0] + "\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(serve())
