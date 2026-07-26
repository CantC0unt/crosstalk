import sys
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import main


class MainCommandTests(unittest.TestCase):
    def test_no_arguments_run_the_mcp_server(self):
        with patch.object(main.mcp, "serve") as mcp_serve:
            self.assertEqual(main.serve([]), 0)
        mcp_serve.assert_called_once_with()

    def test_observe_arguments_are_dispatched(self):
        arguments = ["--silent", "--port", "8788"]
        with patch.object(main.observe, "serve", return_value=0) as observer_serve:
            self.assertEqual(main.serve(["observe", *arguments]), 0)
        observer_serve.assert_called_once_with(arguments)

    def test_help_and_version_do_not_start_a_server(self):
        with patch("sys.stdout") as stdout, patch.object(main.mcp, "serve") as mcp_serve:
            self.assertEqual(main.serve(["--help"]), 0)
            self.assertEqual(main.serve(["--version"]), 0)
        mcp_serve.assert_not_called()
        self.assertEqual(stdout.write.call_args_list[0].args[0], main.USAGE)
        self.assertEqual(stdout.write.call_args_list[1].args[0], main.mcp.SERVER_VERSION + "\n")
        self.assertIn("--silent", main.USAGE)
        self.assertIn("--port PORT", main.USAGE)
        self.assertIn("--poll-interval SECONDS", main.USAGE)
        self.assertIn("--groups-dir PATH", main.USAGE)

    def test_unknown_command_returns_an_error(self):
        with patch("sys.stderr") as stderr:
            self.assertEqual(main.serve(["unknown"]), 2)
        stderr.write.assert_called_once_with("Unknown command: unknown\n")


if __name__ == "__main__":
    unittest.main()
