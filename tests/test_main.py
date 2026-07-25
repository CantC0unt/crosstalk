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

    def test_unknown_command_returns_an_error(self):
        with patch("sys.stderr") as stderr:
            self.assertEqual(main.serve(["unknown"]), 2)
        stderr.write.assert_called_once_with("Unknown command: unknown\n")


if __name__ == "__main__":
    unittest.main()
