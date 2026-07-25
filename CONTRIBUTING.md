# Contributing to Crosstalk

Thanks for contributing. Crosstalk is a dependency-free Python MCP server, so changes should remain focused, well-tested, and compatible with Python 3.9 and later.

## Getting started

Clone the repository and run the test suite:

```sh
python3 -m unittest tests/test_main.py tests/test_mcp.py tests/test_observe.py
```

No runtime dependencies are required. To install the command locally, run:

```sh
python3 -m pip install --user --upgrade .
```

## Making changes

- Keep changes scoped to a single purpose.
- Add or update tests for behavior changes.
- Run the full test suite before opening a pull request.
- Update the README when user-facing behavior, configuration, tools, or security expectations change.
- Do not commit generated files such as `__pycache__`, `dist`, `build`, or `*.egg-info`.

## Commit messages and releases

Releases are managed with [Release Please](https://github.com/googleapis/release-please), which uses [Conventional Commits](https://www.conventionalcommits.org/) to determine the next version.

| Commit prefix | Release effect |
| --- | --- |
| `fix:` | Patch release, such as `1.0.0` to `1.0.1` |
| `feat:` | Minor release, such as `1.0.0` to `1.1.0` |
| `feat!:` or a `BREAKING CHANGE:` footer | Major release, such as `1.0.0` to `2.0.0` |
| `docs:`, `test:`, `chore:` | No release by itself |

When eligible changes reach `main`, Release Please opens or updates a release pull request. Merging that pull request updates `VERSION`, creates the release tag and notes, and publishes the package to PyPI.

Do not edit `VERSION` manually for ordinary changes. It is the package version source of truth and is updated by the release pull request.

## Pull requests

Describe the problem and the change in the pull request. Keep the CI checks green, and call out any behavior change that affects existing MCP clients or persisted group data.
