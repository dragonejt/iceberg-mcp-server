# iceberg-mcp-server
![downloads](https://img.shields.io/pypi/dm/iceberg-mcp-server)
![integration](https://github.com/dragonejt/iceberg-mcp-server/actions/workflows/integrate.yml/badge.svg)
![delivery](https://github.com/dragonejt/iceberg-mcp-server/actions/workflows/deliver.yml/badge.svg)
![codecov](https://codecov.io/gh/dragonejt/iceberg-mcp-server/graph/badge.svg?token=7MEF3IHI00)

iceberg-mcp-server is an MCP Server for Apache Iceberg, enabling users to read, query, and manipulate data within Iceberg catalogs. It supports reading and data manipulation using catalog types supported by PyIceberg, and supports SQL queries using catalog types compatible with DuckDB.

## Quickstart
### Installation
With [uv](https://docs.astral.sh/uv/), installation is easy, the only command you need to run is:
```bash
uvx iceberg-mcp-server
```
This will automatically install and run the latest version of iceberg-mcp-server published to PyPI. Alternative Python package runners like `pipx` are also supported. Once installed, iceberg-mcp-server can be used with any agent that supports STDIO-based MCP servers. For example, with OpenAI's Codex CLI `~/.codex/config.toml`:
```toml
[mcp_servers.iceberg]
command = "uvx"
args = ["iceberg-mcp-server"]
```

### Configuration
#### `.pyiceberg.yaml` File
iceberg-mcp-server supports the [PyIceberg configuration methods](https://py.iceberg.apache.org/configuration/). `.pyiceberg.yaml` is the recommended persistent method of configuration. For example, to connect to a standard REST-based Iceberg catalog with `~/.pyiceberg.yaml`:
```yaml
catalog:
  default: # iceberg-mcp-server loads the catalog named "default" if not in env vars
    uri: <catalog-uri>
    token: <catalog-token>
    warehouse: <warehouse>
```
#### Environment Variables
One of the other PyIceberg configuration methods is setting specific environment variables, which iceberg-mcp-server supports as well. There are also environment variables specific to iceberg-mcp-server that can be set:
```bash
ICEBERG_CATALOG="default"
SENTRY_DSN="https://<sentry-key>@o<organization-id>.ingest.us.sentry.io/<project-id>"
```
* `ICEBERG_CATALOG` allows you to set which catalog will be loaded. By default, the catalog named `default` will be loaded based on PyIceberg behavior.
* Optionally, you may send telemetry to [Sentry](https://sentry.io/welcome/) by specifying a `SENTRY_DSN`. This will send traces, profiles, logs, and default PII to Sentry, as well as enable the Sentry MCP integration.

## Local Development
### Building and Running
This project uses uv for package management and builds. Once this repository has been cloned, running the local development version of iceberg-mcp-server only requires a single command:
```bash
uv run iceberg-mcp-server
```
An Iceberg catalog still needs to be configured, but then it can be integrated into any agent that supports STDIO-based MCP servers as long as the agent is ran from the repository root directory.

### Testing
This repository uses pytest for test running, although the tests themselves are structured in the unittest format. Running tests involves invoking pytest like any other project. If you use VS Code or a fork for development, the [VS Code Python Extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) will enable automatic test discovery and running in the Testing sidebar. Tests will also be run with coverage in the [integration workflow](https://github.com/dragonejt/iceberg-mcp-server/blob/main/.github/workflows/integrate.yml).

### Linting and Formatting
iceberg-mcp-server uses [Ruff](https://docs.astral.sh/ruff/) and [ty](https://docs.astral.sh/ty/) for linting, formatting, and type checking. The standard commands to run are:
```bash
ruff check --fix # linting
ruff format # formatting
ty check # type checking
```
The Ruff configuration is found in `pyproject.toml`, and all autofixable issues will be autofixed. If you use VS Code or a fork for development, the [VS Code Ruff Extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff) will enable viewing Ruff issues within your editor. Additionally, Ruff, ty, and CodeQL analysis will be run in the [integration workflow](https://github.com/dragonejt/iceberg-mcp-server/blob/main/.github/workflows/integrate.yml).