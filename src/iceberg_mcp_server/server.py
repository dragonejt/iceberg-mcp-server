from os import getenv

from fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pyiceberg.catalog import load_catalog

from iceberg_mcp_server.telemetry import setup_telemetry
from iceberg_mcp_server.tools.namespace import NamespaceTools
from iceberg_mcp_server.tools.query import QueryTools, load_duckdb
from iceberg_mcp_server.tools.table import TableTools

setup_telemetry()
catalog = load_catalog(getenv("ICEBERG_CATALOG"))
duckdb = load_duckdb(catalog)
mcp = FastMCP(
    name="Iceberg MCP Server",
)

namespace = NamespaceTools(catalog)
mcp.tool(namespace.list_namespaces, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(namespace.create_namespace)
mcp.tool(namespace.delete_namespace, annotations=ToolAnnotations(destructiveHint=True))

table = TableTools(catalog)
mcp.tool(table.list_tables, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(table.read_table_metadata, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(table.read_table_contents, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(table.download_table_contents, task=True)
mcp.tool(table.read_table_snapshots, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(table.create_table)
mcp.tool(table.update_table, annotations=ToolAnnotations(idempotentHint=True))
mcp.tool(table.write_table)
mcp.tool(table.delete_table, annotations=ToolAnnotations(destructiveHint=True))

if duckdb is not None:
    query = QueryTools(duckdb)
    mcp.tool(query.sql_query, annotations=ToolAnnotations(destructiveHint=True))


if __name__ == "__main__":
    mcp.run()
