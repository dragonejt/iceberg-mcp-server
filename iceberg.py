from fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pyiceberg.catalog import load_catalog

from tools.namespace import NamespaceTools
from tools.query import QueryTools
from tools.table import TableTools

catalog = load_catalog()
duckdb = QueryTools.load_duckdb(catalog)
mcp = FastMCP(
    name="Iceberg MCP Server",
)

namespace = NamespaceTools(catalog)
mcp.tool(namespace.list_namespaces, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(namespace.create_namespace)

table = TableTools(catalog)
mcp.tool(table.list_tables, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(table.read_table_metadata, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(table.read_table_contents, annotations=ToolAnnotations(readOnlyHint=True))

if duckdb is not None:
    query = QueryTools(duckdb)
    mcp.tool(query.sql_query, annotations=ToolAnnotations(destructiveHint=True))


if __name__ == "__main__":
    mcp.run()
