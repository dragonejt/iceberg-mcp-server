from fastmcp import FastMCP
from mcp.types import ToolAnnotations

from catalog import load_catalog, load_duckdb
from tools.namespaces import NamespaceTools
from tools.query import QueryTools
from tools.tables import TableTools

catalog = load_catalog()
duckdb = load_duckdb()
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

query = QueryTools(duckdb)
mcp.tool(query.sql_query, annotations=ToolAnnotations(destructiveHint=True))


if __name__ == "__main__":
    mcp.run()
