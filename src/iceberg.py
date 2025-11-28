from fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pyiceberg.catalog import load_catalog

from src.tools.namespace import NamespaceTools
from src.tools.query import QueryTools, load_duckdb
from src.tools.table import TableTools

catalog = load_catalog()
duckdb = load_duckdb(catalog)
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
mcp.tool(table.create_table_from_contents)

if duckdb is not None:
    query = QueryTools(duckdb)
    mcp.tool(query.sql_query, annotations=ToolAnnotations(destructiveHint=True))


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
