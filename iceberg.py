from fastmcp import FastMCP
from mcp.types import ToolAnnotations

from tools.namespaces import create_namespace, list_namespaces
from tools.tables import list_tables, read_table_contents, read_table_metadata

mcp = FastMCP(
    name="Iceberg MCP Server",
)

mcp.tool(list_namespaces, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(create_namespace)
mcp.tool(list_tables, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(read_table_metadata, annotations=ToolAnnotations(readOnlyHint=True))
mcp.tool(read_table_contents, annotations=ToolAnnotations(readOnlyHint=True))


if __name__ == "__main__":
    mcp.run()
