from fastmcp import FastMCP

from tools.namespaces import list_namespaces
from tools.tables import get_table_contents, get_table_metadata, list_tables

mcp = FastMCP(
    name="iceberg-mcp-server",
)

mcp.tool(list_namespaces)
mcp.tool(list_tables)
mcp.tool(get_table_metadata)
mcp.tool(get_table_contents)


if __name__ == "__main__":
    mcp.run()
