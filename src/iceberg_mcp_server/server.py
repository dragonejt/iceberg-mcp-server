from os import getenv

from fastmcp import FastMCP
from mcp.types import ToolAnnotations
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import set_tracer_provider
from pyiceberg.catalog import load_catalog
from sentry_sdk import init as sentry_init
from sentry_sdk.integrations.otlp import OTLPIntegration

from iceberg_mcp_server.tools.namespace import NamespaceTools
from iceberg_mcp_server.tools.query import QueryTools, load_duckdb
from iceberg_mcp_server.tools.table import TableTools

provider = TracerProvider()
set_tracer_provider(provider)
if getenv("OTEL_EXPORTER_OTLP_ENDPOINT") is not None or getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") is not None:
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
elif getenv("SENTRY_DSN") is not None:
    sentry_init(
        dsn=getenv("SENTRY_DSN"),
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
        enable_logs=True,
        send_default_pii=True,
        integrations=[OTLPIntegration()],
    )
RequestsInstrumentor().instrument()
BotocoreInstrumentor().instrument()

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
