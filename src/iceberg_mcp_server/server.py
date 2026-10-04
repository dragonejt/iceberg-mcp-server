from importlib.metadata import version
from os import environ, getenv

from fastmcp import FastMCP
from fastmcp_tasks import TasksExtension
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


def main() -> None:
    setup_telemetry()
    mcp = FastMCP(
        name="Iceberg MCP Server",
        version=version("iceberg-mcp-server"),
    )
    mcp.add_extension(TasksExtension())
    catalog = load_catalog(getenv("ICEBERG_CATALOG"))

    namespace = NamespaceTools(catalog)
    mcp.tool(namespace.list_namespaces, annotations=ToolAnnotations(read_only_hint=True))
    mcp.tool(namespace.create_namespace)
    mcp.tool(namespace.delete_namespace, annotations=ToolAnnotations(destructive_hint=True))

    table = TableTools(catalog)
    mcp.tool(table.list_tables, annotations=ToolAnnotations(read_only_hint=True))
    mcp.tool(table.read_table_metadata, annotations=ToolAnnotations(read_only_hint=True))
    mcp.tool(table.read_table_contents, annotations=ToolAnnotations(read_only_hint=True))
    mcp.tool(table.download_table_contents, task=True)
    mcp.tool(table.read_table_snapshots, annotations=ToolAnnotations(read_only_hint=True))
    mcp.tool(table.create_table)
    mcp.tool(table.update_table, annotations=ToolAnnotations(idempotent_hint=True))
    mcp.tool(table.write_table)
    mcp.tool(table.delete_table, annotations=ToolAnnotations(destructive_hint=True))

    duckdb = load_duckdb(catalog)
    if duckdb is not None:
        query = QueryTools(duckdb)
        mcp.tool(query.sql_query, annotations=ToolAnnotations(destructive_hint=True))

    mcp.run()


def setup_telemetry() -> None:
    provider = TracerProvider()
    set_tracer_provider(provider)

    if "OTEL_EXPORTER_OTLP_ENDPOINT" in environ or "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" in environ:
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    elif "SENTRY_DSN" in environ:
        sentry_init(
            dsn=getenv("SENTRY_DSN"),
            profiles_sample_rate=1.0,
            enable_logs=True,
            send_default_pii=True,
            integrations=[OTLPIntegration()],
        )

    RequestsInstrumentor().instrument()
    BotocoreInstrumentor().instrument()


if __name__ == "__main__":
    main()
