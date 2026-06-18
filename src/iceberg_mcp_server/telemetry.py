from os import getenv

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import set_tracer_provider
from sentry_sdk import init as sentry_init
from sentry_sdk.integrations.otlp import OTLPIntegration


def setup_telemetry():
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
