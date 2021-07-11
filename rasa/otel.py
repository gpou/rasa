import logging

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter
)
from opentelemetry.propagate import inject, extract
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter

from opentelemetry.sdk.resources import SERVICE_NAME, Resource

logger = logging.getLogger(__name__)


class Tracer:
    service_name = None
    tracer_provider = None
    tracer = None

    @classmethod
    def init(cls, service_name):
        logger.info(f"Starting tracing for {service_name}")

        cls.service_name = service_name
        resource = Resource(attributes={"service.name": cls.service_name})
        trace.set_tracer_provider(TracerProvider(resource=resource))
        cls.tracer = trace.get_tracer(__name__)

        LoggingInstrumentor().instrument()

    @classmethod
    def config(cls, config):
        tracer_provider = trace.get_tracer_provider()
        service_name = tracer_provider.resource.attributes[SERVICE_NAME]
        logger.info(f"Setting up tracing exporters for {service_name}")

        # Setup trace exporters
        if config and config.kwargs.get("exporters"):
            trace_exporters = config.kwargs.get("exporters")
            try:
                for t in trace_exporters:
                    if t == 'console':
                        logger.debug(f"Starting Console Exporter")
                        tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
                    if t == 'jaeger':
                        if trace_exporters["jaeger"]["agent_hostname"]:
                            agent_port = trace_exporters["jaeger"]["agent_port"]
                            agent_host_name = trace_exporters["jaeger"]["agent_hostname"]
                            logger.debug(f"Starting Jaeger Exporter: {agent_host_name}:{agent_port}")
                            jaeger_exporter = JaegerExporter(agent_host_name=agent_host_name, agent_port=agent_port)
                            tracer_provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
            except Exception as e:
                import traceback
                logger.error(f"Exception while setting up telemetry exporter {t} : " + type(e).__name__ + " " + str(e))
                traceback.print_exception(type(e), e, e.__traceback__)

    @classmethod
    def start_span(cls, name, attributes=None, context=None) -> trace.Span:
        logger.info(f"Start span: {name}")
        span = cls.tracer.start_as_current_span(name, attributes=attributes, context=context)
        return span

    @classmethod
    def inject(cls):
        headers = {}
        inject(carrier=headers)
        return headers

    @classmethod
    def extract(cls, headers):
        context = extract(carrier=headers)
        return context
