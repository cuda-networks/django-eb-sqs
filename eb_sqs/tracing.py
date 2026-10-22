"""Optional OpenTelemetry trace-context propagation over SQS MessageAttributes.

If ``opentelemetry-api`` is not installed (or no tracer provider is configured),
every function here is a no-op, so the library behaves exactly as before.
"""
import contextlib
from typing import Any, Dict, Iterator, Optional

try:
    from opentelemetry import context as otel_context
    from opentelemetry import propagate, trace
    from opentelemetry.trace import SpanKind

    OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised by the "no OTel" test via monkeypatch
    otel_context = propagate = trace = SpanKind = None
    OTEL_AVAILABLE = False

# W3C Trace Context headers; SQS allows max 10 message attributes per message.
PROPAGATION_KEYS = ('traceparent', 'tracestate')


def _available() -> bool:
    return OTEL_AVAILABLE


def build_message_attributes() -> Dict[str, Dict[str, str]]:
    """Return SQS MessageAttributes carrying the current trace context ({} if none)."""
    if not _available():
        return {}

    carrier = {}  # type: Dict[str, str]
    propagate.inject(carrier)

    return {
        key: {'DataType': 'String', 'StringValue': value}
        for key, value in carrier.items()
        if key in PROPAGATION_KEYS and value
    }


def _carrier_from_message(message_attributes: Optional[Dict[str, Any]]) -> Dict[str, str]:
    carrier = {}  # type: Dict[str, str]
    for key in PROPAGATION_KEYS:
        attr = (message_attributes or {}).get(key)
        if attr and attr.get('StringValue'):
            carrier[key] = attr['StringValue']
    return carrier


@contextlib.contextmanager
def producer_span(queue_name: str) -> Iterator[None]:
    """Start a PRODUCER span around the send, so the injected context points to it."""
    if not _available():
        yield
        return

    tracer = trace.get_tracer('eb_sqs')
    with tracer.start_as_current_span('eb_sqs send', kind=SpanKind.PRODUCER) as span:
        span.set_attribute('messaging.system', 'aws_sqs')
        span.set_attribute('messaging.destination.name', queue_name)
        yield


@contextlib.contextmanager
def consumer_span(msg: Any) -> Iterator[None]:
    """Run the body in a CONSUMER span, child of the trace context found in the message."""
    if not _available():
        yield
        return

    carrier = _carrier_from_message(getattr(msg, 'message_attributes', None))
    parent_ctx = propagate.extract(carrier) if carrier else None

    tracer = trace.get_tracer('eb_sqs')
    token = otel_context.attach(parent_ctx) if parent_ctx is not None else None
    try:
        with tracer.start_as_current_span('eb_sqs process', kind=SpanKind.CONSUMER) as span:
            span.set_attribute('messaging.system', 'aws_sqs')
            span.set_attribute('messaging.message.id', getattr(msg, 'message_id', '') or '')
            yield
    finally:
        if token is not None:
            otel_context.detach(token)
