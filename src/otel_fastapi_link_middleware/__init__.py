from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import Link
from starlette.types import ASGIApp, Receive, Scope, Send


def _get_otlp_tracer(name: str) -> trace.Tracer:
    """Get a tracer for the given name

    Args:
        name: Name of the tracer
    Returns:
        Tracer instance
    """
    return trace.get_tracer(name)


def _get_current_otlp_span() -> trace.Span:
    """Get the current active span

    Returns:
        Current active Span
    """
    return trace.get_current_span()


def _copy_span_attributes(src_span: trace.Span, dest_span: trace.Span) -> None:
    # src_span._attributes is safe; it’s part of the SDK span implementation
    for k, v in src_span._attributes.items():  # type: ignore[attr-defined]
        dest_span.set_attribute(k, v)


class LinkedTraceMiddleware:
    """
    Middleware that creates new trace roots while linking to upstream traces.

    This middleware extracts upstream trace context from request headers,
    creates a new root span for the current service, and links it to the
    upstream span if valid. This allows maintaining correlation between
    services while creating independent trace hierarchies.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        should_link = False
        if scope["type"] == "http":
            # Headers come as a list of (key: bytes, value: bytes)
            headers = dict(scope["headers"])

            traceparent = headers.get(b"traceparent")

            if traceparent:
                should_link = True

        if not should_link:
            await self.app(scope, receive, send)
        else:
            await self._link_trace(scope, receive, send)

    async def _link_trace(self, scope: Scope, receive: Receive, send: Send) -> None:
        parent_ctx = trace.get_current_span().get_span_context()
        new_context = otel_context.Context()
        current_span = _get_current_otlp_span()

        if not hasattr(current_span, "name"):
            await self.app(scope, receive, send)
            return

        span_name = current_span.name  # type: ignore[attr-defined]
        new_name = "LinkedApiSpan"
        current_span.set_attribute("linked_trace", True)
        current_span.set_attribute("url.path", "" + scope.get("path", ""))

        current_span.update_name(new_name)
        tracer = _get_otlp_tracer(__name__)

        with tracer.start_as_current_span(
            span_name,
            context=new_context,  # start new trace
            links=[Link(parent_ctx)] if parent_ctx.is_valid else None,
        ):
            child_span = _get_current_otlp_span()
            child_span_context = child_span.get_span_context()
            _copy_span_attributes(current_span, child_span)
            current_span.add_link(child_span_context)

            await self.app(scope, receive, send)
