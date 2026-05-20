# Runtime-managed Observability

The framework emits runtime-managed best-effort observability events through
decorators.

Pipeline authors should not emit observability events manually inside ETL
stages.

Contracts and pipelines are instrumented by the framework runtime, which emits
structured operational events for each stage.

External tools can be integrated by implementing the `ObservabilitySink` port.

The core framework does not depend on external observability vendors.

## Principle

Observability is not a dependency of ETL logic. Operational event emission is a
cross-cutting runtime responsibility applied automatically through decorators.

The framework emits local operational events by default, but delivery is
best-effort and vendor-agnostic by design.

## Public API

The daily API for a pipeline author remains:

- `Pipeline`
- `EtlRunConfig`
- `EtlExecutionContext`
- `Extract`
- `Transform`
- `Load`

Pipeline authors implement extraction, transformation and load hooks. They do
not receive loggers, observability services, sinks or vendor
adapters.

## Internal Runtime

The runtime applies decorators to executable stages:

```text
Pipeline author
  -> implements extract/check/transform/validate/load/certify

Framework runtime
  -> applies decorators to stages
      -> collects operational context
          -> builds a structured event
              -> sends it to the central ObservabilityService
                  -> delegates to a pluggable ObservabilitySink
```

The main stage decorator emits:

- stage start;
- stage success;
- warning-level operational events when a decorator explicitly declares one;
- stage failure;
- elapsed time when applicable;
- `run_id`;
- `pipeline_name`;
- stage name;
- status;
- error type and sanitized error message when applicable;
- operational metrics explicitly stored in `context.metrics`.

Real ETL exceptions continue to propagate. Sink failures do not break the ETL
execution in v0.1, so these events must not be treated as guaranteed audit
delivery.

## Event Payload

Payloads keep the v0.1 canonical fields:

```python
{
    "event_schema_version": "1.0",
    "event": "transform_succeeded",
    "pipeline_name": "orders_daily",
    "run_id": "run-123",
    "started_at": "2026-05-17T12:00:00+00:00",
    "event_at": "2026-05-17T12:00:02+00:00",
    "mode": "prod",
    "target_schema": "silver",
    "target_table": "orders",
    "target_path": "/tmp/orders",
    "target": "silver.orders",
    "write_mode": None,
    "stage": "transform",
    "status": "succeeded",
    "level": "info",
    "elapsed_ms": 18.4,
}
```

`etl_framework.utils.observability_events.build_observability_event(...)` builds
this payload without emitting it. Emission belongs to the runtime observability
service.

Small stateless metadata builders used by decorators live in
`etl_framework.utils.stage_metadata`. They are not defined in contracts or
pipeline files.

## ObservabilitySink Port

Advanced maintainers can plug a backend by implementing the minimal port:

```python
from collections.abc import Mapping


class MySink:
    def emit(self, event: Mapping[str, object]) -> None:
        ...
```

The sink only knows how to emit one structured event. It should not know ETL
business rules.

## Runtime Levels

The runtime emits operational events with a simple `level` field:

- `info` for stage start, success and normal runtime evidence;
- `warning` for explicitly declared warning-level events;
- `error` for managed failures and execution summaries after failure.

There is no severity engine or vendor-specific event routing in v0.1.

## Fake External Sink

```python
from collections.abc import Mapping


class FakeExternalSink:
    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []

    def emit(self, event: Mapping[str, object]) -> None:
        self.sent.append(dict(event))
```

Configure it outside pipeline code:

```python
from etl_framework.infra.observability import configure_observability_sink

configure_observability_sink(FakeExternalSink())
```

No `Extract`, `Transform`, `Load` or concrete pipeline class changes are needed.

## Testing A Custom Sink

Use a small in-memory sink:

```python
from collections.abc import Mapping


class InMemoryObservabilitySink:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def emit(self, event: Mapping[str, object]) -> None:
        self.events.append(dict(event))
```

Then configure it at test setup and assert emitted payloads:

```python
from etl_framework.infra.observability import (
    configure_observability_sink,
    reset_observability_sink,
)

sink = InMemoryObservabilitySink()
configure_observability_sink(sink)

try:
    pipeline.run()
finally:
    reset_observability_sink()

assert sink.events[0]["event"] == "run_started"
assert sink.events[0]["run_id"]
```

## Environment Configuration

Applications may configure the standard-library logging backend explicitly:

```python
from etl_framework.infra.observability import configure_observability_from_env

configure_observability_from_env()
```

The function reads:

- `ETL_LOG_LEVEL`, default `INFO`;
- `ETL_LOG_FORMAT`, default
  `%(asctime)s | %(levelname)s | %(name)s | %(message)s`;
- `ETL_LOG_TO_STDOUT`, default enabled.

The framework does not load `.env` files and does not call this function on
import. If an application wants `.env`, it must load it before calling
`configure_observability_from_env()`.

## Out Of Scope For v0.1

- Real OpenTelemetry, Datadog, Loki, ELK, CloudWatch or Sentry integration.
- Vendor-specific schemas.
- Strict mode where observability failure breaks the ETL.
- Guaranteed delivery or replay of observability events.
- Pipeline-level logging configuration.
- Manual logging inside ETL business methods.

## Deliberate Decisions

- Observability failure does not stop the pipeline in v0.1.
- Event field names such as `event_at` and `elapsed_ms` are preserved.
- The root public API remains small.
- Backend replacement is an advanced runtime concern, not a pipeline-author
  concern.
