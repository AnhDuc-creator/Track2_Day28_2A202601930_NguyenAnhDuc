"""Four student-owned boundaries used by the live platform.

Run ``uv run pytest starter-tests -q`` while completing these functions.  Do
not change their signatures: Kafka, Delta, Feast and ``/ready`` call them.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lab28_platform.contracts import FEATURE_REFS, IngestionEvent

TRACEPARENT_HEADER = "traceparent"
IDEMPOTENCY_HEADER = "idempotency-key"


def event_headers(
    traceparent: str | None, idempotency_key: str
) -> list[tuple[str, bytes]]:
    """Return byte-valued Kafka headers for trace and replay correlation.

    ``idempotency-key`` is always required.  Omit ``traceparent`` when no trace
    is active rather than sending an empty, invalid W3C header.
    """
    headers: list[tuple[str, bytes]] = []
    if traceparent:
        headers.append((TRACEPARENT_HEADER, traceparent.encode()))
    headers.append((IDEMPOTENCY_HEADER, idempotency_key.encode()))
    return headers


def dedupe_latest(events: Iterable[IngestionEvent]) -> list[IngestionEvent]:
    """Return one newest event per idempotency key, in deterministic key order.

    Compare ``(occurred_at, event_id)`` so ties do not depend on Kafka delivery
    order.  The Spark Delta MERGE calls this through ``delta_store``.
    """
    newest: dict[str, IngestionEvent] = {}
    for event in events:
        key = event.idempotency_key
        current = newest.get(key)
        if current is None or (event.occurred_at, event.event_id) > (
            current.occurred_at,
            current.event_id,
        ):
            newest[key] = event
    return [newest[key] for key in sorted(newest)]


def feast_online_request(asker_id: str) -> dict[str, Any]:
    """Build the Feast ``/get-online-features`` request for ``asker_activity_v1``."""
    return {
        "entities": {"asker_id": [asker_id]},
        "features": list(FEATURE_REFS),
        "full_feature_names": False,
    }


def readiness_status(probes: Iterable[dict[str, Any]]) -> str:
    """Return ``ready``, ``degraded`` or ``not_ready`` from probe severity."""
    degraded = False
    for probe in probes:
        if probe.get("ready", False):
            continue
        if probe.get("mandatory", False):
            return "not_ready"
        degraded = True
    return "degraded" if degraded else "ready"