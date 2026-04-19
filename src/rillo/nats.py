from typing import Sequence, TypeVar, override

from pydantic import BaseModel, JsonValue

try:
    from nats.errors import TimeoutError as NatsTimeoutError
    from nats.js.client import JetStreamContext
    from nats.js.kv import KeyValue
except ImportError as e:
    raise ImportError(
        "nats-py is required to use NATSRepository. Install it with: uv add rillo[nats]"
    ) from e

import json

from rillo.aggregate import Aggregate
from rillo.repository import OptimisticConcurrencyError, Repository
from rillo.snapshot_store import SnapshotStore

A = TypeVar("A", bound=Aggregate)


class Snapshot(BaseModel):
    state: JsonValue
    version: int


class NATSSnapshotStore(SnapshotStore[A]):
    def __init__(self, kv: KeyValue) -> None:
        self._kv = kv

    @override
    async def _load_state(self, aggregate_id: str) -> tuple[JsonValue, int] | None:
        entry = await self._kv.get(aggregate_id)
        value = entry.value
        if value is None:
            return None
        snapshot = Snapshot.model_validate_json(value.decode("utf-8"))
        return snapshot.state, snapshot.version

    @override
    async def _save_state(
        self, aggregate_id: str, state: JsonValue, version: int
    ) -> None:
        snapshot = Snapshot(
            state=state,
            version=version,
        )
        await self._kv.put(aggregate_id, snapshot.model_dump_json().encode("utf-8"))


class NATSRepository(Repository[A]):
    def __init__(
        self,
        js: JetStreamContext,
        subject_prefix: str,
        schema_discriminator: str,
        snapshot_store: SnapshotStore[A] | None = None,
    ) -> None:
        self._js = js
        self._subject_prefix = subject_prefix
        super().__init__(
            schema_discriminator=schema_discriminator,
            snapshot_store=snapshot_store,
        )

    @override
    async def _save_events(
        self,
        aggregate_id: str,
        events: Sequence[JsonValue],
        expected_version: int,
    ) -> None:
        subject = f"{self._subject_prefix}.{aggregate_id}"

        for i, event in enumerate(events):
            headers = None
            if expected_version is not None:
                headers = {
                    "Nats-Expected-Last-Subject-Sequence": str(expected_version + i)
                }
            try:
                await self._js.publish(
                    subject,
                    json.dumps(event).encode("utf-8"),
                    headers=headers,
                )
            except Exception as e:
                if "wrong last sequence" in str(e).lower():
                    raise OptimisticConcurrencyError(
                        f"Concurrency conflict for aggregate {aggregate_id}"
                    ) from e
                raise

    @override
    async def _load_events(
        self,
        aggregate_id: str,
        from_version: int,
    ) -> Sequence[JsonValue]:
        subject = f"{self._subject_prefix}.{aggregate_id}"

        events = []
        events_to_skip = from_version or 0
        sub: JetStreamContext.PushSubscription | None = None
        try:
            sub = await self._js.subscribe(subject, ordered_consumer=True)
            while True:
                msg = await sub.next_msg(timeout=0.1)
                if events_to_skip > 0:
                    events_to_skip -= 1
                    continue
                events.append(json.loads(msg.data.decode("utf-8")))
        except NatsTimeoutError:
            pass
        finally:
            if sub is not None:
                await sub.unsubscribe()

        return events
