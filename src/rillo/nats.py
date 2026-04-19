from typing import Sequence, TypeVar, override

from pydantic import BaseModel, JsonValue

try:
    from nats.errors import TimeoutError as NatsTimeoutError
    from nats.js.api import ConsumerConfig, DeliverPolicy
    from nats.js.client import JetStreamContext
    from nats.js.kv import KeyValue
except ImportError as e:
    raise ImportError(
        "nats-py is required to use NATSRepository. Install it with: uv add rillo[nats]"
    ) from e


from rillo.aggregate import Aggregate
from rillo.repository import OptimisticConcurrencyError, Repository
from rillo.snapshot_store import SnapshotStore

A = TypeVar("A", bound=Aggregate)


class Snapshot(BaseModel):
    state: JsonValue
    version: str


class EventBatch(BaseModel):
    events: Sequence[JsonValue]


class NATSSnapshotStore(SnapshotStore[A]):
    def __init__(self, kv: KeyValue) -> None:
        self._kv = kv

    @override
    async def _load_state(self, aggregate_id: str) -> tuple[JsonValue, str] | None:
        entry = await self._kv.get(aggregate_id)
        value = entry.value
        if value is None:
            return None
        snapshot = Snapshot.model_validate_json(value.decode("utf-8"))
        return snapshot.state, snapshot.version

    @override
    async def _save_state(
        self,
        aggregate_id: str,
        state: JsonValue,
        version: str,
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
        snapshot_store: SnapshotStore[A] | None = None,
    ) -> None:
        self._js = js
        self._subject_prefix = subject_prefix
        super().__init__(snapshot_store=snapshot_store)

    @override
    async def _save_events(
        self,
        aggregate_id: str,
        events: Sequence[JsonValue],
        expected_version: str | None,
    ) -> str:
        subject = f"{self._subject_prefix}.{aggregate_id}"

        event_batch = EventBatch(events=events)

        expected_version = expected_version or "0"
        headers = {"Nats-Expected-Last-Subject-Sequence": expected_version}
        try:
            result = await self._js.publish(
                subject,
                event_batch.model_dump_json().encode("utf-8"),
                headers=headers,
            )
            return str(result.seq)
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
        from_version: str | None,
    ) -> tuple[Sequence[JsonValue], str]:
        subject = f"{self._subject_prefix}.{aggregate_id}"

        if from_version is not None:
            config = ConsumerConfig(
                deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
                opt_start_seq=int(from_version) + 1,
            )
        else:
            config = ConsumerConfig(
                deliver_policy=DeliverPolicy.ALL,
            )

        sub = await self._js.subscribe(subject, config=config)

        all_events: list[JsonValue] = []
        version = from_version or "0"

        try:
            while True:
                try:
                    msg = await sub.next_msg(timeout=1)
                    event_batch = EventBatch.model_validate_json(
                        msg.data.decode("utf-8")
                    )
                    all_events.extend(event_batch.events)
                    version = str(msg.metadata.sequence.stream)
                except NatsTimeoutError:
                    break
        finally:
            await sub.unsubscribe()

        return all_events, version
