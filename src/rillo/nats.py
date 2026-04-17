from typing import TypeVar, override

from pydantic import BaseModel

try:
    from nats.errors import TimeoutError as NatsTimeoutError
    from nats.js.client import JetStreamContext
    from nats.js.kv import KeyValue
except ImportError as e:
    raise ImportError(
        "nats-py is required to use NATSRepository. Install it with: uv add rillo[nats]"
    ) from e

from rillo.aggregate import Aggregate
from rillo.respository import Repository
from rillo.snapshot_store import SnapshotStore

A = TypeVar("A", bound=Aggregate)


class Snapshot(BaseModel):
    state: str
    version: int


class NATSSnapshotStore(SnapshotStore[A]):
    def __init__(self, kv: KeyValue) -> None:
        self._kv = kv

    @override
    async def load(self, aggregate: A) -> None:
        entry = await self._kv.get(aggregate.id)
        value = entry.value
        if value is None:
            return None
        snapshot = Snapshot.model_validate_json(value.decode("utf-8"))
        state = self._deserialize_state(aggregate, snapshot.state)
        aggregate.load_state(state, version=snapshot.version)

    @override
    async def save(self, aggregate: A) -> None:
        state = aggregate.get_state()
        version = aggregate.version
        if state is None or version is None:
            raise ValueError("Aggregate state or version is None.")
        snapshot = Snapshot(
            state=state.model_dump_json(),
            version=version,
        )
        await self._kv.put(aggregate.id, snapshot.model_dump_json().encode("utf-8"))


class NATSRepository(Repository[A]):
    def __init__(
        self,
        js: JetStreamContext,
        subject_prefix: str,
        event_discriminator: str | None = None,
        snapshot_store: SnapshotStore[A] | None = None,
    ) -> None:
        self._js = js
        self._subject_prefix = subject_prefix
        super().__init__(
            event_discriminator=event_discriminator,
            snapshot_store=snapshot_store,
        )

    @override
    async def _save_events(
        self, aggregate_id: str, events: list[BaseModel], expected_version: int | None
    ) -> None:
        subject = f"{self._subject_prefix}.{aggregate_id}"

        for event in events:
            await self._js.publish(
                subject,
                event.model_dump_json().encode("utf-8"),
            )

    @override
    async def _load_events(
        self,
        aggregate_id: str,
        from_version: int | None,
    ) -> list[BaseModel]:
        subject = f"{self._subject_prefix}.{aggregate_id}"

        events = []
        sub: JetStreamContext.PushSubscription | None = None
        try:
            pass
            sub = await self._js.subscribe(subject)
            while True:
                msg = await sub.next_msg(timeout=0.1)
                events.append(self._deserialize_event(A, msg.data.decode("utf-8")))
        except NatsTimeoutError:
            pass
        finally:
            if sub is not None:
                await sub.unsubscribe()

        return events
