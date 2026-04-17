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
from rillo.snapshot import SnapshotStore

A = TypeVar("A", bound=Aggregate)


class NATSSnapshotStore(SnapshotStore):
    def __init__(self, kv: KeyValue) -> None:
        self._kv = kv

    @override
    async def load(self, aggregate_id: str) -> tuple[BaseModel, int] | None:
        entry = await self._kv.get(aggregate_id)
        value = entry.value
        if value is None:
            return None
        # return value.decode("utf-8")

    @override
    async def save(self, aggregate_id: str, snapshot: tuple[BaseModel, int]) -> None:
        pass


class NATSRepository(Repository[A]):
    def __init__(
        self,
        js: JetStreamContext,
        subject_prefix: str,
        event_discriminator: str | None = None,
        snapshot_store: SnapshotStore | None = None,
    ) -> None:
        self._js = js
        self._subject_prefix = subject_prefix
        super().__init__(
            event_discriminator=event_discriminator,
            snapshot_store=snapshot_store,
        )

    @override
    async def _save_events(
        self, aggregate_id: str, events: list[bytes], expected_version: int | None
    ) -> None:
        subject = f"{self._subject_prefix}.{aggregate_id}"

        for event in events:
            await self._js.publish(
                subject,
                event,
            )

    @override
    async def _load_events(
        self,
        aggregate_id: str,
        from_version: int | None,
    ) -> list[bytes]:
        subject = f"{self._subject_prefix}.{aggregate_id}"

        events = []
        sub: JetStreamContext.PushSubscription | None = None
        try:
            pass
            sub = await self._js.subscribe(subject)
            while True:
                msg = await sub.next_msg(timeout=0.1)
                events.append(msg.data)
        except NatsTimeoutError:
            pass
        finally:
            if sub is not None:
                await sub.unsubscribe()

        return events
