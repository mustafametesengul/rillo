from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from rillo.aggregate import Aggregate
from rillo.snapshot import SnapshotStore, Snapshot


class OptimisticConcurrencyError(Exception):
    pass


A = TypeVar("A", bound=Aggregate)


class Repository(Generic[A], ABC):
    def __init__(self, snapshot_store: SnapshotStore) -> None:
        self._snapshot_store = snapshot_store

    @abstractmethod
    async def _save_events(
        self,
        aggregate_id: str,
        events: list[str],
        expected_version: int | None,
    ) -> None: ...

    @abstractmethod
    async def _load_events(
        self,
        aggregate_id: str,
        from_version: int | None,
    ) -> list[str]: ...

    async def save(self, aggregate: A) -> None:
        if len(aggregate._pending_events) == 0:
            return
        events = [event.model_dump_json() for event in aggregate._pending_events]
        await self._save_events(aggregate._id, events, aggregate._version)
        events.clear()
        version = aggregate._version or 0
        new_version = version + len(events)
        aggregate._version = new_version
        if new_version % 10 == 0 and aggregate._state is not None:
            snapshot = Snapshot(state=aggregate._state, version=new_version)
            await self._snapshot_store.save(aggregate._id, snapshot)

    async def load(self, aggregate: A) -> None:
        snapshot = await self._snapshot_store.load(aggregate._id)
        if snapshot is not None:
            try:
                aggregate.load_state(snapshot.state)
                aggregate._version = snapshot.version
            except Exception:
                pass
        events = await self._load_events(aggregate._id, aggregate._version)
        for event_json in events:
            event = aggregate.deserialize_event(event_json)
            aggregate.apply(event)
