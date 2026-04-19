from abc import ABC, abstractmethod
from typing import Generic, Sequence, TypeVar

from pydantic import JsonValue

from rillo.aggregate import Aggregate
from rillo.snapshot_store import SnapshotStore

A = TypeVar("A", bound=Aggregate)


class OptimisticConcurrencyError(Exception):
    pass


class Repository(Generic[A], ABC):
    def __init__(
        self,
        snapshot_store: SnapshotStore[A] | None = None,
    ) -> None:
        self._snapshot_store = snapshot_store

    @abstractmethod
    async def _save_events(
        self,
        aggregate_id: str,
        events: Sequence[JsonValue],
        expected_version: int,
    ) -> None: ...

    @abstractmethod
    async def _load_events(
        self,
        aggregate_id: str,
        from_version: int,
    ) -> Sequence[JsonValue]: ...

    async def save(self, aggregate: A) -> None:
        events = aggregate.pending_events
        if len(events) == 0:
            return
        expected_version = aggregate.version - len(events)
        await self._save_events(aggregate.id, events, expected_version)

    async def load(self, aggregate: A) -> None:
        if self._snapshot_store is not None:
            await self._snapshot_store.load(aggregate)

        events = await self._load_events(aggregate.id, aggregate.version)
        for event in events:
            aggregate.apply(event)
