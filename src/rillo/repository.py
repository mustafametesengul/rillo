from abc import ABC, abstractmethod
from typing import Generic, Sequence, TypeVar

from pydantic import JsonValue

from rillo.aggregate import Aggregate

A = TypeVar("A", bound=Aggregate)


class OptimisticConcurrencyError(Exception):
    pass


class Repository(Generic[A], ABC):
    @abstractmethod
    async def _save_events(
        self,
        aggregate_id: str,
        events: Sequence[JsonValue],
        expected_version: int,
    ) -> int: ...

    @abstractmethod
    async def _load_events(
        self,
        aggregate_id: str,
        from_version: int,
    ) -> tuple[Sequence[JsonValue], int]: ...

    async def save(self, aggregate: A) -> None:
        events = aggregate.pending_events
        if len(events) == 0:
            return
        new_version = await self._save_events(aggregate.id, events, aggregate.version)
        aggregate.commit(new_version)

    async def load(self, aggregate: A) -> None:
        events, version = await self._load_events(aggregate.id, aggregate.version)
        aggregate.rehydrate(events, version)


__all__ = ["OptimisticConcurrencyError", "Repository"]
