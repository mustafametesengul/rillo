from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import JsonValue

from rillo.aggregate import Aggregate

A = TypeVar("A", bound=Aggregate)


class SnapshotStore(Generic[A], ABC):
    @abstractmethod
    async def _load_state(self, aggregate_id: str) -> tuple[JsonValue, int] | None: ...

    @abstractmethod
    async def _save_state(
        self,
        aggregate_id: str,
        state: JsonValue,
        version: int,
    ) -> None: ...

    async def load(self, aggregate: A) -> None:
        snapshot = await self._load_state(aggregate.id)
        if snapshot is None:
            return
        state, version = snapshot
        aggregate.load_state(state, version)

    async def save(self, aggregate: A) -> None:
        state = aggregate.get_state()
        version = aggregate.version
        if state is not None and version != 0:
            await self._save_state(aggregate.id, state, version)
