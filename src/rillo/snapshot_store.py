from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel

from rillo.aggregate import Aggregate

A = TypeVar("A", bound=Aggregate)


class SnapshotStore(Generic[A], ABC):
    def _deserialize_state(self, aggregate: A, state: str) -> BaseModel:
        return aggregate._state_class.model_validate_json(state)

    @abstractmethod
    async def _load_state(self, aggregate_id: str) -> tuple[BaseModel, int]: ...

    @abstractmethod
    async def _save_state(
        self, aggregate_id: str, state: BaseModel, version: int
    ) -> None: ...

    async def load(self, aggregate: A) -> None:
        state = aggregate.get_state()
        version = aggregate.version
        if state is not None and version is not None:
            loaded_state, loaded_version = await self._load_state(aggregate.id)
            aggregate.load_state(loaded_state, version=loaded_version)

    async def save(self, aggregate: A) -> None:
        state = aggregate.get_state()
        version = aggregate.version
        if state is not None and version is not None:
            await self._save_state(aggregate.id, state, version)
