from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel, JsonValue

from rillo.aggregate import Aggregate

A = TypeVar("A", bound=Aggregate)


class SnapshotStore(Generic[A], ABC):
    def _deserialize_state(self, aggregate: A, state: JsonValue) -> BaseModel:
        return aggregate.state_type.model_validate(state)

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
        state = aggregate.get_state()
        version = aggregate.version
        if state is not None and version is not None:
            snapshot = await self._load_state(aggregate.id)
            if snapshot is None:
                return
            loaded_state, loaded_version = snapshot
            state_model = self._deserialize_state(aggregate, loaded_state)
            aggregate.load_state(state_model, version=loaded_version)

    async def save(self, aggregate: A) -> None:
        state = aggregate.get_state()
        version = aggregate.version
        if state is not None and version is not None:
            await self._save_state(aggregate.id, state.model_dump(mode="json"), version)
