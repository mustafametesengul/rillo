from abc import ABC, abstractmethod
from typing import Annotated, Generic, TypeVar, Union

from pydantic import BaseModel, Discriminator, JsonValue, TypeAdapter

from rillo.aggregate import Aggregate
from rillo.snapshot import SnapshotStore

A = TypeVar("A", bound=Aggregate)


class OptimisticConcurrencyError(Exception):
    pass


class EventBatch(BaseModel):
    events: list[JsonValue]


class Repository(Generic[A], ABC):
    def __init__(
        self,
        event_discriminator: str | None = None,
        snapshot_store: SnapshotStore | None = None,
    ) -> None:
        self._event_discriminator = event_discriminator
        self._snapshot_store = snapshot_store

    def _deserialize_state(self, aggregate: A, state: str) -> BaseModel:
        return aggregate._state_class.model_validate_json(state)

    def _deserialize_event(self, aggregate: A, event: str) -> BaseModel:
        """Deserialize a JSON string into a typed event."""
        if self._event_discriminator is not None:
            union_type = Union[aggregate.event_types]
            adapter = TypeAdapter(
                Annotated[union_type, Discriminator(self._event_discriminator)]
            )
            return adapter.validate_json(event)

        for event_type in aggregate.event_types:
            try:
                return TypeAdapter(event_type).validate_json(event)
            except Exception:
                continue
        raise ValueError("No matching event type found for the provided JSON.")

    @abstractmethod
    async def _save_events(
        self,
        aggregate_id: str,
        events: list[BaseModel],
        expected_version: int | None,
    ) -> None: ...

    @abstractmethod
    async def _load_events(
        self,
        aggregate_id: str,
        from_version: int | None,
    ) -> list[BaseModel]: ...

    async def save(self, aggregate: A) -> None:
        events = aggregate.pending_events
        if len(events) == 0:
            return

        await self._save_events(aggregate.id, events, aggregate.version)

        if self._snapshot_store is None:
            return

        state = aggregate.get_state()
        if state is None:
            return

        version = aggregate.version

        if version is not None and version % 10 == 0:
            snapshot = (state, version)
            await self._snapshot_store.save(aggregate.id, snapshot)

    async def load(self, aggregate: A) -> None:
        if self._snapshot_store is None:
            snapshot = None
        else:
            try:
                snapshot = await self._snapshot_store.load(aggregate.id)
            except Exception:
                snapshot = None

        if snapshot is not None:
            state, version = snapshot
            aggregate.load_state(state, version)

        events = await self._load_events(aggregate.id, aggregate.version)
        for event in events:
            aggregate.apply(event)
