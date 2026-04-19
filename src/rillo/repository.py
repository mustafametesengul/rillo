from abc import ABC, abstractmethod
from typing import Annotated, Generic, Sequence, TypeVar, Union

from pydantic import BaseModel, Discriminator, JsonValue, TypeAdapter

from rillo.aggregate import Aggregate
from rillo.snapshot_store import SnapshotStore

A = TypeVar("A", bound=Aggregate)


class OptimisticConcurrencyError(Exception):
    pass


class Repository(Generic[A], ABC):
    def __init__(
        self,
        schema_discriminator: str,
        snapshot_store: SnapshotStore[A] | None = None,
    ) -> None:
        self._schema_discriminator = schema_discriminator
        self._snapshot_store = snapshot_store

    def _parse_event(self, aggregate: A, event: JsonValue) -> BaseModel:
        union_type = Union[aggregate.event_types]
        adapter = TypeAdapter(
            Annotated[union_type, Discriminator(self._schema_discriminator)]
        )
        return adapter.validate_python(event)

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

        json_events = [event.model_dump(mode="json") for event in events]

        await self._save_events(aggregate.id, json_events, aggregate.version)

        if self._snapshot_store is None:
            return

        await self._snapshot_store.save(aggregate)

    async def load(self, aggregate: A) -> None:
        if self._snapshot_store is not None:
            await self._snapshot_store.load(aggregate)

        events = await self._load_events(aggregate.id, aggregate.version)
        for event in events:
            event_model = self._parse_event(aggregate, event)
            aggregate.apply(event_model)
