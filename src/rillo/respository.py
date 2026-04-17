from abc import ABC, abstractmethod
from typing import Annotated, Generic, TypeVar, Union

from pydantic import BaseModel, Discriminator, JsonValue, TypeAdapter

from rillo.aggregate import Aggregate
from rillo.snapshot import Snapshot, SnapshotStore


class OptimisticConcurrencyError(Exception):
    pass


A = TypeVar("A", bound=Aggregate)


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
        events: list[bytes],
        expected_version: int | None,
    ) -> None: ...

    @abstractmethod
    async def _load_events(
        self,
        aggregate_id: str,
        from_version: int | None,
    ) -> list[bytes]: ...

    async def save(self, aggregate: A) -> None:
        if len(aggregate._pending_events) == 0:
            return

        events = [
            event.model_dump_json().encode() for event in aggregate._pending_events
        ]
        await self._save_events(aggregate._id, events, aggregate._version)
        events.clear()

        if self._snapshot_store is None:
            return

        state = aggregate.get_state()
        if state is None:
            return

        version = aggregate.version

        if version is not None and version % 10 == 0:
            snapshot = Snapshot(state=state.model_dump(), version=version)
            await self._snapshot_store.save(aggregate.id, snapshot)

    async def load(self, aggregate: A) -> None:
        if self._snapshot_store is None:
            snapshot = None
        else:
            snapshot = await self._snapshot_store.load(aggregate._id)
        if snapshot is not None:
            try:
                state = self._deserialize_state(aggregate, snapshot.state)
                aggregate.load_state(state, snapshot.version)
            except Exception:
                pass
        events = await self._load_events(aggregate._id, aggregate._version)
        for event_bytes in events:
            event = self._deserialize_event(aggregate, event_bytes.decode("utf-8"))
            aggregate.apply(event)
