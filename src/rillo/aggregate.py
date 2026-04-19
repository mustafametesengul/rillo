from typing import Annotated, Any, Callable, Generic, Sequence, TypeVar, Union

from pydantic import BaseModel, Discriminator, JsonValue, TypeAdapter

S = TypeVar("S", bound=BaseModel)
E = TypeVar("E", bound=BaseModel)


class Aggregate(Generic[S]):
    def __init__(
        self,
        id: str,
        state_class: type[S],
        schema_discriminator: str = "schema_version",
    ) -> None:
        self._id: str = id
        self._state_class: type[S] = state_class
        self._state: S | None = None
        self._pending_events: list[BaseModel] = []
        self._schema_discriminator: str = schema_discriminator
        self._mutators: dict[type[BaseModel], Callable[[Any], None]] = {}
        self._version: str | None = None

    def _add_mutator(
        self,
        event_type: type[E],
        mutator: Callable[[E], None] | None = None,
    ) -> None:
        mutator = mutator if mutator is not None else lambda _: None
        self._mutators[event_type] = mutator

    def _parse_event(self, event: JsonValue) -> BaseModel:
        event_types = tuple(self._mutators.keys())
        union_type = Union[event_types]  # type: ignore[valid-type]
        adapter = TypeAdapter(
            Annotated[union_type, Discriminator(self._schema_discriminator)]  # ty: ignore[invalid-type-form]
        )
        return adapter.validate_python(event)

    def _parse_state(self, state: JsonValue) -> S:
        adapter = TypeAdapter(self._state_class)
        return adapter.validate_python(state)

    def _apply(self, event: BaseModel) -> None:
        event_type = type(event)
        if event_type not in self._mutators:
            raise ValueError(f"No mutator registered for event type {event_type}.")
        mutator_func = self._mutators[event_type]
        mutator_func(event)

    def _publish(self, event: BaseModel) -> None:
        self._apply(event)
        self._pending_events.append(event)

    @property
    def id(self) -> str:
        return self._id

    @property
    def version(self) -> str | None:
        return self._version

    @property
    def pending_events(self) -> list[JsonValue]:
        return [event.model_dump(mode="json") for event in self._pending_events]

    def apply(self, events: Sequence[JsonValue], version: str) -> None:
        for event in events:
            parsed_event = self._parse_event(event)
            self._apply(parsed_event)
        self._version = version

    def get_state(self) -> JsonValue | None:
        if self._state is None:
            return None
        return self._state.model_dump(mode="json")

    def load_state(self, state: JsonValue, version: str) -> None:
        value = self._parse_state(state)
        self._state = value
        self._version = version
        self._pending_events.clear()

    def mark_events_as_committed(self, version: str) -> None:
        self._pending_events.clear()
        self._version = version
