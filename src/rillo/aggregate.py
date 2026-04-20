from typing import Any, Callable, Generic, Sequence, TypeVar, get_args

from pydantic import BaseModel, JsonValue

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
        self._state: S | None = None
        self._pending_events: list[BaseModel] = []
        self._version: int = 0

        self._state_class: type[S] = state_class
        self._schema_discriminator: str = schema_discriminator
        self._mutators: dict[type[BaseModel], Callable[[Any], None]] = {}
        self._event_types: dict[str, type[BaseModel]] = {}

    def _add_mutator(
        self,
        event_type: type[E],
        mutator: Callable[[E], None] | None = None,
    ) -> None:
        mutator = mutator if mutator is not None else lambda _: None
        self._mutators[event_type] = mutator
        discriminator_annotation = event_type.model_fields[
            self._schema_discriminator
        ].annotation
        discriminator_value = str(get_args(discriminator_annotation)[0])
        self._event_types[discriminator_value] = event_type

    def _parse_event(self, event: JsonValue) -> BaseModel:
        if type(event) is not dict or self._schema_discriminator not in event:
            raise ValueError("Event must be a JSON object.")
        event_type_value = event[self._schema_discriminator]

        if event_type_value not in self._event_types:
            raise ValueError(f"Unknown event type: {event_type_value}")

        event_type = self._event_types[event_type_value]
        return event_type.model_validate(event)

    def _parse_state(self, state: JsonValue) -> S:
        return self._state_class.model_validate(state)

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
    def version(self) -> int:
        return self._version

    @property
    def pending_events(self) -> list[JsonValue]:
        return [event.model_dump(mode="json") for event in self._pending_events]

    def apply(self, events: Sequence[JsonValue], version: int) -> None:
        original_state = None
        if self._state is not None:
            original_state = self._state.model_copy(deep=True)
        original_version = self._version
        try:
            for event in events:
                parsed_event = self._parse_event(event)
                self._apply(parsed_event)
            self._version = version
        except Exception:
            self._state = original_state
            self._version = original_version
            raise

    def get_state(self) -> JsonValue | None:
        if self._state is None:
            return None
        return self._state.model_dump(mode="json")

    def load_state(self, state: JsonValue, version: int) -> None:
        value = self._parse_state(state)
        self._state = value
        self._version = version
        self._pending_events.clear()

    def mark_events_as_committed(self, version: int) -> None:
        self._pending_events.clear()
        self._version = version


__all__ = ["Aggregate"]
