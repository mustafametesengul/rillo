from typing import Any, Callable, Generic, Sequence, TypeVar, get_args

from pydantic import BaseModel, JsonValue

S = TypeVar("S", bound=BaseModel)
E = TypeVar("E", bound=BaseModel)


def mutator(
    event_type: type[E],
) -> Callable[[Callable[..., None]], Callable[..., None]]:
    def decorator(func: Callable[..., None]) -> Callable[..., None]:
        setattr(func, "_event_type", event_type)
        return func

    return decorator


class Aggregate(Generic[S]):
    _state_class: type[S]
    _schema_discriminator: str = "schema_version"
    _mutator_map: dict[type[BaseModel], str] = {}
    _event_types: dict[str, type[BaseModel]] = {}

    def __init_subclass__(
        cls,
        state_class: type[S],
        schema_discriminator: str = "schema_version",
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(**kwargs)
        if state_class is not None:
            cls._state_class = state_class
        cls._schema_discriminator = schema_discriminator

        cls._mutator_map = dict(getattr(cls, "_mutator_map", {}))
        cls._event_types = dict(getattr(cls, "_event_types", {}))

        for attr_name in vars(cls):
            attr = getattr(cls, attr_name)
            if callable(attr) and hasattr(attr, "_event_type"):
                event_type = getattr(attr, "_event_type")
                cls._mutator_map[event_type] = attr_name
                disc_annotation = event_type.model_fields[
                    cls._schema_discriminator
                ].annotation
                disc_value = str(get_args(disc_annotation)[0])
                cls._event_types[disc_value] = event_type

    def __init__(self, id: str) -> None:
        self._id: str = id
        self._state: S | None = None
        self._pending_events: list[BaseModel] = []
        self._version: int = 0

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
        if event_type not in self._mutator_map:
            raise ValueError(f"No mutator registered for event type {event_type}.")
        method_name = self._mutator_map[event_type]
        getattr(self, method_name)(event)

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


__all__ = ["Aggregate", "mutator"]
