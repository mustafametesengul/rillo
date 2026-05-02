from abc import ABC, abstractmethod
from typing import Generic, Sequence, TypeVar, get_args, get_origin

from pydantic import BaseModel, JsonValue, TypeAdapter

S = TypeVar("S", bound=BaseModel)
E = TypeVar("E", bound=BaseModel)
C = TypeVar("C", bound=BaseModel)


class Aggregate(ABC, Generic[S, E, C]):
    _state_adapter: TypeAdapter[S]
    _event_adapter: TypeAdapter[E]
    _command_adapter: TypeAdapter[C]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        state_type, event_type, command_type = cls._aggregate_types()
        cls._state_adapter = TypeAdapter(state_type)
        cls._event_adapter = TypeAdapter(event_type)
        cls._command_adapter = TypeAdapter(command_type)

    @classmethod
    def _aggregate_types(cls) -> tuple[type[S], type[E], type[C]]:
        for base in getattr(cls, "__orig_bases__", ()):
            if get_origin(base) is Aggregate:
                return get_args(base)
        raise TypeError("Aggregate subclasses must specify type parameters S, E, and C")

    def __init__(self, id: str) -> None:
        self._id: str = id
        self._state: S | None = None
        self._pending_events: list[E] = []
        self._version: int = 0

    @abstractmethod
    def apply(self, event: E) -> None: ...

    @abstractmethod
    def execute(self, command: C) -> None: ...

    def _parse_event(self, event: JsonValue) -> E:
        return self._event_adapter.validate_python(event)

    def _parse_state(self, state: JsonValue) -> S:
        return self._state_adapter.validate_python(state)

    def _emit(self, event: E) -> None:
        self.apply(event)
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

    def rehydrate(self, events: Sequence[JsonValue], version: int) -> None:
        original_state = None
        if self._state is not None:
            original_state = self._state.model_copy(deep=True)
        original_version = self._version
        try:
            for event in events:
                parsed_event = self._parse_event(event)
                self.apply(parsed_event)
            self._version = version
        except Exception:
            self._state = original_state
            self._version = original_version
            raise

    def dump_state(self) -> JsonValue | None:
        if self._state is None:
            return None
        return self._state.model_dump(mode="json")

    def load_state(self, state: JsonValue, version: int) -> None:
        value = self._parse_state(state)
        self._state = value
        self._version = version
        self._pending_events.clear()

    def commit(self, version: int) -> None:
        self._pending_events.clear()
        self._version = version


__all__ = ["Aggregate"]
