from typing import Annotated, Any, Callable, Generic, TypeVar, Union

from pydantic import BaseModel, Discriminator, JsonValue, TypeAdapter


class OptimisticConcurrencyError(Exception):
    pass


S = TypeVar("S", bound=BaseModel)
E = TypeVar("E", bound=BaseModel)


class Aggregate(Generic[S]):
    def __init__(
        self,
        id: str,
        state_class: type[S],
        event_discriminator: str | None = None,
    ) -> None:
        self._id: str = id
        self._state_class: type[S] = state_class
        self._state: S | None = None
        self._pending_events: list[BaseModel] = []
        self._mutators: dict[type[BaseModel], Callable[[Any], None]] = {}
        self._event_discriminator: str | None = event_discriminator
        self._version: int | None = None

    def _add_mutator(self, event_type: type[E], mutator: Callable[[E], None]) -> None:
        self._mutators[event_type] = mutator

    def load_state(self, state: JsonValue) -> None:
        self._state = self._state_class.model_validate(state)

    def apply(self, event: BaseModel) -> None:
        """Route the event to the registered mutator."""
        event_type = type(event)
        if event_type not in self._mutators:
            raise ValueError(f"No mutator registered for event type {event_type}.")
        mutator_func = self._mutators[event_type]
        mutator_func(event)

    def deserialize_event(self, event_json: str) -> BaseModel:
        """Deserialize a JSON string into a typed event."""
        if self._event_discriminator is not None:
            event_types = tuple(self._mutators.keys())
            union_type = Union[event_types]
            adapter = TypeAdapter(
                Annotated[union_type, Discriminator(self._event_discriminator)]
            )
            return adapter.validate_json(event_json)

        for event_type in self._mutators:
            try:
                return TypeAdapter(event_type).validate_json(event_json)
            except Exception:
                continue
        raise ValueError("No matching event type found for the provided JSON.")

    def _publish(self, event: BaseModel) -> None:
        self._pending_events.append(event)
        self.apply(event)
