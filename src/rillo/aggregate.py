from typing import Any, Callable, Generic, TypeVar

from pydantic import BaseModel


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

    @property
    def id(self) -> str:
        return self._id

    @property
    def version(self) -> int | None:
        return self._version

    @property
    def pending_events(self) -> list[BaseModel]:
        return self._pending_events.copy()

    @property
    def event_discriminator(self) -> str | None:
        return self._event_discriminator

    def _add_mutator(self, event_type: type[E], mutator: Callable[[E], None]) -> None:
        self._mutators[event_type] = mutator

    def apply(self, event: BaseModel) -> None:
        """Route the event to the registered mutator."""
        event_type = type(event)
        if event_type not in self._mutators:
            raise ValueError(f"No mutator registered for event type {event_type}.")
        mutator_func = self._mutators[event_type]
        mutator_func(event)
        self._version = (self._version or 0) + 1

    def _publish(self, event: BaseModel) -> None:
        self._pending_events.append(event)
        self.apply(event)

    def get_state(self) -> BaseModel | None:
        if self._state is None:
            return None
        return self._state.model_copy()

    def load_state(self, value: BaseModel, version: int) -> None:
        if not isinstance(value, self._state_class):
            raise ValueError(
                f"Invalid state type: expected {self._state_class}, got {type(value)}"
            )
        self._state = value
        self._version = version
        self._pending_events.clear()

    @property
    def event_types(self) -> tuple[type[BaseModel], ...]:
        return tuple(self._mutators.keys())

    @property
    def state_type(self) -> type[BaseModel]:
        return self._state_class
