from abc import ABC, abstractmethod

from pydantic import BaseModel


class SnapshotStore(ABC):
    @abstractmethod
    async def load(self, aggregate_id: str) -> tuple[BaseModel, int] | None: ...

    @abstractmethod
    async def save(
        self, aggregate_id: str, snapshot: tuple[BaseModel, int]
    ) -> None: ...
