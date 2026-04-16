from abc import ABC, abstractmethod
from pydantic import BaseModel, JsonValue


class Snapshot(BaseModel):
    state: JsonValue
    version: int


class SnapshotStore(ABC):
    @abstractmethod
    async def load(self, aggregate_id: str) -> Snapshot | None: ...

    @abstractmethod
    async def save(self, aggregate_id: str, snapshot: Snapshot) -> None: ...
