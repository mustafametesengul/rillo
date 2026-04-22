from typing import Sequence, TypeVar, override

from pydantic import BaseModel, JsonValue

try:
    from nats.js.client import JetStreamContext
    from nats.js.errors import KeyNotFoundError, NotFoundError
    from nats.js.kv import KeyValue
except ImportError as e:
    raise ImportError(
        "nats-py is required to use NATSRepository. "
        "Install it with: pip install 'rillo[nats]'"
    ) from e


from rillo.aggregate import Aggregate
from rillo.repository import OptimisticConcurrencyError, Repository
from rillo.snapshot_store import SnapshotStore

A = TypeVar("A", bound=Aggregate)

_INVALID_SUBJECT_CHARS = set(".*> \t\n\r\0")


def _validate_aggregate_id(aggregate_id: str) -> None:
    if not aggregate_id or any(c in _INVALID_SUBJECT_CHARS for c in aggregate_id):
        raise ValueError(
            "aggregate_id must not be empty or contain '.', '*', '>', "
            "or whitespace characters"
        )


class Snapshot(BaseModel):
    state: JsonValue
    version: int


class EventBatch(BaseModel):
    events: Sequence[JsonValue]


class NATSSnapshotStore(SnapshotStore[A]):
    def __init__(self, kv: KeyValue) -> None:
        self._kv = kv

    @override
    async def _load_state(self, aggregate_id: str) -> tuple[JsonValue, int] | None:
        _validate_aggregate_id(aggregate_id)
        try:
            entry = await self._kv.get(aggregate_id)
        except KeyNotFoundError:
            return None
        value = entry.value
        if value is None:
            return None
        snapshot = Snapshot.model_validate_json(value.decode("utf-8"))
        return snapshot.state, snapshot.version

    @override
    async def _save_state(
        self,
        aggregate_id: str,
        state: JsonValue,
        version: int,
    ) -> None:
        _validate_aggregate_id(aggregate_id)
        snapshot = Snapshot(
            state=state,
            version=version,
        )
        await self._kv.put(aggregate_id, snapshot.model_dump_json().encode("utf-8"))


class NATSRepository(Repository[A]):
    def __init__(
        self,
        js: JetStreamContext,
        stream_name: str,
        subject_prefix: str,
    ) -> None:
        self._js = js
        self._stream_name = stream_name
        self._subject_prefix = subject_prefix

    @override
    async def _save_events(
        self,
        aggregate_id: str,
        events: Sequence[JsonValue],
        expected_version: int,
    ) -> int:
        _validate_aggregate_id(aggregate_id)
        subject = f"{self._subject_prefix}.{aggregate_id}"

        event_batch = EventBatch(events=events)

        headers = {"Nats-Expected-Last-Subject-Sequence": str(expected_version)}
        try:
            result = await self._js.publish(
                subject,
                event_batch.model_dump_json().encode("utf-8"),
                headers=headers,
            )
            return result.seq
        except Exception as e:
            if "wrong last sequence" in str(e).lower():
                raise OptimisticConcurrencyError(
                    f"Concurrency conflict for aggregate {aggregate_id}"
                ) from e
            raise

    @override
    async def _load_events(
        self,
        aggregate_id: str,
        from_version: int,
    ) -> tuple[Sequence[JsonValue], int]:
        _validate_aggregate_id(aggregate_id)
        subject = f"{self._subject_prefix}.{aggregate_id}"

        seq = from_version + 1
        all_events: list[JsonValue] = []
        version = from_version

        while True:
            try:
                msg = await self._js.get_msg(
                    self._stream_name,
                    seq=seq,
                    subject=subject,
                    next=True,
                )
                if msg.data is None or msg.seq is None:
                    break
                event_batch = EventBatch.model_validate_json(msg.data.decode("utf-8"))
                all_events.extend(event_batch.events)
                version = msg.seq
                seq = msg.seq + 1
            except NotFoundError:
                break

        return all_events, version


__all__ = ["NATSRepository", "NATSSnapshotStore"]
