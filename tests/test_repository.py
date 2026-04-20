from typing import Sequence

import pytest
from conftest import User
from pydantic import JsonValue

from rillo import OptimisticConcurrencyError, Repository
from rillo.snapshot_store import SnapshotStore


class InMemoryRepository(Repository[User]):
    """Simple in-memory repository for testing the base Repository logic."""

    def __init__(self) -> None:
        self._streams: dict[str, list[JsonValue]] = {}
        self._seq: dict[str, int] = {}

    async def _save_events(
        self,
        aggregate_id: str,
        events: Sequence[JsonValue],
        expected_version: int,
    ) -> int:
        current_seq = self._seq.get(aggregate_id, 0)
        if expected_version != current_seq:
            raise OptimisticConcurrencyError(
                f"Expected version {expected_version}, but current is {current_seq}"
            )
        if aggregate_id not in self._streams:
            self._streams[aggregate_id] = []
        self._streams[aggregate_id].extend(events)
        self._seq[aggregate_id] = current_seq + len(events)
        return self._seq[aggregate_id]

    async def _load_events(
        self,
        aggregate_id: str,
        from_version: int,
    ) -> tuple[Sequence[JsonValue], int]:
        events = self._streams.get(aggregate_id, [])
        return events[from_version:], len(events)


class InMemorySnapshotStore(SnapshotStore[User]):
    def __init__(self) -> None:
        self._snapshots: dict[str, tuple[JsonValue, int]] = {}

    async def _load_state(self, aggregate_id: str) -> tuple[JsonValue, int] | None:
        return self._snapshots.get(aggregate_id)

    async def _save_state(
        self,
        aggregate_id: str,
        state: JsonValue,
        version: int,
    ) -> None:
        self._snapshots[aggregate_id] = (state, version)


@pytest.fixture
def repo() -> InMemoryRepository:
    return InMemoryRepository()


class TestRepositorySave:
    @pytest.mark.asyncio
    async def test_save_persists_events(
        self, user: User, repo: InMemoryRepository
    ) -> None:
        user.sign_up("alice", "hash123")
        await repo.save(user)
        assert repo._streams["user-1"] == [
            {
                "schema_version": "UserSignedUpV1",
                "username": "alice",
                "password_hash": "hash123",
            }
        ]

    @pytest.mark.asyncio
    async def test_save_clears_pending_events(
        self, user: User, repo: InMemoryRepository
    ) -> None:
        user.sign_up("alice", "hash123")
        await repo.save(user)
        assert user.pending_events == []

    @pytest.mark.asyncio
    async def test_save_updates_version(
        self, user: User, repo: InMemoryRepository
    ) -> None:
        user.sign_up("alice", "hash123")
        await repo.save(user)
        assert user.version == 1

    @pytest.mark.asyncio
    async def test_save_no_events_is_noop(
        self, user: User, repo: InMemoryRepository
    ) -> None:
        await repo.save(user)
        assert "user-1" not in repo._streams

    @pytest.mark.asyncio
    async def test_save_multiple_events(
        self, user: User, repo: InMemoryRepository
    ) -> None:
        user.sign_up("alice", "hash123")
        user.delete_account()
        await repo.save(user)
        assert len(repo._streams["user-1"]) == 2
        assert user.version == 2


class TestRepositoryLoad:
    @pytest.mark.asyncio
    async def test_load_restores_state(self, repo: InMemoryRepository) -> None:
        user = User("user-1")
        user.sign_up("alice", "hash123")
        await repo.save(user)

        loaded_user = User("user-1")
        await repo.load(loaded_user)
        state = loaded_user.get_state()
        assert isinstance(state, dict)
        assert state["username"] == "alice"
        assert loaded_user.version == 1

    @pytest.mark.asyncio
    async def test_load_empty_aggregate(self, repo: InMemoryRepository) -> None:
        user = User("nonexistent")
        await repo.load(user)
        assert user.get_state() is None
        assert user.version == 0


class TestOptimisticConcurrency:
    @pytest.mark.asyncio
    async def test_concurrent_save_raises(self, repo: InMemoryRepository) -> None:
        user1 = User("user-1")
        user1.sign_up("alice", "hash123")
        await repo.save(user1)

        # Simulate two loads of the same aggregate
        user_a = User("user-1")
        await repo.load(user_a)

        user_b = User("user-1")
        await repo.load(user_b)

        # Both try to save - first succeeds
        user_a.delete_account()
        await repo.save(user_a)

        # Second should fail with concurrency error
        user_b.delete_account()
        with pytest.raises(OptimisticConcurrencyError):
            await repo.save(user_b)


class TestRepositoryWithSnapshotStore:
    @pytest.mark.asyncio
    async def test_load_uses_snapshot(self, repo: InMemoryRepository) -> None:
        snapshot_store = InMemorySnapshotStore()

        # Save initial events
        user = User("user-1")
        user.sign_up("alice", "hash123")
        await repo.save(user)

        # Store snapshot
        await snapshot_store.save(user)

        # Add more events after snapshot
        user.delete_account()
        await repo.save(user)

        # Load from scratch - should use snapshot + remaining events
        loaded = User("user-1")
        await snapshot_store.load(loaded)
        await repo.load(loaded)

        state = loaded.get_state()
        assert isinstance(state, dict)
        assert state["username"] == "alice"
        assert state["account_deleted"] is True
