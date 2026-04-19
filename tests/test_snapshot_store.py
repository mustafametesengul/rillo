import pytest
from conftest import User
from test_repository import InMemorySnapshotStore


class TestSnapshotStore:
    @pytest.mark.asyncio
    async def test_save_and_load(self) -> None:
        store = InMemorySnapshotStore()
        user = User("user-1")
        user.sign_up("alice", "hash123")
        user.mark_events_as_committed(1)

        await store.save(user)

        loaded = User("user-1")
        await store.load(loaded)
        state = loaded.get_state()
        assert isinstance(state, dict)
        assert state["username"] == "alice"
        assert loaded.version == 1

    @pytest.mark.asyncio
    async def test_load_nonexistent_is_noop(self) -> None:
        store = InMemorySnapshotStore()
        user = User("no-such-user")
        await store.load(user)
        assert user.get_state() is None
        assert user.version == 0

    @pytest.mark.asyncio
    async def test_save_without_state_is_noop(self) -> None:
        store = InMemorySnapshotStore()
        user = User("user-1")
        await store.save(user)
        assert "user-1" not in store._snapshots

    @pytest.mark.asyncio
    async def test_overwrite_snapshot(self) -> None:
        store = InMemorySnapshotStore()

        user = User("user-1")
        user.sign_up("alice", "hash123")
        user.mark_events_as_committed(1)
        await store.save(user)

        # Apply more events and re-snapshot
        user.apply([{"schema_version": "AccountDeletedV1"}], 2)
        await store.save(user)

        loaded = User("user-1")
        await store.load(loaded)
        state = loaded.get_state()
        assert isinstance(state, dict)
        assert state["account_deleted"] is True
        assert loaded.version == 2
