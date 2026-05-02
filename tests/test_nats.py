from uuid import uuid4

import nats
import pytest
import pytest_asyncio
from conftest import DeleteAccount, SignUpWithUsername, User

from rillo import OptimisticConcurrencyError
from rillo.nats import NATSRepository, NATSSnapshotStore


@pytest_asyncio.fixture
async def js():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()
    yield js
    await nc.close()


@pytest_asyncio.fixture
async def stream(js):
    stream_name = f"TEST_{uuid4().hex[:8]}"
    subject_prefix = f"test-{uuid4().hex[:8]}"
    await js.add_stream(name=stream_name, subjects=[f"{subject_prefix}.>"])
    yield stream_name, subject_prefix
    await js.delete_stream(stream_name)


@pytest_asyncio.fixture
async def repo(js, stream):
    stream_name, subject_prefix = stream
    return NATSRepository[User](js, stream_name, subject_prefix)


@pytest_asyncio.fixture
async def snapshot_store(js):
    bucket_name = f"test-snap-{uuid4().hex[:8]}"
    kv = await js.create_key_value(bucket=bucket_name)
    yield NATSSnapshotStore[User](kv)
    await js.delete_key_value(bucket_name)


class TestNATSRepositorySave:
    @pytest.mark.asyncio
    async def test_save_and_load(self, repo: NATSRepository[User]) -> None:
        user = User("user-1")
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        await repo.save(user)

        loaded = User("user-1")
        await repo.load(loaded)
        state = loaded.dump_state()
        assert isinstance(state, dict)
        assert state["username"] == "alice"
        assert state["password_hash"] == "hash123"
        assert loaded.version == user.version

    @pytest.mark.asyncio
    async def test_save_clears_pending_events(self, repo: NATSRepository[User]) -> None:
        user = User("user-1")
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        await repo.save(user)
        assert user.pending_events == []

    @pytest.mark.asyncio
    async def test_save_updates_version(self, repo: NATSRepository[User]) -> None:
        user = User("user-1")
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        await repo.save(user)
        assert user.version is not None
        assert user.version > 0

    @pytest.mark.asyncio
    async def test_save_multiple_events(self, repo: NATSRepository[User]) -> None:
        user = User("user-1")
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        user.execute(DeleteAccount())
        await repo.save(user)

        loaded = User("user-1")
        await repo.load(loaded)
        state = loaded.dump_state()
        assert isinstance(state, dict)
        assert state["account_deleted"] is True

    @pytest.mark.asyncio
    async def test_save_no_events_is_noop(self, repo: NATSRepository[User]) -> None:
        user = User("user-1")
        await repo.save(user)
        assert user.version == 0


class TestNATSRepositoryLoad:
    @pytest.mark.asyncio
    async def test_load_empty_aggregate(self, repo: NATSRepository[User]) -> None:
        user = User("nonexistent")
        await repo.load(user)
        assert user.dump_state() is None
        assert user.version == 0

    @pytest.mark.asyncio
    async def test_load_multiple_saves(self, repo: NATSRepository[User]) -> None:
        user = User("user-1")
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        await repo.save(user)

        user.execute(DeleteAccount())
        await repo.save(user)

        loaded = User("user-1")
        await repo.load(loaded)
        state = loaded.dump_state()
        assert isinstance(state, dict)
        assert state["account_deleted"] is True
        assert loaded.version == user.version


class TestNATSOptimisticConcurrency:
    @pytest.mark.asyncio
    async def test_concurrent_save_raises(self, repo: NATSRepository[User]) -> None:
        user = User("user-1")
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        await repo.save(user)

        user_a = User("user-1")
        await repo.load(user_a)

        user_b = User("user-1")
        await repo.load(user_b)

        user_a.execute(DeleteAccount())
        await repo.save(user_a)

        user_b.execute(DeleteAccount())
        with pytest.raises(OptimisticConcurrencyError):
            await repo.save(user_b)


class TestNATSSnapshotStore:
    @pytest.mark.asyncio
    async def test_save_and_load_snapshot(
        self, snapshot_store: NATSSnapshotStore[User]
    ) -> None:
        user = User("user-1")
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        user.commit(1)

        await snapshot_store.save(user)

        loaded = User("user-1")
        await snapshot_store.load(loaded)
        state = loaded.dump_state()
        assert isinstance(state, dict)
        assert state["username"] == "alice"
        assert loaded.version == 1

    @pytest.mark.asyncio
    async def test_load_nonexistent_is_noop(
        self, snapshot_store: NATSSnapshotStore[User]
    ) -> None:
        user = User("no-such-user")
        await snapshot_store.load(user)
        assert user.dump_state() is None
        assert user.version == 0


class TestNATSRepositoryWithSnapshots:
    @pytest.mark.asyncio
    async def test_load_uses_snapshot(
        self,
        repo: NATSRepository[User],
        snapshot_store: NATSSnapshotStore[User],
    ) -> None:
        user = User("user-1")
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        await repo.save(user)
        await snapshot_store.save(user)

        user.execute(DeleteAccount())
        await repo.save(user)

        loaded = User("user-1")
        await snapshot_store.load(loaded)
        await repo.load(loaded)

        state = loaded.dump_state()
        assert isinstance(state, dict)
        assert state["account_deleted"] is True
        assert loaded.version == user.version
