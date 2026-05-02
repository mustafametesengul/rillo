import pytest
from conftest import DeleteAccount, SignUpWithUsername, User
from pydantic import JsonValue


class TestAggregateInit:
    def test_id(self, user: User) -> None:
        assert user.id == "user-1"

    def test_initial_version_is_zero(self, user: User) -> None:
        assert user.version == 0

    def test_initial_state_is_none(self, user: User) -> None:
        assert user.dump_state() is None

    def test_no_pending_events(self, user: User) -> None:
        assert user.pending_events == []


class TestExecuteAndApply:
    def test_execute_applies_event(self, user: User) -> None:
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        state = user.dump_state()
        assert isinstance(state, dict)
        assert state["username"] == "alice"
        assert state["password_hash"] == "hash123"
        assert state["account_deleted"] is False

    def test_execute_adds_pending_event(self, user: User) -> None:
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        assert len(user.pending_events) == 1
        pending = user.pending_events[0]
        assert isinstance(pending, dict)
        assert pending["type"] == "UserSignedUpV1"

    def test_multiple_events(self, user: User) -> None:
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        user.execute(DeleteAccount())
        assert len(user.pending_events) == 2
        state = user.dump_state()
        assert isinstance(state, dict)
        assert state["account_deleted"] is True

    def test_pending_events_serialized_as_json(self, user: User) -> None:
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        event = user.pending_events[0]
        assert isinstance(event, dict)
        assert event == {
            "type": "UserSignedUpV1",
            "username": "alice",
            "password_hash": "hash123",
        }


class TestRehydrate:
    def test_rehydrate_single_event(self, user: User) -> None:
        events: list[JsonValue] = [
            {
                "type": "UserSignedUpV1",
                "username": "bob",
                "password_hash": "pw",
            },
        ]
        user.rehydrate(events, 1)
        state = user.dump_state()
        assert isinstance(state, dict)
        assert state["username"] == "bob"
        assert user.version == 1

    def test_rehydrate_multiple_events(self, user: User) -> None:
        events: list[JsonValue] = [
            {
                "type": "UserSignedUpV1",
                "username": "bob",
                "password_hash": "pw",
            },
            {"type": "AccountDeletedV1"},
        ]
        user.rehydrate(events, 2)
        state = user.dump_state()
        assert isinstance(state, dict)
        assert state["account_deleted"] is True
        assert user.version == 2

    def test_rehydrate_does_not_add_pending_events(self, user: User) -> None:
        events: list[JsonValue] = [
            {
                "type": "UserSignedUpV1",
                "username": "bob",
                "password_hash": "pw",
            },
        ]
        user.rehydrate(events, 1)
        assert user.pending_events == []


class TestLoadState:
    def test_load_state_restores_state(self, user: User) -> None:
        state = {
            "type": "UserStateV1",
            "username": "carol",
            "password_hash": "h",
            "account_deleted": False,
        }
        user.load_state(state, 5)
        assert user.dump_state() == state
        assert user.version == 5

    def test_load_state_clears_pending_events(self, user: User) -> None:
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        assert len(user.pending_events) == 1
        state = {
            "type": "UserStateV1",
            "username": "carol",
            "password_hash": "h",
            "account_deleted": False,
        }
        user.load_state(state, 5)
        assert user.pending_events == []


class TestMarkEventsAsCommitted:
    def test_clears_pending_and_sets_version(self, user: User) -> None:
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        assert len(user.pending_events) == 1
        user.commit(10)
        assert user.pending_events == []
        assert user.version == 10


class TestDomainLogicErrors:
    def test_delete_without_signup_raises(self, user: User) -> None:
        with pytest.raises(ValueError, match="User does not exist"):
            user.execute(DeleteAccount())

    def test_double_delete_raises(self, user: User) -> None:
        user.execute(SignUpWithUsername(username="alice", password_hash="hash123"))
        user.execute(DeleteAccount())
        with pytest.raises(ValueError, match="Account is already deleted"):
            user.execute(DeleteAccount())


class TestUnregisteredEvent:
    def test_rehydrate_unregistered_event_raises(self, user: User) -> None:
        with pytest.raises(Exception):
            user.rehydrate([{"type": "UnknownV1"}], 1)
