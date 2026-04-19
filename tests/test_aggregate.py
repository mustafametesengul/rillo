import pytest
from conftest import User
from pydantic import JsonValue


class TestAggregateInit:
    def test_id(self, user: User) -> None:
        assert user.id == "user-1"

    def test_initial_version_is_zero(self, user: User) -> None:
        assert user.version == 0

    def test_initial_state_is_none(self, user: User) -> None:
        assert user.get_state() is None

    def test_no_pending_events(self, user: User) -> None:
        assert user.pending_events == []


class TestPublishAndMutate:
    def test_publish_applies_event(self, user: User) -> None:
        user.sign_up("alice", "hash123")
        state = user.get_state()
        assert isinstance(state, dict)
        assert state["username"] == "alice"
        assert state["password_hash"] == "hash123"
        assert state["account_deleted"] is False

    def test_publish_adds_pending_event(self, user: User) -> None:
        user.sign_up("alice", "hash123")
        assert len(user.pending_events) == 1
        pending = user.pending_events[0]
        assert isinstance(pending, dict)
        assert pending["schema_version"] == "UserSignedUpV1"

    def test_multiple_events(self, user: User) -> None:
        user.sign_up("alice", "hash123")
        user.delete_account()
        assert len(user.pending_events) == 2
        state = user.get_state()
        assert isinstance(state, dict)
        assert state["account_deleted"] is True

    def test_pending_events_serialized_as_json(self, user: User) -> None:
        user.sign_up("alice", "hash123")
        event = user.pending_events[0]
        assert isinstance(event, dict)
        assert event == {
            "schema_version": "UserSignedUpV1",
            "username": "alice",
            "password_hash": "hash123",
        }


class TestApplyFromJson:
    def test_apply_single_event(self, user: User) -> None:
        events: list[JsonValue] = [
            {
                "schema_version": "UserSignedUpV1",
                "username": "bob",
                "password_hash": "pw",
            },
        ]
        user.apply(events, 1)
        state = user.get_state()
        assert isinstance(state, dict)
        assert state["username"] == "bob"
        assert user.version == 1

    def test_apply_multiple_events(self, user: User) -> None:
        events: list[JsonValue] = [
            {
                "schema_version": "UserSignedUpV1",
                "username": "bob",
                "password_hash": "pw",
            },
            {"schema_version": "AccountDeletedV1"},
        ]
        user.apply(events, 2)
        state = user.get_state()
        assert isinstance(state, dict)
        assert state["account_deleted"] is True
        assert user.version == 2

    def test_apply_does_not_add_pending_events(self, user: User) -> None:
        events: list[JsonValue] = [
            {
                "schema_version": "UserSignedUpV1",
                "username": "bob",
                "password_hash": "pw",
            },
        ]
        user.apply(events, 1)
        assert user.pending_events == []


class TestLoadState:
    def test_load_state_restores_state(self, user: User) -> None:
        state = {
            "schema_version": "UserStateV1",
            "username": "carol",
            "password_hash": "h",
            "account_deleted": False,
        }
        user.load_state(state, 5)
        assert user.get_state() == state
        assert user.version == 5

    def test_load_state_clears_pending_events(self, user: User) -> None:
        user.sign_up("alice", "hash123")
        assert len(user.pending_events) == 1
        state = {
            "schema_version": "UserStateV1",
            "username": "carol",
            "password_hash": "h",
            "account_deleted": False,
        }
        user.load_state(state, 5)
        assert user.pending_events == []


class TestMarkEventsAsCommitted:
    def test_clears_pending_and_sets_version(self, user: User) -> None:
        user.sign_up("alice", "hash123")
        assert len(user.pending_events) == 1
        user.mark_events_as_committed(10)
        assert user.pending_events == []
        assert user.version == 10


class TestDomainLogicErrors:
    def test_delete_without_signup_raises(self, user: User) -> None:
        with pytest.raises(ValueError, match="User does not exist"):
            user.delete_account()

    def test_double_delete_raises(self, user: User) -> None:
        user.sign_up("alice", "hash123")
        user.delete_account()
        with pytest.raises(ValueError, match="Account is already deleted"):
            user.delete_account()


class TestUnregisteredEvent:
    def test_apply_unregistered_event_raises(self, user: User) -> None:
        with pytest.raises(Exception):
            user.apply([{"schema_version": "UnknownV1"}], 1)
