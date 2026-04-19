from typing import Literal
from uuid import uuid4

import pytest
from pydantic import BaseModel

from rillo import Aggregate


class UserSignedUp(BaseModel):
    schema_version: Literal["UserSignedUpV1"] = "UserSignedUpV1"
    username: str
    password_hash: str


class AccountDeleted(BaseModel):
    schema_version: Literal["AccountDeletedV1"] = "AccountDeletedV1"


class UserState(BaseModel):
    schema_version: Literal["UserStateV1"] = "UserStateV1"
    username: str
    password_hash: str
    account_deleted: bool


class User(Aggregate[UserState]):
    def __init__(self, id: str | None = None) -> None:
        super().__init__(id or str(uuid4()), UserState)
        self._add_mutator(UserSignedUp, self.apply_user_signed_up)
        self._add_mutator(AccountDeleted, self.apply_account_deleted)

    def apply_user_signed_up(self, event: UserSignedUp) -> None:
        self._state = UserState(
            username=event.username,
            password_hash=event.password_hash,
            account_deleted=False,
        )

    def apply_account_deleted(self, _: AccountDeleted) -> None:
        if self._state is None:
            raise ValueError("User does not exist.")
        self._state.account_deleted = True

    def sign_up(self, username: str, password_hash: str) -> None:
        self._publish(UserSignedUp(username=username, password_hash=password_hash))

    def delete_account(self) -> None:
        if self._state is None:
            raise ValueError("User does not exist.")
        if self._state.account_deleted:
            raise ValueError("Account is already deleted.")
        self._publish(AccountDeleted())


@pytest.fixture
def user() -> User:
    return User("user-1")
