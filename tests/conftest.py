from typing import Annotated, Literal, override

import pytest
from pydantic import BaseModel, Field

from rillo import Aggregate


class UserSignedUp(BaseModel):
    type: Literal["UserSignedUpV1"] = "UserSignedUpV1"
    username: str
    password_hash: str


class AccountDeleted(BaseModel):
    type: Literal["AccountDeletedV1"] = "AccountDeletedV1"


class SignUpWithUsername(BaseModel):
    type: Literal["SignUpWithUsernameV1"] = "SignUpWithUsernameV1"
    username: str
    password_hash: str


class DeleteAccount(BaseModel):
    type: Literal["DeleteAccountV1"] = "DeleteAccountV1"


class State(BaseModel):
    type: Literal["UserStateV1"] = "UserStateV1"
    username: str
    password_hash: str
    account_deleted: bool


type Event = Annotated[UserSignedUp | AccountDeleted, Field(discriminator="type")]
type Command = SignUpWithUsername | DeleteAccount


class User(Aggregate[State, Event, Command]):
    @override
    def apply(self, event: Event) -> None:
        match event:
            case UserSignedUp(username=username, password_hash=password_hash):
                self._state = State(
                    username=username,
                    password_hash=password_hash,
                    account_deleted=False,
                )

            case AccountDeleted():
                if self._state is None:
                    return
                self._state.account_deleted = True

    @override
    def execute(self, command: Command) -> None:
        match command:
            case SignUpWithUsername(username=username, password_hash=password_hash):
                if self._state is not None:
                    raise ValueError("User already exists.")
                self._emit(UserSignedUp(username=username, password_hash=password_hash))

            case DeleteAccount():
                if self._state is None:
                    raise ValueError("User does not exist.")
                if self._state.account_deleted:
                    raise ValueError("Account is already deleted.")
                self._emit(AccountDeleted())


@pytest.fixture
def user() -> User:
    return User("user-1")
