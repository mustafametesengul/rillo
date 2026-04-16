import asyncio
from typing import Literal
from uuid import UUID, uuid4

import nats
from nats.js.client import JetStreamContext
from pydantic import BaseModel

from rillo import Aggregate
from rillo.nats import NATSRepository


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
    def __init__(self, id: UUID | None = None) -> None:
        super().__init__(str(id or uuid4()), UserState, "schema_version")
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

    def sign_up_with_username(self, username: str, password_hash: str) -> None:
        self._publish(
            UserSignedUp(
                username=username,
                password_hash=password_hash,
            )
        )

    def delete_account(self) -> None:
        if self._state is None:
            raise ValueError("User does not exist.")
        if self._state.account_deleted:
            raise ValueError("Account is already deleted.")
        self._publish(AccountDeleted())


class UserRepository(NATSRepository[User]):
    def __init__(self, js: JetStreamContext) -> None:
        super().__init__(js, subject_prefix="user")


async def test_user_repository() -> None:
    host = "localhost"
    port = 4222
    nc = await nats.connect(f"nats://{host}:{port}")
    js = nc.jetstream()

    user_repository = UserRepository(js)
    await user_repository.register("test")

    user = User()
    user.sign_up_with_username("testuser", "hashedpassword")
    await user_repository.save(user)

    await nc.close()


if __name__ == "__main__":
    asyncio.run(test_user_repository())
