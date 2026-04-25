# Rillo

Rillo is a lightweight, type-safe Event Sourcing framework for Python, built on top of [Pydantic](https://docs.pydantic.dev/).

## Installation

Installing the core library using `pip`:

```bash
pip install rillo
```

Install with NATS JetStream support for repositories and snapshot stores:

```bash
pip install 'rillo[nats]'
```

Installing using `uv`:

```bash
uv add rillo
uv add rillo[nats]
```

## Usage

### Defining an Aggregate

Rillo uses Pydantic models for Events and State. Creating an `Aggregate` requires defining your State, Events, and mapping changes via the `@mutator` decorator.

```python
from typing import Literal
from pydantic import BaseModel
from rillo import Aggregate, mutator

# 1. Define events
class UserSignedUp(BaseModel):
    type: Literal["UserSignedUpV1"] = "UserSignedUpV1"
    username: str

class AccountDeleted(BaseModel):
    type: Literal["AccountDeletedV1"] = "AccountDeletedV1"

# 2. Define aggregate state
class UserState(BaseModel):
    type: Literal["UserStateV1"] = "UserStateV1"
    username: str
    account_deleted: bool

# 3. Create the Aggregate
class User(Aggregate[UserState]):

    # Mutators specify how an event modifies the internal state
    @mutator
    def on_user_signed_up(self, event: UserSignedUp) -> None:
        self._state = UserState(
            username=event.username,
            account_deleted=False,
        )

    @mutator
    def on_account_deleted(self, event: AccountDeleted) -> None:
        if self._state is not None:
            self._state.account_deleted = True

    # Business logic generates and applies new events
    def sign_up(self, username: str) -> None:
        if self._state is not None:
            raise ValueError("User already exists.")

        self._apply(UserSignedUp(username=username))

    def delete_account(self) -> None:
        if self._state is None:
            raise ValueError("User does not exist.")

        self._apply(AccountDeleted())

# Using the aggregate
user = User(id="user-1")
user.sign_up("alice")
user.delete_account()

# Pending events are stored and ready to be committed
events = user.pending_events
```

### Repositories and Snapshot Stores

Rillo provides a `Repository` base class to save/load events and a `SnapshotStore` base for capturing aggregate snapshots to optimize load times. Both have built-in support for NATS JetStream (`NATSRepository` & `NATSSnapshotStore`).

```python
import asyncio
from nats.aio.client import Client as NATS
from rillo.nats import NATSRepository

async def main():
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Create a repository instance
    repository = NATSRepository[User](
        js=js,
        stream_name="USERS",
        subject_prefix="users.events"
    )

    user = User("user-123")
    user.sign_up("alice")

    # Persist pending events into NATS JetStream
    await repository.save(user)

    # Rehydrate aggregate state back from the event stream
    loaded_user = User("user-123")
    await repository.load(loaded_user)

if __name__ == "__main__":
    asyncio.run(main())
```
