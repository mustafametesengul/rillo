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

Rillo uses Pydantic models for State, Events, and Commands. Creating an `Aggregate` requires three type parameters and implementing the abstract `apply()` and `execute()` methods.

```python
from typing import Annotated, Literal
from pydantic import BaseModel, Field
from rillo import Aggregate

# 1. Define events
class UserSignedUp(BaseModel):
    type: Literal["UserSignedUpV1"] = "UserSignedUpV1"
    username: str

class AccountDeleted(BaseModel):
    type: Literal["AccountDeletedV1"] = "AccountDeletedV1"

# 2. Define commands
class SignUp(BaseModel):
    type: Literal["SignUpV1"] = "SignUpV1"
    username: str

class DeleteAccount(BaseModel):
    type: Literal["DeleteAccountV1"] = "DeleteAccountV1"

# 3. Define aggregate state
class UserState(BaseModel):
    type: Literal["UserStateV1"] = "UserStateV1"
    username: str
    account_deleted: bool

# Type aliases with discriminators for union types
type Event = Annotated[UserSignedUp | AccountDeleted, Field(discriminator="type")]
type Command = Annotated[SignUp | DeleteAccount, Field(discriminator="type")]

# 4. Create the Aggregate with [State, Event, Command] type parameters
class User(Aggregate[UserState, Event, Command]):

    # apply() maps each event to a state mutation
    def apply(self, event: Event) -> None:
        match event:
            case UserSignedUp(username=username):
                self._state = UserState(username=username, account_deleted=False)
            case AccountDeleted():
                if self._state is not None:
                    self._state.account_deleted = True

    # execute() contains business logic and emits events via _emit()
    def execute(self, command: Command) -> None:
        match command:
            case SignUp(username=username):
                if self._state is not None:
                    raise ValueError("User already exists.")
                self._emit(UserSignedUp(username=username))
            case DeleteAccount():
                if self._state is None:
                    raise ValueError("User does not exist.")
                if self._state.account_deleted:
                    raise ValueError("Account is already deleted.")
                self._emit(AccountDeleted())

# Using the aggregate
user = User(id="user-1")
user.execute(SignUp(username="alice"))
user.execute(DeleteAccount())

# Pending events are stored and ready to be committed
events = user.pending_events
```

### Repositories and Snapshot Stores

Rillo provides a `Repository` base class to save/load events and a `SnapshotStore` base for capturing aggregate snapshots to optimize load times. Both have built-in support for NATS JetStream (`NATSRepository` & `NATSSnapshotStore`).

```python
import asyncio
from nats.aio.client import Client as NATS
from rillo.nats import NATSRepository, NATSSnapshotStore

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
    user.execute(SignUp(username="alice"))

    # Persist pending events into NATS JetStream
    await repository.save(user)

    # Rehydrate aggregate state back from the event stream
    loaded_user = User("user-123")
    await repository.load(loaded_user)

    # Snapshot store uses a NATS KV bucket to cache aggregate state
    kv = await js.key_value("users-snapshots")
    snapshot_store = NATSSnapshotStore[User](kv=kv)

    # Save a snapshot of the current aggregate state
    await snapshot_store.save(loaded_user)

    # Load the snapshot before replaying remaining events
    restored_user = User("user-123")
    await snapshot_store.load(restored_user)
    await repository.load(restored_user)  # replays only events after the snapshot

if __name__ == "__main__":
    asyncio.run(main())
```
