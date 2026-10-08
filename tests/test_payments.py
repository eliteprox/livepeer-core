"""The signer credential stays on payments. Access keys never carry it."""

import pytest

from livepeer_builder.access.service import AccessService
from livepeer_builder.contracts import ActorContext, ProvisionedActor
from livepeer_builder.errors import AccessDenied
from livepeer_builder.payments.provider import BatteriesProvider
from livepeer_builder.testing.fakes import FixedClock, MemoryStore

ALICE = ActorContext(actor_id="alice", scopes=frozenset({"jobs:run"}))


async def test_actor_key_wins_over_the_default_credential() -> None:
    store = MemoryStore()
    await store.record_actor(
        ProvisionedActor(actor_id="alice", grant_id="g", allocation_id="alloc", api_key_ref="key-alice")
    )
    payments = BatteriesProvider(store, None, default_credential="app-key")
    assert await payments.credential(ALICE) == "key-alice"
    assert await payments.credential(ActorContext(actor_id="bob", scopes=frozenset())) == "app-key"


async def test_no_credential_when_the_actor_and_settings_have_none() -> None:
    payments = BatteriesProvider(MemoryStore(), None)
    assert await payments.credential(ALICE) is None


async def test_access_token_does_not_carry_the_payment_key() -> None:
    store = MemoryStore()
    await store.record_actor(
        ProvisionedActor(actor_id="alice", grant_id="g", allocation_id="alloc", api_key_ref="key-alice")
    )
    access = AccessService(store, FixedClock(), admin_token="root")
    issued = await access.issue_key(
        ActorContext(actor_id="operator", scopes=frozenset({"admin"})),
        "alice",
        scopes=["jobs:run"],
    )
    actor = await access.authenticate(issued.token)
    assert "key-alice" not in actor.model_dump_json()
    assert actor.attributes == {"key_id": issued.key.key_id}


async def test_own_allowance_needs_the_jobs_read_scope() -> None:
    payments = BatteriesProvider(MemoryStore(), None)
    with pytest.raises(AccessDenied):
        await payments.allowance(ActorContext(actor_id="alice", scopes=frozenset({"discover"})))
