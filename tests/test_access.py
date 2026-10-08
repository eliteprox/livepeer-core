import pytest

from livepeer_builder.access.service import AccessService
from livepeer_builder.contracts import ActorContext
from livepeer_builder.errors import AccessDenied
from livepeer_builder.testing.fakes import FixedClock, MemoryStore

ADMIN = ActorContext(actor_id="operator", scopes=frozenset({"admin"}))


async def test_issue_authenticate_revoke() -> None:
    store = MemoryStore()
    access = AccessService(store, FixedClock(), admin_token="root-token")

    issued = await access.issue_key(ADMIN, "alice", scopes=["jobs:run", "jobs:read"], application_id="shop")
    assert issued.token.startswith("lpk_")
    assert issued.token not in str(store.keys)  # only the hash is stored

    actor = await access.authenticate(issued.token)
    assert actor.actor_id == "alice"
    assert actor.application_id == "shop"
    assert actor.scopes == frozenset({"jobs:run", "jobs:read"})
    assert actor.attributes == {"key_id": issued.key.key_id}

    assert (await access.authenticate("root-token")).scopes == frozenset({"admin"})
    with pytest.raises(AccessDenied):
        await access.authenticate("lpk_nope")

    await access.revoke_key(ADMIN, issued.key.key_id)
    with pytest.raises(AccessDenied):
        await access.authenticate(issued.token)


async def test_only_admin_issues_keys() -> None:
    access = AccessService(MemoryStore(), FixedClock())
    non_admin = ActorContext(actor_id="alice", scopes=frozenset({"jobs:run"}))
    with pytest.raises(AccessDenied):
        await access.issue_key(non_admin, "bob", scopes=["jobs:run"])
    with pytest.raises(AccessDenied):
        await access.authenticate("anything")  # no admin token configured


async def test_revoking_an_unknown_key_is_a_no_op() -> None:
    await AccessService(MemoryStore(), FixedClock()).revoke_key(ADMIN, "missing")
