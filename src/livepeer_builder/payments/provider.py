from decimal import Decimal
from typing import Any

from livepeer_builder.adapters.batteries import BatteriesClient
from livepeer_builder.contracts import ActorContext, Allowance, ProvisionedActor
from livepeer_builder.errors import AccessDenied, ProviderUnavailable
from livepeer_builder.jobs.service import require
from livepeer_builder.protocols import EngineStore


class BatteriesProvider:
    """Who pays. One Batteries allocation and API key per actor.

    ``credential`` is what the transport presents to the signer. The actor's
    stored key wins; ``default_credential`` (the settings value) covers an
    actor with no allocation. The access authenticator never sees either key.
    The allocation id is what cost sums for that actor's spend.
    """

    def __init__(
        self,
        store: EngineStore,
        batteries: BatteriesClient | None,
        default_credential: str | None = None,
    ) -> None:
        self._store = store
        self._batteries = batteries
        self._default_credential = default_credential

    async def credential(self, actor: ActorContext) -> str | None:
        stored = await self._store.actor(actor.actor_id)
        if stored is not None:
            return stored.api_key_ref
        return self._default_credential

    async def provision(
        self,
        actor: ActorContext,
        actor_id: str,
        *,
        grant_id: str,
        amount_eth: str,
    ) -> ProvisionedActor:
        require(actor, "admin")
        existing = await self._store.actor(actor_id)
        if existing is not None:
            return existing
        client = self._require_batteries()
        allocation_id = await client.create_allocation(grant_id, actor_id, amount_eth, actor_id)
        key = await client.create_api_key(allocation_id, actor_id, actor_id)
        provisioned = ProvisionedActor(
            actor_id=actor_id,
            grant_id=grant_id,
            allocation_id=key["allocation_id"],
            api_key_ref=key["api_key"],
        )
        await self._store.record_actor(provisioned)
        return provisioned

    async def allowance(self, actor: ActorContext, actor_id: str | None = None) -> Allowance:
        target = actor_id or actor.actor_id
        if target != actor.actor_id:
            require(actor, "admin")
        stored = await self._store.actor(target)
        if stored is None:
            raise AccessDenied(f"actor {target!r} is not provisioned")
        body = await self._require_batteries().allocation(stored.allocation_id)
        return allowance_from_body(body)

    def _require_batteries(self) -> BatteriesClient:
        if self._batteries is None:
            raise ProviderUnavailable("Batteries management API is not configured")
        return self._batteries


def allowance_from_body(body: dict[str, Any]) -> Allowance:
    return Allowance(
        allocation_id=str(body["id"]),
        status=str(body.get("status", "")),
        granted_eth=_decimal(body.get("amount_eth")),
        spent_eth=_decimal(body.get("spent_eth")),
        available_eth=_decimal(body.get("available_eth")),
    )


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))
