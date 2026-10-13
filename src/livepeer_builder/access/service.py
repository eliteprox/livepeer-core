import hashlib
import secrets
from collections.abc import Iterable

from livepeer_builder.contracts import AccessKey, ActorContext, IssuedKey
from livepeer_builder.errors import AccessDenied
from livepeer_builder.jobs.service import require
from livepeer_builder.protocols import Clock, EngineStore

_TOKEN_PREFIX = "lpk_"
_ADMIN_ACTOR = "operator"


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class AccessService:
    """Operator-issued keys for the standalone service.

    Tokens are random, shown once, and stored as a SHA-256 hash. An optional
    ``admin_token`` from settings bootstraps the first key. Applications that
    embed the engine build their own ActorContext and never call this.
    """

    def __init__(
        self,
        store: EngineStore,
        clock: Clock,
        admin_token: str | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._admin_hash = token_hash(admin_token) if admin_token else None

    async def issue_key(
        self,
        actor: ActorContext,
        actor_id: str,
        *,
        scopes: Iterable[str],
        application_id: str = "default",
        label: str = "",
    ) -> IssuedKey:
        require(actor, "admin")
        token = _TOKEN_PREFIX + secrets.token_urlsafe(32)
        key = AccessKey(
            key_id=secrets.token_urlsafe(12),
            actor_id=actor_id,
            application_id=application_id,
            scopes=tuple(sorted(set(scopes))),
            label=label,
            created_at=self._clock.now(),
            revoked_at=None,
        )
        await self._store.insert_access_key(key, token_hash(token))
        return IssuedKey(key=key, token=token)

    async def revoke_key(self, actor: ActorContext, key_id: str) -> None:
        require(actor, "admin")
        await self._store.revoke_access_key(key_id, self._clock.now())

    async def authenticate(self, token: str) -> ActorContext:
        digest = token_hash(token)
        if self._admin_hash is not None and secrets.compare_digest(digest, self._admin_hash):
            return ActorContext(actor_id=_ADMIN_ACTOR, scopes=frozenset({"admin"}))
        key = await self._store.access_key_by_hash(digest)
        if key is None or key.revoked_at is not None:
            raise AccessDenied("unknown or revoked key")
        return ActorContext(
            actor_id=key.actor_id,
            application_id=key.application_id,
            scopes=frozenset(key.scopes),
            attributes={"key_id": key.key_id},
        )
