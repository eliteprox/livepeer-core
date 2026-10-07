from livepeer_builder.contracts import Attempt
from livepeer_builder.store.postgres import PostgresStore


class AttemptRecorder:
    def __init__(
        self,
        store: PostgresStore,
    ) -> None:
        self._store = store

    async def record_attempt(
        self,
        actor_id: str,
        attempt: Attempt,
    ) -> None:
        await self._store.record_attempt(actor_id, attempt)
