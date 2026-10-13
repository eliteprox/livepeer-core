import asyncio
import logging
from datetime import datetime
from uuid import UUID

from livepeer_builder.contracts import ActorContext, ActorSpend, JobCost, SyncReport
from livepeer_builder.costs.sync import CostSyncWorker
from livepeer_builder.errors import AccessDenied, ProviderUnavailable
from livepeer_builder.jobs.service import owns, require
from livepeer_builder.protocols import Clock, EngineStore

_LOG = logging.getLogger(__name__)


class CostService:
    """Observed network cost, keyed by (allocation_id, payment_session_id).

    A job's cost is the applied usage rows whose payment_session_id is one of
    the job's recorded auth_ids and whose allocation is the actor's (any
    allocation when the actor paid with the default credential). An actor's
    spend is every applied row on the actor's allocation.
    """

    def __init__(
        self,
        store: EngineStore,
        worker: CostSyncWorker | None,
        clock: Clock,
        interval_s: float = 30.0,
    ) -> None:
        self._store = store
        self._worker = worker
        self._clock = clock
        self._interval_s = interval_s
        self.last_synced_at: datetime | None = None
        self.last_error: str | None = None

    async def sync_once(self) -> SyncReport:
        if self._worker is None:
            raise ProviderUnavailable("no usage source is configured")
        try:
            report = await self._worker.sync_once()
        except Exception as exc:
            self.last_error = str(exc)
            raise
        self.last_synced_at = self._clock.now()
        self.last_error = None
        return report

    async def run(self, stop: asyncio.Event) -> None:
        if self._worker is None:
            return
        while not stop.is_set():
            try:
                await self.sync_once()
            except Exception as exc:
                _LOG.warning("cost sync failed: %s", exc)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval_s)
            except TimeoutError:
                continue

    async def for_job(self, actor: ActorContext, job_id: UUID) -> JobCost:
        require(actor, "jobs:read")
        job = await self._store.get_job(job_id)
        if job is None or not owns(actor, job):
            raise AccessDenied(f"job {job_id} not found for this actor")
        return await self._store.job_cost(job_id)

    async def for_actor(self, actor: ActorContext, actor_id: str | None = None) -> ActorSpend:
        target = actor_id or actor.actor_id
        if target != actor.actor_id:
            require(actor, "admin")
        else:
            require(actor, "jobs:read")
        try:
            return await self._store.actor_spend(target)
        except KeyError as exc:
            raise AccessDenied(f"actor {target!r} is not provisioned") from exc
