from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from livepeer_builder.contracts import (
    AccessKey,
    ActorSpend,
    Attempt,
    Failure,
    Job,
    JobCost,
    JobRequest,
    JobState,
    ManifestCost,
    ProvisionedActor,
    Runner,
    RunnerReply,
    SyncCheckpoint,
    UsagePage,
    UsageRow,
)
from livepeer_builder.costs.sync import observed_fee_eth
from livepeer_builder.errors import OperationExists, RunnerCallError


class FixedClock:
    def __init__(
        self,
        start: datetime | None = None,
    ) -> None:
        self.current = start or datetime(2026, 10, 7, 12, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


class StaticDiscoverySource:
    def __init__(
        self,
        runners: list[Runner],
    ) -> None:
        self.runners = list(runners)
        self.error: Exception | None = None

    async def fetch(self) -> tuple[Runner, ...]:
        if self.error is not None:
            raise self.error
        return tuple(self.runners)


class ScriptedTransport:
    """Answers each call from a script: a RunnerReply to return or a RunnerCallError to raise."""

    def __init__(
        self,
        script: list[RunnerReply | RunnerCallError],
    ) -> None:
        self.script = list(script)
        self.calls: list[tuple[Runner, JobRequest, str | None]] = []

    async def call(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
    ) -> RunnerReply:
        self.calls.append((runner, request, credential))
        step = self.script.pop(0)
        if isinstance(step, RunnerCallError):
            raise step
        return step


class ScriptedUsageSource:
    def __init__(
        self,
        pages: list[UsagePage],
    ) -> None:
        self.pages = list(pages)
        self.calls: list[str | None] = []
        self._index = 0

    async def page(self, cursor: str | None, limit: int = 1000) -> UsagePage:
        del limit
        self.calls.append(cursor)
        page = self.pages[self._index]
        self._index += 1
        return page


class MemoryCostSyncStore:
    def __init__(self) -> None:
        self.rows: dict[str, UsageRow] = {}
        self.checkpoint_value: SyncCheckpoint | None = None
        self.fail_next_apply = False

    async def checkpoint(self) -> SyncCheckpoint | None:
        return self.checkpoint_value

    async def apply(
        self,
        rows: tuple[UsageRow, ...] | list[UsageRow],
        checkpoint: SyncCheckpoint,
    ) -> int:
        if self.fail_next_apply:
            self.fail_next_apply = False
            raise RuntimeError("crash before commit")
        new = 0
        for row in rows:
            if row.id not in self.rows:
                new += 1
            self.rows[row.id] = row
        self.checkpoint_value = checkpoint
        return new

    def observed_fee_eth(self) -> Decimal:
        return observed_fee_eth(tuple(self.rows.values()))


class MemoryStore(MemoryCostSyncStore):
    """EngineStore in process memory. Mirrors the PostgresStore queries."""

    def __init__(self) -> None:
        super().__init__()
        self.actors: dict[str, ProvisionedActor] = {}
        self.jobs: dict[UUID, Job] = {}
        self.attempts: dict[UUID, list[Attempt]] = {}
        self.keys: dict[str, tuple[AccessKey, str]] = {}

    async def record_actor(self, actor: ProvisionedActor) -> None:
        self.actors[actor.actor_id] = actor

    async def actor(self, actor_id: str) -> ProvisionedActor | None:
        return self.actors.get(actor_id)

    async def list_actors(self) -> list[ProvisionedActor]:
        return sorted(self.actors.values(), key=lambda actor: actor.actor_id)

    async def create_job(self, job: Job) -> None:
        if job.operation_ref is not None:
            existing = await self.job_by_operation_ref(job.application_id, job.actor_id, job.operation_ref)
            if existing is not None:
                raise OperationExists(existing)
        self.jobs[job.id] = job
        self.attempts[job.id] = []

    async def job_by_operation_ref(self, application_id: str, actor_id: str, operation_ref: str) -> Job | None:
        for job in self.jobs.values():
            if (job.application_id, job.actor_id, job.operation_ref) == (application_id, actor_id, operation_ref):
                return self._with_attempts(job)
        return None

    async def get_job(self, job_id: UUID) -> Job | None:
        job = self.jobs.get(job_id)
        return self._with_attempts(job) if job is not None else None

    async def list_jobs(self, application_id: str, actor_id: str, limit: int = 50) -> list[Job]:
        matching = [
            job for job in self.jobs.values() if (job.application_id, job.actor_id) == (application_id, actor_id)
        ]
        matching.sort(key=lambda job: job.created_at, reverse=True)
        return [self._with_attempts(job) for job in matching[:limit]]

    async def record_attempt(self, attempt: Attempt) -> None:
        current = self.attempts.setdefault(attempt.job_id, [])
        current[:] = [item for item in current if item.number != attempt.number]
        current.append(attempt)
        current.sort(key=lambda item: item.number)

    async def finish_job(self, job_id: UUID, state: JobState, failure: Failure | None, completed_at: datetime) -> None:
        self.jobs[job_id] = self.jobs[job_id].model_copy(
            update={"state": state, "failure": failure, "completed_at": completed_at}
        )

    async def job_cost(self, job_id: UUID) -> JobCost:
        job = self.jobs.get(job_id)
        attempts = self.attempts.get(job_id, [])
        actor = self.actors.get(job.actor_id) if job is not None else None
        auth_ids = {auth_id for attempt in attempts for auth_id in attempt.auth_ids}
        rows = [
            row
            for row in self.rows.values()
            if row.status == "applied"
            and actor is not None
            and row.allocation_id == actor.allocation_id
            and row.payment_session_id in auth_ids
        ]
        if not rows:
            return JobCost(
                job_id=job_id,
                status="pending" if attempts else "none",
                fee_eth=None,
                fee_usd=None,
                event_count=0,
            )
        return JobCost(
            job_id=job_id,
            status="observed",
            fee_eth=_sum_eth(rows),
            fee_usd=_sum_usd(rows),
            event_count=len(rows),
        )

    async def actor_spend(self, actor_id: str) -> ActorSpend:
        actor = self.actors.get(actor_id)
        if actor is None:
            raise KeyError(actor_id)
        rows = [
            row
            for row in self.rows.values()
            if row.status == "applied" and row.allocation_id == actor.allocation_id
        ]
        return ActorSpend(
            actor_id=actor_id,
            allocation_id=actor.allocation_id,
            fee_eth=_sum_eth(rows),
            fee_usd=_sum_usd(rows) if rows else None,
            event_count=len(rows),
        )

    async def manifest_cost(self, manifest_id: str) -> ManifestCost:
        rows = [row for row in self.rows.values() if row.status == "applied" and row.manifest_id == manifest_id]
        return ManifestCost(
            manifest_id=manifest_id,
            fee_eth=_sum_eth(rows),
            fee_usd=_sum_usd(rows) if rows else None,
            event_count=len(rows),
            allocation_ids=tuple(sorted({row.allocation_id for row in rows if row.allocation_id})),
        )

    async def insert_access_key(self, key: AccessKey, token_hash: str) -> None:
        self.keys[key.key_id] = (key, token_hash)

    async def access_key_by_hash(self, token_hash: str) -> AccessKey | None:
        for key, stored_hash in self.keys.values():
            if stored_hash == token_hash:
                return key
        return None

    async def revoke_access_key(self, key_id: str, revoked_at: datetime) -> None:
        key, stored_hash = self.keys[key_id]
        if key.revoked_at is None:
            self.keys[key_id] = (key.model_copy(update={"revoked_at": revoked_at}), stored_hash)

    def _with_attempts(self, job: Job) -> Job:
        return job.model_copy(update={"attempts": tuple(self.attempts.get(job.id, []))})


def _sum_eth(rows: list[UsageRow]) -> Decimal:
    return sum((row.computed_fee_eth for row in rows if row.computed_fee_eth is not None), Decimal(0))


def _sum_usd(rows: list[UsageRow]) -> Decimal | None:
    priced = [row.computed_fee_usd for row in rows if row.computed_fee_usd is not None]
    if len(priced) != len(rows):
        return None
    return sum(priced, Decimal(0))
