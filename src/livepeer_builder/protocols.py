from collections.abc import Sequence
from datetime import datetime
from typing import Protocol
from uuid import UUID

from livepeer_builder.contracts import (
    ActorContext,
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


class Clock(Protocol):
    def now(self) -> datetime: ...


class DiscoverySource(Protocol):
    async def fetch(self) -> tuple[Runner, ...]: ...


class SelectionPolicy(Protocol):
    def order(self, candidates: Sequence[Runner], request: JobRequest) -> Sequence[Runner]: ...


class RunnerTransport(Protocol):
    """One call to one runner. Raises RunnerCallError on any failure."""

    async def call(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
    ) -> RunnerReply: ...


class CredentialResolver(Protocol):
    """The bearer the signer authorizes for this actor. None means an unpaid call."""

    async def credential(self, actor: ActorContext) -> str | None: ...


class UsageSource(Protocol):
    async def page(self, cursor: str | None, limit: int = 1000) -> UsagePage: ...


class CostSyncStore(Protocol):
    async def checkpoint(self) -> SyncCheckpoint | None: ...

    async def apply(self, rows: tuple[UsageRow, ...] | list[UsageRow], checkpoint: SyncCheckpoint) -> int: ...


class EngineStore(CostSyncStore, Protocol):
    """What job dispatch persists. PostgresStore and MemoryStore implement it."""

    async def record_actor(self, actor: ProvisionedActor) -> None: ...

    async def actor(self, actor_id: str) -> ProvisionedActor | None: ...

    async def list_actors(self) -> list[ProvisionedActor]: ...

    async def create_job(self, job: Job) -> None: ...  # raises OperationExists

    async def job_by_operation_ref(self, application_id: str, actor_id: str, operation_ref: str) -> Job | None: ...

    async def get_job(self, job_id: UUID) -> Job | None: ...

    async def list_jobs(self, application_id: str, actor_id: str, limit: int = 50) -> list[Job]: ...

    async def record_attempt(self, attempt: Attempt) -> None: ...

    async def finish_job(self, job_id: UUID, state: JobState, failure: Failure | None, completed_at: datetime) -> None: ...

    async def job_cost(self, job_id: UUID) -> JobCost: ...

    async def actor_spend(self, actor_id: str) -> ActorSpend: ...

    async def manifest_cost(self, manifest_id: str) -> ManifestCost: ...
