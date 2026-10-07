from collections.abc import Sequence
from datetime import datetime
from typing import Protocol
from uuid import UUID

from livepeer_builder.contracts import (
    AccessKey,
    ActorContext,
    ActorSpend,
    Allowance,
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


class RunnerTransport(Protocol):
    """One call to one runner. Raises RunnerCallError on any failure."""

    async def call(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
    ) -> RunnerReply: ...


class SelectionPolicy(Protocol):
    def order(self, candidates: Sequence[Runner], request: JobRequest) -> Sequence[Runner]: ...


class CredentialResolver(Protocol):
    """The bearer the signer authorizes for this actor. None means an unpaid call.

    PaymentProvider implements this. The authenticator does not.
    """

    async def credential(self, actor: ActorContext) -> str | None: ...


class PaymentProvider(CredentialResolver, Protocol):
    """Allowance and the signer credential. Access keys are a separate protocol."""

    async def provision(
        self,
        actor: ActorContext,
        actor_id: str,
        *,
        grant_id: str,
        amount_eth: str,
    ) -> ProvisionedActor: ...

    async def allowance(self, actor: ActorContext, actor_id: str | None = None) -> Allowance: ...


class Authenticator(Protocol):
    async def authenticate(self, token: str) -> ActorContext: ...


class UsageSource(Protocol):
    async def page(self, cursor: str | None, limit: int = 1000) -> UsagePage: ...


class CostSyncStore(Protocol):
    async def checkpoint(self) -> SyncCheckpoint | None: ...

    async def apply(self, rows: tuple[UsageRow, ...] | list[UsageRow], checkpoint: SyncCheckpoint) -> int: ...


class EngineStore(CostSyncStore, Protocol):
    """Everything the engine persists. PostgresStore and MemoryStore implement it."""

    # actors
    async def record_actor(self, actor: ProvisionedActor) -> None: ...

    async def actor(self, actor_id: str) -> ProvisionedActor | None: ...

    async def list_actors(self) -> list[ProvisionedActor]: ...

    # jobs
    async def create_job(self, job: Job) -> None: ...  # raises OperationExists

    async def job_by_operation_ref(self, application_id: str, actor_id: str, operation_ref: str) -> Job | None: ...

    async def get_job(self, job_id: UUID) -> Job | None: ...

    async def list_jobs(self, application_id: str, actor_id: str, limit: int = 50) -> list[Job]: ...

    async def record_attempt(self, attempt: Attempt) -> None: ...

    async def finish_job(self, job_id: UUID, state: JobState, failure: Failure | None, completed_at: datetime) -> None: ...

    # cost
    async def job_cost(self, job_id: UUID) -> JobCost: ...

    async def actor_spend(self, actor_id: str) -> ActorSpend: ...

    async def manifest_cost(self, manifest_id: str) -> ManifestCost: ...

    # access keys
    async def insert_access_key(self, key: AccessKey, token_hash: str) -> None: ...

    async def access_key_by_hash(self, token_hash: str) -> AccessKey | None: ...

    async def revoke_access_key(self, key_id: str, revoked_at: datetime) -> None: ...
