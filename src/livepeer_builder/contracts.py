from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Contract(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


# Discovery


class NetworkRate(Contract):
    amount: Decimal
    currency: str
    unit: str


class Runner(Contract):
    url: str
    app: str
    runner_id: str | None
    mode: str | None
    orchestrator_url: str | None
    capacity: int | None
    capacity_available: int | None
    rate: NetworkRate | None


class DiscoverySnapshot(Contract):
    runners: tuple[Runner, ...]
    observed_at: datetime | None
    last_attempt_at: datetime | None
    last_error: str | None


class Offering(Contract):
    app: str
    rate_low: NetworkRate | None
    rate_high: NetworkRate | None
    runner_count: int
    ready_count: int
    observed_at: datetime


class JobRequest(Contract):
    app: str  # the runner app a discovery entry advertises
    path: str = ""  # appended to the runner URL; must start with "/"
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    payload: dict[str, Any] | None = None
    operation_ref: str | None = None  # the caller's key; makes a retry safe
    capability: str | None = None  # labels only; the engine does not resolve them
    model: str | None = None
    max_attempts: int = 3
    timeout_s: float = 300.0

    @field_validator("path")
    @classmethod
    def _stays_on_the_runner(cls, value: str) -> str:
        # A leading "/" ends the URL authority, so the call cannot leave the runner's host.
        if value and not value.startswith("/"):
            raise ValueError("path must be empty or start with '/'")
        return value


class UsageRow(Contract):
    id: str
    event_id: str | None
    status: Literal["applied", "quarantined", "ignored", "duplicate"]
    manifest_id: str | None
    allocation_id: str | None
    payment_session_id: str | None
    request_id: str | None
    pipeline: str | None
    computed_fee_eth: Decimal | None
    computed_fee_usd: Decimal | None
    currency: str | None
    created_at: datetime


class UsagePage(Contract):
    items: tuple[UsageRow, ...]
    next_cursor: str = ""


class SyncCheckpoint(Contract):
    resume_cursor: str | None
    filters: dict[str, str] = Field(default_factory=dict)


class SyncReport(Contract):
    pages: int
    rows_seen: int
    rows_new: int


class ProvisionedActor(Contract):
    actor_id: str
    grant_id: str
    allocation_id: str
    api_key_ref: str


# Jobs


Scope = Literal["discover", "jobs:run", "jobs:read", "admin"]


class ActorContext(Contract):
    """Who is asking. Produced by the application's own auth or by AccessService.

    ``actor_id`` is opaque to the engine and is the key that joins a job to the
    actor's allocation. No payment secret is ever carried here; the engine
    resolves the signer credential from the actor through a CredentialResolver.
    """

    actor_id: str
    application_id: str = "default"
    scopes: frozenset[str] = frozenset()
    attributes: dict[str, str] = Field(default_factory=dict)


class AccessKey(Contract):
    key_id: str
    actor_id: str
    application_id: str
    scopes: tuple[str, ...]
    label: str
    created_at: datetime
    revoked_at: datetime | None


class IssuedKey(Contract):
    key: AccessKey
    token: str  # shown once; only its hash is stored


AttemptOutcome = Literal["succeeded", "refused", "unreachable", "payment", "timeout", "http"]


class Attempt(Contract):
    job_id: UUID
    number: int
    runner_url: str | None
    orchestrator_url: str | None
    auth_ids: tuple[str, ...]  # signer payment sessions this attempt opened; the cost key
    manifest_id: str | None  # the orchestrator's label; never a billing key
    payment_sent: bool
    outcome: AttemptOutcome
    status_code: int | None
    started_at: datetime
    ended_at: datetime | None


FailureKind = Literal[
    "no_offering",
    "refused",
    "unreachable",
    "payment",
    "timeout",
    "runner_rejected",
    "runner_error",
]


class Failure(Contract):
    kind: FailureKind
    message: str
    status_code: int | None
    body: str | None  # the runner's own words, bounded
    payment_sent: bool


JobState = Literal["running", "succeeded", "failed", "uncertain"]


class Job(Contract):
    id: UUID
    actor_id: str
    application_id: str
    operation_ref: str | None
    app: str
    capability: str | None
    model: str | None
    state: JobState
    attempts: tuple[Attempt, ...]
    failure: Failure | None
    created_at: datetime
    completed_at: datetime | None


class RunnerReply(Contract):
    """What a RunnerTransport returns for one successful call."""

    status_code: int
    content: bytes
    content_type: str
    data: dict[str, Any] | None
    auth_ids: tuple[str, ...]
    manifest_id: str | None
    payment_sent: bool


class JobResult(Contract):
    job: Job
    status_code: int
    content: bytes
    content_type: str
    data: dict[str, Any] | None


class JobCost(Contract):
    job_id: UUID
    status: Literal["none", "pending", "observed", "corrected"]
    fee_eth: Decimal | None
    fee_usd: Decimal | None
    event_count: int


class ActorSpend(Contract):
    actor_id: str
    allocation_id: str
    fee_eth: Decimal
    fee_usd: Decimal | None
    event_count: int


class ManifestCost(Contract):
    manifest_id: str
    fee_eth: Decimal
    fee_usd: Decimal | None
    event_count: int
    allocation_ids: tuple[str, ...]


class Allowance(Contract):
    allocation_id: str
    status: str
    granted_eth: Decimal | None
    spent_eth: Decimal | None
    available_eth: Decimal | None


class EngineHealth(Contract):
    discovery_fresh: bool
    discovery_observed_at: datetime | None
    discovery_error: str | None
    cost_sync_at: datetime | None
    cost_sync_error: str | None
