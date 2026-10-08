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


class Attempt(Contract):
    job_id: UUID
    number: int
    auth_ids: tuple[str, ...]
    manifest_id: str | None
    payment_sent: bool
    outcome: str


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
