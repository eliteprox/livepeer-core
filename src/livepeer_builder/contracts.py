from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class Contract(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


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
