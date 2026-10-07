import os
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from livepeer_core.contracts import Attempt, ProvisionedActor, SyncCheckpoint, UsageRow
from livepeer_core.costs.sync import CostSyncWorker
from livepeer_core.store.postgres import PostgresStore
from livepeer_core.testing.fakes import ScriptedUsageSource
from livepeer_core.contracts import UsagePage

pytestmark = pytest.mark.asyncio


def _row(
    row_id: str,
    *,
    allocation_id: str | None,
    session_id: str | None,
    manifest_id: str | None,
    status: str = "applied",
    fee: str = "0.01",
) -> UsageRow:
    return UsageRow(
        id=row_id,
        event_id=row_id,
        status=status,  # type: ignore[arg-type]
        manifest_id=manifest_id,
        allocation_id=allocation_id,
        payment_session_id=session_id,
        request_id="req",
        pipeline="fixed",
        computed_fee_eth=Decimal(fee),
        computed_fee_usd=None,
        currency="eth" if allocation_id else None,
        created_at=datetime(2026, 10, 7, tzinfo=UTC),
    )


@pytest.fixture
async def store():
    dsn = os.environ.get(
        "DATABASE_URL",
        "postgresql://livepeer:livepeer@127.0.0.1:55432/livepeer",
    )
    try:
        opened = await PostgresStore.connect(dsn)
    except Exception as exc:
        pytest.skip(f"Postgres is not available: {exc}")
    yield opened
    async with opened._pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE lpb_attempts, lpb_jobs, lpb_usage_events, lpb_cost_cursor, lpb_actors
            """
        )
    await opened.close()


async def test_spend_job_and_manifest_queries(store: PostgresStore) -> None:
    await store.record_actor(
        ProvisionedActor(
            actor_id="alice",
            grant_id="grant-1",
            allocation_id="alloc-alice",
            api_key_ref="secret-alice",
        )
    )
    await store.record_actor(
        ProvisionedActor(
            actor_id="bob",
            grant_id="grant-1",
            allocation_id="alloc-bob",
            api_key_ref="secret-bob",
        )
    )
    alice_session = "auth-alice"
    bob_session = "auth-bob"
    forged = "forged-manifest"
    page = UsagePage(
        items=(
            _row("alice-1", allocation_id="alloc-alice", session_id=alice_session, manifest_id="manifest-alice"),
            _row("alice-2", allocation_id="alloc-alice", session_id=alice_session, manifest_id=forged),
            _row("bob-1", allocation_id="alloc-bob", session_id=bob_session, manifest_id=forged),
            _row(
                "quarantined",
                allocation_id=None,
                session_id=None,
                manifest_id=None,
                status="quarantined",
                fee="0",
            ),
        ),
        next_cursor="",
    )
    report = await CostSyncWorker(ScriptedUsageSource([page]), store, {}).sync_once()
    assert report.rows_new == 4

    alice = await store.actor_spend("alice")
    bob = await store.actor_spend("bob")
    assert alice.fee_eth == Decimal("0.02")
    assert alice.event_count == 2
    assert bob.fee_eth == Decimal("0.01")

    job_id = uuid4()
    await store.record_attempt(
        "alice",
        Attempt(
            job_id=job_id,
            number=1,
            auth_ids=(alice_session,),
            manifest_id="manifest-alice",
            payment_sent=True,
            outcome="succeeded",
        ),
    )
    cost = await store.job_cost(job_id)
    assert cost.status == "observed"
    assert cost.fee_eth == Decimal("0.02")
    assert cost.event_count == 2

    other = uuid4()
    await store.record_attempt(
        "bob",
        Attempt(
            job_id=other,
            number=1,
            auth_ids=(),
            manifest_id=forged,
            payment_sent=True,
            outcome="succeeded",
        ),
    )
    assert (await store.job_cost(other)).status == "pending"
    forged_cost = await store.manifest_cost(forged)
    assert forged_cost.event_count == 2
    assert forged_cost.allocation_ids == ("alloc-alice", "alloc-bob")
    assert forged_cost.fee_eth == Decimal("0.02")

    again = await CostSyncWorker(ScriptedUsageSource([page]), store, {}).sync_once()
    assert again.rows_new == 0
    assert await store.checkpoint() == SyncCheckpoint(resume_cursor=None, filters={})
