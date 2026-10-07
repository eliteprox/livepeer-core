import os
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from livepeer_builder.contracts import (
    AccessKey,
    Attempt,
    Failure,
    Job,
    ProvisionedActor,
    SyncCheckpoint,
    UsagePage,
    UsageRow,
)
from livepeer_builder.costs.sync import CostSyncWorker
from livepeer_builder.errors import OperationExists
from livepeer_builder.store.postgres import PostgresStore
from livepeer_builder.testing.fakes import ScriptedUsageSource

pytestmark = pytest.mark.asyncio

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


def _job(job_id: UUID, actor_id: str, operation_ref: str | None = None) -> Job:
    return Job(
        id=job_id,
        actor_id=actor_id,
        application_id="test",
        operation_ref=operation_ref,
        app="livepeer-example/hello-world",
        capability=None,
        model=None,
        state="running",
        attempts=(),
        failure=None,
        created_at=NOW,
        completed_at=None,
    )


def _attempt(job_id: UUID, *, auth_ids: tuple[str, ...], manifest_id: str | None) -> Attempt:
    return Attempt(
        job_id=job_id,
        number=1,
        runner_url="http://runner/hello",
        orchestrator_url="https://orch:8935",
        auth_ids=auth_ids,
        manifest_id=manifest_id,
        payment_sent=True,
        outcome="succeeded",
        status_code=200,
        started_at=NOW,
        ended_at=NOW,
    )


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
            TRUNCATE lpb_attempts, lpb_jobs, lpb_usage_events, lpb_cost_cursor, lpb_actors, lpb_access_keys
            """
        )
    await opened.close()


async def test_job_round_trip_and_operation_ref(store: PostgresStore) -> None:
    await store.record_actor(
        ProvisionedActor(
            actor_id="alice",
            grant_id="grant-1",
            allocation_id="alloc-alice",
            api_key_ref="secret-alice",
        )
    )
    assert (await store.actor("alice")) is not None
    assert (await store.actor("nobody")) is None

    job_id = uuid4()
    await store.create_job(_job(job_id, "alice", operation_ref="order-1"))
    await store.record_attempt(_attempt(job_id, auth_ids=("auth-1",), manifest_id="m-1"))
    failure = Failure(kind="runner_error", message="boom", status_code=500, body="x", payment_sent=True)
    await store.finish_job(job_id, "failed", failure, NOW)

    loaded = await store.get_job(job_id)
    assert loaded is not None
    assert loaded.state == "failed"
    assert loaded.failure == failure
    assert loaded.attempts[0].auth_ids == ("auth-1",)
    assert loaded.attempts[0].runner_url == "http://runner/hello"
    assert await store.job_by_operation_ref("test", "alice", "order-1") == loaded
    assert [job.id for job in await store.list_jobs("test", "alice")] == [job_id]

    duplicate = _job(uuid4(), "alice", operation_ref="order-1")
    with pytest.raises(OperationExists) as info:
        await store.create_job(duplicate)
    assert info.value.job.id == job_id
    await store.create_job(_job(uuid4(), "bob-ref", operation_ref="order-1"))  # another actor may reuse it


async def test_access_keys(store: PostgresStore) -> None:
    key = AccessKey(
        key_id="k1",
        actor_id="alice",
        application_id="test",
        scopes=("jobs:run", "jobs:read"),
        label="ci",
        created_at=NOW,
        revoked_at=None,
    )
    await store.insert_access_key(key, "hash-1")
    assert await store.access_key_by_hash("hash-1") == key
    assert await store.access_key_by_hash("hash-2") is None
    await store.revoke_access_key("k1", NOW)
    revoked = await store.access_key_by_hash("hash-1")
    assert revoked is not None
    assert revoked.revoked_at == NOW


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
    await store.create_job(_job(job_id, "alice"))
    await store.record_attempt(_attempt(job_id, auth_ids=(alice_session,), manifest_id="manifest-alice"))
    cost = await store.job_cost(job_id)
    assert cost.status == "observed"
    assert cost.fee_eth == Decimal("0.02")
    assert cost.event_count == 2

    other = uuid4()
    await store.create_job(_job(other, "bob"))
    await store.record_attempt(_attempt(other, auth_ids=(), manifest_id=forged))
    assert (await store.job_cost(other)).status == "pending"
    forged_cost = await store.manifest_cost(forged)
    assert forged_cost.event_count == 2
    assert forged_cost.allocation_ids == ("alloc-alice", "alloc-bob")
    assert forged_cost.fee_eth == Decimal("0.02")

    again = await CostSyncWorker(ScriptedUsageSource([page]), store, {}).sync_once()
    assert again.rows_new == 0
    assert await store.checkpoint() == SyncCheckpoint(resume_cursor=None, filters={})
