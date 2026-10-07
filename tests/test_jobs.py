"""The dispatch rules the contract fixes, run against the in-memory fakes."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from livepeer_builder.contracts import (
    ActorContext,
    JobRequest,
    NetworkRate,
    ProvisionedActor,
    Runner,
    RunnerReply,
    SyncCheckpoint,
    UsageRow,
)
from livepeer_builder.costs.service import CostService
from livepeer_builder.discovery.service import DiscoveryService, MostCapacityFirst
from livepeer_builder.errors import AccessDenied, JobFailed, OperationExists, RunnerCallError
from livepeer_builder.jobs.service import JobService
from livepeer_builder.testing.fakes import FixedClock, MemoryStore, ScriptedTransport, StaticDiscoverySource

APP = "livepeer-example/hello-world"
ALICE = ActorContext(actor_id="alice", application_id="shop", scopes=frozenset({"jobs:run", "jobs:read"}))
BOB = ActorContext(actor_id="bob", application_id="shop", scopes=frozenset({"jobs:run", "jobs:read"}))


def _runner(url: str, available: int | None = 1) -> Runner:
    return Runner(
        url=url,
        app=APP,
        runner_id=None,
        mode="single-shot",
        orchestrator_url="https://orch:8935",
        capacity=2,
        capacity_available=available,
        rate=NetworkRate(amount=Decimal("0.0001"), currency="usd", unit="fixed"),
    )


def _reply(auth_ids: tuple[str, ...] = ("auth-1",)) -> RunnerReply:
    return RunnerReply(
        status_code=200,
        content=b'{"message": "Hello"}',
        content_type="application/json",
        data={"message": "Hello"},
        auth_ids=auth_ids,
        manifest_id="manifest-1",
        payment_sent=bool(auth_ids),
    )


def _row(row_id: str, session_id: str, allocation_id: str = "alloc-alice") -> UsageRow:
    return UsageRow(
        id=row_id,
        event_id=row_id,
        status="applied",
        manifest_id="manifest-1",
        allocation_id=allocation_id,
        payment_session_id=session_id,
        request_id=None,
        pipeline="fixed",
        computed_fee_eth=Decimal("0.00001"),
        computed_fee_usd=None,
        currency="eth",
        created_at=datetime(2026, 10, 7, tzinfo=UTC),
    )


class Harness:
    def __init__(
        self,
        script: list[RunnerReply | RunnerCallError],
        runners: list[Runner] | None = None,
    ) -> None:
        self.clock = FixedClock()
        self.store = MemoryStore()
        self.transport = ScriptedTransport(script)
        self.discovery = DiscoveryService(
            StaticDiscoverySource(runners if runners is not None else [_runner("http://a"), _runner("http://b")]),
            self.clock,
        )
        self.jobs = JobService(
            self.store,
            self.discovery,
            self.transport,
            MostCapacityFirst(),
            self,
            self.clock,
        )
        self.costs = CostService(self.store, None, self.clock)

    async def credential(self, actor: ActorContext) -> str | None:
        stored = await self.store.actor(actor.actor_id)
        return stored.api_key_ref if stored else None

    async def ready(self) -> "Harness":
        await self.store.record_actor(
            ProvisionedActor(actor_id="alice", grant_id="g", allocation_id="alloc-alice", api_key_ref="key-alice")
        )
        await self.discovery.refresh()
        return self


async def test_success_records_auth_ids_and_passes_the_actor_credential() -> None:
    h = await Harness([_reply()]).ready()
    result = await h.jobs.run(ALICE, JobRequest(app=APP, path="/hello", payload={"name": "x"}, operation_ref="o-1"))
    assert result.data == {"message": "Hello"}
    assert result.job.state == "succeeded"
    assert result.job.attempts[0].auth_ids == ("auth-1",)
    assert result.job.attempts[0].manifest_id == "manifest-1"
    assert h.transport.calls[0][2] == "key-alice"
    stored = await h.store.get_job(result.job.id)
    assert stored is not None
    assert stored.state == "succeeded"
    assert len(stored.attempts) == 1


async def test_fails_over_only_before_payment() -> None:
    h = await Harness(
        [
            RunnerCallError("refused", "503", payment_sent=False, status_code=503),
            _reply(),
        ]
    ).ready()
    result = await h.jobs.run(ALICE, JobRequest(app=APP))
    assert [a.outcome for a in result.job.attempts] == ["refused", "succeeded"]
    assert [a.runner_url for a in result.job.attempts] == ["http://a", "http://b"]


async def test_bound_after_payment_and_unknown_outcome_is_uncertain() -> None:
    h = await Harness(
        [RunnerCallError("unreachable", "reset", payment_sent=True, auth_ids=("auth-x",)), _reply()]
    ).ready()
    request = JobRequest(app=APP)
    with pytest.raises(JobFailed) as info:
        await h.jobs.run(ALICE, request)
    job = info.value.job
    assert job.state == "uncertain"
    assert len(job.attempts) == 1
    assert job.attempts[0].auth_ids == ("auth-x",)  # the signer may have charged
    assert len(h.transport.calls) == 1
    assert (await h.store.job_cost(job.id)).status == "pending"


async def test_runner_4xx_is_the_callers_mistake_and_does_not_fail_over() -> None:
    h = await Harness(
        [RunnerCallError("http", "bad input", payment_sent=True, status_code=422, body='{"detail":"x"}')]
    ).ready()
    request = JobRequest(app=APP)
    with pytest.raises(JobFailed) as info:
        await h.jobs.run(ALICE, request)
    failure = info.value.job.failure
    assert info.value.job.state == "failed"
    assert failure is not None
    assert failure.kind == "runner_rejected"
    assert failure.status_code == 422
    assert len(h.transport.calls) == 1


async def test_payment_failure_before_dispatch_is_a_payment_failure_not_uncertain() -> None:
    h = await Harness([RunnerCallError("payment", "signer 402", payment_sent=False)]).ready()
    request = JobRequest(app=APP)
    with pytest.raises(JobFailed) as info:
        await h.jobs.run(ALICE, request)
    assert info.value.job.state == "failed"
    assert info.value.job.failure is not None
    assert info.value.job.failure.kind == "payment"


async def test_all_runners_refused_is_a_refused_failure_with_cost_none() -> None:
    h = await Harness(
        [
            RunnerCallError("refused", "503", payment_sent=False, status_code=503),
            RunnerCallError("unreachable", "down", payment_sent=False),
        ]
    ).ready()
    request = JobRequest(app=APP, max_attempts=2)
    with pytest.raises(JobFailed) as info:
        await h.jobs.run(ALICE, request)
    assert info.value.job.failure is not None
    assert info.value.job.failure.kind == "unreachable"
    assert (await h.store.job_cost(info.value.job.id)).status == "pending"


async def test_no_runner_for_the_app_is_no_offering() -> None:
    h = await Harness([], runners=[]).ready()
    request = JobRequest(app="missing/app")
    with pytest.raises(JobFailed) as info:
        await h.jobs.run(ALICE, request)
    assert info.value.job.failure is not None
    assert info.value.job.failure.kind == "no_offering"
    assert (await h.store.job_cost(info.value.job.id)).status == "none"


async def test_operation_ref_dispatches_once_per_actor() -> None:
    h = await Harness([_reply(), _reply()]).ready()
    first = await h.jobs.run(ALICE, JobRequest(app=APP, operation_ref="order-7"))
    duplicate = JobRequest(app=APP, operation_ref="order-7")
    with pytest.raises(OperationExists) as info:
        await h.jobs.run(ALICE, duplicate)
    assert info.value.job.id == first.job.id
    assert len(h.transport.calls) == 1
    other = await h.jobs.run(BOB, JobRequest(app=APP, operation_ref="order-7"))  # bob's ref is bob's
    assert other.job.id != first.job.id


async def test_selection_prefers_free_capacity_and_respects_max_attempts() -> None:
    h = await Harness(
        [RunnerCallError("refused", "503", payment_sent=False, status_code=503)],
        runners=[_runner("http://full", available=0), _runner("http://free", available=2), _runner("http://c")],
    ).ready()
    request = JobRequest(app=APP, max_attempts=1)
    with pytest.raises(JobFailed):
        await h.jobs.run(ALICE, request)
    assert [call[0].url for call in h.transport.calls] == ["http://free"]


async def test_reads_are_scoped_to_the_owner() -> None:
    h = await Harness([_reply()]).ready()
    result = await h.jobs.run(ALICE, JobRequest(app=APP))
    assert (await h.jobs.get(ALICE, result.job.id)).id == result.job.id
    with pytest.raises(AccessDenied):
        await h.jobs.get(BOB, result.job.id)
    with pytest.raises(AccessDenied):
        await h.costs.for_job(BOB, result.job.id)
    read_only = ActorContext(actor_id="alice", scopes=frozenset({"jobs:read"}))
    request = JobRequest(app=APP)
    with pytest.raises(AccessDenied):
        await h.jobs.run(read_only, request)
    admin = ActorContext(actor_id="op", scopes=frozenset({"admin"}))
    assert (await h.jobs.get(admin, result.job.id)).id == result.job.id
    assert [job.id for job in await h.jobs.list(ALICE)] == [result.job.id]
    assert await h.jobs.list(BOB) == []


async def test_job_cost_joins_on_auth_id_and_the_actors_allocation() -> None:
    h = await Harness([_reply(("auth-1",))]).ready()
    result = await h.jobs.run(ALICE, JobRequest(app=APP))
    assert (await h.costs.for_job(ALICE, result.job.id)).status == "pending"
    await h.store.apply(
        (
            _row("r1", "auth-1"),
            _row("r2", "auth-1"),
            _row("r3", "auth-1", allocation_id="alloc-someone-else"),  # same manifest, other allocation
            _row("r4", "auth-other"),
        ),
        SyncCheckpoint(resume_cursor=None),
    )
    cost = await h.costs.for_job(ALICE, result.job.id)
    assert cost.status == "observed"
    assert cost.event_count == 2
    assert cost.fee_eth == Decimal("0.00002")
    spend = await h.costs.for_actor(ALICE)
    assert spend.event_count == 3  # every applied row on alice's allocation, whichever job
    with pytest.raises(AccessDenied):
        await h.costs.for_actor(BOB, "alice")
