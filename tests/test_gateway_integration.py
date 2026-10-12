"""The engine on the real SDK adapter, against an in-process signer and runners.

Discovery, dispatch, payment, failover and cost all run through
``SdkRunnerTransport`` and ``SignerDiscoverySource`` into the pinned
livepeer-gateway, unpatched. Only the network on the far side is fake.
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal
from typing import Any

import httpx
import pytest

from conftest import FakeRunner, FakeSigner, free_port
from livepeer_builder.contracts import ActorContext, JobRequest, NetworkRate, ProvisionedActor
from livepeer_builder.engine import BuilderEngine, LivepeerSettings
from livepeer_builder.errors import JobFailed
from livepeer_builder.server.app import JOB_ID_HEADER, create_app
from livepeer_builder.testing.fakes import MemoryStore

APP = "livepeer-example/hello-world"
ORCH = "https://orch.example:8935"
ALICE = ActorContext(actor_id="alice", application_id="shop", scopes=frozenset({"jobs:run", "jobs:read"}))

MakeRunner = Callable[..., Awaitable[FakeRunner]]
BuildEngine = Callable[..., Awaitable[BuilderEngine]]


def _advertise(url: str, *, capacity: int = 4, busy: int = 0, price: str = "0.0001") -> dict[str, Any]:
    return {
        "url": url,
        "app": APP,
        "runner_id": f"runner-{url.rsplit(':', 1)[-1]}",
        "mode": "single-shot",
        "capacity": capacity,
        "session_ids": [f"session-{n}" for n in range(busy)],
        "price_info": {"price": price, "currency": "USD", "unit": "fixed"},
    }


@pytest.fixture
async def build_engine(signer: FakeSigner) -> AsyncIterator[BuildEngine]:
    """An engine whose discovery, transport and usage feed all point at the fake network.

    Runners are advertised in the order given, each with less free capacity
    than the one before, so dispatch tries them in that order.
    """
    engines: list[BuilderEngine] = []

    async def build(
        *runner_urls: str, signer_credential: str | None = "key-operator", admin_token: str | None = None
    ) -> BuilderEngine:
        signer.discovery = [
            {
                "address": ORCH,
                "runners": [
                    _advertise(url, busy=index) for index, url in enumerate(runner_urls)
                ],
            }
        ]
        store = MemoryStore()
        await store.record_actor(
            ProvisionedActor(
                actor_id="alice",
                grant_id="grant-1",
                allocation_id="alloc-alice",
                api_key_ref="key-alice",
            )
        )
        engine = await BuilderEngine.from_settings(
            LivepeerSettings(
                signer_url=signer.url,
                signer_credential=signer_credential,  # type: ignore[arg-type]
                admin_token=admin_token,  # type: ignore[arg-type]
            ),
            store=store,
            usage_source=signer,
        )
        engines.append(engine)
        await engine.discovery.refresh()
        return engine

    yield build
    for engine in engines:
        await engine.stop()


def _request(timeout_s: float = 5.0) -> JobRequest:
    return JobRequest(app=APP, path="/hello", payload={"name": "builder"}, timeout_s=timeout_s)


async def test_discovery_reads_runners_through_the_signer(
    signer: FakeSigner, make_runner: MakeRunner, build_engine: BuildEngine
) -> None:
    runner = await make_runner()
    engine = await build_engine(runner.url)

    (found,) = engine.discovery.snapshot.runners
    assert found.url == runner.url
    assert found.app == APP
    assert found.orchestrator_url == ORCH
    assert (found.capacity, found.capacity_available) == (4, 4)
    assert found.rate == NetworkRate(amount=Decimal("0.0001"), currency="usd", unit="fixed")
    # Discovery uses the operator credential, and only the signer sees it.
    assert signer.discovery_headers[0]["Authorization"] == "Bearer key-operator"


async def test_paid_job_records_the_auth_id_and_its_cost(
    signer: FakeSigner, make_runner: MakeRunner, build_engine: BuildEngine
) -> None:
    runner = await make_runner(statuses=[402, 200])
    engine = await build_engine(runner.url)

    result = await engine.jobs.run(ALICE, _request())

    assert result.job.state == "succeeded"
    assert result.data == {"greeting": "hello", "n": 2}
    (attempt,) = result.job.attempts
    assert attempt.outcome == "succeeded"
    assert attempt.payment_sent is True
    assert attempt.auth_ids == ("auth-1",)
    assert attempt.manifest_id == "manifest-1"
    # The actor's own key pays, never the operator's, and no key reaches the runner.
    assert signer.payment_headers[0]["Authorization"] == "Bearer key-alice"
    assert all("Authorization" not in request.headers for request in runner.requests)
    assert runner.requests[1].path == "/hello"

    assert (await engine.costs.for_job(ALICE, result.job.id)).status == "pending"
    await engine.costs.sync_once()
    cost = await engine.costs.for_job(ALICE, result.job.id)
    assert (cost.status, cost.fee_eth, cost.event_count) == ("observed", Decimal("0.01"), 1)
    assert (await engine.costs.for_actor(ALICE)).fee_eth == Decimal("0.01")


async def test_refused_runner_fails_over_before_payment(
    signer: FakeSigner, make_runner: MakeRunner, build_engine: BuildEngine
) -> None:
    busy = await make_runner(statuses=[503])
    ready = await make_runner(statuses=[402, 200])
    engine = await build_engine(busy.url, ready.url)

    result = await engine.jobs.run(ALICE, _request())

    assert [a.outcome for a in result.job.attempts] == ["refused", "succeeded"]
    assert [a.payment_sent for a in result.job.attempts] == [False, True]
    assert result.job.attempts[0].status_code == 503
    assert [s.auth_id for s in signer.signs] == ["auth-1"]


async def test_unreachable_runner_fails_over(make_runner: MakeRunner, build_engine: BuildEngine) -> None:
    ready = await make_runner(statuses=[402, 200])
    engine = await build_engine(f"http://127.0.0.1:{free_port()}", ready.url)

    result = await engine.jobs.run(ALICE, _request())

    assert [a.outcome for a in result.job.attempts] == ["unreachable", "succeeded"]
    assert result.job.attempts[0].payment_sent is False


async def test_runner_error_after_payment_does_not_fail_over(
    make_runner: MakeRunner, build_engine: BuildEngine
) -> None:
    broken = await make_runner(statuses=[402, 500])
    spare = await make_runner()
    engine = await build_engine(broken.url, spare.url)

    request = _request()
    with pytest.raises(JobFailed) as info:
        await engine.jobs.run(ALICE, request)

    job = info.value.job
    assert job.state == "failed"
    assert job.failure is not None
    assert (job.failure.kind, job.failure.payment_sent) == ("runner_error", True)
    (attempt,) = job.attempts
    assert (attempt.outcome, attempt.status_code, attempt.auth_ids) == ("http", 500, ("auth-1",))
    assert spare.requests == []

    # The charge still lands on the job that paid it.
    await engine.costs.sync_once()
    assert (await engine.costs.for_job(ALICE, job.id)).status == "observed"


async def test_timeout_after_payment_leaves_the_job_uncertain(
    make_runner: MakeRunner, build_engine: BuildEngine
) -> None:
    slow = await make_runner(statuses=[402, "hang"])
    spare = await make_runner()
    engine = await build_engine(slow.url, spare.url)

    request = _request(timeout_s=0.3)
    with pytest.raises(JobFailed) as info:
        await engine.jobs.run(ALICE, request)

    job = info.value.job
    assert job.state == "uncertain"
    (attempt,) = job.attempts
    assert (attempt.outcome, attempt.payment_sent) == ("timeout", True)
    assert spare.requests == []


async def test_signer_refusal_is_a_payment_failure(
    signer: FakeSigner, make_runner: MakeRunner, build_engine: BuildEngine
) -> None:
    runner = await make_runner(statuses=[402])
    spare = await make_runner()
    signer.fail_with = [401]
    engine = await build_engine(runner.url, spare.url)

    request = _request()
    with pytest.raises(JobFailed) as info:
        await engine.jobs.run(ALICE, request)

    job = info.value.job
    assert job.failure is not None
    assert (job.failure.kind, job.failure.payment_sent) == ("payment", False)
    assert len(runner.requests) == 1
    assert spare.requests == []


async def test_runner_rejection_keeps_its_status_and_body(make_runner: MakeRunner, build_engine: BuildEngine) -> None:
    runner = await make_runner(statuses=[422])
    engine = await build_engine(runner.url)

    request = _request()
    with pytest.raises(JobFailed) as info:
        await engine.jobs.run(ALICE, request)

    failure = info.value.job.failure
    assert failure is not None
    assert (failure.kind, failure.status_code, failure.body) == ("runner_rejected", 422, "runner says 422")
    assert failure.payment_sent is False


async def test_actor_without_a_credential_calls_unpaid(
    signer: FakeSigner, make_runner: MakeRunner, build_engine: BuildEngine
) -> None:
    runner = await make_runner()
    engine = await build_engine(runner.url, signer_credential=None)
    stranger = ActorContext(actor_id="stranger", application_id="shop", scopes=frozenset({"jobs:run"}))

    result = await engine.jobs.run(stranger, _request())

    (attempt,) = result.job.attempts
    assert (attempt.outcome, attempt.payment_sent, attempt.auth_ids) == ("succeeded", False, ())
    assert "Livepeer-Payer-Address" not in runner.requests[0].headers
    assert signer.payment_requests == []


async def test_http_job_reaches_the_runner_and_reports_its_cost(
    signer: FakeSigner, make_runner: MakeRunner, build_engine: BuildEngine
) -> None:
    runner = await make_runner(statuses=[402, 200])
    engine = await build_engine(runner.url, admin_token="root")
    app = create_app(engine, manage_lifecycle=False)
    admin = {"Authorization": "Bearer root"}

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
        issued = await http.post(
            "/v1/keys", json={"actor_id": "alice", "scopes": ["jobs:run", "jobs:read"]}, headers=admin
        )
        assert issued.status_code == 201, issued.text
        alice = {"Authorization": f"Bearer {issued.json()['token']}"}

        response = await http.post("/v1/jobs", json={"app": APP, "path": "/hello", "payload": {}}, headers=alice)
        assert response.status_code == 200, response.text
        assert response.json() == {"greeting": "hello", "n": 2}
        job_id = response.headers[JOB_ID_HEADER]

        # The access key authenticates; the stored Batteries key pays.
        assert signer.payment_headers[0]["Authorization"] == "Bearer key-alice"
        job = (await http.get(f"/v1/jobs/{job_id}", headers=alice)).json()
        assert job["attempts"][0]["auth_ids"] == ["auth-1"]

        assert (await http.post("/v1/costs/sync", headers=admin)).status_code == 200
        cost = (await http.get(f"/v1/jobs/{job_id}/cost", headers=alice)).json()
        assert cost["status"] == "observed"
        assert Decimal(str(cost["fee_eth"])) == Decimal("0.01")
