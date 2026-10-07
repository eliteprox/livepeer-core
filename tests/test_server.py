"""The REST adapter over an engine built on the in-memory fakes."""

from collections.abc import AsyncIterator
from decimal import Decimal

import httpx
import pytest

from livepeer_builder.contracts import NetworkRate, ProvisionedActor, Runner, RunnerReply
from livepeer_builder.engine import BuilderEngine, LivepeerSettings
from livepeer_builder.errors import RunnerCallError
from livepeer_builder.server.app import JOB_ID_HEADER, create_app
from livepeer_builder.testing.fakes import FixedClock, MemoryStore, ScriptedTransport, StaticDiscoverySource

APP = "livepeer-example/hello-world"
ADMIN = {"Authorization": "Bearer root"}


def _runner() -> Runner:
    return Runner(
        url="http://runner/hello",
        app=APP,
        runner_id="r1",
        mode="single-shot",
        orchestrator_url="https://orch:8935",
        capacity=1,
        capacity_available=1,
        rate=NetworkRate(amount=Decimal("0.0001"), currency="usd", unit="fixed"),
    )


@pytest.fixture
async def client() -> AsyncIterator[tuple[httpx.AsyncClient, ScriptedTransport, MemoryStore]]:
    store = MemoryStore()
    await store.record_actor(ProvisionedActor(actor_id="alice", grant_id="g", allocation_id="a", api_key_ref="k"))
    transport = ScriptedTransport([])
    engine = await BuilderEngine.from_settings(
        LivepeerSettings(admin_token="root"),  # type: ignore[arg-type]
        store=store,
        discovery_source=StaticDiscoverySource([_runner()]),
        transport=transport,
        clock=FixedClock(),
    )
    await engine.discovery.refresh()
    app = create_app(engine, manage_lifecycle=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
        yield http, transport, store


async def _alice_key(http: httpx.AsyncClient) -> dict[str, str]:
    response = await http.post(
        "/v1/keys", json={"actor_id": "alice", "scopes": ["discover", "jobs:run", "jobs:read"]}, headers=ADMIN
    )
    assert response.status_code == 201, response.text
    return {"Authorization": f"Bearer {response.json()['token']}"}


async def test_offerings_require_a_key_and_the_discover_scope(client) -> None:
    http, _, _ = client
    assert (await http.get("/v1/offerings")).status_code == 401
    assert (await http.get("/v1/offerings", headers={"Authorization": "Bearer nope"})).status_code == 401
    alice = await _alice_key(http)
    offerings = (await http.get("/v1/offerings", headers=alice)).json()
    assert offerings[0]["app"] == APP and offerings[0]["ready_count"] == 1
    runners = (await http.get("/v1/runners", params={"app": APP}, headers=alice)).json()
    assert runners[0]["url"] == "http://runner/hello"
    assert (await http.get("/v1/offerings", params={"app": "other/app"}, headers=alice)).json() == []


async def test_run_job_forwards_the_runner_body_and_job_id(client) -> None:
    http, transport, _ = client
    transport.script.append(
        RunnerReply(
            status_code=200,
            content=b'{"message": "Hello, x!"}',
            content_type="application/json",
            data={"message": "Hello, x!"},
            auth_ids=("auth-1",),
            manifest_id="m-1",
            payment_sent=True,
        )
    )
    alice = await _alice_key(http)
    response = await http.post(
        "/v1/jobs", json={"app": APP, "path": "/hello", "payload": {"name": "x"}, "operation_ref": "o-1"}, headers=alice
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"message": "Hello, x!"}
    job_id = response.headers[JOB_ID_HEADER]
    assert transport.calls[0][2] == "k"  # alice's provisioned key reached the transport

    job = (await http.get(f"/v1/jobs/{job_id}", headers=alice)).json()
    assert job["state"] == "succeeded" and job["attempts"][0]["auth_ids"] == ["auth-1"]
    cost = (await http.get(f"/v1/jobs/{job_id}/cost", headers=alice)).json()
    assert cost["status"] == "pending"

    again = await http.post("/v1/jobs", json={"app": APP, "operation_ref": "o-1"}, headers=alice)
    assert again.status_code == 409 and again.headers[JOB_ID_HEADER] == job_id

    bob = await http.post("/v1/keys", json={"actor_id": "bob", "scopes": ["jobs:read"]}, headers=ADMIN)
    bob_key = {"Authorization": f"Bearer {bob.json()['token']}"}
    assert (await http.get(f"/v1/jobs/{job_id}", headers=bob_key)).status_code == 403


async def test_failed_job_answers_with_the_failure(client) -> None:
    http, transport, _ = client
    transport.script.append(RunnerCallError("http", "bad input", payment_sent=True, status_code=422, body="nope"))
    alice = await _alice_key(http)
    response = await http.post("/v1/jobs", json={"app": APP}, headers=alice)
    assert response.status_code == 422
    assert response.json()["kind"] == "runner_rejected"
    assert JOB_ID_HEADER in response.headers

    transport.script.append(RunnerCallError("refused", "full", payment_sent=False, status_code=503))
    response = await http.post("/v1/jobs", json={"app": APP, "max_attempts": 1}, headers=alice)
    assert response.status_code == 503 and response.headers["Retry-After"] == "5"

    response = await http.post("/v1/jobs", json={"app": "missing/app"}, headers=alice)
    assert response.status_code == 404 and response.json()["kind"] == "no_offering"


async def test_admin_routes_and_health(client) -> None:
    http, _, _ = client
    assert (await http.get("/livez")).json() == {"status": "ok"}
    ready = await http.get("/readyz")
    assert ready.status_code == 200 and ready.json()["discovery_fresh"] is True
    sync = await http.post("/v1/costs/sync", headers=ADMIN)
    assert sync.status_code == 503  # no usage source configured
    spend = await http.get("/v1/spend", params={"actor_id": "alice"}, headers=ADMIN)
    assert spend.status_code == 200 and spend.json()["event_count"] == 0
    alice = await _alice_key(http)
    assert (await http.get("/v1/spend", params={"actor_id": "bob"}, headers=alice)).status_code == 403
    assert (await http.post("/v1/keys", json={"actor_id": "x", "scopes": []}, headers=alice)).status_code == 403
