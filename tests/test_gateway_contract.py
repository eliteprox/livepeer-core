"""What livepeer_builder relies on from livepeer-gateway, against a real signer and runner protocol.

Each test calls the gateway's public API unpatched. A pin bump that changes
how payment is signed, what reaches the runner, or what comes back to record
fails here first.
"""

from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from livepeer_gateway.errors import LivepeerHTTPError
from livepeer_gateway.live_runner import (
    LiveRunnerCallResult,
    LiveRunnerInstance,
    LiveRunnerPriceInfo,
    call_runner,
)
from livepeer_gateway.remote_signer import LivePaymentChallenge, LivePaymentSession

from conftest import FakeRunner, FakeSigner
from livepeer_builder.contracts import Attempt, ProvisionedActor, UsagePage
from livepeer_builder.costs.sync import CostSyncWorker
from livepeer_builder.store.postgres import PostgresStore
from livepeer_builder.testing.fakes import ScriptedUsageSource

APP = "livepeer-example/hello-world"
PRICE = LiveRunnerPriceInfo(price="0.0001", currency="usd", unit="fixed")


def _instance(runner: FakeRunner) -> LiveRunnerInstance:
    return LiveRunnerInstance(
        url=runner.url,
        app=APP,
        runner_id="runner-1",
        mode="single-shot",
        orchestrator_url="https://orch.example:8935",
        raw={},
        price_info=PRICE,
    )


async def _paid_call(signer: FakeSigner, runner: FakeRunner, key: str = "key-alice") -> LiveRunnerCallResult:
    return await call_runner(
        f"{runner.url}/hello",
        runner=_instance(runner),
        payload={"name": "builder"},
        signer_url=signer.url,
        signer_headers={"Authorization": f"Bearer {key}"},
    )


def _challenge(runner: FakeRunner, manifest_id: str | None = None) -> LivePaymentChallenge:
    return LivePaymentChallenge(
        payment_params=runner.payment_params,
        manifest_id=manifest_id or runner.manifest_id,
        payment_url=f"{runner.url}/payment",
    )


async def test_paid_call_signs_with_the_actor_key_and_returns_the_auth_id(
    signer: FakeSigner, runner: FakeRunner
) -> None:
    runner.statuses = [402, 200]

    result = await _paid_call(signer, runner)

    sign = signer.payment_requests[0]
    assert signer.payment_headers[0]["Authorization"] == "Bearer key-alice"
    assert sign["ManifestID"] == "manifest-1"
    assert sign["orchestrator"] == runner.payment_params
    assert sign["type"] == "fixed"
    assert sign["app"] == APP
    assert sign["maxPrice"]["unit"] == "fixed"
    assert Decimal(str(sign["maxPrice"]["price"])) >= Decimal("0.0001")

    unpaid, paid = runner.requests
    assert "Livepeer-Payment" not in unpaid.headers
    assert unpaid.headers["Livepeer-Payer-Address"] == signer.address
    assert paid.headers["Livepeer-Payment"] == signer.payment
    assert paid.headers["Livepeer-Segment"] == signer.seg_creds
    assert paid.path == "/hello"

    assert result.data["greeting"] == "hello"
    assert result.session_id == "manifest-1"
    assert result.auth_ids == ("auth-1",)


async def test_one_session_signing_twice_keeps_its_auth_id(signer: FakeSigner, runner: FakeRunner) -> None:
    # A reserved session signs again with the state the signer returned: same charge key.
    session = LivePaymentSession(
        signer.url,
        signer_headers={"Authorization": "Bearer key-alice"},
        type="fixed",
        challenge=_challenge(runner),
    )
    assert session.auth_id == ""

    await session.get_payment()
    await session.get_payment()

    assert session.auth_id == "auth-1"
    assert [(s.auth_id, s.seq) for s in signer.signs] == [("auth-1", 0), ("auth-1", 1)]
    assert "state" not in signer.payment_requests[0]
    assert signer.payment_requests[1]["state"]["State"]


async def test_auth_id_comes_from_the_signer_not_the_manifest(signer: FakeSigner, runner: FakeRunner) -> None:
    session = LivePaymentSession(
        signer.url,
        signer_headers={"Authorization": "Bearer key-alice"},
        type="fixed",
        challenge=_challenge(runner, manifest_id="forged-by-client"),
    )

    await session.get_payment()

    assert session.auth_id == "auth-1"
    assert signer.signs[0].manifest_id == "forged-by-client"
    assert signer.signs[0].allocation_id == "alloc-alice"


@pytest.mark.parametrize("status", [500, 422])
async def test_runner_error_after_payment_carries_the_auth_id(
    signer: FakeSigner, runner: FakeRunner, status: int
) -> None:
    runner.statuses = [402, status]

    with pytest.raises(LivepeerHTTPError) as info:
        await _paid_call(signer, runner)

    assert info.value.status_code == status
    assert info.value.payment_sent is True
    assert info.value.auth_ids == ("auth-1",)


async def test_signer_refusal_happens_before_payment(signer: FakeSigner, runner: FakeRunner) -> None:
    runner.statuses = [402]
    signer.fail_with = [401]

    with pytest.raises(LivepeerHTTPError) as info:
        await _paid_call(signer, runner)

    assert info.value.status_code == 401
    assert info.value.url.startswith(signer.url)
    assert info.value.payment_sent is False
    assert info.value.auth_ids == ()
    assert len(runner.requests) == 1
    assert signer.signs == []


async def test_signed_auth_ids_attribute_cost_to_the_job_and_actor(
    signer: FakeSigner, runner: FakeRunner, store: PostgresStore
) -> None:
    for actor_id in ("alice", "bob"):
        await store.record_actor(
            ProvisionedActor(
                actor_id=actor_id,
                grant_id="grant-1",
                allocation_id=f"alloc-{actor_id}",
                api_key_ref=f"key-{actor_id}",
            )
        )
    runner.statuses = [402, 200]

    # Recorded with the auth id the gateway returned: the job finds its charge.
    paid = await _paid_call(signer, runner)
    observed_job = uuid4()
    await store.record_attempt("alice", _attempt(observed_job, paid.auth_ids, paid.session_id))

    # Recorded with the manifest only: charged to alice, but no job can claim it.
    runner.statuses = [402, 200]
    unclaimed = await _paid_call(signer, runner)
    pending_job = uuid4()
    await store.record_attempt("alice", _attempt(pending_job, (), unclaimed.session_id))

    # A client-chosen manifest still bills the key that signed it.
    forged = LivePaymentSession(
        signer.url,
        signer_headers={"Authorization": "Bearer key-bob"},
        type="fixed",
        challenge=_challenge(runner, manifest_id="forged-by-client"),
    )
    await forged.get_payment()
    forged_job = uuid4()
    await store.record_attempt("bob", _attempt(forged_job, (forged.auth_id,), "forged-by-client"))

    page = UsagePage(items=signer.usage_rows(fee_eth="0.01"), next_cursor="")
    report = await CostSyncWorker(ScriptedUsageSource([page]), store, {}).sync_once()
    assert report.rows_new == 3

    cost = await store.job_cost(observed_job)
    assert (cost.status, cost.fee_eth, cost.event_count) == ("observed", Decimal("0.01"), 1)
    assert (await store.job_cost(pending_job)).status == "pending"
    forged_cost = await store.job_cost(forged_job)
    assert (forged_cost.status, forged_cost.fee_eth) == ("observed", Decimal("0.01"))

    # Spend per actor matches what the signer charged to each allocation.
    assert (await store.actor_spend("alice")).fee_eth == Decimal("0.02")
    assert (await store.actor_spend("bob")).fee_eth == Decimal("0.01")
    assert (await store.manifest_cost("forged-by-client")).allocation_ids == ("alloc-bob",)


def _attempt(job_id: UUID, auth_ids: tuple[str, ...], manifest_id: str | None) -> Attempt:
    return Attempt(
        job_id=job_id,
        number=1,
        auth_ids=auth_ids,
        manifest_id=manifest_id or None,
        payment_sent=True,
        outcome="succeeded",
    )
