import json
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from livepeer_gateway.errors import NoRunnerAvailableError
from livepeer_gateway.remote_signer import LivePaymentChallenge, LivePaymentSession, get_signer_info
from livepeer_gateway.selection import runner_selector

from livepeer_builder.contracts import Attempt, ProvisionedActor
from livepeer_builder.store.postgres import PostgresStore
from settings import discovery_url, signer_url

HELLO_APP = "livepeer-example/hello-world"
HOLD_APP = "livepeer-core/hold"


@dataclass(frozen=True)
class PaidCall:
    auth_ids: tuple[str, ...]
    manifest_id: str | None
    payment_sent: bool
    outcome: str


def headers_for(actor: ProvisionedActor) -> dict[str, str]:
    return {"Authorization": f"Bearer {actor.api_key_ref}"}


async def hello_call(actor: ProvisionedActor) -> PaidCall:
    cursor = await runner_selector(
        discovery_url=discovery_url(),
        app=HELLO_APP,
        signer_url=signer_url(),
        signer_headers=headers_for(actor),
        timeout=60.0,
    )
    try:
        result = await cursor.next()
    except NoRunnerAvailableError as exc:
        if exc.auth_ids:
            return PaidCall(
                auth_ids=tuple(exc.auth_ids),
                manifest_id=None,
                payment_sent=exc.payment_sent,
                outcome="orch_rejected",
            )
        detail = "; ".join(f"{item.url}: {item.reason}" for item in cursor.rejections)
        raise RuntimeError(detail or str(exc)) from exc
    return PaidCall(
        auth_ids=tuple(result.auth_ids),
        manifest_id=result.session_id or None,
        payment_sent=True,
        outcome="succeeded",
    )


async def record(
    store: PostgresStore,
    actor: ProvisionedActor,
    *,
    auth_ids: tuple[str, ...],
    manifest_id: str | None,
    payment_sent: bool,
    outcome: str,
    job_id: UUID | None = None,
) -> UUID:
    job_id = job_id or uuid4()
    await store.record_attempt(
        actor.actor_id,
        Attempt(
            job_id=job_id,
            number=1,
            auth_ids=auth_ids,
            manifest_id=manifest_id,
            payment_sent=payment_sent,
            outcome=outcome,
        ),
    )
    return job_id


async def sign_twice(actor: ProvisionedActor, app: str, payment_type: str) -> str:
    challenge = await _unpaid_challenge(app)
    session = LivePaymentSession(
        signer_url(),
        signer_headers=headers_for(actor),
        type=payment_type,
        challenge=challenge,
    )
    await session.get_payment()
    await session.get_payment()
    if not session.auth_id:
        raise RuntimeError("signer state did not contain AuthID")
    return session.auth_id


async def _unpaid_challenge(app: str) -> LivePaymentChallenge:
    cursor = await runner_selector(
        discovery_url=discovery_url(),
        app=app,
        timeout=30.0,
    )
    if not cursor.candidates:
        raise RuntimeError(f"no runner for {app}")
    signer = await get_signer_info(signer_url())
    async with httpx.AsyncClient(verify=False, timeout=30.0, follow_redirects=True) as client:
        response = await client.post(
            cursor.candidates[0].url,
            json={"name": "challenge"},
            headers={"Livepeer-Payer-Address": signer.address or ""},
        )
    if response.status_code != 402:
        raise RuntimeError(f"{app} challenge failed: HTTP {response.status_code}: {response.text}")
    return _challenge(response.text)


async def forged_payment(actor: ProvisionedActor) -> tuple[str, str]:
    challenge = await _unpaid_challenge(HELLO_APP)
    forged = LivePaymentChallenge(
        payment_params=challenge.payment_params,
        manifest_id="forged-by-client",
        payment_url=challenge.payment_url,
    )
    session = LivePaymentSession(
        signer_url(),
        signer_headers=headers_for(actor),
        type="fixed",
        challenge=forged,
    )
    await session.get_payment()
    if not session.auth_id:
        raise RuntimeError("signer state did not contain AuthID")
    return session.auth_id, forged.manifest_id


def _challenge(body: str) -> LivePaymentChallenge:
    data = json.loads(body)
    return LivePaymentChallenge(
        payment_params=data["payment_params"],
        manifest_id=data["manifest_id"],
        payment_url=data["payment_url"],
    )
