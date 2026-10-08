"""The only module that imports livepeer_gateway."""

import json
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp
from livepeer_gateway.discovery import discover_runners
from livepeer_gateway.errors import (
    LivepeerGatewayError,
    LivepeerHTTPError,
    PaymentError,
    SignerRefreshRequired,
    SkipPaymentCycle,
)
from livepeer_gateway.live_runner import LiveRunnerInstance, LiveRunnerPriceInfo, call_runner
from livepeer_gateway.remote_signer import RemoteSignerError

from livepeer_builder.contracts import AttemptOutcome, JobRequest, NetworkRate, Runner, RunnerReply
from livepeer_builder.errors import RunnerCallError

_BODY_LIMIT = 4096
_PAYMENT_ERRORS = (PaymentError, SignerRefreshRequired, SkipPaymentCycle, RemoteSignerError)


class SignerDiscoverySource:
    """Reads the signer's /discover-orchestrators, or a discovery URL that overrides it."""

    def __init__(
        self,
        *,
        signer_url: str | None = None,
        discovery_url: str | None = None,
        signer_headers: dict[str, str] | None = None,
    ) -> None:
        self._signer_url = signer_url
        self._discovery_url = discovery_url
        self._signer_headers = signer_headers

    async def fetch(self) -> tuple[Runner, ...]:
        # The signer bearer goes to the signer only, never to a separate discovery URL.
        entries = await discover_runners(
            signer_url=self._signer_url,
            signer_headers=self._signer_headers,
            discovery_url=self._discovery_url,
        )
        return tuple(runners_from_entries(entries))


def runners_from_entries(entries: list[dict[str, Any]]) -> list[Runner]:
    runners: list[Runner] = []
    for entry in entries:
        orchestrator_url = _text(entry.get("address"))
        for raw in entry.get("runners") or ():
            runner = _runner_from_raw(raw, orchestrator_url)
            if runner is not None:
                runners.append(runner)
    return runners


def _runner_from_raw(raw: object, orchestrator_url: str | None) -> Runner | None:
    if not isinstance(raw, dict):
        return None
    url = _text(raw.get("url"))
    app = _text(raw.get("app"))
    if not url or not app:
        return None
    capacity = raw.get("capacity") if isinstance(raw.get("capacity"), int) else None
    sessions = raw.get("session_ids")
    available = capacity - len(sessions) if capacity is not None and isinstance(sessions, list) else None
    return Runner(
        url=url,
        app=app,
        runner_id=_text(raw.get("runner_id")),
        mode=_text(raw.get("mode")),
        orchestrator_url=orchestrator_url,
        capacity=capacity,
        capacity_available=available,
        rate=_rate(raw.get("price_info")),
    )


class SdkRunnerTransport:
    """call_runner with the actor's credential as the signer bearer."""

    def __init__(
        self,
        signer_url: str | None,
        max_payment_challenge_retries: int = 3,
    ) -> None:
        self._signer_url = signer_url
        self._retries = max_payment_challenge_retries

    async def call(
        self,
        runner: Runner,
        request: JobRequest,
        credential: str | None,
    ) -> RunnerReply:
        paid = credential is not None and self._signer_url is not None
        instance = LiveRunnerInstance(
            url=runner.url,
            app=runner.app,
            runner_id=runner.runner_id or "",
            mode=runner.mode or "",
            orchestrator_url=runner.orchestrator_url or "",
            raw={},
            price_info=_price_info(runner.rate),
        )
        url = (runner.url.rstrip("/") + request.path).strip()  # call_runner strips it too
        try:
            result = await call_runner(
                url,
                runner=instance,
                payload=request.payload,
                method=request.method,
                signer_url=self._signer_url if paid else None,
                signer_headers={"Authorization": f"Bearer {credential}"} if paid else None,
                timeout=request.timeout_s,
                max_payment_challenge_retries=self._retries,
            )
        except LivepeerGatewayError as error:
            raise _call_error(error, url) from error
        except (aiohttp.ClientError, OSError) as error:
            raise RunnerCallError(_network_kind(error), str(error), payment_sent=False) from error
        content = result.content if result.content is not None else json.dumps(result.data).encode()
        return RunnerReply(
            status_code=200,
            content=content,
            content_type=result.content_type or "application/json",
            data=result.data if result.content is None else None,
            auth_ids=result.auth_ids,
            manifest_id=result.session_id or None,
            payment_sent=bool(result.session_id) or bool(result.auth_ids),
        )


def _call_error(error: LivepeerGatewayError, runner_url: str) -> RunnerCallError:
    kind: AttemptOutcome
    status: int | None = None
    body: str | None = None
    if isinstance(error, LivepeerHTTPError) and error.url == runner_url:
        status = error.status_code
        body = error.body[:_BODY_LIMIT] if error.body else None
        kind = "refused" if status == 503 else "http"
    elif isinstance(error, (*_PAYMENT_ERRORS, LivepeerHTTPError)):
        # An HTTP error from any other URL came from the signer.
        kind = "payment"
    else:
        kind = _network_kind(error)
    return RunnerCallError(
        kind,
        str(error),
        payment_sent=error.payment_sent,
        auth_ids=error.auth_ids,
        status_code=status,
        body=body,
    )


def _network_kind(error: BaseException) -> AttemptOutcome:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, TimeoutError):
            return "timeout"
        if isinstance(current, (ConnectionError, OSError, aiohttp.ClientConnectionError)):
            return "unreachable"
        current = current.__cause__
    return "http"


def _rate(value: object) -> NetworkRate | None:
    if not isinstance(value, dict):
        return None
    try:
        amount = Decimal(str(value.get("price")))
    except InvalidOperation:
        return None
    if not amount.is_finite() or amount < 0:
        return None
    return NetworkRate(
        amount=amount,
        currency=str(value.get("currency") or "usd").lower(),
        unit=str(value.get("unit") or "hour").lower(),
    )


def _price_info(rate: NetworkRate | None) -> LiveRunnerPriceInfo | None:
    if rate is None:
        return None
    return LiveRunnerPriceInfo(price=str(rate.amount), currency=rate.currency, unit=rate.unit)


def _text(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
