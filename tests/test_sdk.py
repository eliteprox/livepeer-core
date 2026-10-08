"""How the SDK adapter reads discovery prices and classifies gateway errors."""

from livepeer_gateway.errors import LivepeerGatewayError, LivepeerHTTPError

from livepeer_builder.adapters.sdk import _call_error, _rate

RUNNER = "http://runner:8000/payment-intents"


def test_rate_rejects_non_finite_and_negative_prices() -> None:
    assert _rate({"price": "0.5", "currency": "USD"}) is not None
    for price in ("NaN", "sNaN", "Infinity", "-1", None):
        assert _rate({"price": price}) is None


def test_runner_http_error_is_http_and_signer_http_error_is_payment() -> None:
    assert _call_error(LivepeerHTTPError(503, RUNNER), RUNNER).kind == "refused"
    rejected = _call_error(LivepeerHTTPError(422, RUNNER, body="bad"), RUNNER)
    assert (rejected.kind, rejected.status_code) == ("http", 422)
    signer = _call_error(LivepeerHTTPError(401, "http://signer:7936/sign-orchestrator-info"), RUNNER)
    assert (signer.kind, signer.status_code) == ("payment", None)


def test_a_url_containing_payment_is_not_a_payment_error() -> None:
    error = LivepeerGatewayError(f"HTTP JSON error: failed to reach endpoint (url={RUNNER})")
    error.__cause__ = TimeoutError()
    assert _call_error(error, RUNNER).kind == "timeout"
