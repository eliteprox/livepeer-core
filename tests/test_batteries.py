from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from livepeer_core.adapters.batteries import BatteriesUsageSource, idempotency_key, usage_row_from_item
from livepeer_core.errors import CursorMismatch


def test_idempotency_key_is_stable_base64url() -> None:
    key = idempotency_key("engine", "alice", "allocation")
    assert key == idempotency_key("engine", "alice", "allocation")
    assert "=" not in key
    assert len(key) == 43


def test_usage_row_reads_eth_amounts_and_nulls() -> None:
    row = usage_row_from_item(
        {
            "id": "01J0USAGE",
            "event_id": "evt-1",
            "status": "applied",
            "computed_fee_eth": "0.00000000000000001",
            "computed_fee_usd": "0.25",
            "created_at_ms": 1759766400000,
            "payment_session_id": "01J0SESSION",
            "request_id": "req-1",
            "pipeline": "fixed",
            "manifest_id": "manifest-1",
            "allocation_id": "01J0ALLOC",
            "currency": "eth",
        }
    )
    assert row.computed_fee_eth == Decimal("0.00000000000000001")
    assert row.created_at == datetime.fromtimestamp(1759766400000 / 1000, tz=UTC)
    empty = usage_row_from_item(
        {
            "id": "quarantined",
            "event_id": None,
            "status": "quarantined",
            "computed_fee_eth": "",
            "computed_fee_usd": None,
            "created_at_ms": 1759766400000,
            "payment_session_id": None,
            "request_id": "",
            "pipeline": None,
            "manifest_id": None,
            "allocation_id": None,
            "currency": None,
        }
    )
    assert empty.computed_fee_eth is None
    assert empty.manifest_id is None


@pytest.mark.asyncio
async def test_usage_source_raises_on_cursor_mismatch() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["cursor"] == "stale"
        return httpx.Response(400, json={"error": "invalid cursor"})

    source = BatteriesUsageSource(
        "http://batteries",
        "token",
        client=httpx.AsyncClient(base_url="http://batteries", transport=httpx.MockTransport(handler)),
    )
    with pytest.raises(CursorMismatch):
        await source.page("stale")
    await source.aclose()
