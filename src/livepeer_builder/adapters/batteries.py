import base64
import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx

from livepeer_builder.contracts import UsagePage, UsageRow
from livepeer_builder.errors import BatteriesError, CursorMismatch

_TOKEN_HEADER = "Livepeer-Clearinghouse-Token"
_STATUSES = {"applied", "quarantined", "ignored", "duplicate"}


def idempotency_key(engine_id: str, operation_ref: str, step: str) -> str:
    digest = hashlib.sha256(f"{engine_id}:{operation_ref}:{step}".encode()).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


class BatteriesClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        engine_id: str = "livepeer-builder",
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._engine_id = engine_id
        self._client = client or httpx.AsyncClient(
            base_url=self._base_url,
            headers={_TOKEN_HEADER: token},
            timeout=30.0,
        )
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def create_grant(
        self,
        name: str,
        amount_eth: str,
    ) -> str:
        body = await self._request(
            "POST",
            "/v1/grants",
            json={"name": name, "amount_eth": amount_eth, "status": "active"},
        )
        return str(body["id"])

    async def create_allocation(
        self,
        grant_id: str,
        name: str,
        amount_eth: str,
        operation_ref: str,
    ) -> str:
        body = await self._request(
            "POST",
            "/v1/allocations",
            json={
                "name": name,
                "grant_id": grant_id,
                "amount_eth": amount_eth,
                "status": "active",
            },
            headers={
                "Idempotency-Key": idempotency_key(self._engine_id, operation_ref, "allocation"),
            },
        )
        return str(body["id"])

    async def create_api_key(
        self,
        allocation_id: str,
        name: str,
        operation_ref: str,
    ) -> dict[str, str]:
        body = await self._request(
            "POST",
            "/v1/api-keys",
            json={"allocation_id": allocation_id, "name": name},
            headers={
                "Idempotency-Key": idempotency_key(self._engine_id, operation_ref, "api-key"),
            },
        )
        return {
            "id": str(body["id"]),
            "allocation_id": str(body["allocation_id"]),
            "api_key": str(body["api_key"]),
        }

    async def allocation(self, allocation_id: str) -> dict[str, Any]:
        body = await self._request("GET", f"/v1/allocations/{allocation_id}")
        return dict(body)

    async def _request(
        self,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
    ) -> Any:
        response = await self._client.request(method, path, json=json, headers=headers, params=params)
        if response.status_code >= 400:
            raise BatteriesError(response.status_code, _error_message(response))
        if not response.content:
            return {}
        return response.json()


class BatteriesUsageSource:
    def __init__(
        self,
        base_url: str,
        token: str,
        filters: Mapping[str, str] | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._filters = dict(filters or {})
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={_TOKEN_HEADER: token},
            timeout=30.0,
        )
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def page(self, cursor: str | None, limit: int = 1000) -> UsagePage:
        params = {**self._filters, "limit": str(limit)}
        if cursor:
            params["cursor"] = cursor
        response = await self._client.get("/v1/usage", params=params)
        if response.status_code == 400 and "cursor" in _error_message(response):
            raise CursorMismatch(_error_message(response))
        if response.status_code >= 400:
            raise BatteriesError(response.status_code, _error_message(response))
        body = response.json()
        items = tuple(usage_row_from_item(item) for item in body["items"])
        return UsagePage(items=items, next_cursor=str(body.get("next_cursor") or ""))


def usage_row_from_item(item: dict[str, Any]) -> UsageRow:
    status = item.get("status")
    if status not in _STATUSES:
        raise BatteriesError(200, f"unknown usage status {status!r}")
    return UsageRow(
        id=str(item["id"]),
        event_id=_text(item.get("event_id")),
        status=status,
        manifest_id=_text(item.get("manifest_id")),
        allocation_id=_text(item.get("allocation_id")),
        payment_session_id=_text(item.get("payment_session_id")),
        request_id=_text(item.get("request_id")),
        pipeline=_text(item.get("pipeline")),
        computed_fee_eth=_decimal(item.get("computed_fee_eth")),
        computed_fee_usd=_decimal(item.get("computed_fee_usd")),
        currency=_text(item.get("currency")),
        created_at=datetime.fromtimestamp(int(item["created_at_ms"]) / 1000, tz=UTC),
    )


def _text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _error_message(response: httpx.Response) -> str:
    try:
        body = response.json()
    except json.JSONDecodeError:
        return response.text
    if isinstance(body, dict) and isinstance(body.get("error"), str):
        return str(body["error"])
    return response.text
