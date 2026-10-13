"""Shared fixtures: Postgres, and the signer and runner livepeer-gateway talks to.

The signer and runner are real HTTP servers on ephemeral ports, so the gateway
code under test runs unpatched: a gateway change to a wire format or to what
it reports back fails these tests.
"""

import asyncio
import base64
import json
import os
import socket
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

import asyncpg
import pytest
from aiohttp import web

from livepeer_builder.contracts import UsagePage, UsageRow
from livepeer_builder.store.postgres import PostgresStore

TEST_DATABASE = "livepeer_test"
DEFAULT_DATABASE_URL = f"postgresql://livepeer:livepeer@127.0.0.1:55432/{TEST_DATABASE}"
# Migration failures must fail the test. Only "cannot connect" is allowed to skip.
_CONNECTION_ERRORS = (
    OSError,
    TimeoutError,
    asyncpg.CannotConnectNowError,
    asyncpg.InvalidCatalogNameError,
    asyncpg.InvalidPasswordError,
    asyncpg.PostgresConnectionError,
    asyncpg.TooManyConnectionsError,
)


def _require_test_database(dsn: str) -> None:
    name = urlparse(dsn).path.removeprefix("/").rstrip("/")
    if name != TEST_DATABASE:
        pytest.fail(
            f"Refusing to migrate or truncate database {name!r}; tests only use {TEST_DATABASE!r}"
        )


@pytest.fixture
async def store() -> AsyncIterator[PostgresStore]:
    """A migrated store, emptied afterwards.

    Skips when Postgres is unreachable, unless REQUIRE_POSTGRES is set (CI).
    Connects only to the database named livepeer_test, then truncates that database.
    """
    dsn = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    _require_test_database(dsn)
    try:
        opened = await PostgresStore.connect(dsn)
    except _CONNECTION_ERRORS as exc:
        if os.environ.get("REQUIRE_POSTGRES"):
            pytest.fail(f"Postgres is required but not available: {exc}")
        pytest.skip(f"Postgres is not available: {exc}")
    yield opened
    async with opened._pool.acquire() as conn:
        connected = await conn.fetchval("SELECT current_database()")
        if connected == TEST_DATABASE:
            await conn.execute(
                """
                TRUNCATE lpb_attempts, lpb_jobs, lpb_usage_events, lpb_cost_cursor, lpb_actors, lpb_access_keys
                """
            )
    await opened.close()
    if connected != TEST_DATABASE:
        pytest.fail(
            f"Refusing to truncate database {connected!r}; tests only use {TEST_DATABASE!r}"
        )


@asynccontextmanager
async def serve(app: web.Application) -> AsyncIterator[str]:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


def free_port() -> int:
    """A local port nothing listens on."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass(frozen=True)
class Sign:
    """One ticket the signer issued: what Batteries records as a usage row."""

    allocation_id: str
    auth_id: str
    manifest_id: str
    seq: int


@dataclass
class FakeSigner:
    """The go-livepeer remote signer as livepeer-gateway reads it, plus the Batteries link.

    The bearer key selects an allocation. The first sign of a payment session
    mints an AuthID sealed in the returned state; a sign that sends that state
    back keeps it. Each sign is one applied usage row keyed by
    (allocation, AuthID), which is how Batteries charges in the real stack.
    """

    allocations: Mapping[str, str]  # bearer key -> allocation id
    address: str = "0x00000000000000000000000000000000000000aa"
    payment: str = "payment-b64"
    seg_creds: str = "segment-b64"
    url: str = ""
    # Statuses the next /generate-live-payment requests answer with; None signs.
    fail_with: list[int | None] = field(default_factory=list)
    signs: list[Sign] = field(default_factory=list)
    payment_requests: list[dict[str, Any]] = field(default_factory=list)
    payment_headers: list[dict[str, str]] = field(default_factory=list)
    # What /discover-orchestrators returns: orchestrator entries with their runners.
    discovery: list[dict[str, Any]] = field(default_factory=list)
    discovery_headers: list[dict[str, str]] = field(default_factory=list)

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/sign-orchestrator-info", self._sign_orchestrator_info)
        app.router.add_post("/generate-live-payment", self._generate_live_payment)
        app.router.add_get("/discover-orchestrators", self._discover_orchestrators)
        return app

    async def page(self, cursor: str | None, limit: int = 1000) -> UsagePage:
        """The usage feed: one applied row per sign so far, as Batteries serves it."""
        del cursor, limit
        return UsagePage(items=self.usage_rows(), next_cursor="")

    def usage_rows(self, fee_eth: str = "0.01") -> tuple[UsageRow, ...]:
        return tuple(
            UsageRow(
                id=f"usage-{index}",
                event_id=f"event-{index}",
                status="applied",
                manifest_id=sign.manifest_id,
                allocation_id=sign.allocation_id,
                payment_session_id=sign.auth_id,
                request_id=f"req-{index}",
                pipeline="fixed",
                computed_fee_eth=Decimal(fee_eth),
                computed_fee_usd=None,
                currency="eth",
                created_at=datetime(2026, 10, 7, tzinfo=UTC),
            )
            for index, sign in enumerate(self.signs, start=1)
        )

    async def _discover_orchestrators(self, request: web.Request) -> web.Response:
        self.discovery_headers.append(dict(request.headers))
        return web.json_response(self.discovery)

    async def _sign_orchestrator_info(self, request: web.Request) -> web.Response:
        return web.json_response({"address": self.address, "signature": "0xsignature"})

    async def _generate_live_payment(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.payment_requests.append(body)
        self.payment_headers.append(dict(request.headers))
        failure = self.fail_with.pop(0) if self.fail_with else None
        if failure is not None:
            return web.Response(status=failure, text="refused")
        bearer = request.headers.get("Authorization", "").removeprefix("Bearer ")
        allocation_id = self.allocations.get(bearer)
        if allocation_id is None:
            return web.Response(status=401, text="unknown key")
        sealed = (body.get("state") or {}).get("State")
        if sealed:
            previous = json.loads(base64.standard_b64decode(sealed))
            auth_id, seq = previous["AuthID"], previous["Seq"] + 1
        else:
            auth_id, seq = f"auth-{len({s.auth_id for s in self.signs}) + 1}", 0
        self.signs.append(Sign(allocation_id, auth_id, body["ManifestID"], seq))
        state = base64.standard_b64encode(json.dumps({"AuthID": auth_id, "Seq": seq}).encode()).decode()
        return web.json_response(
            {
                "payment": self.payment,
                "segCreds": self.seg_creds,
                "state": {"State": state, "Sig": "0xstate-signature"},
            }
        )


@dataclass(frozen=True)
class RunnerRequest:
    path: str
    headers: dict[str, str]
    body: bytes


@dataclass
class FakeRunner:
    """A live runner behind an orchestrator: answers each request with the next scripted status.

    402 carries a payment challenge, ``"hang"`` never answers in time, and any
    other status answers with that status. The last entry repeats.
    """

    statuses: list[int | str] = field(default_factory=lambda: [200])
    manifest_id: str = "manifest-1"
    payment_params: str = "orchestrator-params-b64"
    url: str = ""
    requests: list[RunnerRequest] = field(default_factory=list)

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_route("*", "/{path:.*}", self._handle)
        return app

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        self.requests.append(RunnerRequest(request.path, dict(request.headers), await request.read()))
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        if status == "hang":
            await asyncio.sleep(1)
            return web.Response(status=504)
        if status == 402:
            return web.json_response(
                {
                    "payment_params": self.payment_params,
                    "manifest_id": self.manifest_id,
                    "payment_url": f"{self.url}/payment",
                },
                status=402,
            )
        if status == 200:
            return web.json_response({"greeting": "hello", "n": len(self.requests)})
        return web.Response(status=int(status), text=f"runner says {status}")


@pytest.fixture
async def signer() -> AsyncIterator[FakeSigner]:
    fake = FakeSigner(allocations={"key-alice": "alloc-alice", "key-bob": "alloc-bob"})
    async with serve(fake.app()) as url:
        fake.url = url
        yield fake


@pytest.fixture
async def runner() -> AsyncIterator[FakeRunner]:
    fake = FakeRunner()
    async with serve(fake.app()) as url:
        fake.url = url
        yield fake


@pytest.fixture
async def make_runner() -> AsyncIterator[Callable[..., Awaitable[FakeRunner]]]:
    """Start more runners in one test: ``await make_runner(statuses=[402, 200])``."""
    async with AsyncExitStack() as stack:

        async def start(**kwargs: Any) -> FakeRunner:
            fake = FakeRunner(**kwargs)
            fake.url = await stack.enter_async_context(serve(fake.app()))
            return fake

        yield start
