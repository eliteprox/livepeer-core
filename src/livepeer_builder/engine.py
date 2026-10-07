import asyncio
import os
from collections.abc import Mapping
from datetime import UTC, datetime

from pydantic import SecretStr

from livepeer_builder.access.service import AccessService
from livepeer_builder.adapters.batteries import BatteriesClient, BatteriesUsageSource
from livepeer_builder.adapters.sdk import SdkRunnerTransport, SignerDiscoverySource
from livepeer_builder.contracts import Contract, EngineHealth
from livepeer_builder.costs.service import CostService
from livepeer_builder.costs.sync import CostSyncWorker
from livepeer_builder.discovery.service import DiscoveryService, MostCapacityFirst
from livepeer_builder.jobs.service import JobService
from livepeer_builder.payments.provider import BatteriesProvider
from livepeer_builder.protocols import (
    Clock,
    CredentialResolver,
    DiscoverySource,
    EngineStore,
    PaymentProvider,
    RunnerTransport,
    SelectionPolicy,
    UsageSource,
)
from livepeer_builder.store.postgres import PostgresStore
from livepeer_builder.testing.fakes import MemoryStore, StaticDiscoverySource

_ENV_PREFIX = "LIVEPEER_"


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class LivepeerSettings(Contract):
    """Read from the environment with the LIVEPEER_ prefix, or built in code."""

    signer_url: str | None = None
    signer_credential: SecretStr | None = None  # default bearer when the actor has no allocation
    discovery_url: str | None = None  # overrides the signer's discovery endpoint
    discovery_interval_s: float = 15.0
    batteries_url: str | None = None  # management API and usage feed
    batteries_token: SecretStr | None = None
    cost_sync_interval_s: float = 30.0
    database_url: str | None = None  # None selects the in-memory store
    admin_token: SecretStr | None = None  # bootstraps AccessService in service mode
    engine_id: str = "livepeer-builder"  # namespaces Batteries idempotency keys

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "LivepeerSettings":
        source = os.environ if env is None else env
        values: dict[str, str] = {}
        for name in cls.model_fields:
            raw = source.get(_ENV_PREFIX + name.upper(), "").strip()
            if raw:
                values[name] = raw
        return cls.model_validate(values)


class BuilderEngine:
    """One facade over the services. Build it with ``BuilderEngine.from_settings``.

    Imported mode: the application builds an ActorContext from its own auth
    and calls ``engine.jobs.run``. Service mode: ``livepeer_builder.server``
    authenticates a bearer key with ``engine.access`` and makes the same calls.
    ``engine.payments`` holds the signer credential; ``engine.access`` does not.
    """

    def __init__(
        self,
        *,
        settings: LivepeerSettings,
        store: EngineStore,
        discovery: DiscoveryService,
        jobs: JobService,
        costs: CostService,
        payments: PaymentProvider,
        access: AccessService,
        closers: tuple[object, ...] = (),
    ) -> None:
        self.settings = settings
        self.store = store
        self.discovery = discovery
        self.jobs = jobs
        self.costs = costs
        self.payments = payments
        self.access = access
        self._closers = closers
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    @classmethod
    async def from_settings(
        cls,
        settings: LivepeerSettings,
        *,
        store: EngineStore | None = None,
        discovery_source: DiscoverySource | None = None,
        transport: RunnerTransport | None = None,
        usage_source: UsageSource | None = None,
        batteries: BatteriesClient | None = None,
        credentials: CredentialResolver | None = None,
        selection: SelectionPolicy | None = None,
        clock: Clock | None = None,
    ) -> "BuilderEngine":
        clock = clock or SystemClock()
        closers: list[object] = []
        if store is None:
            store = await _default_store(settings.database_url)
            closers.append(store)
        token = settings.batteries_token.get_secret_value() if settings.batteries_token else None
        if batteries is None and settings.batteries_url and token:
            batteries = BatteriesClient(settings.batteries_url, token, engine_id=settings.engine_id)
            closers.append(batteries)
        if usage_source is None and settings.batteries_url and token:
            usage_source = BatteriesUsageSource(settings.batteries_url, token)
            closers.append(usage_source)
        if discovery_source is None:
            discovery_source = _default_discovery_source(settings)
        if transport is None:
            transport = SdkRunnerTransport(settings.signer_url)

        discovery = DiscoveryService(discovery_source, clock, settings.discovery_interval_s)
        default_credential = settings.signer_credential.get_secret_value() if settings.signer_credential else None
        payments = BatteriesProvider(store, batteries, default_credential=default_credential)
        jobs = JobService(
            store,
            discovery,
            transport,
            selection or MostCapacityFirst(),
            credentials or payments,
            clock,
        )
        worker = CostSyncWorker(usage_source, store) if usage_source is not None else None
        costs = CostService(store, worker, clock, settings.cost_sync_interval_s)
        admin = settings.admin_token.get_secret_value() if settings.admin_token else None
        access = AccessService(store, clock, admin)
        return cls(
            settings=settings,
            store=store,
            discovery=discovery,
            jobs=jobs,
            costs=costs,
            payments=payments,
            access=access,
            closers=tuple(closers),
        )

    async def start(self) -> None:
        """One discovery poll now, then the discovery poller and the cost sync loop."""
        await self.discovery.refresh()
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self.discovery.run(self._stop)),
            asyncio.create_task(self.costs.run(self._stop)),
        ]

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks = []
        for closer in self._closers:
            close = getattr(closer, "aclose", None) or getattr(closer, "close", None)
            if close is not None:
                await close()

    def health(self) -> EngineHealth:
        snapshot = self.discovery.snapshot
        return EngineHealth(
            discovery_fresh=self.discovery.is_fresh(),
            discovery_observed_at=snapshot.observed_at,
            discovery_error=snapshot.last_error,
            cost_sync_at=self.costs.last_synced_at,
            cost_sync_error=self.costs.last_error,
        )


async def _default_store(database_url: str | None) -> EngineStore:
    if database_url is None:
        return MemoryStore()
    return await PostgresStore.connect(database_url)


def _default_discovery_source(settings: LivepeerSettings) -> DiscoverySource:
    if settings.signer_url is None and settings.discovery_url is None:
        return StaticDiscoverySource([])
    return SignerDiscoverySource(signer_url=settings.signer_url, discovery_url=settings.discovery_url)
