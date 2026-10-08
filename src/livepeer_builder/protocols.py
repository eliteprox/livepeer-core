from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from livepeer_builder.contracts import JobRequest, Runner, SyncCheckpoint, UsagePage, UsageRow


class Clock(Protocol):
    def now(self) -> datetime: ...


class DiscoverySource(Protocol):
    async def fetch(self) -> tuple[Runner, ...]: ...


class SelectionPolicy(Protocol):
    def order(self, candidates: Sequence[Runner], request: JobRequest) -> Sequence[Runner]: ...


class UsageSource(Protocol):
    async def page(self, cursor: str | None, limit: int = 1000) -> UsagePage: ...


class CostSyncStore(Protocol):
    async def checkpoint(self) -> SyncCheckpoint | None: ...

    async def apply(self, rows: tuple[UsageRow, ...] | list[UsageRow], checkpoint: SyncCheckpoint) -> int: ...
