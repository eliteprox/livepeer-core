from typing import Protocol

from livepeer_builder.contracts import SyncCheckpoint, UsagePage, UsageRow


class UsageSource(Protocol):
    async def page(self, cursor: str | None, limit: int = 1000) -> UsagePage: ...


class CostSyncStore(Protocol):
    async def checkpoint(self) -> SyncCheckpoint | None: ...

    async def apply(self, rows: tuple[UsageRow, ...] | list[UsageRow], checkpoint: SyncCheckpoint) -> int: ...
