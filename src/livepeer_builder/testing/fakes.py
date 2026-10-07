from decimal import Decimal

from livepeer_builder.contracts import SyncCheckpoint, UsagePage, UsageRow
from livepeer_builder.costs.sync import observed_fee_eth


class ScriptedUsageSource:
    def __init__(
        self,
        pages: list[UsagePage],
    ) -> None:
        self.pages = list(pages)
        self.calls: list[str | None] = []
        self._index = 0

    async def page(self, cursor: str | None, limit: int = 1000) -> UsagePage:
        del limit
        self.calls.append(cursor)
        page = self.pages[self._index]
        self._index += 1
        return page


class MemoryCostSyncStore:
    def __init__(self) -> None:
        self.rows: dict[str, UsageRow] = {}
        self.checkpoint_value: SyncCheckpoint | None = None
        self.fail_next_apply = False

    async def checkpoint(self) -> SyncCheckpoint | None:
        return self.checkpoint_value

    async def apply(
        self,
        rows: tuple[UsageRow, ...] | list[UsageRow],
        checkpoint: SyncCheckpoint,
    ) -> int:
        if self.fail_next_apply:
            self.fail_next_apply = False
            raise RuntimeError("crash before commit")
        new = 0
        for row in rows:
            if row.id not in self.rows:
                new += 1
            self.rows[row.id] = row
        self.checkpoint_value = checkpoint
        return new

    def observed_fee_eth(self) -> Decimal:
        return observed_fee_eth(tuple(self.rows.values()))
