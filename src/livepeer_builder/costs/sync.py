from collections.abc import Mapping, Sequence
from decimal import Decimal

from livepeer_builder.contracts import SyncCheckpoint, SyncReport, UsageRow
from livepeer_builder.errors import CursorMismatch
from livepeer_builder.protocols import CostSyncStore, UsageSource


class CostSyncWorker:
    def __init__(
        self,
        source: UsageSource,
        store: CostSyncStore,
        filters: Mapping[str, str] | None = None,
    ) -> None:
        self._source = source
        self._store = store
        self._filters = dict(filters or {})

    async def sync_once(self) -> SyncReport:
        checkpoint = await self._store.checkpoint()
        cursor = None
        if checkpoint is not None and checkpoint.filters == self._filters:
            cursor = checkpoint.resume_cursor
        pages = 0
        rows_seen = 0
        rows_new = 0
        restarted = False
        while True:
            try:
                page = await self._source.page(cursor, limit=1000)
            except CursorMismatch:
                if restarted:
                    raise
                restarted = True
                cursor = None
                await self._store.apply(
                    (),
                    SyncCheckpoint(resume_cursor=None, filters=self._filters),
                )
                continue
            fetched_with = cursor
            resume = page.next_cursor if page.next_cursor else fetched_with
            rows_new += await self._store.apply(
                page.items,
                SyncCheckpoint(resume_cursor=resume, filters=self._filters),
            )
            pages += 1
            rows_seen += len(page.items)
            if page.next_cursor == "":
                return SyncReport(pages=pages, rows_seen=rows_seen, rows_new=rows_new)
            cursor = page.next_cursor


def observed_fee_eth(rows: Sequence[UsageRow]) -> Decimal:
    total = Decimal(0)
    for row in rows:
        if row.status == "applied" and row.computed_fee_eth is not None:
            total += row.computed_fee_eth
    return total
