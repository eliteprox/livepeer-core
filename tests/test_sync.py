from datetime import UTC, datetime
from decimal import Decimal

import pytest

from livepeer_core.contracts import SyncCheckpoint, UsagePage, UsageRow
from livepeer_core.costs.sync import CostSyncWorker
from livepeer_core.errors import CursorMismatch
from livepeer_core.testing.fakes import MemoryCostSyncStore, ScriptedUsageSource


def _row(
    row_id: str,
    *,
    status: str = "applied",
    manifest_id: str | None = "manifest-1",
    allocation_id: str | None = "alloc-1",
    fee: str | None = "0.01",
) -> UsageRow:
    return UsageRow(
        id=row_id,
        event_id=f"evt-{row_id}",
        status=status,  # type: ignore[arg-type]
        manifest_id=manifest_id,
        allocation_id=allocation_id,
        payment_session_id="sess-1" if allocation_id else None,
        request_id="req-1",
        pipeline="fixed",
        computed_fee_eth=Decimal(fee) if fee is not None else None,
        computed_fee_usd=None,
        currency="eth" if allocation_id else None,
        created_at=datetime(2026, 10, 7, tzinfo=UTC),
    )


def test_observed_fee_ignores_quarantined_rows() -> None:
    store = MemoryCostSyncStore()
    applied = _row("applied")
    quarantined = _row(
        "quarantined",
        status="quarantined",
        manifest_id=None,
        allocation_id=None,
        fee=None,
    )

    async def _run() -> None:
        await store.apply((applied, quarantined), SyncCheckpoint(resume_cursor=None, filters={}))

    import asyncio

    asyncio.run(_run())
    assert quarantined.id in store.rows
    assert store.observed_fee_eth() == Decimal("0.01")


@pytest.mark.asyncio
async def test_reread_of_last_page_does_not_count_duplicates() -> None:
    row = _row("row-1")
    page = UsagePage(items=(row,), next_cursor="")
    source = ScriptedUsageSource([page, page])
    store = MemoryCostSyncStore()
    worker = CostSyncWorker(source, store, {})

    first = await worker.sync_once()
    second = await worker.sync_once()

    assert first.rows_new == 1
    assert second.rows_new == 0
    assert second.rows_seen == 1
    assert store.checkpoint_value is not None
    assert store.checkpoint_value.resume_cursor is None


@pytest.mark.asyncio
async def test_reread_picks_up_a_row_that_arrives_on_the_last_page() -> None:
    first_row = _row("row-1")
    second_row = _row("row-2")
    source = ScriptedUsageSource(
        [
            UsagePage(items=(first_row,), next_cursor="cursor-1"),
            UsagePage(items=(first_row,), next_cursor=""),
            UsagePage(items=(first_row, second_row), next_cursor=""),
        ]
    )
    store = MemoryCostSyncStore()
    worker = CostSyncWorker(source, store, {})

    forward = await worker.sync_once()
    arrived = await worker.sync_once()

    assert forward.rows_new == 1
    assert store.checkpoint_value is not None
    assert store.checkpoint_value.resume_cursor == "cursor-1"
    assert source.calls[2] == "cursor-1"
    assert arrived.rows_new == 1
    assert set(store.rows) == {"row-1", "row-2"}


@pytest.mark.asyncio
async def test_quarantined_row_is_stored_and_excluded_from_observed_cost() -> None:
    row = _row(
        "quarantined",
        status="quarantined",
        manifest_id=None,
        allocation_id=None,
        fee=None,
    )
    source = ScriptedUsageSource([UsagePage(items=(row,), next_cursor="")])
    store = MemoryCostSyncStore()
    worker = CostSyncWorker(source, store, {})

    report = await worker.sync_once()

    assert report.rows_new == 1
    assert store.rows["quarantined"].allocation_id is None
    assert store.observed_fee_eth() == Decimal(0)


class _MismatchThenPage:
    def __init__(self, ready: UsagePage) -> None:
        self.ready = ready
        self.calls: list[str | None] = []

    async def page(self, cursor: str | None, limit: int = 1000) -> UsagePage:
        del limit
        self.calls.append(cursor)
        if cursor == "stale":
            raise CursorMismatch("cursor does not match resource or filters")
        return self.ready


@pytest.mark.asyncio
async def test_cursor_mismatch_clears_checkpoint_and_syncs_from_the_start() -> None:
    row = _row("row-1")
    source = _MismatchThenPage(UsagePage(items=(row,), next_cursor=""))
    store = MemoryCostSyncStore()
    store.checkpoint_value = SyncCheckpoint(resume_cursor="stale", filters={})
    worker = CostSyncWorker(source, store, {})

    report = await worker.sync_once()

    assert source.calls == ["stale", None]
    assert report.rows_new == 1
    assert store.checkpoint_value is not None
    assert store.checkpoint_value.resume_cursor is None


@pytest.mark.asyncio
async def test_crash_before_apply_retries_the_same_page_without_new_rows() -> None:
    row = _row("row-1")
    page = UsagePage(items=(row,), next_cursor="")
    source = ScriptedUsageSource([page, page, page])
    store = MemoryCostSyncStore()
    worker = CostSyncWorker(source, store, {})

    await worker.sync_once()
    store.fail_next_apply = True
    with pytest.raises(RuntimeError, match="crash"):
        await worker.sync_once()
    assert store.checkpoint_value is not None
    assert store.checkpoint_value.resume_cursor is None

    report = await worker.sync_once()

    assert report.rows_new == 0
    assert len(store.rows) == 1
