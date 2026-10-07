import asyncio
import logging
from collections.abc import Sequence
from datetime import timedelta

from livepeer_builder.contracts import DiscoverySnapshot, JobRequest, NetworkRate, Offering, Runner
from livepeer_builder.protocols import Clock, DiscoverySource

_LOG = logging.getLogger(__name__)


class MostCapacityFirst:
    """Ready runners first, most free capacity first; unknown capacity counts as ready."""

    def order(self, candidates: Sequence[Runner], request: JobRequest) -> Sequence[Runner]:
        del request
        return sorted(candidates, key=_free_capacity, reverse=True)


def _free_capacity(runner: Runner) -> int:
    if runner.capacity_available is None:
        return 1
    return runner.capacity_available


def _is_ready(runner: Runner) -> bool:
    return runner.capacity_available is None or runner.capacity_available > 0


class DiscoveryService:
    def __init__(
        self,
        source: DiscoverySource,
        clock: Clock,
        interval_s: float = 15.0,
    ) -> None:
        self._source = source
        self._clock = clock
        self._interval_s = interval_s
        self._snapshot = DiscoverySnapshot(
            runners=(),
            observed_at=None,
            last_attempt_at=None,
            last_error=None,
        )

    @property
    def snapshot(self) -> DiscoverySnapshot:
        return self._snapshot

    def is_fresh(self) -> bool:
        observed = self._snapshot.observed_at
        if observed is None:
            return False
        return self._clock.now() - observed <= timedelta(seconds=self._interval_s * 3)

    async def refresh(self) -> DiscoverySnapshot:
        attempted = self._clock.now()
        try:
            runners = await self._source.fetch()
        except Exception as exc:
            _LOG.warning("discovery failed: %s", exc)
            self._snapshot = DiscoverySnapshot(
                runners=self._snapshot.runners,
                observed_at=self._snapshot.observed_at,
                last_attempt_at=attempted,
                last_error=str(exc),
            )
            return self._snapshot
        self._snapshot = DiscoverySnapshot(
            runners=runners,
            observed_at=attempted,
            last_attempt_at=attempted,
            last_error=None,
        )
        return self._snapshot

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.refresh()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval_s)
            except TimeoutError:
                continue

    def runners(self, app: str) -> list[Runner]:
        return [runner for runner in self._snapshot.runners if runner.app == app]

    def offerings(self, *, app: str | None = None) -> list[Offering]:
        observed = self._snapshot.observed_at
        if observed is None:
            return []
        by_app: dict[str, list[Runner]] = {}
        for runner in self._snapshot.runners:
            if app is not None and runner.app != app:
                continue
            by_app.setdefault(runner.app, []).append(runner)
        offerings = []
        for name in sorted(by_app):
            group = by_app[name]
            rates = sorted((r.rate for r in group if r.rate is not None), key=_rate_key)
            offerings.append(
                Offering(
                    app=name,
                    rate_low=rates[0] if rates else None,
                    rate_high=rates[-1] if rates else None,
                    runner_count=len(group),
                    ready_count=sum(1 for r in group if _is_ready(r)),
                    observed_at=observed,
                )
            )
        return offerings


def _rate_key(rate: NetworkRate) -> tuple[str, str, object]:
    return (rate.currency, rate.unit, rate.amount)
