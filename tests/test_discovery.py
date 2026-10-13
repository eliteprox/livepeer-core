"""Offerings and runner order, with no job dispatch."""

from decimal import Decimal

from livepeer_builder.contracts import JobRequest, NetworkRate, Runner
from livepeer_builder.discovery.service import DiscoveryService, MostCapacityFirst
from livepeer_builder.testing.fakes import FixedClock, StaticDiscoverySource

APP = "livepeer-example/hello-world"


def _rate(amount: str, *, unit: str = "fixed") -> NetworkRate:
    return NetworkRate(amount=Decimal(amount), currency="usd", unit=unit)


def _runner(
    url: str,
    *,
    app: str = APP,
    capacity_available: int | None = 1,
    rate: NetworkRate | None = None,
) -> Runner:
    return Runner(
        url=url,
        app=app,
        runner_id=None,
        mode="single-shot",
        orchestrator_url=None,
        capacity=None,
        capacity_available=capacity_available,
        rate=rate,
    )


async def test_offerings_group_apps_and_span_the_common_rate() -> None:
    clock = FixedClock()
    source = StaticDiscoverySource(
        [
            _runner("http://a", capacity_available=2, rate=_rate("0.0002")),
            _runner("http://b", capacity_available=0, rate=_rate("0.0001")),
            _runner("http://c", capacity_available=None, rate=_rate("9", unit="second")),
            _runner("http://other", app="other/app", capacity_available=1, rate=_rate("1")),
        ]
    )
    discovery = DiscoveryService(source, clock)
    assert discovery.offerings() == []

    await discovery.refresh()
    offerings = discovery.offerings()
    assert [item.app for item in offerings] == [APP, "other/app"]
    hello = offerings[0]
    assert hello.runner_count == 3
    assert hello.ready_count == 2
    assert hello.rate_low is not None and hello.rate_low.amount == Decimal("0.0001")
    assert hello.rate_high is not None and hello.rate_high.amount == Decimal("0.0002")
    assert hello.observed_at == clock.now()
    assert discovery.offerings(app="other/app")[0].runner_count == 1
    assert discovery.runners(APP)[0].url == "http://a"


def test_most_capacity_first_puts_free_capacity_ahead_of_a_full_runner() -> None:
    policy = MostCapacityFirst()
    ordered = policy.order(
        [
            _runner("http://full", capacity_available=0),
            _runner("http://busy", capacity_available=5),
            _runner("http://unknown", capacity_available=None),
        ],
        JobRequest(app=APP),
    )
    assert [runner.url for runner in ordered] == ["http://busy", "http://unknown", "http://full"]


async def test_a_failed_refresh_keeps_the_last_runners() -> None:
    clock = FixedClock()
    source = StaticDiscoverySource([_runner("http://a")])
    discovery = DiscoveryService(source, clock, interval_s=15)
    await discovery.refresh()
    source.error = RuntimeError("down")

    snapshot = await discovery.refresh()
    assert [runner.url for runner in snapshot.runners] == ["http://a"]
    assert snapshot.last_error == "down"
    assert snapshot.observed_at == clock.now()
    assert discovery.is_fresh()

    clock.advance(46)
    assert discovery.is_fresh() is False
