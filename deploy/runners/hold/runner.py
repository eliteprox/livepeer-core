"""Persistent runner with a price, so a reserved session mints tickets."""

import asyncio
import os

from livepeer_gateway.live_runner import register_runner


async def main() -> None:
    registration = await register_runner(
        os.environ["ORCH_URL"],
        secret=os.environ["ORCH_SECRET"],
        runner_url=os.environ["RUNNER_URL"],
        app="livepeer-core/hold",
        price="0.10",
        currency="usd",
        unit="hour",
        mode="persistent",
        auto_detect_gpu=False,
    )
    try:
        await asyncio.Event().wait()
    finally:
        await registration.close()


if __name__ == "__main__":
    asyncio.run(main())
