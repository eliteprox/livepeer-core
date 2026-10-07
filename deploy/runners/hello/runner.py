"""Fixed-price hello runner.

The example app calls register_runner(price_unit=...), which SDK commit 3250c08
does not accept. This runner keeps that app's /hello behavior and registers with
unit="fixed".
"""

import argparse
import logging
from contextlib import suppress

from aiohttp import web
from livepeer_gateway.live_runner import register_runner

APP_ID = "livepeer-example/hello-world"
PORT = 8989


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orchestrator", default="https://localhost:8935")
    parser.add_argument("--orchSecret", default="abcdef")
    parser.add_argument("--runner-url", default=f"http://127.0.0.1:{PORT}")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--price", type=float, default=0.0001)
    return parser.parse_args()


async def _handle_hello(request: web.Request) -> web.Response:
    payload = await request.json()
    name = str(payload.get("name", "world"))
    return web.json_response({"message": f"Hello, {name}!"})


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args()

    async def _on_startup(app: web.Application) -> None:
        app["registration"] = await register_runner(
            args.orchestrator,
            secret=args.orchSecret,
            runner_url=args.runner_url,
            app=APP_ID,
            mode="single-shot",
            price=args.price,
            currency="usd",
            unit="fixed",
            auto_detect_gpu=False,
        )

    async def _on_cleanup(app: web.Application) -> None:
        with suppress(Exception):
            await app["registration"].close()

    app = web.Application()
    app.router.add_post("/hello", _handle_hello)
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    web.run_app(app, host=args.host, port=PORT)


if __name__ == "__main__":
    main()
