"""Run the REST adapter: ``python -m livepeer_builder.server``.

Settings come from LIVEPEER_* environment variables (see LivepeerSettings).
When ``LIVEPEER_BATTERIES_URL`` or ``LIVEPEER_BATTERIES_TOKEN`` is unset,
``deploy/.env`` fills it from ``BATTERIES_URL`` and ``BATTERIES_TOKEN``.
Signer, discovery, and ``DATABASE_URL`` stay unset unless their ``LIVEPEER_``
variables are set, so a debug session can run on the in-memory store.
HOST and PORT default to 127.0.0.1:8080.
"""

import asyncio
import os
from pathlib import Path

import uvicorn

from livepeer_builder.engine import BuilderEngine, LivepeerSettings
from livepeer_builder.server.app import create_app

_DEPLOY_ENV = {
    "BATTERIES_URL": "LIVEPEER_BATTERIES_URL",
    "BATTERIES_TOKEN": "LIVEPEER_BATTERIES_TOKEN",
}


def _apply_deploy_env() -> None:
    path = Path(__file__).resolve().parents[3] / "deploy" / ".env"
    if not path.is_file():
        return
    found: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        found[key.strip()] = value.strip().strip('"').strip("'")
    for source, dest in _DEPLOY_ENV.items():
        if os.environ.get(dest, "").strip():
            continue
        value = found.get(source, "").strip()
        if value:
            os.environ[dest] = value


async def _serve() -> None:
    _apply_deploy_env()
    engine = await BuilderEngine.from_settings(LivepeerSettings.from_env())
    config = uvicorn.Config(
        create_app(engine),
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8080")),
        log_level=os.environ.get("LOG_LEVEL", "info"),
    )
    await uvicorn.Server(config).serve()


if __name__ == "__main__":
    asyncio.run(_serve())
