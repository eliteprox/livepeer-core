"""Run the REST adapter: ``python -m livepeer_builder.server``.

Settings come from LIVEPEER_* environment variables (see LivepeerSettings).
HOST and PORT default to 127.0.0.1:8080.
"""

import asyncio
import os

import uvicorn

from livepeer_builder.engine import BuilderEngine, LivepeerSettings
from livepeer_builder.server.app import create_app


async def _serve() -> None:
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
