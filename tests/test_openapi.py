"""The review spec lists every application route and no invented ones."""

import re
from pathlib import Path

from fastapi.routing import APIRoute

from livepeer_builder.engine import BuilderEngine, LivepeerSettings
from livepeer_builder.server.app import create_app

_SPEC = Path(__file__).parents[1] / "docs" / "openapi.yaml"
_FRAMEWORK = {"/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"}


def _spec_paths() -> set[str]:
    paths: set[str] = set()
    in_paths = False
    for line in _SPEC.read_text().splitlines():
        if line == "paths:":
            in_paths = True
            continue
        if in_paths and line.startswith("components:"):
            break
        match = re.fullmatch(r"  (/[^:]+):", line)
        if in_paths and match:
            paths.add(match.group(1))
    return paths


async def test_openapi_paths_match_the_application() -> None:
    engine = await BuilderEngine.from_settings(LivepeerSettings())
    app = create_app(engine, manage_lifecycle=False)
    served = {route.path for route in app.routes if isinstance(route, APIRoute)} - _FRAMEWORK
    assert _spec_paths() == served
