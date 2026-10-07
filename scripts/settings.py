import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_env() -> None:
    path = ROOT / "deploy" / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value)


def required(name: str) -> str:
    load_env()
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"missing {name}")
    return value


def database_url() -> str:
    load_env()
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://livepeer:livepeer@127.0.0.1:55434/livepeer",
    )


def batteries_url() -> str:
    load_env()
    return os.environ.get("BATTERIES_URL", "http://127.0.0.1:18081").rstrip("/")


def signer_url() -> str:
    load_env()
    return os.environ.get("SIGNER_URL", "http://127.0.0.1:7936").rstrip("/")


def discovery_url() -> str:
    load_env()
    return os.environ.get("DISCOVERY_URL", "https://127.0.0.1:8935/discovery")
