"""Write Batteries credentials and the matching signer webhook token.

Re-running keeps the existing files so a signer and a database stay paired.
"""

import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CREDS = ROOT / "creds.toml"
ENV = ROOT / ".env"


def main() -> None:
    if CREDS.exists():
        print("creds.toml already exists")
        return
    operator = secrets.token_hex(32)
    webhook = secrets.token_hex(32)
    CREDS.write_text(
        "\n".join(
            [
                "[[credentials]]",
                'id = "operator"',
                f'secret = "{operator}"',
                "[credentials.management]",
                'allow = ["grants.*", "allocations.*", "api_keys.*", "sessions.*", "usage.read", "ledger.read"]',
                "",
                "[[credentials]]",
                'id = "signer"',
                f'secret = "{webhook}"',
                "[credentials.webhook]",
                "authorize = true",
                "",
            ]
        )
    )
    CREDS.chmod(0o600)
    existing = ENV.read_text() if ENV.exists() else ""
    with ENV.open("a") as handle:
        if "BATTERIES_TOKEN=" not in existing:
            handle.write(f"BATTERIES_TOKEN={operator}\n")
        if "SIGNER_WEBHOOK_TOKEN=" not in existing:
            handle.write(f"SIGNER_WEBHOOK_TOKEN={webhook}\n")
    print("wrote creds.toml")


if __name__ == "__main__":
    main()
