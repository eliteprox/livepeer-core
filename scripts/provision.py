import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from livepeer_builder.adapters.batteries import BatteriesClient
from livepeer_builder.contracts import ProvisionedActor
from livepeer_builder.store.postgres import PostgresStore
from settings import batteries_url, database_url, load_env, required

ACTORS = ("alice", "bob", "shared")
GRANT_ETH = "0.01"
ALLOCATION_ETH = "0.002"


async def main() -> None:
    load_env()
    token = required("BATTERIES_TOKEN")
    client = BatteriesClient(batteries_url(), token)
    store = await PostgresStore.connect(database_url())
    try:
        existing = {actor.actor_id for actor in await store.list_actors()}
        grant_id = os.environ.get("GRANT_ID", "").strip()
        if not grant_id:
            grant_id = await client.create_grant("livepeer-core", GRANT_ETH)
            _remember_grant(grant_id)
            print(f"grant {grant_id}")
        for actor_id in ACTORS:
            if actor_id in existing:
                print(f"actor {actor_id} already stored")
                continue
            allocation_id = await client.create_allocation(
                grant_id,
                actor_id,
                ALLOCATION_ETH,
                actor_id,
            )
            key = await client.create_api_key(allocation_id, actor_id, actor_id)
            await store.record_actor(
                ProvisionedActor(
                    actor_id=actor_id,
                    grant_id=grant_id,
                    allocation_id=key["allocation_id"],
                    api_key_ref=key["api_key"],
                )
            )
            print(f"actor {actor_id} allocation {key['allocation_id']} key {key['id']}")
    finally:
        await client.aclose()
        await store.close()


def _remember_grant(grant_id: str) -> None:
    path = Path(__file__).resolve().parents[1] / "deploy" / ".env"
    text = path.read_text() if path.exists() else ""
    if "GRANT_ID=" in text:
        return
    with path.open("a") as handle:
        handle.write(f"GRANT_ID={grant_id}\n")


if __name__ == "__main__":
    asyncio.run(main())
