"""Paid calls recorded the way an upstream SDK can: manifest id only, no auth id."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from calls import hello_call, record
from livepeer_builder.store.postgres import PostgresStore
from settings import database_url

CALLS = {"alice": 2, "bob": 1}


async def main() -> None:
    store = await PostgresStore.connect(database_url())
    try:
        actors = {actor.actor_id: actor for actor in await store.list_actors()}
        for actor_id, count in CALLS.items():
            actor = actors[actor_id]
            for index in range(count):
                result = await hello_call(actor)
                job_id = await record(
                    store,
                    actor,
                    auth_ids=(),
                    manifest_id=result.manifest_id,
                    payment_sent=result.payment_sent,
                    outcome=result.outcome,
                )
                print(
                    f"{actor_id} call {index + 1} job {job_id} manifest {result.manifest_id} "
                    f"outcome {result.outcome} ignored_auth_ids {len(result.auth_ids)}"
                )
    finally:
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
