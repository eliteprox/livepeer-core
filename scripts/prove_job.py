"""Paid calls recorded with the signer auth ids from SDK commit 3250c08.

The reserved-session case signs twice on one payment session, so several usage
rows share one auth id.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from calls import HELLO_APP, hello_call, record, sign_twice
from livepeer_core.store.postgres import PostgresStore
from settings import database_url


async def main() -> None:
    store = await PostgresStore.connect(database_url())
    try:
        actors = {actor.actor_id: actor for actor in await store.list_actors()}
        for actor_id in ("alice", "bob", "shared"):
            actor = actors[actor_id]
            result = await hello_call(actor)
            job_id = await record(
                store,
                actor,
                auth_ids=result.auth_ids,
                manifest_id=result.manifest_id,
                payment_sent=result.payment_sent,
                outcome=result.outcome,
            )
            print(f"{actor_id} job {job_id} auth_ids {list(result.auth_ids)} outcome {result.outcome}")
        auth_id = await sign_twice(actors["shared"], HELLO_APP, "fixed")
        job_id = await record(
            store,
            actors["shared"],
            auth_ids=(auth_id,),
            manifest_id=None,
            payment_sent=True,
            outcome="reserved",
        )
        print(f"shared reservation job {job_id} auth_id {auth_id}")
    finally:
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
