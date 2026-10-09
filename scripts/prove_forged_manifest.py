"""Sign one ticket whose manifest id was chosen by the client, not the signer."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from calls import forged_payment, record
from livepeer_builder.store.postgres import PostgresStore
from settings import database_url


async def main() -> None:
    store = await PostgresStore.connect(database_url())
    try:
        actors = {actor.actor_id: actor for actor in await store.list_actors()}
        auth_id, manifest_id = await forged_payment(actors["alice"])
        job_id = await record(
            store,
            actors["alice"],
            auth_ids=(auth_id,),
            manifest_id=manifest_id,
            payment_sent=False,
            outcome="succeeded",  # sign-only: the ticket was signed, no runner was called
        )
        print(f"forged job {job_id} manifest {manifest_id} auth_id {auth_id}")
    finally:
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
