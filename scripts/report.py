import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from livepeer_builder.adapters.batteries import BatteriesClient, BatteriesUsageSource
from livepeer_builder.costs.sync import CostSyncWorker
from livepeer_builder.store.postgres import PostgresStore
from settings import batteries_url, database_url, required


async def main() -> None:
    store = await PostgresStore.connect(database_url())
    source = BatteriesUsageSource(batteries_url(), required("BATTERIES_TOKEN"))
    client = BatteriesClient(batteries_url(), required("BATTERIES_TOKEN"))
    try:
        report = await CostSyncWorker(source, store, {}).sync_once()
        print(
            f"sync pages={report.pages} rows_seen={report.rows_seen} rows_new={report.rows_new}"
        )
        actors = await store.list_actors()
        seen_allocations: set[str] = set()
        for actor in actors:
            spend = await store.actor_spend(actor.actor_id)
            print(
                f"actor {actor.actor_id} allocation {spend.allocation_id} "
                f"applied_events {spend.event_count} fee_eth {spend.fee_eth}"
            )
            if spend.allocation_id in seen_allocations:
                continue
            seen_allocations.add(spend.allocation_id)
            remote = await client.allocation(spend.allocation_id)
            spent = remote.get("spent_eth", remote.get("spent_usd"))
            print(f"  batteries spent {spent} status {remote.get('status')}")
        manifests: set[str] = set()
        for actor_id, attempt in await store.list_attempts():
            cost = await store.job_cost(attempt.job_id)
            print(
                f"job {attempt.job_id} actor {actor_id} outcome {attempt.outcome} "
                f"auth_ids {list(attempt.auth_ids)} manifest {attempt.manifest_id} "
                f"cost {cost.status} events {cost.event_count} fee_eth {cost.fee_eth}"
            )
            if attempt.manifest_id:
                manifests.add(attempt.manifest_id)
        for manifest_id in sorted(manifests):
            cost = await store.manifest_cost(manifest_id)
            print(
                f"manifest {manifest_id} events {cost.event_count} "
                f"fee_eth {cost.fee_eth} allocations {list(cost.allocation_ids)}"
            )
        print("usage rows:")
        for row in await store.list_usage():
            print(
                f"  {row.status} allocation {row.allocation_id} session {row.payment_session_id} "
                f"manifest {row.manifest_id} fee_eth {row.computed_fee_eth}"
            )
    finally:
        await source.aclose()
        await client.aclose()
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
