import json
import os
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import asyncpg

from livepeer_core.contracts import (
    ActorSpend,
    Attempt,
    JobCost,
    ManifestCost,
    ProvisionedActor,
    SyncCheckpoint,
    UsageRow,
)

_MIGRATIONS = Path(__file__).resolve().parents[3] / "migrations"


class PostgresStore:
    def __init__(
        self,
        pool: asyncpg.Pool,
    ) -> None:
        self._pool = pool

    @classmethod
    async def connect(cls, dsn: str) -> "PostgresStore":
        pool = await asyncpg.create_pool(dsn)
        store = cls(pool)
        await store.migrate()
        return store

    async def close(self) -> None:
        await self._pool.close()

    async def migrate(self) -> None:
        directory = Path(os.environ.get("LIVEPEER_MIGRATIONS", _MIGRATIONS))
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lpb_schema_migrations (
                  version text PRIMARY KEY,
                  applied_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            for path in sorted(directory.glob("*.sql")):
                applied = await conn.fetchval(
                    "SELECT 1 FROM lpb_schema_migrations WHERE version = $1",
                    path.name,
                )
                if applied:
                    continue
                async with conn.transaction():
                    await conn.execute(path.read_text())
                    await conn.execute(
                        "INSERT INTO lpb_schema_migrations (version) VALUES ($1)",
                        path.name,
                    )

    async def checkpoint(self) -> SyncCheckpoint | None:
        row = await self._pool.fetchrow(
            "SELECT resume_cursor, filters FROM lpb_cost_cursor WHERE id = 1"
        )
        if row is None:
            return None
        return SyncCheckpoint(
            resume_cursor=row["resume_cursor"],
            filters=json.loads(row["filters"]),
        )

    async def apply(
        self,
        rows: tuple[UsageRow, ...] | list[UsageRow],
        checkpoint: SyncCheckpoint,
    ) -> int:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                inserted = await _insert_usage(conn, tuple(rows))
                await conn.execute(
                    """
                    INSERT INTO lpb_cost_cursor (id, resume_cursor, filters)
                    VALUES (1, $1, $2)
                    ON CONFLICT (id) DO UPDATE
                      SET resume_cursor = EXCLUDED.resume_cursor,
                          filters = EXCLUDED.filters
                    """,
                    checkpoint.resume_cursor,
                    json.dumps(checkpoint.filters),
                )
                return inserted

    async def record_actor(self, actor: ProvisionedActor) -> None:
        await self._pool.execute(
            """
            INSERT INTO lpb_actors (actor_id, grant_id, allocation_id, api_key_ref)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (actor_id) DO UPDATE
              SET grant_id = EXCLUDED.grant_id,
                  allocation_id = EXCLUDED.allocation_id,
                  api_key_ref = EXCLUDED.api_key_ref
            """,
            actor.actor_id,
            actor.grant_id,
            actor.allocation_id,
            actor.api_key_ref,
        )

    async def list_actors(self) -> list[ProvisionedActor]:
        rows = await self._pool.fetch(
            """
            SELECT actor_id, grant_id, allocation_id, api_key_ref
            FROM lpb_actors
            ORDER BY actor_id
            """
        )
        return [
            ProvisionedActor(
                actor_id=row["actor_id"],
                grant_id=row["grant_id"],
                allocation_id=row["allocation_id"],
                api_key_ref=row["api_key_ref"],
            )
            for row in rows
        ]

    async def record_attempt(self, actor_id: str, attempt: Attempt) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO lpb_jobs (id, actor_id)
                    VALUES ($1, $2)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    attempt.job_id,
                    actor_id,
                )
                await conn.execute(
                    """
                    INSERT INTO lpb_attempts (
                      job_id, number, auth_ids, manifest_id, payment_sent, outcome
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (job_id, number) DO UPDATE
                      SET auth_ids = EXCLUDED.auth_ids,
                          manifest_id = EXCLUDED.manifest_id,
                          payment_sent = EXCLUDED.payment_sent,
                          outcome = EXCLUDED.outcome
                    """,
                    attempt.job_id,
                    attempt.number,
                    list(attempt.auth_ids),
                    attempt.manifest_id,
                    attempt.payment_sent,
                    attempt.outcome,
                )

    async def list_usage(self) -> list[UsageRow]:
        rows = await self._pool.fetch(
            """
            SELECT id, event_id, status, manifest_id, allocation_id, payment_session_id,
                   request_id, pipeline, computed_fee_eth, computed_fee_usd, currency, created_at
            FROM lpb_usage_events
            ORDER BY created_at, id
            """
        )
        return [
            UsageRow(
                id=row["id"],
                event_id=row["event_id"],
                status=row["status"],
                manifest_id=row["manifest_id"],
                allocation_id=row["allocation_id"],
                payment_session_id=row["payment_session_id"],
                request_id=row["request_id"],
                pipeline=row["pipeline"],
                computed_fee_eth=Decimal(row["computed_fee_eth"]) if row["computed_fee_eth"] is not None else None,
                computed_fee_usd=Decimal(row["computed_fee_usd"]) if row["computed_fee_usd"] is not None else None,
                currency=row["currency"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    async def list_attempts(self) -> list[tuple[str, Attempt]]:
        rows = await self._pool.fetch(
            """
            SELECT j.actor_id, t.job_id, t.number, t.auth_ids, t.manifest_id,
                   t.payment_sent, t.outcome
            FROM lpb_attempts t
            JOIN lpb_jobs j ON j.id = t.job_id
            ORDER BY j.created_at, t.number
            """
        )
        return [
            (
                row["actor_id"],
                Attempt(
                    job_id=row["job_id"],
                    number=row["number"],
                    auth_ids=tuple(row["auth_ids"] or ()),
                    manifest_id=row["manifest_id"],
                    payment_sent=row["payment_sent"],
                    outcome=row["outcome"],
                ),
            )
            for row in rows
        ]

    async def actor_spend(self, actor_id: str) -> ActorSpend:
        row = await self._pool.fetchrow(
            """
            SELECT a.actor_id,
                   a.allocation_id,
                   coalesce(sum(u.computed_fee_eth) FILTER (WHERE u.status = 'applied'), 0) AS fee_eth,
                   count(u.computed_fee_usd) FILTER (
                     WHERE u.status = 'applied' AND u.computed_fee_usd IS NOT NULL
                   ) AS usd_count,
                   coalesce(sum(u.computed_fee_usd) FILTER (WHERE u.status = 'applied'), 0) AS fee_usd,
                   count(u.id) FILTER (WHERE u.status = 'applied') AS event_count
            FROM lpb_actors a
            LEFT JOIN lpb_usage_events u ON u.allocation_id = a.allocation_id
            WHERE a.actor_id = $1
            GROUP BY a.actor_id, a.allocation_id
            """,
            actor_id,
        )
        if row is None:
            raise KeyError(actor_id)
        event_count = int(row["event_count"])
        fee_usd = Decimal(row["fee_usd"]) if event_count and int(row["usd_count"]) == event_count else None
        return ActorSpend(
            actor_id=row["actor_id"],
            allocation_id=row["allocation_id"],
            fee_eth=Decimal(row["fee_eth"]),
            fee_usd=fee_usd,
            event_count=event_count,
        )

    async def job_cost(self, job_id: UUID) -> JobCost:
        attempts = await self._pool.fetchval(
            "SELECT count(*) FROM lpb_attempts WHERE job_id = $1",
            job_id,
        )
        rows = await self._pool.fetch(
            """
            SELECT DISTINCT u.id, u.computed_fee_eth, u.computed_fee_usd
            FROM lpb_jobs j
            JOIN lpb_actors a ON a.actor_id = j.actor_id
            JOIN lpb_attempts t ON t.job_id = j.id
            JOIN lpb_usage_events u
              ON u.payment_session_id = ANY(t.auth_ids)
             AND u.allocation_id = a.allocation_id
             AND u.status = 'applied'
            WHERE j.id = $1
            """,
            job_id,
        )
        if not rows:
            return JobCost(
                job_id=job_id,
                status="pending" if attempts else "none",
                fee_eth=None,
                fee_usd=None,
                event_count=0,
            )
        fee_eth = sum((Decimal(row["computed_fee_eth"]) for row in rows if row["computed_fee_eth"] is not None), Decimal(0))
        priced = [row for row in rows if row["computed_fee_usd"] is not None]
        fee_usd = sum((Decimal(row["computed_fee_usd"]) for row in priced), Decimal(0)) if len(priced) == len(rows) else None
        return JobCost(
            job_id=job_id,
            status="observed",
            fee_eth=fee_eth,
            fee_usd=fee_usd,
            event_count=len(rows),
        )

    async def manifest_cost(self, manifest_id: str) -> ManifestCost:
        rows = await self._pool.fetch(
            """
            SELECT id, allocation_id, computed_fee_eth, computed_fee_usd
            FROM lpb_usage_events
            WHERE manifest_id = $1 AND status = 'applied'
            """,
            manifest_id,
        )
        fee_eth = sum((Decimal(row["computed_fee_eth"]) for row in rows if row["computed_fee_eth"] is not None), Decimal(0))
        priced = [row for row in rows if row["computed_fee_usd"] is not None]
        fee_usd = sum((Decimal(row["computed_fee_usd"]) for row in priced), Decimal(0)) if rows and len(priced) == len(rows) else None
        allocations = tuple(sorted({row["allocation_id"] for row in rows if row["allocation_id"]}))
        return ManifestCost(
            manifest_id=manifest_id,
            fee_eth=fee_eth,
            fee_usd=fee_usd,
            event_count=len(rows),
            allocation_ids=allocations,
        )


async def _insert_usage(conn: asyncpg.Connection, rows: tuple[UsageRow, ...]) -> int:
    if not rows:
        return 0
    inserted = await conn.fetch(
        """
        INSERT INTO lpb_usage_events (
          id, event_id, status, manifest_id, allocation_id, payment_session_id,
          request_id, pipeline, computed_fee_eth, computed_fee_usd, currency, created_at
        )
        SELECT * FROM unnest(
          $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
          $7::text[], $8::text[], $9::numeric[], $10::numeric[], $11::text[], $12::timestamptz[]
        )
        ON CONFLICT (id) DO NOTHING
        RETURNING id
        """,
        [row.id for row in rows],
        [row.event_id for row in rows],
        [row.status for row in rows],
        [row.manifest_id for row in rows],
        [row.allocation_id for row in rows],
        [row.payment_session_id for row in rows],
        [row.request_id for row in rows],
        [row.pipeline for row in rows],
        [row.computed_fee_eth for row in rows],
        [row.computed_fee_usd for row in rows],
        [row.currency for row in rows],
        [row.created_at for row in rows],
    )
    return len(inserted)
