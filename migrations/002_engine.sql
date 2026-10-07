-- A job belongs to an opaque actor. The actor need not hold a Batteries
-- allocation: unpaid calls still record jobs, and their cost stays pending.
ALTER TABLE lpb_jobs DROP CONSTRAINT lpb_jobs_actor_id_fkey;

ALTER TABLE lpb_jobs
  ADD COLUMN application_id text NOT NULL DEFAULT 'default',
  ADD COLUMN operation_ref text,
  ADD COLUMN app text NOT NULL DEFAULT '',
  ADD COLUMN capability text,
  ADD COLUMN model text,
  ADD COLUMN state text NOT NULL DEFAULT 'running'
    CHECK (state IN ('running', 'succeeded', 'failed', 'uncertain')),
  ADD COLUMN failure jsonb,
  ADD COLUMN completed_at timestamptz;

CREATE UNIQUE INDEX lpb_jobs_operation_ref
  ON lpb_jobs (application_id, actor_id, operation_ref)
  WHERE operation_ref IS NOT NULL;

CREATE INDEX lpb_jobs_actor_created ON lpb_jobs (application_id, actor_id, created_at DESC);

ALTER TABLE lpb_attempts
  ADD COLUMN runner_url text,
  ADD COLUMN orchestrator_url text,
  ADD COLUMN status_code integer,
  ADD COLUMN started_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN ended_at timestamptz;

CREATE TABLE lpb_access_keys (
  key_id text PRIMARY KEY,
  token_hash text NOT NULL UNIQUE,
  actor_id text NOT NULL,
  application_id text NOT NULL,
  scopes text[] NOT NULL,
  label text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL,
  revoked_at timestamptz
);
