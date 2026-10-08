-- The initial schema. Edited in place until the first release is tagged;
-- after that, changes go in new numbered migrations.

CREATE TABLE lpb_actors (
  actor_id text PRIMARY KEY,
  grant_id text NOT NULL,
  allocation_id text NOT NULL UNIQUE,
  api_key_ref text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- A job belongs to an opaque actor. The actor need not hold a Batteries
-- allocation: unpaid calls still record jobs.
CREATE TABLE lpb_jobs (
  id uuid PRIMARY KEY,
  actor_id text NOT NULL,
  application_id text NOT NULL,
  operation_ref text,
  app text NOT NULL,
  capability text,
  model text,
  state text NOT NULL CHECK (state IN ('running', 'succeeded', 'failed', 'uncertain')),
  failure jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE UNIQUE INDEX lpb_jobs_operation_ref
  ON lpb_jobs (application_id, actor_id, operation_ref)
  WHERE operation_ref IS NOT NULL;

CREATE INDEX lpb_jobs_actor_created ON lpb_jobs (application_id, actor_id, created_at DESC);

CREATE TABLE lpb_attempts (
  job_id uuid NOT NULL REFERENCES lpb_jobs(id),
  number integer NOT NULL CHECK (number >= 1),
  runner_url text,
  orchestrator_url text,
  auth_ids text[] NOT NULL,
  manifest_id text,
  payment_sent boolean NOT NULL,
  outcome text NOT NULL
    CHECK (outcome IN ('succeeded', 'refused', 'unreachable', 'payment', 'timeout', 'http')),
  status_code integer,
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  PRIMARY KEY (job_id, number)
);

CREATE TABLE lpb_usage_events (
  id text PRIMARY KEY,
  event_id text,
  status text NOT NULL CHECK (status IN ('applied', 'quarantined', 'ignored', 'duplicate')),
  manifest_id text,
  allocation_id text,
  payment_session_id text,
  request_id text,
  pipeline text,
  computed_fee_eth numeric,
  computed_fee_usd numeric,
  currency text,
  created_at timestamptz NOT NULL
);

CREATE INDEX lpb_usage_events_allocation ON lpb_usage_events (allocation_id);
CREATE INDEX lpb_usage_events_session ON lpb_usage_events (payment_session_id);
CREATE INDEX lpb_usage_events_manifest ON lpb_usage_events (manifest_id);

CREATE TABLE lpb_cost_cursor (
  id integer PRIMARY KEY CHECK (id = 1),
  resume_cursor text,
  filters text NOT NULL
);
