CREATE TABLE lpb_actors (
  actor_id text PRIMARY KEY,
  grant_id text NOT NULL,
  allocation_id text NOT NULL UNIQUE,
  api_key_ref text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE lpb_jobs (
  id uuid PRIMARY KEY,
  actor_id text NOT NULL REFERENCES lpb_actors(actor_id),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE lpb_attempts (
  job_id uuid NOT NULL REFERENCES lpb_jobs(id),
  number integer NOT NULL CHECK (number >= 1),
  auth_ids text[] NOT NULL,
  manifest_id text,
  payment_sent boolean NOT NULL,
  outcome text NOT NULL,
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
