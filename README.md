# livepeer_builder

Builder engine for a Livepeer gateway: discovery, paid jobs, and network cost by actor.

```python
from livepeer_builder import BuilderEngine, JobRequest, LivepeerSettings

engine = await BuilderEngine.from_settings(LivepeerSettings())
await engine.start()
result = await engine.jobs.run(actor, JobRequest(app="example/app", payload={"input": "..."}))
cost = await engine.costs.for_job(actor, result.job.id)
```

`engine.access` authenticates operator-issued keys. `engine.payments` holds the signer credential and the Batteries allowance. An actor's stored API key is presented to the signer; `LIVEPEER_SIGNER_CREDENTIAL` is the default when the actor has no allocation.

Unit tests:

```sh
DATABASE_URL=postgresql://livepeer:livepeer@127.0.0.1:55433/livepeer uv run pytest
```

The HTTP adapter is optional: `uv sync --extra server`, then `python -m livepeer_builder.server`.

Real-node proof, after `deploy/.env` is filled in:

```sh
python3 deploy/render_secrets.py
docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --build
uv run python scripts/provision.py
uv run python scripts/prove_allocation.py
uv run python scripts/prove_job.py
uv run python scripts/prove_forged_manifest.py
uv run python scripts/report.py
```

`prove_allocation.py` records manifest ids and ignores `auth_ids`. `prove_job.py` and `prove_forged_manifest.py` record the signer auth id from gateway commit `3250c08`.

## How a user meets a usage row

The cost-sync feed copies Batteries rows. It does not contain the enterprise user. Postgres joins those rows to a user the gateway already stored.

```mermaid
flowchart TB
    user["Enterprise user"] --> actors["lpb_actors"]
    actors --> apikey["Bearer API key"]
    apikey --> mint["auth_id minted"]
    mint -->|signed| state["Sealed signer state"]
    state --> attempt["lpb_attempts"]
    attempt -->|auth_id| job["Job cost"]
    mint -->|charged| usage["Batteries usage row"]
    usage --> events["lpb_usage_events"]
    events -->|applied rows| spend["User spend"]
    events -->|applied rows| job
    actors -->|allocation_id| spend
```

`auth_id` is the payment session Batteries mints. Job cost sums applied usage rows whose `payment_session_id` is one of the attempt's auth ids. User spend sums applied rows on the allocation stored for that actor. `manifest_id` is a label.
