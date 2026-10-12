# livepeer_builder

Cost-sync worker and Postgres store for attributing Batteries usage by allocation and signer auth id.

Tests:

```sh
docker run -d --rm -p 127.0.0.1:55432:5432 -e POSTGRES_USER=livepeer -e POSTGRES_PASSWORD=livepeer -e POSTGRES_DB=livepeer postgres:16
uv run --all-groups pytest
```

Postgres-backed tests skip when the database is unreachable; set `REQUIRE_POSTGRES=1` to fail instead, as CI does. `DATABASE_URL` overrides the default `postgresql://livepeer:livepeer@127.0.0.1:55432/livepeer`.

`tests/test_gateway_contract.py` runs the pinned `livepeer-gateway` against an in-process signer and runner that speak the real wire formats. It checks what this package records from a paid call: the actor's key reaches the signer, the signer's `AuthID` comes back as `auth_ids`, `payment_sent` is set on failures after payment, and those auth ids attribute applied usage rows to the right job and allocation. A gateway pin bump that changes any of that fails CI.

`deploy/` brings up a real-node stack (go-livepeer signer and orchestrator, Batteries, Redpanda) for manual runs, after `deploy/.env` is filled in:

```sh
python3 deploy/render_secrets.py
docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --build
```

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
