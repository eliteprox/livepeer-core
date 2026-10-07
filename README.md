# livepeer_core

Cost-sync worker and Postgres store for attributing Batteries usage by allocation and signer auth id.

Unit tests:

```sh
DATABASE_URL=postgresql://livepeer:livepeer@127.0.0.1:55433/livepeer uv run pytest
```

Real-node proof, after `deploy/.env` is filled in:

```sh
python3 deploy/render_secrets.py
docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --build
uv run --group proof python scripts/provision.py
uv run --group proof python scripts/prove_allocation.py
uv run --group proof python scripts/prove_job.py
uv run --group proof python scripts/prove_forged_manifest.py
uv run --group proof python scripts/report.py
```

`prove_allocation.py` records manifest ids and ignores `auth_ids`. `prove_job.py` and `prove_forged_manifest.py` record the signer auth id from gateway commit `3250c08`.
