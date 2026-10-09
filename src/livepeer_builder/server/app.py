"""REST adapter. Each route is one service call made with the authenticated ActorContext."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from livepeer_builder.contracts import (
    ActorContext,
    ActorSpend,
    Allowance,
    Contract,
    EngineHealth,
    Failure,
    IssuedKey,
    Job,
    JobCost,
    JobRequest,
    Offering,
    ProvisionedActor,
    Runner,
    Scope,
    SyncReport,
)
from livepeer_builder.engine import BuilderEngine
from livepeer_builder.errors import AccessDenied, BatteriesError, JobFailed, OperationExists, ProviderUnavailable
from livepeer_builder.jobs.service import require

JOB_ID_HEADER = "Livepeer-Job-Id"

_FAILURE_STATUS = {
    "no_offering": 404,
    "refused": 503,
    "unreachable": 503,
    "payment": 503,
    "timeout": 504,
    "runner_error": 502,
}
_RETRY_AFTER = {"refused": "5", "unreachable": "5", "payment": "10"}
# Batteries answers that describe the caller's request; any other status is an upstream fault.
_BATTERIES_CLIENT_STATUS = frozenset({400, 404, 409, 422})


class IssueKeyBody(BaseModel):
    actor_id: str
    scopes: list[Scope]
    application_id: str = "default"
    label: str = ""


class ProvisionBody(BaseModel):
    actor_id: str
    grant_id: str
    amount_eth: str


def create_app(engine: BuilderEngine, *, manage_lifecycle: bool = True) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if manage_lifecycle:
            await engine.start()
        try:
            yield
        finally:
            if manage_lifecycle:
                await engine.stop()

    app = FastAPI(title="livepeer-builder", lifespan=lifespan)
    app.state.engine = engine
    bearer = HTTPBearer(auto_error=False, scheme_name="bearerAuth")

    async def actor_from(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> ActorContext:
        if credentials is None or not credentials.credentials.strip():
            raise HTTPException(401, "bearer token required")
        try:
            return await engine.access.authenticate(credentials.credentials.strip())
        except AccessDenied as exc:
            raise HTTPException(401, str(exc)) from exc

    Actor = Annotated[ActorContext, Depends(actor_from)]

    @app.exception_handler(AccessDenied)
    async def _denied(_: Request, exc: AccessDenied) -> Response:
        return JSONResponse({"error": str(exc)}, status_code=403)

    @app.exception_handler(ProviderUnavailable)
    async def _provider(_: Request, exc: ProviderUnavailable) -> Response:
        return JSONResponse({"error": str(exc)}, status_code=503)

    @app.exception_handler(BatteriesError)
    async def _batteries(_: Request, exc: BatteriesError) -> Response:
        status = exc.status_code if exc.status_code in _BATTERIES_CLIENT_STATUS else 502
        return JSONResponse({"error": str(exc)}, status_code=status)

    @app.exception_handler(OperationExists)
    async def _exists(_: Request, exc: OperationExists) -> Response:
        return _json(exc.job, 409, {JOB_ID_HEADER: str(exc.job.id)})

    @app.exception_handler(JobFailed)
    async def _failed(_: Request, exc: JobFailed) -> Response:
        return failure_response(exc.job)

    # App names contain "/" (livepeer-example/hello-world), so they travel as a query parameter.
    @app.get("/v1/offerings")
    async def offerings(actor: Actor, app_name: Annotated[str | None, Query(alias="app")] = None) -> list[Offering]:
        require(actor, "discover")
        return engine.discovery.offerings(app=app_name)

    @app.get("/v1/runners")
    async def runners(actor: Actor, app_name: Annotated[str, Query(alias="app")]) -> list[Runner]:
        require(actor, "discover")
        return engine.discovery.runners(app_name)

    @app.post("/v1/jobs")
    async def run_job(actor: Actor, request: JobRequest) -> Response:
        result = await engine.jobs.run(actor, request)
        return Response(
            content=result.content,
            status_code=result.status_code,
            media_type=result.content_type,
            headers={JOB_ID_HEADER: str(result.job.id)},
        )

    @app.get("/v1/jobs")
    async def list_jobs(actor: Actor, limit: int = 50) -> list[Job]:
        return list(await engine.jobs.list(actor, limit=min(max(limit, 1), 500)))

    @app.get("/v1/jobs/{job_id}")
    async def get_job(actor: Actor, job_id: UUID) -> Job:
        return await engine.jobs.get(actor, job_id)

    @app.get("/v1/jobs/{job_id}/cost")
    async def job_cost(actor: Actor, job_id: UUID) -> JobCost:
        return await engine.costs.for_job(actor, job_id)

    @app.get("/v1/spend")
    async def spend(actor: Actor, actor_id: str | None = None) -> ActorSpend:
        return await engine.costs.for_actor(actor, actor_id)

    @app.get("/v1/allowance")
    async def allowance(actor: Actor, actor_id: str | None = None) -> Allowance:
        return await engine.payments.allowance(actor, actor_id)

    @app.post("/v1/costs/sync", status_code=200)
    async def sync(actor: Actor) -> SyncReport:
        require(actor, "admin")
        return await engine.costs.sync_once()

    @app.post("/v1/allocations", status_code=201)
    async def provision(actor: Actor, body: ProvisionBody) -> ProvisionedActor:
        provisioned = await engine.payments.provision(
            actor, body.actor_id, grant_id=body.grant_id, amount_eth=body.amount_eth
        )
        return provisioned.model_copy(update={"api_key_ref": "***"})

    @app.post("/v1/keys", status_code=201)
    async def issue_key(actor: Actor, body: IssueKeyBody) -> IssuedKey:
        return await engine.access.issue_key(
            actor,
            body.actor_id,
            scopes=body.scopes,
            application_id=body.application_id,
            label=body.label,
        )

    @app.post("/v1/keys/{key_id}/revoke", status_code=204)
    async def revoke_key(actor: Actor, key_id: str) -> Response:
        await engine.access.revoke_key(actor, key_id)
        return Response(status_code=204)

    @app.get("/livez")
    async def livez() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> Response:
        health = engine.health()
        return _json(health, 200 if health.discovery_fresh else 503)

    return app


def failure_response(job: Job) -> Response:
    failure = job.failure or Failure(
        kind="runner_error",
        message=f"job {job.id} ended {job.state}",
        status_code=None,
        body=None,
        payment_sent=False,
    )
    headers = {JOB_ID_HEADER: str(job.id)}
    # A retry is only safe when no payment went out: otherwise it could pay and run twice.
    if failure.kind in _RETRY_AFTER and job.state != "uncertain" and not failure.payment_sent:
        headers["Retry-After"] = _RETRY_AFTER[failure.kind]
    if job.state == "uncertain":
        status = 504
    elif failure.kind == "runner_rejected":
        status = failure.status_code or 400
    else:
        status = _FAILURE_STATUS[failure.kind]
    return _json(failure, status, headers)


def _json(model: Contract | EngineHealth, status: int, headers: dict[str, str] | None = None) -> Response:
    return Response(
        content=model.model_dump_json(),
        status_code=status,
        media_type="application/json",
        headers=headers,
    )
