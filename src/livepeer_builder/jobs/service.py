import logging
from collections.abc import Sequence
from typing import cast
from uuid import UUID, uuid4

from livepeer_builder.contracts import (
    ActorContext,
    Attempt,
    Failure,
    FailureKind,
    Job,
    JobRequest,
    JobResult,
    JobState,
    RunnerReply,
)
from livepeer_builder.discovery.service import DiscoveryService
from livepeer_builder.errors import AccessDenied, JobFailed, OperationExists, RunnerCallError
from livepeer_builder.protocols import Clock, CredentialResolver, EngineStore, RunnerTransport, SelectionPolicy

_LOG = logging.getLogger(__name__)
_BODY_LIMIT = 4096

# A job may move to another runner only while no payment has been sent and the
# runner refused or never answered. Everything else binds the job to that runner.
_FAILOVER_OUTCOMES = frozenset({"refused", "unreachable"})
# After payment, these outcomes leave the result unknown: the runner may have run.
_UNCERTAIN_OUTCOMES = frozenset({"unreachable", "timeout"})


class JobService:
    def __init__(
        self,
        store: EngineStore,
        discovery: DiscoveryService,
        transport: RunnerTransport,
        selection: SelectionPolicy,
        credentials: CredentialResolver,
        clock: Clock,
    ) -> None:
        self._store = store
        self._discovery = discovery
        self._transport = transport
        self._selection = selection
        self._credentials = credentials
        self._clock = clock

    async def run(self, actor: ActorContext, request: JobRequest) -> JobResult:
        """Dispatch one job and wait for its result.

        Writes: one job row before the first call, one attempt row after each
        call, one update when the job ends. Raises OperationExists, JobFailed.
        """
        require(actor, "jobs:run")
        job = await self._create_job(actor, request)
        attempts: list[Attempt] = []
        try:
            return await self._dispatch(job, actor, request, attempts)
        except JobFailed:
            raise
        except BaseException as exc:
            # Never leave the job running. Once a payment went out the result is unknown.
            paid = any(attempt.payment_sent for attempt in attempts)
            failure = Failure(
                kind="runner_error",
                message=f"engine error: {type(exc).__name__}",
                status_code=None,
                body=None,
                payment_sent=paid,
            )
            try:
                await self._finish(job, "uncertain" if paid else "failed", failure, attempts)
            except Exception:
                _LOG.exception("could not close job %s", job.id)
            raise

    async def _dispatch(
        self,
        job: Job,
        actor: ActorContext,
        request: JobRequest,
        attempts: list[Attempt],
    ) -> JobResult:
        candidates = self._discovery.runners(request.app)
        if not candidates:
            failure = Failure(
                kind="no_offering",
                message=f"no runner advertises app {request.app!r}",
                status_code=None,
                body=None,
                payment_sent=False,
            )
            raise JobFailed(await self._finish(job, "failed", failure))

        credential = await self._credentials.credential(actor)
        ordered = list(self._selection.order(candidates, request))[: max(1, request.max_attempts)]
        for number, runner in enumerate(ordered, start=1):
            started = self._clock.now()
            try:
                reply = await self._transport.call(runner, request, credential)
            except RunnerCallError as error:
                attempt = Attempt(
                    job_id=job.id,
                    number=number,
                    runner_url=runner.url,
                    orchestrator_url=runner.orchestrator_url,
                    auth_ids=error.auth_ids,
                    manifest_id=None,
                    payment_sent=error.payment_sent,
                    outcome=error.kind,
                    status_code=error.status_code,
                    started_at=started,
                    ended_at=self._clock.now(),
                )
                attempts.append(attempt)
                await self._store.record_attempt(attempt)
                if error.payment_sent or error.kind not in _FAILOVER_OUTCOMES:
                    state: JobState = (
                        "uncertain" if error.payment_sent and error.kind in _UNCERTAIN_OUTCOMES else "failed"
                    )
                    raise JobFailed(await self._finish(job, state, _failure(error), attempts))
                continue
            attempt = Attempt(
                job_id=job.id,
                number=number,
                runner_url=runner.url,
                orchestrator_url=runner.orchestrator_url,
                auth_ids=reply.auth_ids,
                manifest_id=reply.manifest_id,
                payment_sent=reply.payment_sent,
                outcome="succeeded",
                status_code=reply.status_code,
                started_at=started,
                ended_at=self._clock.now(),
            )
            attempts.append(attempt)
            await self._store.record_attempt(attempt)
            return _result(await self._finish(job, "succeeded", None, attempts), reply)

        # Only refused or unreachable attempts reach this point (see _FAILOVER_OUTCOMES).
        last = attempts[-1]
        failure = Failure(
            kind=cast(FailureKind, last.outcome),
            message=f"all {len(attempts)} runners refused or were unreachable",
            status_code=None,
            body=None,
            payment_sent=False,
        )
        raise JobFailed(await self._finish(job, "failed", failure, attempts))

    async def _create_job(self, actor: ActorContext, request: JobRequest) -> Job:
        if request.operation_ref is not None:
            existing = await self._store.job_by_operation_ref(
                actor.application_id, actor.actor_id, request.operation_ref
            )
            if existing is not None:
                raise OperationExists(existing)
        job = Job(
            id=uuid4(),
            actor_id=actor.actor_id,
            application_id=actor.application_id,
            operation_ref=request.operation_ref,
            app=request.app,
            capability=request.capability,
            model=request.model,
            state="running",
            attempts=(),
            failure=None,
            created_at=self._clock.now(),
            completed_at=None,
        )
        await self._store.create_job(job)
        return job

    async def get(self, actor: ActorContext, job_id: UUID) -> Job:
        require(actor, "jobs:read")
        job = await self._store.get_job(job_id)
        if job is None or not owns(actor, job):
            raise AccessDenied(f"job {job_id} not found for this actor")
        return job

    async def list(self, actor: ActorContext, *, limit: int = 50) -> Sequence[Job]:
        require(actor, "jobs:read")
        return await self._store.list_jobs(actor.application_id, actor.actor_id, limit=limit)

    async def _finish(
        self,
        job: Job,
        state: JobState,
        failure: Failure | None,
        attempts: Sequence[Attempt] = (),
    ) -> Job:
        completed = self._clock.now()
        await self._store.finish_job(job.id, state, failure, completed)
        return job.model_copy(
            update={
                "state": state,
                "failure": failure,
                "attempts": tuple(attempts),
                "completed_at": completed,
            }
        )


def require(actor: ActorContext, scope: str) -> None:
    if scope in actor.scopes or "admin" in actor.scopes:
        return
    raise AccessDenied(f"scope {scope!r} required")


def owns(actor: ActorContext, job: Job) -> bool:
    if "admin" in actor.scopes:
        return True
    return job.actor_id == actor.actor_id and job.application_id == actor.application_id


def _failure(error: RunnerCallError) -> Failure:
    kind: FailureKind
    if error.kind == "http":
        status = error.status_code or 0
        kind = "runner_rejected" if 400 <= status < 500 else "runner_error"
    else:
        kind = cast(FailureKind, error.kind)
    return Failure(
        kind=kind,
        message=str(error),
        status_code=error.status_code,
        body=error.body[:_BODY_LIMIT] if error.body else None,
        payment_sent=error.payment_sent,
    )


def _result(job: Job, reply: RunnerReply) -> JobResult:
    return JobResult(
        job=job,
        status_code=reply.status_code,
        content=reply.content,
        content_type=reply.content_type,
        data=reply.data,
    )
