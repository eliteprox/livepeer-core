from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from livepeer_builder.contracts import AttemptOutcome, Job


class EngineError(Exception):
    """Base error for the engine."""


class AccessDenied(EngineError):
    """Bad credential, missing scope, or another actor's record."""


class OperationExists(EngineError):
    """This (application_id, actor_id, operation_ref) already dispatched a job."""

    def __init__(
        self,
        job: "Job",
    ) -> None:
        super().__init__(f"operation_ref {job.operation_ref!r} already used by job {job.id}")
        self.job = job


class JobFailed(EngineError):
    """The job ended without a result. Switch on ``job.failure.kind``."""

    def __init__(
        self,
        job: "Job",
    ) -> None:
        failure = job.failure
        super().__init__(failure.message if failure is not None else f"job {job.id} {job.state}")
        self.job = job


class RunnerCallError(EngineError):
    """One runner call failed. Raised by a RunnerTransport.

    ``payment_sent`` is True once tickets went out with the request: the job is
    then bound to this runner and must not fail over. ``auth_ids`` lists the
    signer payment sessions the call opened, which can be non-empty even when
    ``payment_sent`` is False, because the signer charges when it signs.
    """

    def __init__(
        self,
        kind: "AttemptOutcome",
        message: str,
        *,
        payment_sent: bool,
        auth_ids: tuple[str, ...] = (),
        status_code: int | None = None,
        body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.payment_sent = payment_sent
        self.auth_ids = auth_ids
        self.status_code = status_code
        self.body = body


class CursorMismatch(Exception):
    """Batteries rejected a usage cursor for this route or filter set."""


class BatteriesError(Exception):
    def __init__(
        self,
        status_code: int,
        message: str,
    ) -> None:
        super().__init__(f"Batteries HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message
