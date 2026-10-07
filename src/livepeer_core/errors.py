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
