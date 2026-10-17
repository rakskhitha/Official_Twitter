"""The warnings and errors of one run, as entries {time, account, message}.

Every WARNING / ERROR line logged during the run is recorded here, so run.log, the run summary
(SQLite runs, _runs.jsonl) and the dashboard (twitter_pipeline_runs) always show the same messages.
The split follows the log level:
    error    something failed: data was not collected or saved this run (retried next run), or the
             run was stopped, or stored data needs attention
    warning  something unusual the program handled itself; no data lost
To name the account a message is about, log it with extra=account(name).
"""

import logging
from datetime import datetime, timezone


def account(name: str | None) -> dict:
    """`extra=` for a log call about one account, e.g. log.warning(msg, extra=account("BBCWorld"))."""
    return {"account": name}


class RunProblems(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.errors: list[dict] = []
        self.warnings: list[dict] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = {"time": datetime.fromtimestamp(record.created, timezone.utc).replace(microsecond=0).isoformat(),
                     "account": getattr(record, "account", None),
                     "message": record.getMessage()}
        except Exception:  # noqa: BLE001 - a badly formatted log call must never break the run
            self.handleError(record)
            return
        (self.errors if record.levelno >= logging.ERROR else self.warnings).append(entry)

    def attach(self) -> "RunProblems":
        logging.getLogger().addHandler(self)
        return self

    def detach(self) -> None:
        logging.getLogger().removeHandler(self)
