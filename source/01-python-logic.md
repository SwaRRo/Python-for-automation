# Module 1 — Advanced Python Logic & Data Wrangling for Backend Systems

**Module ID:** `python-logic`  
**Title:** Advanced Python logic and data wrangling  
**Baseline:** Python 3.11 or newer. The examples use standard-library typing, dataclasses, `Decimal`, timezone-aware `datetime`, `sqlite3`, FastAPI, and Pydantic APIs; pin and test dependency versions in a real project rather than copying an unverified version number. The three chapters build one service: **Reliable Data Intake & Processing Service (RDI)**. It accepts an authorized JSON request or local CSV/JSON file, validates a business event, and idempotently stores it.

## Chapter 1 — Model the domain before the endpoint

### 1.1 Overview, objectives, and architectural reason

A backend is reliable when invalid states are difficult to create, not merely when the happy path returns HTTP 200. In this chapter learners will: (1) turn a business statement into explicit entities and invariants; (2) distinguish static type hints from runtime validation; (3) use a frozen dataclass for a trusted domain value and Pydantic for an untrusted transport boundary; (4) define specific exceptions and map them to HTTP responses; and (5) keep the route thin by separating API, service, and repository responsibilities.

This matters in server architecture because a route is an integration boundary. Type hints help editors and checkers but Python does not enforce them at runtime [1]. FastAPI uses Pydantic models to parse and validate request bodies and expose their JSON Schema in OpenAPI [2]. The result should be a domain object whose invariants hold whether it came from HTTP, a file, a queue, or a test.

**Business contract for RDI:** an intake event has a non-empty stable `event_id`, a bounded `customer_id`, a positive monetary `amount` with exactly two fractional places, and an unambiguous UTC instant. Replaying the same `event_id` must not create a second business effect. These are business invariants, not UI conveniences.

### 1.2 Ordered topics

1. **Domain language and invariants.** Write examples of valid and invalid events first. Separate syntactic rules (required fields, formats, maximum lengths) from semantic rules (positive money, UTC instant, stable identity). OWASP recommends validating untrusted data as early as possible and applying both syntactic and semantic validation [3].
2. **Boundary versus core.** Pydantic validates untrusted dictionaries/JSON. A frozen dataclass represents a normalized, trusted value. `@dataclass(frozen=True)` prevents ordinary field assignment after construction; it does not make nested objects magically immutable [4].
3. **Control flow and exceptions.** Catch expected validation errors at the boundary, log with a correlation/request ID, and let unexpected exceptions reach the server error handler. Python’s tutorial recommends specific exception handlers and re-raising unexpected failures [5].
4. **Service/repository boundary.** The service owns business decisions; the repository owns persistence. Neither should fetch credentials or parse HTTP request details.
5. **Development demo versus production.** The local demo below uses an in-memory list repository. Production replaces it with a database repository and authentication/authorization middleware. It is intentionally not a durable queue or a transaction coordinator.

### 1.3 Code blueprint: `domain.py` and `app.py`

The following is near-runnable (`pip install fastapi pydantic uvicorn`; pin versions in your lock file). Send `amount` as a JSON string so decimal intent is unambiguous.

```python
# domain.py — Python 3.11+
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re

_EVENT_ID = re.compile(r"^[A-Za-z0-9._:-]{1,100}$")
_CENT = Decimal("0.01")

class DomainError(ValueError):
    """Expected business-rule failure; safe to expose as 400/422."""

@dataclass(frozen=True, slots=True)
class IntakeEvent:
    event_id: str
    customer_id: str
    amount: Decimal
    occurred_at: datetime

    def __post_init__(self) -> None:
        if not _EVENT_ID.fullmatch(self.event_id):
            raise DomainError("event_id has an unsupported format")
        if not 1 <= len(self.customer_id) <= 80:
            raise DomainError("customer_id length is outside the allowed range")
        if self.amount <= 0 or self.amount != self.amount.quantize(_CENT):
            raise DomainError("amount must be positive and have two decimal places")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise DomainError("occurred_at must be timezone-aware")

    @property
    def occurred_at_utc(self) -> datetime:
        return self.occurred_at.astimezone(timezone.utc)
```

```python
# app.py — development demo; persistence is intentionally non-durable.
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from domain import DomainError, IntakeEvent

class IntakeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    event_id: str = Field(min_length=1, max_length=100)
    customer_id: str = Field(min_length=1, max_length=80)
    amount: Decimal
    occurred_at: datetime

    @field_validator("amount")
    @classmethod
    def money(cls, value: Decimal) -> Decimal:
        try:
            if value <= 0 or value != value.quantize(Decimal("0.01")):
                raise ValueError("amount must be positive with two decimal places")
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(str(exc)) from exc
        return value

    @field_validator("occurred_at")
    @classmethod
    def aware_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value

class MemoryRepository:
    def __init__(self) -> None:
        self.rows: dict[str, IntakeEvent] = {}

    def upsert(self, event: IntakeEvent) -> IntakeEvent:
        # Demo semantics: same key replaces the representation. Production must
        # make this atomic in a database and define whether conflicting payloads
        # are rejected rather than silently updated.
        self.rows[event.event_id] = event
        return event

repo = MemoryRepository()
app = FastAPI(title="Reliable Data Intake & Processing Service")

@app.post("/intake", status_code=201)
def intake(payload: IntakeRequest, request: Request) -> dict[str, Any]:
    request_id = request.headers.get("x-request-id", "generated-locally")
    try:
        event = IntakeEvent(**payload.model_dump())
        saved = repo.upsert(event)
    except DomainError as exc:
        # Do not log the full payload: it may contain personal or financial data.
        raise HTTPException(status_code=422, detail=str(exc),
                            headers={"X-Request-ID": request_id}) from exc
    return {"event_id": saved.event_id, "status": "accepted",
            "request_id": request_id}
```

### 1.4 Local, server, and cloud operations context

**Local development demo:** create a virtual environment, run `uvicorn app:app --reload`, and exercise `/docs` or `curl`; the dictionary is not durable. **Server deployment:** replace it before using multiple workers, then add TLS, an authenticated proxy, body-size limits, and structured stdout logs. **Cloud path:** place containers behind a managed load balancer and adopt managed PostgreSQL plus a durable queue before claiming availability or durability.

### 1.5 Failure, logging, retry, idempotency, and security notes

Malformed JSON, oversized bodies, missing timezones, invalid money, and unknown fields are 4xx failures: increment a metric and log type plus request ID, not payloads. Never log bearer tokens, passwords, database URLs, or personal/financial data; OWASP lists these for masking or exclusion [6]. Validation is not retryable. Retry transient storage/queue failures only with a stable idempotency key. Do not call this exactly-once: a process may commit before a reply is lost.

### 1.6 Hands-on build step

Implement `domain.py`, `app.py`, and four invariant tests. Add a `request_id` to responses/logs and demonstrate that replaying `event_id` does not add a second key. This is **development-demo only** and establishes the contract for Chapters 2–3.

## Chapter 2 — Wrangle CSV/JSON safely and cross the repository boundary

### 2.1 Overview, objectives, and architectural reason

Learners will: (1) process files as bounded iterators; (2) parse CSV/JSON with explicit encoding and limits; (3) preserve money with `Decimal` and aware UTC datetimes; (4) quarantine bad rows; and (5) persist normalized events through a repository. Every file, feed, and queue message is external input, even on an internal network.

Python’s CSV reader expects files opened with `newline=''` and returns strings without automatic conversion [7]. JSON parsing can consume considerable CPU and memory for a maliciously large string, so size limits are an application responsibility [8]. `Decimal` represents values such as `1.1` exactly and is preferred for strict accounting equality [9]. A naive datetime cannot unambiguously locate an instant; use timezone-aware values and normalize to UTC [10].

### 2.2 Ordered topics

1. **Source adapters.** Make a CSV adapter and JSON adapter that both yield `dict[str, object]`. The service consumes an iterator, so it can apply backpressure or batch commits later.
2. **Canonical conversion.** Parse money from text, reject non-finite values, quantize to cents at the domain boundary, and convert timestamps to UTC. Never use binary float for a monetary invariant.
3. **Validation and quarantine.** Record source name and row number in a quarantine stream. Continue only when the product contract says bad rows should not poison the complete file; otherwise fail the batch and make the caller repair it.
4. **Repository contract.** The repository accepts `IntakeEvent`, not raw dictionaries. SQL uses placeholders; Python’s `sqlite3` documentation explicitly warns to bind values rather than format SQL strings [11].
5. **Development demo versus production.** Local SQLite is a teaching and single-process path. Production PostgreSQL should store a unique event key and use `INSERT ... ON CONFLICT`; this gives atomic insert-or-update, not global exactly-once processing [12] [13].

### 2.3 Code blueprint: `wrangling.py` and `repository.py`

```python
# wrangling.py — streaming adapters plus normalization
import csv, json
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from pydantic import ValidationError
from app import IntakeRequest
from domain import DomainError, IntakeEvent

MAX_INPUT_BYTES = 5 * 1024 * 1024  # product limit; tune and test it

def iter_records(path: Path) -> Iterator[dict[str, Any]]:
    if path.stat().st_size > MAX_INPUT_BYTES:
        raise ValueError("input exceeds configured size limit")
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                yield dict(row)
    elif path.suffix.lower() == ".json":
        # The size check above bounds the whole decode. For larger files use
        # a streaming JSON format/parser chosen and reviewed for the workload.
        with path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
        if not isinstance(document, list):
            raise ValueError("JSON intake must be an array of objects")
        for row in document:
            if not isinstance(row, dict):
                raise ValueError("each JSON item must be an object")
            yield row
    else:
        raise ValueError("only .csv and .json are accepted")

def normalize(raw: dict[str, Any]) -> IntakeEvent:
    # Pydantic performs boundary parsing; the dataclass repeats invariants so
    # non-HTTP callers cannot bypass them.
    try:
        request = IntakeRequest.model_validate(raw)
        return IntakeEvent(**request.model_dump())
    except (ValidationError, DomainError) as exc:
        raise ValueError(f"invalid intake record: {exc}") from exc
```

```python
# repository.py — local durable demo; one connection per process/thread policy.
import sqlite3
from pathlib import Path
from domain import IntakeEvent

SCHEMA = """
CREATE TABLE IF NOT EXISTS intake_events (
  event_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  amount TEXT NOT NULL,
  occurred_at TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

class SQLiteRepository:
    def __init__(self, database: Path) -> None:
        self.connection = sqlite3.connect(database, timeout=10)
        self.connection.execute(SCHEMA)
        self.connection.commit()

    def upsert(self, event: IntakeEvent) -> None:
        with self.connection:  # commits or rolls back the transaction
            self.connection.execute(
                """INSERT INTO intake_events
                   (event_id, customer_id, amount, occurred_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(event_id) DO UPDATE SET
                     customer_id=excluded.customer_id,
                     amount=excluded.amount,
                     occurred_at=excluded.occurred_at,
                     updated_at=CURRENT_TIMESTAMP""",
                (event.event_id, event.customer_id, str(event.amount),
                 event.occurred_at_utc.isoformat()),
            )

    def close(self) -> None:
        self.connection.close()
```

### 2.4 Local, server, and cloud operations context

Run a local CLI that opens `SQLiteRepository("rdi.db")`, iterates, normalizes, and upserts. Keep SQLite on a persistent local volume, not a shared filesystem; archive the source only after recording the batch outcome. Containers may mount SQLite for development, but production containers should be stateless and use PostgreSQL.

For the realistic production path, upload an authorized file to object storage, publish an intake-job message to a managed queue, and have workers read the object, validate rows, and write PostgreSQL in transactions. Standard queues such as Amazon SQS document at-least-once delivery and instruct consumers to be idempotent [14]. A queue is useful for backpressure and retries, but it does not make downstream side effects exactly once.

### 2.5 Failure, logging, retry, idempotency, and security notes

Handle decoding, CSV/JSON, validation, and database errors separately. Log source, row, error class, and a redacted key. Quarantine bad rows; never execute URLs, shell commands, or serialized objects from a file. Enforce size, row/field, extension, and authorization limits; use parameterized SQL. Retry only transient errors with bounded backoff. Choose and document whether same-key/different-payload replays conflict or update; silent overwrite is dangerous for financial data.

### 2.6 Hands-on build step

Add `python -m rdi_import sample.csv --db rdi.db`, producing quarantine JSON Lines with row, source, category, and a redacted message. Import twice and assert the row count is unchanged. This **local demo** turns Chapter 1’s contract into a repeatable pipeline.

## Chapter 3 — Make processing transactional, observable, and deployable

### 3.1 Overview, objectives, and architectural reason

Learners will: (1) make the write boundary atomic; (2) distinguish idempotent upsert from exactly-once delivery; (3) add a retrying worker seam; (4) test invariants, rollback, and replay; and (5) package and operate the RDI service.

PostgreSQL’s default isolation is Read Committed, and its `ON CONFLICT DO UPDATE` path provides an atomic insert-or-update outcome for each row when there is no independent error [12] [13]. That protects the unique-key write under concurrency. It does not coordinate a database commit with an external email, webhook, or queue acknowledgment. If a worker crashes after committing and before acknowledging, the message can run again; Celery likewise recommends idempotent tasks when late acknowledgment is used [15].

### 3.2 Ordered topics

1. **Transaction scope.** Validate first; keep the write transaction short.
2. **Idempotency record.** Make `event_id` (or a provider key) unique; optionally store a payload hash and reject conflicting replays.
3. **Retry taxonomy.** Retry transient network/database errors, not validation or authorization. HTTPX exposes separate timeout categories [16].
4. **Testing.** Cover duplicate/conflicting replay, rollback, malformed files, timezone conversion, and size limits.
5. **Operations.** Docker `HEALTHCHECK` reports container health after consecutive failures [17]; use separate dependency readiness and alert on quarantine, retry age, and queue lag.
6. **Migration.** Replace SQLite with PostgreSQL and local calls with a durable queue; use authorized APIs only, never access-control or anti-bot bypasses.

### 3.3 Code blueprint: service, tests, and worker seam

```python
# service.py — cumulative capstone service using repository.py
import hashlib, json, logging
from pathlib import Path
from domain import IntakeEvent
from repository import SQLiteRepository

log = logging.getLogger("rdi")

def fingerprint(event: IntakeEvent) -> str:
    canonical = json.dumps({
        "event_id": event.event_id, "customer_id": event.customer_id,
        "amount": str(event.amount),
        "occurred_at": event.occurred_at_utc.isoformat(),
    }, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()

def process_file(path: Path, repo: SQLiteRepository) -> tuple[int, int]:
    from wrangling import iter_records, normalize
    accepted = rejected = 0
    for record_no, raw in enumerate(iter_records(path), start=1):
        try:
            event = normalize(raw)
            repo.upsert(event)
            accepted += 1
            log.info("intake.accepted event_id=%s record=%d", event.event_id, record_no)
        except ValueError as exc:
            rejected += 1
            log.warning("intake.rejected record=%d error=%s", record_no, type(exc).__name__)
    return accepted, rejected

if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")
    db = SQLiteRepository(Path("rdi.db"))
    try:
        print(process_file(Path("sample.csv"), db))
    finally:
        db.close()
```

For a production queue, make the worker function idempotent and acknowledge only after the database transaction succeeds. A Celery-shaped seam is illustrative, not a claim that a broker has been configured:

```python
# worker.py — production-shaped seam; configure broker/result URLs via secrets.
import os
from celery import Celery
from service import process_file
from repository import SQLiteRepository
from pathlib import Path

celery_app = Celery("rdi", broker=os.environ["BROKER_URL"])

@celery_app.task(bind=True, autoretry_for=(ConnectionError,),
                 retry_backoff=True, max_retries=5, acks_late=True)
def process_object(self, object_path: str) -> tuple[int, int]:
    # The object must be authorized, size-limited, and immutable/versioned.
    repo = SQLiteRepository(Path("rdi.db"))  # replace with PostgreSQL in prod
    try:
        return process_file(Path(object_path), repo)
    finally:
        repo.close()
```

A minimal standard-library test exercises the most important delivery promise:

```python
# test_rdi.py
import tempfile, unittest
from pathlib import Path
from domain import IntakeEvent
from repository import SQLiteRepository
from service import process_file

CSV = "event_id,customer_id,amount,occurred_at\ne-1,c-7,12.30,2024-01-02T03:04:05+00:00\n"

class RDITest(unittest.TestCase):
    def test_replay_is_one_row(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); source = root / "a.csv"; source.write_text(CSV)
            repo = SQLiteRepository(root / "rdi.sqlite")
            try:
                self.assertEqual(process_file(source, repo), (1, 0))
                self.assertEqual(process_file(source, repo), (1, 0))
                count = repo.connection.execute(
                    "SELECT COUNT(*) FROM intake_events").fetchone()[0]
                self.assertEqual(count, 1)
            finally:
                repo.close()

if __name__ == "__main__":
    unittest.main()
```

The PostgreSQL migration should add `UNIQUE (event_id)`, a payload hash, and an outbox if downstream effects exist. The outbox commits the record and publish intent together; a publisher retries delivery. Consumers still need idempotency and reconciliation.

### 3.4 Deployment and operations context

**Local:** run SQLite tests and use Docker Compose for a disposable API plus PostgreSQL after migration; Docker documents both the virtualenv/`uvicorn` loop and Compose database workflow [18]. **Server:** ship a non-root container with resource limits, rotated logs, `/healthz`, and `/readyz`; a Docker health check reports container health, not correctness [17]. Direct Linux services need a dedicated user, restrictive credential files, and restart policy; never commit secrets.

**Cloud:** deploy API and workers separately with managed PostgreSQL/TLS, encrypted object storage, a queue/dead-letter queue, and least-privilege access. Use the provider secret manager, rotate credentials, and monitor latency, 4xx/5xx, database errors, queue age, retries, conflicts, and quarantine rate.

### 3.5 Failure, logging, retry, idempotency, and security notes

A crash before commit rolls back; after commit it may replay. The unique key protects the stored event, but external effects need their own idempotency key or outbox consumer. Cap retries, back off, dead-letter poison messages, and require authorization for replay. Log event type, key hash, attempt, latency, outcome, and correlation ID while excluding secrets/PII [6]. Protect routes and docs with authentication, use TLS and dependency scanning, restrict file permissions, parameterize SQL, and call approved APIs only.

### 3.6 Hands-on build step: capstone completion

Create a repository layout with `domain.py`, `app.py`, `wrangling.py`, `repository.py`, `service.py`, `worker.py`, and `test_rdi.py`. Demonstrate this sequence: POST one valid event; import a CSV containing one invalid row; replay the file; inject a database exception before commit; restart the worker; and inspect logs/metrics. Replace the SQLite repository with PostgreSQL and the local file path with an authorized object-store reference only after the local tests pass. Document the guarantee precisely: **validated events are atomically upserted by key, and processing is at-least-once with idempotent replay—not exactly-once delivery.**

## References

[1]: https://docs.python.org/3/library/typing.html "Python typing — Support for type hints"
[2]: https://fastapi.tiangolo.com/tutorial/body/ "FastAPI Request Body"
[3]: https://cheatsheetseries.owasp.org/cheatsheets/Input_Validation_Cheat_Sheet.html "OWASP Input Validation Cheat Sheet"
[4]: https://docs.python.org/3/library/dataclasses.html "Python dataclasses — Data Classes"
[5]: https://docs.python.org/3/tutorial/errors.html "Python Tutorial — Errors and Exceptions"
[6]: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html "OWASP Logging Cheat Sheet"
[7]: https://docs.python.org/3/library/csv.html "Python csv — CSV File Reading and Writing"
[8]: https://docs.python.org/3/library/json.html "Python json — JSON encoder and decoder"
[9]: https://docs.python.org/3/library/decimal.html "Python decimal — Decimal fixed-point and floating-point arithmetic"
[10]: https://docs.python.org/3/library/datetime.html "Python datetime — Basic date and time types"
[11]: https://docs.python.org/3/library/sqlite3.html "Python sqlite3 — DB-API 2.0 interface for SQLite databases"
[12]: https://www.postgresql.org/docs/current/transaction-iso.html "PostgreSQL Transaction Isolation"
[13]: https://www.postgresql.org/docs/current/sql-insert.html "PostgreSQL INSERT and ON CONFLICT"
[14]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues-at-least-once-delivery.html "Amazon SQS at-least-once delivery"
[15]: https://docs.celeryq.dev/en/stable/userguide/tasks.html "Celery Tasks"
[16]: https://www.python-httpx.org/advanced/timeouts/ "HTTPX Timeouts"
[17]: https://docs.docker.com/reference/dockerfile/ "Dockerfile reference — HEALTHCHECK"
[18]: https://docs.docker.com/guides/python/ "Docker Python language-specific guide"
