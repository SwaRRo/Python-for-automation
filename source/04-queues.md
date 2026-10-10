# Module 4 — Asyncio, scheduling, Celery, and background work

**Module id:** `queues`  
**Python baseline:** Python 3.11+ (the blueprint uses `asyncio.TaskGroup`, added in 3.11). Examples are development demos unless marked **Production**. Delivery is treated as **at least once**: a retry, worker crash, broker redelivery, or timeout can run a task more than once. Exactly-once side effects are not claimed; idempotency and database constraints make repeats safe.

The capstone is a **Reliable Data Intake & Processing Service**: accept an authorized HTTP source reference, record an intake, enqueue processing, validate and normalize data, and expose status. Use provider APIs and keep credentials out of task payloads; never bypass access controls, terms, rate limits, or anti-bot defenses.

## Chapter 1 — Async foundations: event loops, cancellation, and backpressure

### Overview, objectives, and architectural importance

Synchronous code blocks its thread while waiting for I/O. `asyncio` provides concurrency, not automatic CPU parallelism: an event loop switches coroutines at `await` points. Python positions it for I/O-bound network code and queues [1]. Learners will explain sync/async, use the event loop and `TaskGroup`, propagate cancellation, and design bounded queues.

This matters in a server because a request handler that performs blocking work can starve other requests. Conversely, unbounded “fire-and-forget” tasks can exhaust memory and disappear at shutdown. A bounded in-process queue protects memory and gives the service an explicit overload policy, but it is **not durable**: process loss loses queued work. That is an acceptable development step, not a production queue.

### Ordered topics

1. **Sync versus async.** Use `async def` with an async I/O library; keep blocking SDKs out of the loop by using `asyncio.to_thread` or a worker process.
2. **The event loop.** `asyncio.run(main())` owns a loop. `create_task()` schedules a coroutine, but a task must be retained and awaited; the official docs warn that the loop keeps only weak references [2].
3. **Structured concurrency.** `TaskGroup` scopes child tasks and waits for them. If one child fails, remaining children are cancelled and failures are grouped. A coroutine should clean up in `finally` and re-raise `CancelledError`; swallowing it breaks structured-concurrency and timeout machinery [2].
4. **Backpressure.** A bounded `asyncio.Queue(maxsize=N)` makes producers wait when consumers lag. Choose an explicit timeout, reject with HTTP 429/503 at an API boundary, or persist to a durable queue; never let queue size grow without limit.
5. **Pipeline boundaries.** Keep intake, fetch, validate, and persist small and keyed by a stable `job_id`; pass references, not secrets or giant payloads.

### Complete local blueprint (development demo)

Save as `async_demo.py`; it uses only the standard library and an in-memory queue plus SQLite for observable status. SQLite calls are moved to a thread so this small demo does not block the event loop. Run with `python async_demo.py`.

```python
from __future__ import annotations
import asyncio, json, logging, sqlite3
from dataclasses import dataclass
from pathlib import Path

DB = Path("intake-demo.db")
QUEUE_LIMIT, WORKERS = 4, 2
log = logging.getLogger("intake")

@dataclass(frozen=True)
class Job:
    job_id: str
    source_url: str                 # In production validate/allow-list this first.

async def db_call(sql: str, params: tuple = ()) -> None:
    def run() -> None:
        with sqlite3.connect(DB) as con:
            con.execute(sql, params); con.commit()
    await asyncio.to_thread(run)

async def process(job: Job) -> None:
    # Development mock: replace with an authorized async HTTP client in production.
    await asyncio.sleep(0.05)
    if not job.source_url.startswith("https://api.example.test/"):
        raise ValueError("source is not on the approved API host")
    normalized = json.dumps({"source": job.source_url, "valid": True})
    await db_call("UPDATE jobs SET status=?, result=? WHERE job_id=?",
                  ("done", normalized, job.job_id))

async def worker(name: str, q: asyncio.Queue[Job | None]) -> None:
    while True:
        job = await q.get()
        try:
            if job is None: return
            log.info("worker=%s job=%s start", name, job.job_id)
            await process(job)
        except asyncio.CancelledError:
            log.info("worker=%s cancelled", name)
            raise
        except Exception:
            log.exception("worker=%s job=%s failed", name, job.job_id)
            await db_call("UPDATE jobs SET status=? WHERE job_id=?",
                          ("failed", job.job_id))
        finally:
            q.task_done()

async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    await db_call("CREATE TABLE IF NOT EXISTS jobs (job_id TEXT PRIMARY KEY, source_url TEXT NOT NULL, status TEXT NOT NULL, result TEXT)")
    q: asyncio.Queue[Job | None] = asyncio.Queue(maxsize=QUEUE_LIMIT)
    async with asyncio.TaskGroup() as tg:
        for n in range(WORKERS): tg.create_task(worker(f"w{n}", q))
        for i in range(6):
            job = Job(f"job-{i}", "https://api.example.test/items/" + str(i))
            await db_call("INSERT OR IGNORE INTO jobs(job_id,source_url,status) VALUES(?,?,?)",
                          (job.job_id, job.source_url, "queued"))
            # wait_for makes overload visible instead of waiting forever.
            await asyncio.wait_for(q.put(job), timeout=2)
        await q.join()
        for _ in range(WORKERS): await q.put(None)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
```

### Deployment and operations context

**Local:** inspect `intake-demo.db`; slow workers should make producers pause at four queued jobs. **Server:** an in-process queue is tied to one process; restart loses queued memory. **Production:** keep `process()`, replace SQLite with PostgreSQL, and publish a small JSON command to a durable queue. Use a unique job/source key [8] and monitor depth, age, latency, failures, and event-loop lag.

### Failure, logging, retry, idempotency, and security notes

Treat malformed URLs and authorization failures as permanent; retry only bounded transient timeouts with backoff and jitter. Log `job_id`, operation, attempt, duration, and outcome, never tokens or sensitive payloads. Re-raise cancellation after cleanup. SQLite is not a multi-replica queue: **this demo has no crash recovery for queued items**; production needs durable ack/visibility and an outbox or transaction.

### Hands-on build step

Add `POST /intakes` later, but first implement a CLI intake producer that generates stable UUIDs and inserts `queued` rows. Add a replay command that submits the same `job_id` twice and prove `INSERT OR IGNORE` leaves one row. This is the first capstone slice: bounded processing, status persistence, and a repeat-safe identity.

## Chapter 2 — Choosing a scheduler: cron, in-process jobs, and durable background work

### Overview, objectives, and architectural importance

Learners will distinguish a **trigger** from a **work queue**, choose cron/systemd, in-process scheduling, or durable queues, explain overlap/DST duplicates, and use FastAPI `BackgroundTasks` only for small non-critical work.

Cron examines entries each minute and runs matching commands; DST missing times do not run and repeated times can run twice [3]. It is host-local and has no durable job record or distributed election. An in-process scheduler dies with its process and can duplicate across replicas; a durable queue separates trigger from scalable execution and can redeliver unacknowledged work.

FastAPI `BackgroundTasks` runs after the response and suits small notifications. FastAPI recommends a larger tool such as Celery for heavy work across processes or servers [4]. Treat it as non-durable.

### Ordered topics

1. **Cron/systemd timer.** Best for “invoke a command at 02:00,” database backups, or a reconciliation command on a controlled host. Redirect output to journald/a log sink, use a least-privilege service account, and make the command idempotent. Use UTC where possible and document DST behavior.
2. **In-process scheduler.** A loop using `asyncio.sleep` is fine for a one-process demo. Add a lock/lease if only one instance may schedule; otherwise two replicas enqueue duplicates.
3. **Durable queue.** The scheduler publishes a command; workers fetch, process, and acknowledge. Queue systems normally provide at-least-once delivery, so use idempotency keys and status transitions.
4. **FastAPI boundary.** Return `202 Accepted` after recording the intake and enqueueing/persisting the job. A `BackgroundTasks` callback is a demo shortcut only; a crash after response can lose it.
5. **Overlap and clock discipline.** A periodic task can overlap its previous run. Use a DB lease/advisory lock or a unique schedule window; use timezone-aware UTC timestamps.

### Complete FastAPI + SQLite/mock blueprint (development demo)

Install the development dependencies with `python -m pip install fastapi uvicorn`. Save as `api_demo.py`; run `uvicorn api_demo:app --reload`. It records before scheduling and uses `BackgroundTasks` to demonstrate the boundary, not durability.

```python
from __future__ import annotations
import asyncio, sqlite3, uuid
from fastapi import BackgroundTasks, FastAPI, HTTPException, status
from pydantic import BaseModel, AnyHttpUrl

DB = "intake-demo.db"; APPROVED_HOST = "api.example.test"
app = FastAPI(title="Reliable Intake demo")
class Intake(BaseModel): source_url: AnyHttpUrl

def insert(job_id: str, url: str) -> bool:
    with sqlite3.connect(DB) as con:
        con.execute("CREATE TABLE IF NOT EXISTS jobs (job_id TEXT PRIMARY KEY, source_url TEXT NOT NULL, status TEXT NOT NULL, result TEXT)")
        cur = con.execute("INSERT OR IGNORE INTO jobs VALUES (?, ?, 'queued', NULL)", (job_id, url))
        con.commit(); return cur.rowcount == 1

def mark_done(job_id: str) -> None:
    # Demo only: a process crash between response and this call loses the work.
    with sqlite3.connect(DB) as con:
        con.execute("UPDATE jobs SET status='done', result='mocked' WHERE job_id=?", (job_id,)); con.commit()

def schedule_one(job_id: str, url: str) -> None:
    # Demo only; production publishes {"job_id": job_id} to a durable queue.
    assert url.startswith("https://api.example.test/")
    mark_done(job_id)

@app.post("/intakes", status_code=status.HTTP_202_ACCEPTED)
async def create_intake(item: Intake, background_tasks: BackgroundTasks):
    if item.source_url.host != APPROVED_HOST:
        raise HTTPException(400, "Only the approved API host is allowed")
    job_id = str(uuid.uuid4())
    await asyncio.to_thread(insert, job_id, str(item.source_url))
    background_tasks.add_task(schedule_one, job_id, str(item.source_url))
    return {"job_id": job_id, "status": "queued", "durability": "demo-only"}
```

For the capstone, replace `background_tasks.add_task` with a transactional outbox: insert `jobs` and an `outbox` row in one PostgreSQL transaction, then publish and mark the outbox record. This closes the database-commit/publish gap.

### Deployment and operations context

**Local:** kill Uvicorn between response and task execution to demonstrate loss. **Server:** use a systemd timer or one elected scheduler. Compose waits only for running containers unless healthchecks and `service_healthy` are configured [7]; readiness is not durability. Docker recommends restart policies [6]. **Production:** scale API, scheduler, workers, PostgreSQL, and broker separately. Managed queues fit teams wanting provider-operated availability, IAM, metrics, DLQ, and elastic consumers. SQS visibility timeout hides a received message and redelivers it if not deleted; standard SQS is at-least-once [9]. Pub/Sub dead-letter forwarding and attempt counts are best effort/approximate [11].

### Failure, logging, retry, idempotency, and security notes

A scheduler outage can miss a trigger; define catch-up behavior. Overlap requires a lease or unique schedule window. A broker outage must produce an error or outbox backlog, not a false `queued` claim. Log schedule, UTC fire time, enqueue result, and correlation ID. Validate scheme/host, block private destinations, cap responses, and set timeouts. Keep credentials in secret management with TLS and least privilege; task-message readers can access arguments even when logs redact them [5].

### Hands-on build step

Add `GET /intakes/{job_id}` and a `reconcile` command that republishes old `queued` rows. Run two API replicas with the same periodic loop to reproduce duplicate scheduling; then add a unique `(source_url, schedule_window)` constraint. This creates the capstone’s safe intake API and recovery path.

## Chapter 3 — Celery in production: broker, workers, Beat, and reliable task boundaries

### Overview, objectives, and architectural importance

Learners will trace producer→broker→worker flow, separate Beat from execution, configure retries/acks/visibility/time limits/DLQs, and design a PostgreSQL-safe task boundary. Celery acknowledges before execution by default; late-acked tasks can be redelivered after a worker dies [5].

Celery Beat publishes periodic tasks; available workers execute them. Celery’s docs require only one scheduler for a schedule or duplicate tasks result [10]. Beat is not a lock, and a periodic task can overlap itself, so use a database lease or idempotency key. Celery’s retry creates another message with the same task ID [5]; it is not a guarantee of one side effect.

### Ordered topics

1. **Broker and workers.** The broker carries messages; workers execute them. A result backend is optional, not a business database; persist authoritative status in PostgreSQL.
2. **Task boundary.** Send `{job_id}` and immutable metadata, not a file blob or secret. The worker loads the row, claims it, performs authorized I/O, and commits an idempotent result.
3. **Retry policy.** Retry transient network/5xx/connection failures with capped backoff and jitter, not validation or authorization failures. Cap attempts and quarantine exhausted work.
4. **Acknowledgement and visibility.** `acks_late=True` acknowledges after execution, which can cause duplicate execution after a crash; enable only for idempotent work. A visibility timeout must exceed normal processing plus margin, or the broker may redeliver while work is still running. Celery exposes broker transport visibility settings [5].
5. **Time limits and prefetch.** Put explicit HTTP/database timeouts in code. Soft limits allow cleanup; hard limits terminate a worker process, so they are a last guard, not a substitute for I/O timeouts [5]. Tune prefetch for long tasks; monitor queue age.
6. **Dead letters and operations.** Configure a DLQ, alarm on depth/oldest age, inspect safely, fix the cause, and replay a controlled attempt. Provider delivery, retention, ordering, and IAM semantics differ.

### Complete near-runnable Celery blueprint (development-to-production shape)

**Development assumptions:** a reachable Redis broker at `redis://localhost:6379/0`, SQLite for demonstration, and Celery installed in the virtual environment. **Production replacement:** PostgreSQL URL, TLS broker, secret manager, and a configured DLQ. Save as `celery_app.py`; start `celery -A celery_app worker --loglevel=INFO` and, separately, `celery -A celery_app beat --loglevel=INFO`.

```python
from __future__ import annotations
import os, sqlite3, time, logging
from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded

BROKER = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
app = Celery("intake", broker=BROKER)  # Add result_backend only if task state is needed.
app.conf.update(
    task_serializer="json", accept_content=["json"], timezone="UTC", enable_utc=True,
    task_acks_late=True, task_reject_on_worker_lost=True,
    task_soft_time_limit=45, task_time_limit=60,
    broker_transport_options={"visibility_timeout": 120},
    worker_prefetch_multiplier=1,
    beat_schedule={"reconcile-every-minute": {
        "task": "celery_app.reconcile", "schedule": 60.0,
    }},
)
log = logging.getLogger(__name__)
DB = os.environ.get("INTAKE_DB", "intake-demo.db")

def claim(job_id: str) -> bool:
    # Production: one PostgreSQL transaction with SELECT ... FOR UPDATE SKIP LOCKED,
    # or an atomic UPDATE guarded by status and a lease expiry.
    with sqlite3.connect(DB) as con:
        cur = con.execute("UPDATE jobs SET status='processing' WHERE job_id=? AND status='queued'", (job_id,))
        con.commit(); return cur.rowcount == 1

def release_for_retry(job_id: str) -> None:
    # Handles a caught/graceful failure. A hard process kill cannot run this;
    # production must reclaim expired processing leases during reconciliation.
    with sqlite3.connect(DB) as con:
        con.execute("UPDATE jobs SET status='queued' WHERE job_id=? AND status='processing'", (job_id,))
        con.commit()

def complete(job_id: str, result: str) -> None:
    with sqlite3.connect(DB) as con:
        con.execute("UPDATE jobs SET status='done', result=? WHERE job_id=?", (result, job_id)); con.commit()

@app.task(bind=True, autoretry_for=(TimeoutError, ConnectionError), retry_backoff=True,
          retry_jitter=True, max_retries=5, acks_late=True)
def process_intake(self, job_id: str, source_url: str) -> str:
    if not source_url.startswith("https://api.example.test/"):
        raise ValueError("unapproved source")  # permanent: no retry
    if not claim(job_id):
        log.info("duplicate_or_already_claimed job_id=%s", job_id)
        return "already-claimed-or-complete"
    try:
        # Replace mock with an authorized HTTP client and explicit connect/read timeout.
        time.sleep(0.1)
        complete(job_id, "validated mock payload")
        log.info("processed job_id=%s task_id=%s", job_id, self.request.id)
        return "done"
    except SoftTimeLimitExceeded:
        log.warning("soft_timeout job_id=%s", job_id)
        release_for_retry(job_id)
        raise
    except Exception:
        log.exception("failed job_id=%s attempt=%s", job_id, self.request.retries)
        release_for_retry(job_id)
        raise

@app.task
def reconcile() -> int:
    # Beat only triggers; it should enqueue/recover bounded work, not do heavy work.
    # This local demo requeues only rows still marked queued. Production also
    # expires/reclaims processing rows whose lease_until has elapsed.
    with sqlite3.connect(DB) as con:
        rows = con.execute("SELECT job_id, source_url FROM jobs WHERE status='queued' LIMIT 100").fetchall()
    for job_id, url in rows:
        process_intake.delay(job_id, url)
    return len(rows)
```

`autoretry_for` is intentionally narrow; classify exceptions and make updates idempotent. A crash after `complete()` but before acknowledgement may redeliver; the guarded claim makes the second run a no-op. Add a PostgreSQL `UNIQUE` external identity and atomic state machine, but do not call this exactly once: a crash can split an external side effect from local commit. Use an outbox, external idempotency key, or compensation.

### Deployment and operations context

**Local:** run Redis and the app with Compose healthchecks [7]. **Server:** run API, worker, and one Beat separately; use container restart policies [6]. **Production:** use PostgreSQL, a durable or managed broker, and separate I/O/CPU worker pools. Use TLS, rotated credentials, least privilege, JSON serialization, and alerts for queue age, crashes, retries, DLQ, runtime, and DB contention. SQS uses `maxReceiveCount` and operator redrive [10]; Google/Azure semantics differ [11] [12].

### Failure, logging, retry, idempotency, and security notes

A broker outage can make publishing fail; expose an outbox backlog metric and reconcile. A worker kill can duplicate work; late acknowledgement improves loss behavior only when effects are idempotent. A too-short visibility timeout causes concurrent duplicates; a too-long one delays recovery. A hard time limit can terminate a process without application cleanup. A poison message needs a bounded retry and quarantine, not infinite retry. Correlate HTTP request ID, `job_id`, Celery task ID, attempt, queue, and source; emit JSON logs and traces. Redact URLs containing query secrets and all credentials. Enforce source allow-lists and response-size/time limits, and fetch only from authorized APIs. Review broker ACLs so producers cannot consume admin/DLQ queues.

### Hands-on build step

Replace `BackgroundTasks` with `process_intake.delay(job_id, source_url)`. Add PostgreSQL `jobs(job_id primary key, external_key unique, status, attempts, lease_until, result, created_at, updated_at)`. Kill a worker during processing and verify redelivery produces one final result; send a poison payload, verify bounded retries/DLQ alert, then replay after fixing validation. The capstone now has an API, durable state, reconciliation, scalable workers, and honest at-least-once semantics.

## References

[1]: https://docs.python.org/3/library/asyncio.html "Python asyncio — Asynchronous I/O"
[2]: https://docs.python.org/3/library/asyncio-task.html "Python asyncio — Coroutines and Tasks"
[3]: https://man7.org/linux/man-pages/man5/crontab.5.html "crontab(5) — Linux manual page"
[4]: https://fastapi.tiangolo.com/tutorial/background-tasks/ "FastAPI Background Tasks"
[5]: https://docs.celeryq.dev/en/stable/userguide/tasks.html "Celery Tasks"
[6]: https://docs.docker.com/engine/containers/start-containers-automatically/ "Docker Start containers automatically"
[7]: https://docs.docker.com/compose/how-tos/startup-order/ "Docker Compose startup order"
[8]: https://www.postgresql.org/docs/current/ddl-constraints.html "PostgreSQL Constraints"
[9]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html "Amazon SQS visibility timeout"
[10]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html "Amazon SQS dead-letter queues"
[11]: https://cloud.google.com/pubsub/docs/dead-letter-topics "Google Cloud Pub/Sub dead-letter topics"
[12]: https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dead-letter-queues "Azure Service Bus dead-letter queues"
[13]: https://docs.celeryq.dev/en/main/userguide/periodic-tasks.html "Celery Periodic Tasks"
