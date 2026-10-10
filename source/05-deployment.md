# Module 5 — Linux, containers, observability, and production security

**Module id:** `deployment`  
**Python baseline:** Python 3.11+ (the examples avoid version-specific third-party APIs).  
**Capstone:** Reliable Data Intake & Processing Service (RDIPS): accept an authorized JSON payload, persist it, process it asynchronously, expose health/metrics, and operate it safely.

## Chapter 1 — Linux foundations and a reliable local service

### Overview, objectives, and why this matters

A production API is a set of processes with identities, files, ports, startup rules, and failure behavior—not merely Python code. Learners create a non-root user, reason about permissions, run a supervised process, and build RDIPS with SQLite and a mock queue. This is a **development demo**; the production boundary is explicit.

The architecture is: client → API → durable intake row → queue → worker → processed result. SQLite is a useful prototype because it is a disk database with no separate server and can later be ported to PostgreSQL, but it is not a substitute for a multi-client production database [1]. A queue gives the API a short response path and lets workers retry independently. Do not promise exactly-once delivery: the worker can crash after the side effect and before acknowledgement, so design the operation to be idempotent.

**Learning objectives:** (1) inspect users, groups, modes, ownership, and least privilege; (2) write a systemd service and timer; (3) implement logs, timeouts, health/readiness, and idempotency; (4) test failure/restart behavior; and (5) explain why production replaces SQLite and the local queue.

### Ordered topics

1. **Linux identity and files.** Create a dedicated `rdips` user with no interactive shell; keep code readable by that user, data writable only in `/var/lib/rdips`, and secrets outside the repository. Use `chmod`, `chown`, and `umask` deliberately. Never run the API as root.
2. **Processes and supervision.** Run the app in the foreground. A `.service` unit has `[Unit]`, `[Service]`, and `[Install]`; `ExecStart` names the process, and `Restart=on-failure` delegates recovery to systemd. systemd documents service units as processes it controls and supervises [2].
3. **Timers.** A `.timer` activates a matching `.service`; `OnCalendar=` is wall-clock based and is subject to timer accuracy, so scheduled work must be safe to run late or twice [3]. Use a timer for cleanup or backup, not as a substitute for a durable queue.
4. **Application contract.** `POST /intake` requires an idempotency key. `/healthz` means the process is alive; `/readyz` checks that the database is usable. Log event name, request id, job id, duration, and outcome, but never payload secrets.
5. **Authorized sources only.** Permit only documented, authorized upstream APIs and respect terms, authentication, rate limits, and access controls; this course does not teach bypasses or anti-bot evasion.

### Code blueprint: local SQLite + mock queue

Save as `app.py`; install `fastapi` and an ASGI server, then run `uvicorn app:app --host 127.0.0.1 --port 8000`. `DB_PATH` defaults to `./rdips.db`.

```python
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hashlib, json, logging, os, queue, sqlite3, threading, time, uuid
from typing import Any
from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

DB_PATH = os.getenv("DB_PATH", "./rdips.db")
JOBS: queue.Queue[str] = queue.Queue()
STOP = threading.Event()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(message)s")
log = logging.getLogger("rdips")

class Intake(BaseModel):
    source: str = Field(min_length=1, max_length=120)
    data: dict[str, Any]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=5)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000")
    return con


def init_db() -> None:
    with db() as con:
        con.execute("""CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY, idem TEXT NOT NULL UNIQUE, source TEXT NOT NULL,
          payload TEXT NOT NULL, status TEXT NOT NULL, result TEXT,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")


def worker() -> None:
    while not STOP.is_set():
        try: job_id = JOBS.get(timeout=0.5)
        except queue.Empty: continue
        try:
            with db() as con:
                row = con.execute("SELECT payload,status FROM jobs WHERE id=?", (job_id,)).fetchone()
                if not row or row["status"] == "done": continue  # idempotent replay
                con.execute("UPDATE jobs SET status='processing',updated_at=? WHERE id=?", (now(), job_id))
                payload = json.loads(row["payload"])
                # Replace this deterministic demo with an authorized API call or transform.
                digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
                con.execute("UPDATE jobs SET status='done',result=?,updated_at=? WHERE id=?",
                            (json.dumps({"sha256": digest}), now(), job_id))
                log.info(json.dumps({"event":"job_done", "job_id":job_id}))
        except Exception:
            log.exception("job_failed job_id=%s", job_id)
            with db() as con: con.execute("UPDATE jobs SET status='failed',updated_at=? WHERE id=?", (now(), job_id))
        finally: JOBS.task_done()

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db(); t = threading.Thread(target=worker, daemon=True); t.start()
    yield; STOP.set(); t.join(timeout=2)

app = FastAPI(title="RDIPS", lifespan=lifespan)

@app.get("/healthz")
def health() -> dict[str, str]: return {"status": "ok"}

@app.get("/readyz")
def ready() -> dict[str, str]:
    try:
        with db() as con: con.execute("SELECT 1").fetchone()
        return {"status": "ready"}
    except sqlite3.Error as exc:
        raise HTTPException(503, "database unavailable") from exc

@app.post("/intake", status_code=202)
def intake(item: Intake, response: Response, idempotency_key: str | None = Header(default=None)):
    if not idempotency_key: raise HTTPException(400, "Idempotency-Key is required")
    job_id = str(uuid.uuid4())
    try:
        with db() as con:
            con.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?)",
              (job_id, idempotency_key, item.source, item.model_dump_json(), "queued", None, now(), now()))
    except sqlite3.IntegrityError:
        with db() as con:
            row = con.execute("SELECT id,status FROM jobs WHERE idem=?", (idempotency_key,)).fetchone()
        response.status_code = 200
        return dict(row)
    JOBS.put(job_id); log.info(json.dumps({"event":"accepted", "job_id":job_id}))
    return {"id": job_id, "status": "queued"}

@app.get("/jobs/{job_id}")
def job(job_id: str):
    with db() as con: row = con.execute("SELECT id,status,result FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row: raise HTTPException(404, "job not found")
    return dict(row)

@app.get("/metrics")
def metrics():
    with db() as con:
        counts = con.execute("SELECT status,count(*) n FROM jobs GROUP BY status").fetchall()
    body = "".join(f'rdips_jobs{{status="{r["status"]}"}} {r["n"]}\n' for r in counts)
    return Response(body, media_type="text/plain; version=0.0.4")
```

The SQL uses placeholders, as Python recommends to prevent injection [1]. HTTPX integrations should set explicit connect/read/write/pool timeouts; its default is five seconds of network inactivity [4]. Production replaces `queue.Queue` with Celery/Redis or a cloud queue and stores a unique job key in PostgreSQL.

### Local/server operations, failures, and build step

**Local:** run the app, submit twice with the same `Idempotency-Key`, kill it during processing, restart it, and observe that the local queue loses unstarted jobs. That loss is an intentional lesson. **Server:** create `/etc/systemd/system/rdips.service`:

```ini
[Unit]
Description=RDIPS API (development-style host service)
After=network.target
[Service]
User=rdips
WorkingDirectory=/opt/rdips
ExecStart=/opt/rdips/.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000
Restart=on-failure
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true
[Install]
WantedBy=multi-user.target
```

Use `systemctl daemon-reload && systemctl enable --now rdips`, and `journalctl -u rdips -f`. Retries must be bounded with backoff; classify 4xx as usually non-retryable and timeouts/5xx as retryable. Never log tokens, database URLs, or raw sensitive payloads. **Hands-on build:** add a `cleanup.service` (`Type=oneshot`) and matching `cleanup.timer` for old demo rows. Verify `systemctl list-timers`, then document why production cleanup needs retention and backup checks.

## Chapter 2 — Containers and the API/PostgreSQL/Redis/worker/Beat layout

### Overview, objectives, and why this matters

Containers package one artifact for laptop, CI, and VPS. Learners build a non-root image, use Compose for the five-service RDIPS topology, add health checks, migrate the schema, and switch to PostgreSQL/Redis/Celery. Docker says `HEALTHCHECK` distinguishes a stuck process from a healthy one, and Compose can wait for `service_healthy` dependencies [5] [6]. This is a **development/staging blueprint**, not high availability.

**Objectives:** (1) separate build-time code from run-time configuration; (2) use volumes, networks, health checks, and dependencies; (3) make tasks safe under duplicate delivery; (4) use secrets correctly; and (5) compare self-hosted and managed data/queue services.

### Ordered topics and blueprint

1. **Image.** Pin a Python base by organizational policy, do not copy `.env` or keys into the image, set a non-root `USER`, and run one foreground process per container. FastAPI’s deployment guidance notes that containers commonly run one Uvicorn process while the platform handles replication [7].
2. **Compose topology.** `api` exposes HTTP; `postgres` owns durable rows; `redis` is broker/result transport; `worker` consumes tasks; `beat` emits periodic tasks. Only `api` and the reverse proxy should be reachable from outside the private Compose network.
3. **Task semantics.** Acknowledge only after the database transaction commits. A worker crash can cause redelivery; the task must check the unique idempotency key or a processed marker. This is at-least-once processing, not exactly once.
4. **Secrets.** Compose secrets mount files under `/run/secrets/<name>` and grant them per service; Docker warns that environment variables can leak through processes or logs [8]. In production, prefer a cloud secret manager or Vault, rotation, audit, and least privilege.

`Dockerfile`:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN useradd --create-home --uid 10001 appuser
USER appuser
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')"
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

Add a `.dockerignore` so the build context cannot accidentally copy local credentials, data, or virtual environments into the image:

```text
.venv/
__pycache__/
*.py[cod]
.env
.env.*
!.env.example
secrets/
var/
*.sqlite
.git/
```

`requirements.txt` (choose and review current compatible versions in a lock file; no version is asserted here): `fastapi`, `uvicorn[standard]`, `celery`, `redis`, `psycopg[binary]`, `prometheus-client`, `alembic`.

`celery_app.py` and `tasks.py` (near-runnable production queue adapter):

```python
# celery_app.py
import os
from celery import Celery
broker = os.environ["CELERY_BROKER_URL"]
celery = Celery("rdips", broker=broker, backend=os.getenv("CELERY_RESULT_BACKEND", broker))
celery.conf.update(task_acks_late=True, task_reject_on_worker_lost=True,
                   broker_connection_retry_on_startup=True, timezone="UTC")

# tasks.py
import hashlib, json, os, psycopg
from pathlib import Path
from celery_app import celery

@celery.task(bind=True, autoretry_for=(ConnectionError, TimeoutError),
             retry_backoff=True, retry_kwargs={"max_retries": 5})
def process_job(self, job_id: str) -> None:
    # Compose mounts this secret only into API/worker services that need it.
    password_file = Path(os.environ.get("DB_PASSWORD_FILE", "/run/secrets/db_password"))
    password = password_file.read_text(encoding="utf-8").strip()
    with psycopg.connect(host=os.environ.get("DB_HOST", "postgres"),
                         dbname=os.environ.get("DB_NAME", "rdips"),
                         user=os.environ.get("DB_USER", "rdips"), password=password,
                         connect_timeout=5) as con, con.cursor() as cur:
        cur.execute("SELECT payload,status FROM jobs WHERE id=%s FOR UPDATE", (job_id,))
        row = cur.fetchone()
        if not row or row[1] == "done": return
        digest = hashlib.sha256(json.dumps(row[0], sort_keys=True).encode()).hexdigest()
        cur.execute("UPDATE jobs SET status='done', result=%s, updated_at=now() WHERE id=%s",
                    (json.dumps({"sha256": digest}), job_id))
```

Adapt the API’s insert transaction to call `process_job.delay(job_id)` only after commit (an outbox table is safer if the API must not lose a publish between commit and enqueue). `compose.yaml`:

```yaml
services:
  api:
    build: .
    environment:
      DB_HOST: postgres
      DB_NAME: rdips
      DB_USER: rdips
      DB_PASSWORD_FILE: /run/secrets/db_password
      CELERY_BROKER_URL: redis://redis:6379/0
    secrets: [db_password]
    depends_on: {postgres: {condition: service_healthy}, redis: {condition: service_healthy}}
    ports: ["127.0.0.1:8000:8000"]
  worker:
    build: .
    command: celery -A celery_app.celery worker --loglevel=INFO
    environment:
      DB_HOST: postgres
      DB_NAME: rdips
      DB_USER: rdips
      DB_PASSWORD_FILE: /run/secrets/db_password
      CELERY_BROKER_URL: redis://redis:6379/0
    secrets: [db_password]
    depends_on: {postgres: {condition: service_healthy}, redis: {condition: service_healthy}}
  beat:
    build: .
    command: celery -A celery_app.celery beat --loglevel=INFO
    environment: {CELERY_BROKER_URL: redis://redis:6379/0}
    depends_on: {redis: {condition: service_healthy}}
  postgres:
    image: postgres
    environment: {POSTGRES_DB: rdips, POSTGRES_USER: rdips, POSTGRES_PASSWORD_FILE: /run/secrets/db_password}
    secrets: [db_password]
    volumes: [pgdata:/var/lib/postgresql/data]
    healthcheck: {test: ["CMD-SHELL", "pg_isready -U rdips -d rdips"], interval: 10s, timeout: 5s, retries: 5}
  redis:
    image: redis
    command: ["redis-server", "--appendonly", "yes"]
    volumes: [redisdata:/data]
    healthcheck: {test: ["CMD", "redis-cli", "ping"], interval: 10s, timeout: 3s, retries: 5}
volumes: {pgdata: {}, redisdata: {}}
secrets:
  db_password: {file: ./secrets/db_password.txt}
```

The YAML is illustrative: use a lockfile, image digests, non-default credentials, resource limits, a private network, and a current PostgreSQL/Redis policy before shipping. Compose’s specification is recommended [9]; Celery recommends init scripts or process supervision in production [10].

### Operations, tradeoffs, and build step

**Local:** `docker compose up --build`, run migrations, submit an intake request, stop the worker, and watch the broker retain work. **VPS:** use a private network, expose only the proxy, persist data on encrypted disks, and ship logs centrally. **Managed alternatives:** managed PostgreSQL supplies patching/backups; a managed queue (SQS, Cloud Tasks, or equivalent) supplies durable delivery and elastic consumers. AWS documents at-least-once SQS-triggered Lambda processing, so duplicates and idempotency remain mandatory [11]. Cloud Tasks dispatches asynchronous work to HTTP endpoints [12]. Serverless trades host operations for provider limits, cold starts, execution limits, egress cost, and less network control. A queue does not make a non-idempotent side effect safe.

Failure cases include a database not ready (health gating is not a substitute for retries), Redis loss, a poisoned payload, and a worker killed after the side effect. Log task id, attempt, latency, and database outcome. **Hands-on build:** migrate the Chapter 1 schema to PostgreSQL, route `/intake` to `process_job.delay`, add a Beat task to mark abandoned rows, and invoke the task twice. Record the invariant: one job row and final result, possibly multiple attempts.

## Chapter 3 — VPS production runbook: TLS, observability, security, backups, and release safety

### Overview, objectives, and why this matters

A single VPS is a teachable RDIPS target: Nginx terminates TLS, Compose runs internal services, UFW exposes only SSH/HTTP/HTTPS, and backups leave the machine. It is one failure domain, not high availability.

**Objectives:** configure TLS and least-privilege keys; define health, logs, metrics, and alerts; harden SSH/firewall/secrets/updates; rehearse PostgreSQL restore; use safe migrations and rollback; and select managed components when a VPS is insufficient.

### Ordered production topics and code/config blueprint

1. **Host hardening.** Create an unprivileged deploy user, disable password SSH after testing keys, patch on a schedule, restrict `ufw` to trusted SSH plus 80/443, and do not publish Postgres or Redis. Ubuntu documents `ufw` and its status/rule workflow [13]. Keep time synchronized because timers depend on wall-clock behavior [3].
2. **Reverse proxy/TLS.** Nginx listens on 80/443 and proxies to `127.0.0.1:8000`. Its official example specifies certificate paths and TLS 1.2/1.3; private keys need restricted access [14]. Automate and test ACME renewal, redirect HTTP, and pass `X-Request-ID`.
3. **Observability.** Emit JSON logs to stdout/journald and forward them off-host. Expose Prometheus counters for request latency, queue depth, failures, retries, and backup age; client libraries expose a scrape endpoint [15]. Alert on 5xx, readiness, queue age, disk, certificate, backup, and restore failures. Redact both logs and metrics.
4. **Database safety.** Run Alembic migrations as a release step, not on worker start. Alembic tracks revisions and applies `upgrade head` [16]. Prefer expand/contract: add new structures, deploy compatible code, backfill, then enforce constraints and remove old fields.
5. **Backup/restore.** A nightly encrypted `pg_dump -Fc` to off-host storage is a baseline; custom dumps restore with `pg_restore` and are internally consistent snapshots [17]. Configure `PGSERVICEFILE` as a mode-0600 protected service definition so the password is not placed in a command-line argument. Back up recovery material separately. A backup is untrusted until an isolated restore succeeds.
6. **Release and rollback.** Build an immutable commit-tagged image, test and migrate in staging, back up, deploy, smoke-test, and monitor. Roll back code, not an irreversible data migration; use a compatibility fix or restore/cut over.

`/etc/nginx/sites-available/rdips` (production-shaped; replace the domain and certificate paths):

```nginx
server { listen 80; server_name intake.example.com; return 301 https://$host$request_uri; }
server {
  listen 443 ssl; server_name intake.example.com;
  ssl_certificate /etc/letsencrypt/live/intake.example.com/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/intake.example.com/privkey.pem;
  ssl_protocols TLSv1.2 TLSv1.3;
  client_max_body_size 1m;
  location / {
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto https;
    proxy_set_header X-Request-ID $request_id;
    proxy_pass http://127.0.0.1:8000;
  }
}
```

`backup.sh` (run as a protected systemd service/timer, with a real off-host uploader added):

```bash
#!/usr/bin/env bash
set -euo pipefail
umask 077
stamp=$(date -u +%Y%m%dT%H%M%SZ)
out="/var/backups/rdips/rdips-${stamp}.dump"
mkdir -p "$(dirname "$out")"
: "${PGSERVICEFILE:?Set a protected libpq service file before running this job}"
export PGSERVICEFILE
PGSERVICE=${PGSERVICE:-rdips-backup}
pg_dump --format=custom --no-owner --file="$out" "$PGSERVICE"
sha256sum "$out" > "$out.sha256"
# This root-owned wrapper must return success only after both files are stored
# off-host and the remote checksum is verified. Never pass credentials on argv.
/usr/local/bin/rdips-backup-upload "$out" "$out.sha256"
find /var/backups/rdips -type f -mtime +14 -delete
```

### Failure, security, and hands-on build

A TLS renewal failure is caught by expiry alerts and a dry-run renewal; disk alerts precede database failure; staging and backups catch bad migrations. A compromised secret triggers revoke/rotate, log review, and redeploy. OWASP recommends least privilege, rotation, revocation, expiry, auditing, encrypted backups, and tested restore [18]. Never put secrets in Git, image layers, URLs, exception text, or metric labels.

**Hands-on build:** provision an Ubuntu VPS; install Docker, Nginx, UFW, and ACME; deploy Compose under systemd; add `backup.service`/`backup.timer`; scrape `/metrics`; trigger a failed task and alert; restore into disposable PostgreSQL; and roll back a tagged image. Finish a runbook with owner, RPO/RTO, restore date, alert actions, and rollback commands.

Managed PostgreSQL supplies backups/patching; managed Redis/queues remove broker operations; container platforms handle rollout/replacement; Lambda plus SQS/Cloud Tasks suits short, bursty handlers but still needs idempotency and dead letters. The VPS exposes failure modes before managed abstractions hide them.

## References

[1]: https://docs.python.org/3/library/sqlite3.html "Python sqlite3 — DB-API 2.0 interface for SQLite databases"
[2]: https://www.freedesktop.org/software/systemd/man/systemd.service.html "systemd.service — Service unit configuration"
[3]: https://man7.org/linux/man-pages/man5/systemd.timer.5.html "systemd.timer(5) — timer unit configuration"
[4]: https://www.python-httpx.org/advanced/timeouts/ "HTTPX timeouts"
[5]: https://docs.docker.com/reference/dockerfile/ "Dockerfile reference — HEALTHCHECK"
[6]: https://docs.docker.com/reference/compose-file/services/ "Compose services reference — healthcheck and depends_on"
[7]: https://fastapi.tiangolo.com/deployment/server-workers/ "FastAPI — Server Workers"
[8]: https://docs.docker.com/compose/how-tos/use-secrets/ "Docker Compose — Manage secrets securely"
[9]: https://docs.docker.com/reference/compose-file/ "Docker Compose file reference"
[10]: https://docs.celeryq.dev/en/stable/userguide/workers.html "Celery Workers Guide"
[11]: https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html "AWS Lambda — Using Lambda with Amazon SQS"
[12]: https://docs.cloud.google.com/tasks/docs "Google Cloud Tasks documentation"
[13]: https://ubuntu.com/server/docs/how-to/security/firewalls/ "Ubuntu Server — Firewall"
[14]: https://nginx.org/en/docs/http/configuring_https_servers.html "Nginx — Configuring HTTPS servers"
[15]: https://prometheus.io/docs/instrumenting/clientlibs/ "Prometheus — Client libraries"
[16]: https://alembic.sqlalchemy.org/en/latest/tutorial.html "Alembic — Tutorial and migration workflow"
[17]: https://www.postgresql.org/docs/current/backup-dump.html "PostgreSQL — SQL dump backup and restore"
[18]: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html "OWASP — Secrets Management Cheat Sheet"
