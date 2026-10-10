# Module 2 — Robust scripting and local/remote automation

**Module id:** `scripting`  
**Python baseline:** Python 3.11+. Examples use standard-library APIs available on that baseline; third-party packages are named without invented version pins. The capstone is one coherent **Reliable Data Intake & Processing Service**.

## Chapter 1 — Contract-first CLI, configuration, and durable file intake

### Overview, objectives, and architecture relevance

A production script is a small service: it has an input contract, configuration boundary, observable outcomes, and a recovery story. By the end of this chapter, learners can (1) design an explicit CLI with safe defaults and a `--dry-run` mode, (2) layer environment/configuration without putting secrets in source, (3) stream CSV and JSON Lines instead of loading unbounded files, (4) validate records before side effects, (5) calculate a SHA-256 content identity, and (6) make checkpoints and output publication restartable.

This matters in server architecture because intake jobs often run under cron, systemd, a container, or a queue worker. A predictable exit code, bounded memory, and durable checkpoint let an orchestrator decide whether to retry. A database uniqueness constraint is a useful *deduplication mechanism*, not proof of exactly-once delivery: a process can crash between any two external side effects.

### Ordered topics

1. **Interface first.** Define input format, required fields, maximum file/field sizes, exit codes, and whether a repeated `item_id` means “same item” or “conflict.” Keep CLI flags for operator intent and environment variables for deployment-specific values. Environment values arrive as strings and must be converted and validated in code; FastAPI’s settings guidance documents this pattern and dependency-cached settings for tests [1].
2. **Paths and formats.** Use `pathlib.Path`, explicit UTF-8, and `newline=''` for CSV. Python’s CSV reader is iterable and does not perform automatic type conversion; JSON is not a framed protocol, so a stream of repeated `json.dump()` calls is invalid JSON [2] [3]. For large feeds, choose CSV rows or JSON Lines (one JSON object per line), not one giant JSON array.
3. **Validation before effects.** Reject missing/unknown identifiers, oversized fields, invalid JSON, and unsupported formats before writing to the database or invoking a remote command. Treat filenames, metadata, and record values as untrusted.
4. **Identity and durability.** Hash the input bytes with SHA-256 in chunks. Python documents incremental `update()` and `file_digest()` for file-like objects [4]. Write a temporary file in the destination directory, flush and `fsync`, then `Path.replace()` it. A directory `fsync` may be added on filesystems where crash durability is required; atomic publication prevents readers from seeing a half-written file, but cannot replace backups, replication, or a database transaction.
5. **Concurrency.** Use a local advisory lock for one-host jobs. The sample uses Linux `fcntl.flock`; for multiple hosts, use the database/queue’s coordination or PostgreSQL advisory locks rather than pretending a local lock is distributed.

### Runnable development demo: `intake_cli.py`

```python
#!/usr/bin/env python3
"""Development demo: stream CSV/JSONL into a local SQLite intake ledger."""
from __future__ import annotations
import argparse, csv, hashlib, json, logging, os, sqlite3, tempfile
from contextlib import contextmanager
from pathlib import Path
import fcntl                         # Linux/macOS demo; use a DB lock in production

LOG = logging.getLogger("intake")
REQUIRED = {"id", "value"}

@contextmanager
def exclusive_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try: yield
        finally: fcntl.flock(handle, fcntl.LOCK_UN)

def sha256_file(path: Path) -> str:
    with path.open("rb") as src:
        return hashlib.file_digest(src, "sha256").hexdigest()

def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as dst:
            dst.write(data); dst.flush(); os.fsync(dst.fileno())
        Path(name).replace(path)       # same-filesystem atomic name switch
    finally:
        Path(name).unlink(missing_ok=True)

def records(path: Path, fmt: str):
    if path.stat().st_size > 512 * 1024 * 1024:
        raise ValueError("input exceeds configured 512 MiB demo limit")
    if fmt == "csv":
        with path.open("r", encoding="utf-8", newline="") as src:
            reader = csv.DictReader(src)
            for row in reader: yield dict(row)
    else:                              # JSON Lines, deliberately streaming
        with path.open("r", encoding="utf-8") as src:
            for line_no, line in enumerate(src, 1):
                if len(line.encode("utf-8")) > 1_048_576:
                    raise ValueError(f"line {line_no} exceeds 1 MiB")
                if line.strip():
                    row = json.loads(line)
                    if not isinstance(row, dict):
                        raise ValueError(f"line {line_no} must contain a JSON object")
                    yield row

def validate(row: dict) -> dict:
    if not REQUIRED.issubset(row): raise ValueError("record requires id and value")
    item_id, value = str(row["id"]).strip(), str(row["value"])
    if not item_id or len(item_id) > 128: raise ValueError("invalid id")
    if len(value.encode("utf-8")) > 64 * 1024: raise ValueError("value too large")
    return {"id": item_id, "value": value, "source": str(row.get("source", ""))}

def run(input_path: Path, db_path: Path, archive: Path, fmt: str, dry_run: bool) -> int:
    digest = sha256_file(input_path)
    with exclusive_lock(db_path.with_suffix(".lock")):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as db:
            db.execute("CREATE TABLE IF NOT EXISTS records(id TEXT PRIMARY KEY, digest TEXT NOT NULL, payload TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS checkpoints(file_digest TEXT PRIMARY KEY, count INTEGER NOT NULL)")
            count = 0
            for raw in records(input_path, fmt):
                item = validate(raw); count += 1
                payload = json.dumps(item, sort_keys=True, separators=(",", ":"))
                if dry_run:
                    LOG.info("would accept id=%s", item["id"])
                    continue
                # Same id + same data is a replay; same id + different data is a conflict.
                old = db.execute("SELECT digest FROM records WHERE id=?", (item["id"],)).fetchone()
                item_digest = hashlib.sha256(payload.encode()).hexdigest()
                if old and old[0] != item_digest: raise ValueError(f"id conflict: {item['id']}")
                db.execute("INSERT OR IGNORE INTO records VALUES (?, ?, ?)", (item["id"], item_digest, payload))
            if not dry_run:
                db.execute("INSERT OR REPLACE INTO checkpoints VALUES (?, ?)", (digest, count))
                db.commit()
                atomic_write(archive / f"{digest}.json", json.dumps({"source": input_path.name, "records": count}).encode())
    LOG.info("accepted=%d file_sha256=%s", count, digest)
    return 0

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("input", type=Path); p.add_argument("--format", choices=("csv", "jsonl"), required=True)
    p.add_argument("--db", type=Path, default=Path(os.getenv("INTAKE_DB", "./var/intake.sqlite")))
    p.add_argument("--archive", type=Path, default=Path("./var/archive")); p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(); logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try: return run(args.input, args.db, args.archive, args.format, args.dry_run)
    except (OSError, ValueError, json.JSONDecodeError, sqlite3.Error) as exc:
        LOG.exception("intake failed: %s", exc); return 2

if __name__ == "__main__": raise SystemExit(main())
```

### Deployment and operations context

**Development demo:** run `python intake_cli.py sample.csv --format csv --dry-run`, then without dry-run against `./var/intake.sqlite`. Unit-test malformed lines, duplicate IDs, conflicting IDs, partial temporary files, and a killed process. **Server:** run as a dedicated non-root user with a writable data directory, systemd `Restart=on-failure`, bounded start/stop timeouts, and rate limits. systemd documents `Restart=on-failure` as the recommended choice for long-running services [5]. **Container/cloud:** mount input/archive storage as a volume or object-storage staging area; keep the container ephemeral and run as non-root. Docker recommends small trusted images, regular rebuilds, multi-stage builds, and `USER` when privileges are unnecessary [6]. A cloud version can place immutable objects in provider storage and put object keys plus the checksum on a managed queue; the worker still validates and deduplicates.

### Failure, logging, retry, idempotency, and security notes

Log structured fields such as `job_id`, `file_sha256`, `item_id`, count, elapsed time, and error class, never full payloads or credentials. Retry transient I/O and database-busy errors with bounded exponential backoff; do not retry malformed input or an ID conflict. The checkpoint says what this process observed, not that downstream processing completed. Keep raw inputs quarantined until validation succeeds, enforce disk quotas, and set permissions on the SQLite file and archive. SHA-256 detects accidental corruption and supports deduplication; it is not an authenticity signature. For authenticity, verify a separately managed signature or authenticated transport.

### Hands-on build step

Add the CLI to the capstone as the **file intake adapter**. Define the canonical `item_id`, archive accepted input by checksum, and expose a `replay` command that reads the checkpoint ledger. Demonstrate a killed run followed by a replay: the same records are not duplicated, while a changed payload for the same ID is reported as a conflict. This is at-least-once-friendly processing, not exactly-once delivery.

## Chapter 2 — Safe local subprocesses and authorized SSH/SFTP automation

### Overview, objectives, and architecture relevance

Learners will (1) run a fixed local command without shell injection, (2) enforce timeouts and inspect exit status, (3) implement dry-run and redacted logs, (4) use SSH host-key verification and key-based least privilege, and (5) upload to SFTP via a temporary remote name followed by a rename. These patterns matter because automation is an architecture boundary: a worker may control a database migration, converter, backup tool, or remote landing zone. Treat every command, path, host, and credential as policy-controlled input.

### Ordered topics

1. **Prefer argument vectors.** Python recommends `subprocess.run()` for supported cases; with `shell=False` (the default), pass a list and avoid interpreting user data as shell syntax [7]. Resolve an executable with `shutil.which`, use a constrained environment, and capture bounded output.
2. **Timeouts are not cancellation semantics.** `run(timeout=...)` kills and waits for the child when the wait expires, but process creation itself cannot always be interrupted. Record `TimeoutExpired`; if a tool forks children, use a process group/job-object strategy and verify cleanup.
3. **Remote trust.** Paramiko’s default missing-host-key policy rejects unknown keys; load approved system/application known-hosts and do not replace that with `AutoAddPolicy` in production [8]. Use a dedicated remote account, a restricted key, a fixed command wrapper, and no interactive shell.
4. **Remote completion.** Read stdout/stderr and call `recv_exit_status()`. A successful TCP connection or SFTP upload is not proof that the remote processor succeeded. Transfer to `.part`, verify size (and preferably checksum), then rename to a ready name. Paramiko documents `put(..., confirm=True)`, `settimeout`, and `posix_rename` [9].

### Near-runnable deployment demo: `safe_remote.py`

```python
#!/usr/bin/env python3
from __future__ import annotations
import argparse, logging, os, shlex, shutil, subprocess
from pathlib import Path
LOG = logging.getLogger("automation")

def local(argv: list[str], *, timeout: float, dry_run: bool) -> int:
    if not argv or shutil.which(argv[0]) is None: raise ValueError("executable is not allow-listed")
    safe = ["<secret>" if "TOKEN" in x.upper() else x for x in argv]
    if dry_run: LOG.info("DRY RUN local argv=%s", safe); return 0
    try:
        result = subprocess.run(argv, shell=False, check=False, capture_output=True,
                                text=True, timeout=timeout, env={"PATH": os.environ["PATH"], "LANG": "C"})
    except subprocess.TimeoutExpired:
        LOG.error("local timeout argv=%s", safe); return 124
    # Child output can contain secrets or personal data; keep it out of ordinary logs.
    LOG.info("local exit=%d stdout_chars=%d stderr_chars=%d", result.returncode,
             len(result.stdout), len(result.stderr))
    return result.returncode

def upload_and_run(host: str, user: str, key: Path, local_file: Path, remote_dir: str,
                   *, timeout: float, dry_run: bool) -> int:
    # pip install paramiko; use only with an authorized host and approved command.
    import paramiko
    ready = f"{remote_dir}/{local_file.name}"; part = ready + ".part"
    if dry_run: LOG.info("DRY RUN SFTP %s -> %s and fixed remote wrapper", local_file, ready); return 0
    ssh = paramiko.SSHClient(); ssh.load_system_host_keys(); ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        ssh.connect(hostname=host, username=user, key_filename=str(key), timeout=timeout,
                    banner_timeout=timeout, auth_timeout=timeout, channel_timeout=timeout,
                    allow_agent=False, look_for_keys=False)
        sftp = ssh.open_sftp(); sftp.put(str(local_file), part, confirm=True)
        # Server must grant this account rename permission in remote_dir.
        sftp.posix_rename(part, ready); sftp.close()
        command = f"/usr/local/bin/intake-process --path {shlex.quote(ready)}"  # fixed wrapper only
        stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout, get_pty=False)
        out, err = stdout.read(), stderr.read(); code = stdout.channel.recv_exit_status()
        LOG.info("remote exit=%d stdout=%s stderr=%s", code, out[-500:], err[-500:])
        return code
    finally:
        ssh.close()

def main() -> int:
    p = argparse.ArgumentParser(); sub = p.add_subparsers(dest="mode", required=True)
    l = sub.add_parser("local"); l.add_argument("argv", nargs=argparse.REMAINDER); l.add_argument("--timeout", type=float, default=30); l.add_argument("--dry-run", action="store_true")
    r = sub.add_parser("sftp"); r.add_argument("file", type=Path); r.add_argument("--host", required=True); r.add_argument("--user", required=True); r.add_argument("--key", type=Path, required=True); r.add_argument("--remote-dir", required=True); r.add_argument("--timeout", type=float, default=30); r.add_argument("--dry-run", action="store_true")
    a = p.parse_args(); logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.mode == "local": return local(a.argv, timeout=a.timeout, dry_run=a.dry_run)
    return upload_and_run(a.host, a.user, a.key, a.file, a.remote_dir, timeout=a.timeout, dry_run=a.dry_run)
if __name__ == "__main__": raise SystemExit(main())
```

### Deployment and operations context

**Development demo:** use a harmless fixed executable such as `python --version`, a local OpenSSH container or a staging VM, and a test known-hosts file. **Server:** install a wrapper such as `/usr/local/bin/intake-process` owned by root and executable by `intake-worker`; give the SSH key only that account and directory. Do not accept arbitrary operator-supplied shell strings. **Cloud:** prefer a provider API/object-storage SDK and short-lived workload identity for cloud resources; use SFTP only when the authorized partner requires it. Put private keys in a secret manager or a narrowly mounted secret file, not in an image or environment dump. Docker Compose notes that secrets can be mounted under `/run/secrets/<name>` with per-service access, avoiding broad environment exposure [10].

### Failure, logging, retry, idempotency, and security notes

Classify DNS/connect/authentication failures, timeout, non-zero remote exit, checksum mismatch, and permission denial separately. Retry only transient network/server failures, with jitter and a finite budget; never blindly retry authentication or host-key failures. Include remote path, host alias, duration, exit code, and checksum in logs, but redact command arguments and never log private-key contents. A `.part` file is safe to clean after a lease/age threshold. The same checksum and final name make a transfer replayable; remote processing must itself use an idempotency key. SSH does not make an arbitrary shell command safe: shell quoting reduces one injection path but does not authorize the command or constrain its effects. This course uses API-first, authorized sources only and does not teach bypassing access controls or anti-bot defenses.

### Hands-on build step

Extend the capstone with a **delivery adapter**. In development, run a local normalizer through `subprocess.run`; in staging, upload the validated archive to a partner SFTP landing directory, wait for the fixed wrapper’s exit status, and record the result alongside the input checksum. Add failure-injection tests for a hung converter, rejected host key, partial upload, remote non-zero exit, and a replay after a timeout.

## Chapter 3 — Queue boundaries, operational deployment, and production promotion

### Overview, objectives, and architecture relevance

Learners will (1) expose a validated intake endpoint, (2) separate acceptance from processing, (3) make the worker idempotent, (4) configure retries and timeouts, (5) understand the local SQLite/mock path versus a PostgreSQL plus queue path, and (6) operate the service under Docker or systemd. This is the architecture step where a script becomes a service: the API should acknowledge only what it durably accepted, while workers can scale and recover independently.

### Ordered topics

1. **Accept, persist, then enqueue.** Validate at the API boundary, compute a canonical digest, write an intake row with a unique `item_id`, and only then publish a task. A real production design uses an outbox table and a dispatcher or a broker transaction pattern so a database commit and message publish are not falsely presented as one atomic operation.
2. **Local-first implementation.** SQLite plus synchronous processing is the learning path. SQLite is excellent for a single-host demo, but its locking, write concurrency, backups, and failover are not a substitute for a managed PostgreSQL deployment.
3. **Production promotion.** Use PostgreSQL with a unique constraint and `INSERT ... ON CONFLICT`; PostgreSQL documents that `ON CONFLICT DO UPDATE` guarantees an atomic insert-or-update outcome under concurrency [11]. Use a durable broker and Celery (or a cloud queue/worker) for asynchronous work. Celery documents that late acknowledgments can redeliver a task after a worker crash, so tasks must be idempotent [12].
4. **Operations.** Add health/readiness checks, structured logs, metrics for accepted/duplicate/conflict/retry/dead-letter counts, bounded payloads, and graceful shutdown. Separate web and worker processes so a slow converter does not block API traffic. FastAPI’s deployment guidance emphasizes startup, restart, replication, memory, and HTTPS as deployment concerns [13].

### Near-runnable capstone blueprint: `service.py`

```python
"""Development demo. Install fastapi uvicorn celery; local mode uses SQLite."""
from __future__ import annotations
import hashlib, json, logging, os, sqlite3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from celery import Celery
LOG = logging.getLogger("intake-service")
DB = os.getenv("INTAKE_DB", "./var/intake.sqlite")
LOCAL_MODE = os.getenv("INTAKE_LOCAL_MODE", "1") == "1"
celery_app = Celery("intake", broker=os.getenv("CELERY_BROKER_URL", "memory://"))

class Item(BaseModel):
    item_id: str = Field(min_length=1, max_length=128)
    payload: dict[str, object]

def digest(item: Item) -> str:
    raw = json.dumps({"item_id": item.item_id, "payload": item.payload}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()

def connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB) or ".", exist_ok=True)
    db = sqlite3.connect(DB, timeout=10); db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
      CREATE TABLE IF NOT EXISTS intake(item_id TEXT PRIMARY KEY, digest TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS processed(item_id TEXT PRIMARY KEY, result TEXT NOT NULL);
    """); return db

def accept(item: Item) -> tuple[bool, str]:
    d = digest(item); payload = json.dumps(item.payload, sort_keys=True, separators=(",", ":"))
    with connect() as db:
        old = db.execute("SELECT digest FROM intake WHERE item_id=?", (item.item_id,)).fetchone()
        if old and old[0] != d: raise ValueError("item_id already exists with different content")
        if old: return False, d
        db.execute("INSERT INTO intake VALUES (?, ?, ?, 'accepted')", (item.item_id, d, payload)); db.commit()
    return True, d

def process_once(item_id: str) -> None:
    with connect() as db:
        row = db.execute("SELECT payload, status FROM intake WHERE item_id=?", (item_id,)).fetchone()
        if not row: raise ValueError("unknown item")
        if row[1] == "done": return
        payload = json.loads(row[0])
        # Pure, deterministic demo transform; real side effects need the same item_id as idempotency key.
        result = json.dumps({"keys": sorted(payload), "payload": payload}, sort_keys=True)
        db.execute("INSERT OR IGNORE INTO processed VALUES (?, ?)", (item_id, result))
        db.execute("UPDATE intake SET status='done' WHERE item_id=?", (item_id,)); db.commit()

class TransientFailure(Exception): pass
@celery_app.task(bind=True, acks_late=True, autoretry_for=(TransientFailure,), retry_backoff=True, max_retries=5)
def process_task(self, item_id: str):
    # Production: use PostgreSQL transaction + unique key and a real broker, not memory://.
    process_once(item_id); return {"item_id": item_id, "status": "done"}

app = FastAPI(title="Reliable Data Intake")
@app.get("/healthz")
def healthz(): return {"ok": True}
@app.post("/intake", status_code=202)
def intake(item: Item):
    try: new, d = accept(item)
    except ValueError as exc: raise HTTPException(409, str(exc))
    if new:
        if LOCAL_MODE: process_once(item.item_id)
        else: process_task.delay(item.item_id)  # production: outbox/dispatcher closes publish gap
    return {"item_id": item.item_id, "digest": d, "duplicate": not new}
```

### Deployment and operations context

**Development demo:** run `uvicorn service:app --reload`, post a bounded JSON item, and inspect SQLite. Run the deterministic `process_task` in eager/mock mode for tests; `memory://` is not a durable production broker. **Production server:** build a small non-root image, run web and worker as separate services, mount secrets narrowly, and use a managed PostgreSQL plus durable queue. Compose health checks can gate dependent startup; Docker’s build guidance recommends ephemeral containers and one concern per container [6]. **systemd/VM:** use distinct units such as `intake-web.service` and `intake-worker.service`, `Restart=on-failure`, explicit `TimeoutStartSec`/`TimeoutStopSec`, `NoNewPrivileges=yes`, and a dedicated writable directory. **Cloud:** use managed TLS at the ingress, managed database backups, queue visibility timeouts/dead-letter handling, object storage for large payloads, and workload identity. Scale workers based on queue depth and memory, not only HTTP request count.

### Failure, logging, retry, idempotency, and security notes

Validation failures return 4xx and are not retried. A duplicate with the same digest is a successful replay; a duplicate ID with different content is a conflict requiring operator review. Network/database/broker failures are retryable with bounded exponential backoff and a dead-letter path. A worker can complete the database transaction and crash before acknowledgment, or acknowledge before an external side effect is durable; therefore delivery is normally **at least once**, and “exactly once” is not claimed. Make database writes unique and side effects idempotent, and reconcile the outbox/ledger periodically. Keep timeouts on HTTP, database, subprocess, and SSH operations; no timeout means one stuck task can exhaust workers. Never put secrets in request logs, images, source, or broad environment dumps. OWASP recommends centralized lifecycle management, fine-grained access control, rotation, and auditing for API keys, database credentials, SSH keys, and certificates [14].

### Hands-on build step

Promote the chapter-1 CLI into the capstone’s API/worker pipeline: `POST /intake` validates and records the item, a local SQLite/mock path processes it synchronously, and a production profile routes it through PostgreSQL, an outbox, and a durable queue. Add dashboards and run a game day: kill the worker after acceptance, duplicate a message, corrupt an archive, exhaust a timeout, and restore from backup. The acceptance ledger, checksum, unique key, retry policy, and reconciliation report should explain every outcome without claiming impossible exactly-once delivery.

## References

[1]: https://fastapi.tiangolo.com/advanced/settings/ "FastAPI Settings and Environment Variables"
[2]: https://docs.python.org/3/library/csv.html "Python csv — CSV File Reading and Writing"
[3]: https://docs.python.org/3/library/json.html "Python json — JSON encoder and decoder"
[4]: https://docs.python.org/3/library/hashlib.html "Python hashlib — Secure hashes and message digests"
[5]: https://www.freedesktop.org/software/systemd/man/systemd.service.html "systemd.service"
[6]: https://docs.docker.com/build/building/best-practices/ "Docker Building best practices"
[7]: https://docs.python.org/3/library/subprocess.html "Python subprocess — Subprocess management"
[8]: https://docs.paramiko.org/en/latest/api/client.html "Paramiko SSHClient API"
[9]: https://docs.paramiko.org/en/latest/api/sftp.html "Paramiko SFTP API"
[10]: https://docs.docker.com/compose/how-tos/use-secrets/ "Docker Compose secrets"
[11]: https://www.postgresql.org/docs/current/sql-insert.html "PostgreSQL INSERT and ON CONFLICT"
[12]: https://docs.celeryq.dev/en/stable/userguide/tasks.html "Celery Tasks"
[13]: https://fastapi.tiangolo.com/deployment/concepts/ "FastAPI Deployment Concepts"
[14]: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html "OWASP Secrets Management Cheat Sheet"
