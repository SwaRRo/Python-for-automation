# Module 3 — Web scraping, API integration, and headless browser automation

**Module id:** `integrations`  
**Course baseline:** Python 3.11+; use a virtual environment and lock the dependency versions that pass your tests. This module’s capstone is one **Reliable Data Intake & Processing Service**. It starts with a local SQLite/mock path, then identifies the production PostgreSQL and queue boundary. “Reliable” means observable, bounded, retry-aware, and idempotent where designed; it does **not** promise exactly-once delivery.

## Chapter 1 — HTTPX API clients, schemas, pagination, and safe retries

### Overview, objectives, and architecture value

An API client is an integration boundary, not a loop around `httpx.get()`. By the end of this chapter, learners can (1) build a reusable HTTPX client with connection pooling, base URL, headers, auth, resource limits, and explicit timeouts; (2) validate untrusted JSON with Pydantic; (3) implement cursor or page-number pagination; (4) classify retryable failures and honor `Retry-After`; and (5) persist a normalized record with a deterministic idempotency key.

This matters in server architecture because upstream APIs are slower, less reliable, and independently rate-limited. A bounded client prevents one integration from exhausting worker threads or sockets. Validation stops malformed upstream data from becoming corrupt internal state. A durable intake record gives operators a place to resume, inspect, and replay work.

HTTPX recommends a `Client` for more than experiments because it reuses pooled connections and supports shared configuration; a context manager closes the pool cleanly. [1] HTTPX applies timeouts by default and distinguishes connect, read, write, and pool timeouts. [2] Pydantic models validate untrusted input and produce typed model instances and JSON Schema. [3]

### Ordered topics

1. **Contract first.** Identify the upstream endpoint, authentication method, page/cursor semantics, maximum page size, field meanings, and provider terms. Prefer the provider’s API and published quota over HTML extraction.
2. **Configuration.** Read the base URL, token, timeout budget, page size, retry limit, and user-agent from environment or a secret manager. Never put a token in source, logs, a URL query string, or a committed `.env` file.
3. **Client lifecycle.** Create one client per process or dependency scope, with pooled connections and a deliberately small `Limits` setting. Do not create a new client for every item.
4. **Auth.** Use the provider’s documented bearer/API-key/OAuth flow. Scope tokens to read-only resources when possible and rotate them. Treat a `401`/`403` as an integration or authorization problem, not a reason to retry indefinitely.
5. **Pagination.** Follow the provider’s `next` cursor or documented page field. Bound total pages and records. Persist the last successful page/cursor so a crash can resume without silently skipping data.
6. **Retries and rate limits.** Retry only safe/idempotent reads for transient transport errors, `408`, `429`, and selected `5xx` responses. Use exponential backoff with jitter, cap attempts, and honor a valid `Retry-After`; RFC 9110 defines that field as the server’s wait guidance for a follow-up request. [4] Do not retry every exception and do not amplify a provider outage.
7. **Schema and storage.** Validate each item, record rejected payloads separately, and upsert by a stable `(source, external_id)` key. A database unique constraint is stronger than a Python “already seen” set.

### Code blueprint: `api_intake.py` (development demo)

This is runnable with `pip install httpx pydantic` and a reachable JSON API whose response is `{"items": [...], "next_cursor": "..."}`. Set `API_BASE_URL`, `API_TOKEN` (optional), and `API_PATH`. The SQLite file is intentionally local and is replaced by PostgreSQL in production.

```python
from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import sqlite3
import time
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("api_intake")

BASE_URL = os.environ["API_BASE_URL"]                 # e.g. https://api.example.test
API_PATH = os.getenv("API_PATH", "/v1/items")
TOKEN = os.getenv("API_TOKEN")
DB_PATH = os.getenv("SQLITE_PATH", "intake.db")
MAX_PAGES = int(os.getenv("MAX_PAGES", "100"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "4"))

class Item(BaseModel):
    model_config = ConfigDict(extra="ignore")
    external_id: str = Field(min_length=1, max_length=200)
    title: str = Field(min_length=1, max_length=500)
    updated_at: str | None = None

class Page(BaseModel):
    items: list[Item]
    next_cursor: str | None = None

def init_db() -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS records (
          source TEXT NOT NULL, external_id TEXT NOT NULL, payload TEXT NOT NULL,
          intake_key TEXT NOT NULL UNIQUE, received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (source, external_id))""")

def retry_delay(response: httpx.Response | None, attempt: int) -> float:
    if response is not None and response.status_code == 429:
        try:
            return min(float(response.headers.get("Retry-After", "0")), 60.0)
        except ValueError:
            pass
    return min(30.0, (2 ** (attempt - 1)) + random.uniform(0, 0.5))

def get_page(client: httpx.Client, cursor: str | None) -> Page:
    params = {"limit": "100"}
    if cursor:
        params["cursor"] = cursor
    retryable_statuses = {408, 429, 500, 502, 503, 504}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        response: httpx.Response | None = None
        try:
            response = client.get(API_PATH, params=params)
            if response.status_code in retryable_statuses and attempt < MAX_ATTEMPTS:
                delay = retry_delay(response, attempt)
                log.warning("transient upstream status=%s attempt=%s delay=%.2f", response.status_code, attempt, delay)
                time.sleep(delay)
                continue
            response.raise_for_status()
            return Page.model_validate(response.json())
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            if attempt == MAX_ATTEMPTS:
                raise
            delay = retry_delay(response, attempt)
            log.warning("transport failure attempt=%s error=%s", attempt, type(exc).__name__)
            time.sleep(delay)
        except (json.JSONDecodeError, ValidationError) as exc:
            log.error("invalid upstream payload; not retrying error=%s", type(exc).__name__)
            raise
    raise RuntimeError("unreachable")

def save_item(item: Item) -> None:
    key = hashlib.sha256(f"api:{item.external_id}".encode()).hexdigest()
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""INSERT INTO records(source, external_id, payload, intake_key)
          VALUES (?, ?, ?, ?) ON CONFLICT(source, external_id) DO UPDATE SET
          payload=excluded.payload, intake_key=excluded.intake_key""",
          ("api", item.external_id, item.model_dump_json(), key))

def main() -> None:
    init_db()
    headers = {"User-Agent": "reliable-intake/0.1"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    timeout = httpx.Timeout(10.0, connect=5.0, read=20.0, write=10.0, pool=5.0)
    limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    with httpx.Client(base_url=BASE_URL, headers=headers, timeout=timeout,
                      limits=limits, follow_redirects=False) as client:
        cursor = None
        for page_no in range(1, MAX_PAGES + 1):
            page = get_page(client, cursor)
            for item in page.items:
                save_item(item)
            log.info("page=%s accepted=%s", page_no, len(page.items))
            if not page.next_cursor:
                break
            cursor = page.next_cursor
        else:
            raise RuntimeError("pagination limit reached; investigate upstream or configuration")

if __name__ == "__main__":
    main()
```

### Deployment and operations context

**Development demo:** run the script against a local mock server and SQLite; use fixtures for `429`, malformed JSON, duplicate IDs, and a cursor that terminates. **Server:** run intake as a bounded background worker, not inside a request that waits for all pages. On a VM, a `systemd` unit can restart a long-running worker on failure; `Restart=on-failure` is the documented choice for automatic recovery, subject to start-rate limiting. [10] **Cloud:** package the worker in a container, inject secrets through the platform’s secret facility, send JSON logs to a central sink, and use managed PostgreSQL. Docker’s Python guide describes the image as a portable unit containing runtime and dependencies, and demonstrates Compose with PostgreSQL for development. [9]

Failure operations should log source, endpoint path, page number, attempt, status, latency, and a correlation/intake key—but never authorization headers or full personal payloads. Metrics should include request count, latency, retry count, validation failures, records accepted, and oldest pending cursor. Use a dead-letter/quarantine table for invalid payloads. A timeout bounds waiting; it does not make a remote operation safe to repeat. Upserts make this code repeat-safe for the chosen key, but delivery can still duplicate around a crash between a remote fetch and a local commit. That is **at-least-once-ish processing with deduplication**, not exactly-once delivery.

**Hands-on build step (capstone step 1):** add `POST /runs` to the service from the previous module (or a minimal FastAPI app) that records a run and invokes `main()` in a local process. Add a `source_runs` table with status, cursor, timestamps, and error summary. Demonstrate a killed run resuming from the stored cursor and a repeated page producing no duplicate `(source, external_id)` rows. In production, replace SQLite with PostgreSQL transactions and put the run on a durable queue; retain the unique constraint.

## Chapter 2 — API-first responsible scraping with policy checks and selector parsing

### Overview, objectives, and architecture value

Sometimes permitted public data has no usable API. This chapter teaches the narrow fallback: fetch a published, public HTML page with an ordinary HTTP client and parse it with BeautifulSoup or CSS selectors. Learners will (1) document permission and data minimization; (2) check `robots.txt` and site policies before fetching; (3) enforce a hostname allowlist and safe schemes; (4) parse stable selectors with explicit limits; and (5) quarantine layout drift instead of guessing.

This matters architecturally because a scraper is a dependency on another team’s presentation layer. It should be isolated behind an adapter, rate-limited, cancellable, and easy to remove when an API becomes available. HTML is untrusted input. It can include huge documents, misleading links, sensitive personal data, or content that changes without notice.

Python’s `urllib.robotparser.RobotFileParser` exposes `can_fetch`, `crawl_delay`, and `request_rate` for the published robots file. [5] Robots rules are one signal, not a license: also check terms, policy, copyright/privacy obligations, contractual permission, and the owner’s contact guidance. The service must never bypass a login, CAPTCHA, paywall, robots/policy restriction, or anti-bot control.

### Ordered topics

1. **Authorization record.** Store the owner, allowed hosts/paths, purpose, retention period, contact, and review date. Stop if permission is ambiguous.
2. **Robots and terms.** Fetch `https://host/robots.txt` using the declared user-agent, fail closed when the policy cannot be checked, and honor a published crawl delay. Do not infer that `robots.txt` overrides terms or law.
3. **API-first decision.** Search documented feeds, exports, webhooks, or APIs before HTML. Ask for a data feed rather than increasing request volume.
4. **SSRF boundary.** Accept only `https`; allowlist exact hostnames and path prefixes; disable redirects; cap response size and parse time; isolate egress in a worker network. OWASP recommends allowlists where destinations are known and warns about redirects and DNS/parser tricks. [6] Hostname checking alone is not a complete defense against DNS rebinding; production should resolve and validate addresses at connection time or use a controlled egress proxy.
5. **Parsing.** Select only the fields needed, normalize whitespace, validate the result, and attach source URL and retrieval timestamp. Keep a fixture HTML file for tests.
6. **Drift handling.** If a required selector is missing, fail loudly and quarantine the page. Never silently return an empty successful dataset.

### Code blueprint: `html_intake.py` (development demo)

Install with `pip install httpx beautifulsoup4 pydantic`. Set `ALLOWED_HOSTS=public.example.test`, `ALLOWED_PATH_PREFIX=/catalog`, and `TARGET_URL`. The allowlist is intentionally strict and must be replaced with a reviewed policy record, not user-controlled arbitrary URLs.

```python
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timezone
from urllib.parse import urlparse
import urllib.robotparser

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, ValidationError

log = logging.getLogger("html_intake")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
TARGET_URL = os.environ["TARGET_URL"]
ALLOWED_HOSTS = {h.strip().lower() for h in os.environ["ALLOWED_HOSTS"].split(",")}
PATH_PREFIX = os.getenv("ALLOWED_PATH_PREFIX", "/")
DB_PATH = os.getenv("SQLITE_PATH", "intake.db")
USER_AGENT = "reliable-intake/0.1 (+mailto:data-owner@example.test)"

class Card(BaseModel):
    external_id: str = Field(min_length=1, max_length=200)
    title: str = Field(min_length=1, max_length=500)
    source_url: str
    fetched_at: datetime

def allowed(url: str) -> bool:
    parsed = urlparse(url)
    prefix = PATH_PREFIX.rstrip("/")
    path_allowed = (PATH_PREFIX == "/" or parsed.path == prefix
                    or parsed.path.startswith(prefix + "/"))
    return (parsed.scheme == "https" and parsed.hostname is not None
            and parsed.hostname.lower() in ALLOWED_HOSTS
            and parsed.username is None and parsed.password is None
            and path_allowed)

def check_robots(url: str) -> None:
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)
    # Use the same bounded HTTP client policy; RobotFileParser.read() alone
    # does not let this adapter apply its own timeout and response-size limit.
    timeout = httpx.Timeout(4.0, connect=2.0, read=4.0)
    try:
        with httpx.Client(timeout=timeout, follow_redirects=False) as client:
            with client.stream("GET", robots_url, headers={"User-Agent": USER_AGENT}) as response:
                response.raise_for_status()
                chunks = bytearray()
                for chunk in response.iter_bytes():
                    chunks.extend(chunk)
                    if len(chunks) > 256_000:
                        raise ValueError("robots policy exceeds configured size limit")
        rp.parse(bytes(chunks).decode("utf-8", errors="replace").splitlines())
    except (OSError, httpx.HTTPError) as exc:
        raise RuntimeError("robots policy unavailable; fail closed") from exc
    if not rp.can_fetch(USER_AGENT, url):
        raise PermissionError("robots.txt disallows this user-agent and URL")
    delay = rp.crawl_delay(USER_AGENT) or rp.crawl_delay("*")
    if delay:
        log.info("policy crawl_delay=%s seconds; scheduler must enforce it", delay)

def save(card: Card) -> None:
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS html_records (
          external_id TEXT PRIMARY KEY, title TEXT NOT NULL, source_url TEXT NOT NULL,
          fetched_at TEXT NOT NULL)""")
        db.execute("""INSERT INTO html_records VALUES (?, ?, ?, ?)
          ON CONFLICT(external_id) DO UPDATE SET title=excluded.title,
          source_url=excluded.source_url, fetched_at=excluded.fetched_at""",
          (card.external_id, card.title, card.source_url, card.fetched_at.isoformat()))

def ingest() -> int:
    if not allowed(TARGET_URL):
        raise PermissionError("URL is outside the reviewed HTTPS allowlist")
    check_robots(TARGET_URL)
    timeout = httpx.Timeout(10.0, connect=5.0, read=15.0)
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=timeout,
                      follow_redirects=False) as client:
        with client.stream("GET", TARGET_URL) as response:
            response.raise_for_status()
            content = bytearray()
            for chunk in response.iter_bytes():
                content.extend(chunk)
                if len(content) > 2_000_000:
                    raise ValueError("response exceeds configured size limit")
    soup = BeautifulSoup(bytes(content), "html.parser")
    cards = soup.select("article[data-id]")[:500]
    if not cards:
        raise ValueError("required selector returned no records; quarantine for layout review")
    count = 0
    for node in cards:
        try:
            card = Card(external_id=node["data-id"],
                        title=node.select_one("h2").get_text(" ", strip=True),
                        source_url=TARGET_URL,
                        fetched_at=datetime.now(timezone.utc))
            save(card)
            count += 1
        except (KeyError, AttributeError, ValidationError) as exc:
            log.warning("quarantined malformed card error=%s", type(exc).__name__)
    return count

if __name__ == "__main__":
    log.info("accepted_records=%s", ingest())
```

### Deployment and operations context

**Development demo:** serve a checked-in HTML fixture with a local mock server and test the parser without internet access; run policy checks in an integration test against a permitted staging host. **Server:** schedule one host at a time, persist the last retrieval and policy decision, and use a per-host token bucket or delay. A single process-wide sleep is not a multi-worker rate limiter. **Cloud:** run the parser in a restricted worker subnet with no access to cloud metadata, databases, or control-plane addresses. Store only the fields needed for the stated purpose; redact or hash identifiers where possible and set deletion jobs.

Log policy decision, host, path, response status/size, selector counts, and schema errors. Do not log the HTML body by default. Do not retry a policy denial, `401`/`403`, CAPTCHA page, or parser drift. Retry transient network errors only within the owner-approved rate limit. Idempotency is the same source-key upsert pattern as Chapter 1. Credential notes are simple: this chapter should normally need no credential; adding a session cookie to defeat a gate is out of scope.

**Hands-on build step (capstone step 2):** implement a `HtmlSourceAdapter` that shares the Chapter 1 `source_runs` and `records` tables. Add an approved-source registry and a fixture test that proves: disallowed hosts fail before network I/O, redirects are rejected, robots denial is recorded, a 2 MB limit is enforced, and selector drift enters `quarantine` rather than producing a false-success run. Keep the API adapter as the preferred path and make source choice explicit in the run record.

## Chapter 3 — Authorized Playwright workflows and production queue boundaries

### Overview, objectives, and architecture value

A real browser is justified only when a permitted user-owned or public workflow requires JavaScript rendering, a documented interaction, or browser-specific behavior that an HTTP client cannot reproduce. Playwright supports Chromium, Firefox, and WebKit and Python sync/async APIs. [7] Browser contexts are isolated, non-persistent sessions that do not write browsing data to disk by default; closing a context closes its pages. [8]

Learners will (1) define an authorization boundary and allowed origin; (2) use a short-lived, isolated context; (3) set navigation/action timeouts and avoid persistent profiles; (4) extract a small, validated result; (5) run browser work outside the web request process; and (6) design queue retries and deduplication without claiming exactly-once delivery.

This matters in server architecture because browsers are memory-heavy, failure-prone processes. A web API worker must remain responsive while a queue worker owns browser lifecycle, concurrency, artifacts, and timeouts. Celery describes a task queue as messages delivered through a broker to dedicated workers; its docs also recommend idempotent task functions and explain that late acknowledgements can cause multiple execution after a crash. [11] [12]

### Ordered topics

1. **Permission gate.** Obtain written authorization or document that the workflow is public and permitted. No login automation unless the owner explicitly provides and authorizes the account. Never bypass CAPTCHA, anti-bot, access controls, paywalls, or rate limits.
2. **Browser isolation.** Launch a managed browser, create a fresh non-persistent context per job, block downloads/popups unless required, and close page/context/browser in `finally` blocks.
3. **Navigation safety.** Allowlist exact origins, reject credentials in URLs, enforce a navigation timeout, inspect the final URL, and stop on unexpected redirects. Network egress policy remains necessary; browser URL validation alone is not an SSRF defense.
4. **Selectors and waits.** Prefer role/text/label locators or stable `data-*` attributes. Wait for a specific locator, not an arbitrary long sleep. Capture a small diagnostic screenshot only on an authorized test/staging environment and protect it as potentially sensitive.
5. **Queue boundary.** API request creates a run and enqueues a job. Worker fetches, validates, commits, and records status. Use a durable broker and PostgreSQL in production; SQLite/mock execution remains the local learning path.
6. **Retry policy.** Retry a browser crash or transient upstream timeout with a cap and jitter. Do not retry a policy/access denial or selector drift. Use a deterministic run key and database uniqueness. A broker can redeliver; a worker can die after side effects; design for at-least-once execution plus deduplication.

### Code blueprint: `browser_intake.py` (near-runnable local demo)

Install `pip install playwright pydantic` and then run `playwright install` as documented by Playwright. [7] Set `ALLOWED_ORIGINS=https://public.example.test`, `TARGET_URL`, and use a public/staging page that you own or are authorized to test. The `run_browser_job` function is the unit to call from a queue worker; the `__main__` path is local-only.

```python
from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
from urllib.parse import urlparse

from pydantic import BaseModel, Field
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

log = logging.getLogger("browser_intake")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
DB_PATH = os.getenv("SQLITE_PATH", "intake.db")
ALLOWED_ORIGINS = {x.strip().lower() for x in os.environ["ALLOWED_ORIGINS"].split(",")}
TARGET_URL = os.environ["TARGET_URL"]

class BrowserRecord(BaseModel):
    external_id: str = Field(min_length=1, max_length=200)
    title: str = Field(min_length=1, max_length=500)

def allowed_origin(url: str) -> bool:
    p = urlparse(url)
    origin = f"{p.scheme}://{p.netloc}".lower()
    return p.scheme == "https" and not p.username and not p.password and origin in ALLOWED_ORIGINS

def persist(record: BrowserRecord, run_id: str) -> None:
    key = hashlib.sha256(f"browser:{record.external_id}".encode()).hexdigest()
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS records (
          source TEXT, external_id TEXT, payload TEXT, intake_key TEXT UNIQUE,
          run_id TEXT, PRIMARY KEY(source, external_id))""")
        db.execute("""INSERT INTO records VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(source, external_id) DO UPDATE SET payload=excluded.payload,
          run_id=excluded.run_id""", ("browser", record.external_id,
          record.model_dump_json(), key, run_id))

def run_browser_job(run_id: str, url: str) -> int:
    if not allowed_origin(url):
        raise PermissionError("browser URL is outside the reviewed origin allowlist")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=False)
        context.set_default_timeout(8_000)
        page = context.new_page()
        try:
            response = page.goto(url, wait_until="domcontentloaded", timeout=15_000)
            if response is None or response.status >= 400:
                raise RuntimeError(f"navigation failed status={response.status if response else 'none'}")
            final_url = page.url
            if not allowed_origin(final_url):
                raise PermissionError("unexpected redirect outside allowed origin")
            title = page.locator("[data-testid='record-title']").inner_text()
            external_id = page.locator("[data-testid='record-id']").inner_text()
            record = BrowserRecord(external_id=external_id.strip(), title=title.strip())
            persist(record, run_id)
            log.info("run_id=%s browser_record_saved source=browser", run_id)
            return 1
        except PlaywrightTimeout:
            log.exception("run_id=%s browser timeout", run_id)
            raise
        finally:
            context.close()
            browser.close()

if __name__ == "__main__":
    run_browser_job(os.getenv("RUN_ID", "local-demo"), TARGET_URL)
```

### Deployment and operations context

**Development demo:** run one browser job against a local mock page or owned staging app. Do not put real user cookies in the repository. **Server:** package browser binaries in the image, run a separate worker with a low concurrency, CPU/memory limits, and a hard job deadline. Expose only a small FastAPI `POST /runs` that validates the source and returns a run ID; it should not hold the HTTP connection open for navigation. **Cloud:** use a managed queue/broker (for example Redis or RabbitMQ where supported), PostgreSQL for run/record state, object storage only for explicitly approved diagnostics, and network egress controls. A container orchestrator generally runs one Uvicorn process per container and scales replicas; FastAPI’s deployment guidance contrasts that with multiple local workers. [13]

Operational signals include queue depth, job age, browser launch failures, navigation latency, memory per job, timeout count, selector drift, and duplicate suppression. Correlate every browser job with `run_id`; redact URLs if query strings can contain personal data. Never print cookies, authorization headers, page text, or screenshots by default. Rotate any authorized test credential and grant the minimum role.

**Hands-on build step (capstone step 3):** add the browser adapter behind the same `SourceAdapter` interface. Implement `POST /runs` to insert a run with a unique idempotency key and enqueue `{run_id, source, url}`; the worker calls the adapter, validates records, upserts them, and marks the run `succeeded`, `quarantined`, or `retrying`. In local mode, call `run_browser_job` directly and use SQLite/mock pages. In production, use PostgreSQL transactions plus an outbox or queue publisher so a committed run is not silently lost before enqueueing; use a queue worker with bounded retries and a dead-letter path. Document that the system achieves durable state plus repeat-safe writes, while network calls, queue delivery, process crashes, and provider behavior mean exactly-once delivery is not asserted.

## References

[1]: https://www.python-httpx.org/advanced/clients/ "HTTPX Clients"
[2]: https://www.python-httpx.org/advanced/timeouts/ "HTTPX Timeouts"
[3]: https://pydantic.dev/docs/validation/dev/concepts/models/ "Pydantic Models and Validation"
[4]: https://www.rfc-editor.org/rfc/rfc9110 "RFC 9110: HTTP Semantics"
[5]: https://docs.python.org/3/library/urllib.robotparser.html "Python urllib.robotparser"
[6]: https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html "OWASP SSRF Prevention Cheat Sheet"
[7]: https://playwright.dev/python/docs/intro "Playwright Python Installation"
[8]: https://playwright.dev/python/docs/api/class-browsercontext "Playwright BrowserContext"
[9]: https://docs.docker.com/guides/python/ "Docker Python Language Guide"
[10]: https://www.freedesktop.org/software/systemd/man/systemd.service.html "systemd.service"
[11]: https://docs.celeryq.dev/en/stable/getting-started/introduction.html "Celery Introduction and Task Queues"
[12]: https://docs.celeryq.dev/en/stable/userguide/tasks.html "Celery Tasks, Acknowledgements, and Idempotency"
[13]: https://fastapi.tiangolo.com/deployment/server-workers/ "FastAPI Server Workers"
