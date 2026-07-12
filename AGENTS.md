# Agent Knowledge & Rules Guide (AGENTS.md)

This file contains crucial architectural, structural, and behavioral knowledge about the **OCR Summary Worker** project. Any AI agent modifying or maintaining this codebase must review and adhere to these specifications.

---

## 1. Core Architecture: Decoupled Async Worker Pool

Unlike traditional sequential Kafka consumers, this pipeline decouples message ingestion from processing to handle long-running (average 5 minutes) jobs without Kafka lag or timeout issues.

### Ingress Loop (`app/worker.py`)
* Consumes messages from the Kafka input topic.
* Resolves or creates a job in PostgreSQL (`state = 'RECEIVED'`).
* **Immediately commits the offset back to Kafka**.

### Distributed Claim Loop (`app/worker.py` & `app/core/database.py`)
* Runs an infinite background loop polling PostgreSQL using an atomic row-level lock:
  ```sql
  SELECT id FROM job_states
  WHERE state != 'SUCCESS' AND (worker_id IS NULL OR leased_until < NOW())
  ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED;
  ```
* Claims the job by setting `worker_id` (pod hostname) and `leased_until = NOW() + 15 minutes`.
* Processes tasks locally inside an `asyncio.Semaphore` (bounded by `settings.worker_concurrency`).

---

## 2. Database Schema & State Machine

* **State Persistence**: The `state` column tracks the last **successfully completed** pipeline step:
  `RECEIVED` → `DOWNLOADED` → `UPLOADED_S3` → `OCR_COMPLETED` → `SUMMARY_COMPLETED` → `ARCHIVED` → `SUCCESS`.
* **Lock Columns**: `worker_id`, `locked_at`, and `leased_until` manage the lease.
* **Failure Handling**: If a step fails, the `state` is **not** overwritten with a generic `FAILED` state. Instead:
  1. The `state` column remains at the last successfully completed step (e.g. `UPLOADED_S3`).
  2. The `error_message` column is populated with the traceback.
  3. The lock columns (`worker_id`, `locked_at`, `leased_until`) are cleared (`NULL`).
* **Recovery (Idempotent Resume)**: When claimed again, the processor reads the `state` column, clears `error_message`, and resumes immediately from the next step (skipping completed ones).

---

## 3. Storage & Object Layout Rules

* **Deterministic S3 Keys**: S3 keys are **never** stored in the database. They are reconstructed dynamically using the `job_id` (UUID):
  * Raw Document: `jobs/{job_id}/raw.{extension}`
  * Extracted OCR Text: `jobs/{job_id}/ocr.txt`
  * Summary Report: `jobs/{job_id}/report.txt`
* **Filename Tracking**: The database stores the original downloaded file name in the `filename` column (updated during the download step). The raw S3 object extension (`.pdf`, `.docx`, etc.) is parsed dynamically from this column.

---

## 4. Coding & Maintenance Constraints

* **Async-First Stack**: Do not introduce blocking/synchronous client calls. Use `asyncpg` for PostgreSQL, `aiokafka` for Kafka, and `httpx.AsyncClient` for API requests. Note: `boto3` calls are synchronous and should not be awaited.
* **Memory Constraints**: Documents can be large. Always stream transfers via `httpx` chunked iteration and `boto3` upload_fileobj/download_fileobj helper wrappers. Do not load entire files into memory.
* **tenacity Retry Scoping**: Scopes retries to flaky external endpoints (Secure GPT OCR and summary completions). Infrastructure (DB, S3, local Kafka) should fail fast rather than retrying indefinitely inside the pipeline code.
* **Pydantic Configuration**: Casing is handled case-insensitively via `SettingsConfigDict(case_sensitive=False)`. Always access configuration parameters using lowercase attributes (e.g. `settings.kafka_bootstrap_servers`).
* **OpenShift Security Constraints**: The application runs under strict SCC (Security Context Constraints) on OpenShift:
  * Running as non-root user `1001` (configured in `Dockerfile` and `openshift/deployment.yaml`).
  * Drops all Linux capabilities.
  * Root filesystem is read-only (the application operates entirely in-memory and does not write to local storage).

---

## 5. OIDC M2M Token Caching Rules

To authenticate calls to DOCSYS, Secure GPT, and DOCSYSARC APIs without hitting identity provider rate limits, we cache OIDC access tokens in the database.

* **Cache Schema**: The `openid_tokens` table stores the cached token and its expiration:
  `service_name` (PK) | `access_token` | `expires_at` (TIMESTAMPTZ) | `updated_at` (TIMESTAMPTZ).
* **Two-Tier Cache Access (Multi-Pod Safe)**:
  1. **Fast-Path (Lockless)**: Always check the DB cache first without a transaction or lock. If a valid, non-expired token is found (with a 30-second expiry buffer), return it immediately.
  2. **Row Lock-on-Refresh**: If the token is missing or expired, acquire an exclusive row lock (`SELECT ... FOR UPDATE` inside a transaction on `openid_tokens`). Once the lock is acquired, perform a **post-lock check** on expiration to see if another pod refreshed it while this pod was waiting. Only execute the external OIDC client credentials request if it is still expired.
* **Timezone Safety**: PostgreSQL stores and returns `TIMESTAMPTZ` with timezone offsets. Always compare expiration using UTC timezone-aware datetime objects: `datetime.now(timezone.utc)`.
* **Retries Context**: GPT service tokens must be retrieved *inside* the request retry loops. If a long-running GPT request retries and the token expires during the retry attempts, it must resolve a new valid token.

