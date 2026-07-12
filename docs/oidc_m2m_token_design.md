# Design Document — Distributed OIDC M2M Token Caching and Authentication

This document details the design of the OpenID Connect (OIDC) Machine-to-Machine (M2M) token authentication mechanism integrated into the OCR Summary Worker pipeline.

---

## 1. Architectural Overview

To secure interactions with the downstream APIs (**DOCSYS**, **Secure GPT**, and **DOCSYSARC**), the pipeline requires dynamic retrieval of OAuth 2.0 bearer tokens using Client Credentials Grant flows. 

In a horizontally scaled environment (running on multiple OpenShift pods), querying the OIDC server on every task run causes high latency and puts the system at risk of provider rate limits. This design introduces a **shared database-backed caching layer** using PostgreSQL that ensures only one pod requests a new token when it expires, while other pods block, wait, and reuse the newly acquired token.

```
                    ┌─────────────────────────────────┐
                    │      Worker Pods (1 ... N)      │
                    └────┬────────────────────────┬───┘
                         │                        │
               (1. Read Cache)          (2. Row Lock & Refresh)
                         ▼                        ▼
        ┌──────────────────────────────────────────────────┐
        │                 PostgreSQL DB                    │
        │           Table: `openid_tokens`                 │
        └────────────────────────┬─────────────────────────┘
                                 │
                     (3. If Expired, Fetch)
                                 ▼
                     ┌────────────────────────┐
                     │ Identity Provider (IdP)│
                     └────────────────────────┘
```

---

## 2. Database Schema

A new table `openid_tokens` is created during database initialization (`app/core/database.py`):

```sql
CREATE TABLE IF NOT EXISTS openid_tokens (
    service_name VARCHAR(50) PRIMARY KEY,
    access_token TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Table Properties
* **`service_name`**: The key identifier of the downstream API (`docsys`, `docsysarc`, or `gpt`).
* **`access_token`**: The raw string token used in HTTP `Authorization` headers.
* **`expires_at`**: The absolute expiration timestamp calculated dynamically using the token's `expires_in` response field.
* **`updated_at`**: Timestamp recording when the token cache was last updated.

---

## 3. Multi-Pod Concurrency Control (Locking Strategy)

To coordinate token retrieval safely across multiple concurrently scaling pods, `OidcTokenService` implements a two-tiered check:

### Tier 1: Fast-Path Read (Lockless Check)
Before entering a database transaction, a pod queries the cache with a simple select statement:
```sql
SELECT access_token, expires_at FROM openid_tokens WHERE service_name = $1;
```
If the token is found and is valid (having more than `30 seconds` remaining before expiration to prevent clock-drift issues), the pod immediately returns the token without locking or initiating any transactions.

### Tier 2: Atomic Lock-on-Refresh (SELECT FOR UPDATE)
If the token is expired or missing:
1. The pod opens a transaction and acquires an exclusive row lock on the service name row using PostgreSQL row-level locks:
   ```sql
   INSERT INTO openid_tokens (service_name, access_token, expires_at)
   VALUES ($1, '', NOW() - INTERVAL '1 day')
   ON CONFLICT (service_name) DO NOTHING;

   SELECT access_token, expires_at FROM openid_tokens
   WHERE service_name = $1
   FOR UPDATE;
   ```
2. **Post-Lock Check**: Once the lock is acquired, the pod checks the row's expiration timestamp again. This handles the race condition where Pod A and Pod B both saw the token as expired. If Pod A got the lock first and refreshed the token, Pod B will see the newly updated, valid token as soon as it acquires the lock. Pod B will reuse this token instead of sending a duplicate request to the Identity Provider.
3. **Identity Provider Request**: If the token is still expired under the lock, the pod executes the HTTP POST request to get a new token, updates the `openid_tokens` row, and commits the transaction, automatically releasing the lock for other pods.

---

## 4. OIDC Configuration Reference

The system supports a fallback configuration hierarchy. If service-specific settings are not configured, the service will fall back to the global default setting:

| Downstream Service | Token URL | Client ID | Client Secret | Scope | Audience |
|---|---|---|---|---|---|
| **Global Default** | `OIDC_TOKEN_URL` | `OIDC_CLIENT_ID` | `OIDC_CLIENT_SECRET` | `OIDC_SCOPE` | `OIDC_AUDIENCE` |
| **DOCSYS API** | `DOCSYS_OIDC_TOKEN_URL` | `DOCSYS_OIDC_CLIENT_ID` | `DOCSYS_OIDC_CLIENT_SECRET` | `DOCSYS_OIDC_SCOPE` | `DOCSYS_OIDC_AUDIENCE` |
| **DOCSYSARC API** | `DOCSYSARC_OIDC_TOKEN_URL` | `DOCSYSARC_OIDC_CLIENT_ID` | `DOCSYSARC_OIDC_CLIENT_SECRET` | `DOCSYSARC_OIDC_SCOPE` | `DOCSYSARC_OIDC_AUDIENCE` |
| **Secure GPT API** | `GPT_OIDC_TOKEN_URL` | `GPT_OIDC_CLIENT_ID` | `GPT_OIDC_CLIENT_SECRET` | `GPT_OIDC_SCOPE` | `GPT_OIDC_AUDIENCE` |

*Note: If OIDC configurations are completely omitted, the worker automatically falls back to using the static authorization keys/tokens configured in the environment (`DOCSYS_API_TOKEN`, `DOCSYSARC_API_TOKEN`, and `GPT_API_KEY`).*

---

## 5. Implementation Integration

### DOCSYS & DOCSYSARC API client (`app/services/docsys_service.py`)
Accepts the `OidcTokenService` and queries it to retrieve dynamic tokens before compiling the HTTP request headers:
```python
token = await self._get_docsys_token()
headers = {"Authorization": f"Bearer {token}"}
```

### Secure GPT API client (`app/services/gpt_service.py`)
Secure GPT calls are protected by a transient retry decorator (`tenacity`). To prevent failures from token expiration during long-running tasks, the dynamic token lookup is called inside the retried request loop:
```python
async def _do_request() -> dict[str, Any]:
    token = await self._get_gpt_token()
    headers = {"Authorization": f"Bearer {token}"}
    ...
```
