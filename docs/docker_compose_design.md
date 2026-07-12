# Local Development & Testing Environment Design

This document details the architecture and design of the containerized local development environment for the **OCR Summary Worker**.

---

## 1. Overview

To support local debugging, verification, and end-to-end integration tests without relying on real cloud APIs (which may require specific VPN routing, authentication keys, or incur usage costs), the pipeline’s dependencies are entirely containerized and mocked using Docker Compose.

```
                     ┌──────────────────────────────────┐
                     │           docker-compose         │
                     │                                  │
  ┌───────────────┐  │  ┌──────────────┐  ┌──────────┐  │
  │ Ingress Event │ ─┼─►│    Kafka     │─►│  Worker  │  │
  └───────────────┘  │  └──────────────┘  └────┬─────┘  │
                     │                         │        │
                     │  ┌──────────────┐◄──────┼───────┐│
                     │  │  PostgreSQL  │◄──┐   │       ││
                     │  └──────────────┘   │   │       ││
                     │                     │   ▼       ││
                     │  ┌──────────────┐   │┌───────┐  ││
                     │  │    MinIO     │◄──┼┤  S3   │  ││
                     │  └──────────────┘   │└───────┘  ││
                     │                     │           ││
                     │  ┌──────────────┐   │┌───────┐  ││
                     │  │  Mock APIs   │◄──┴┤ OIDC  │  ││
                     │  │  - OIDC      │    │Clients│  ││
                     │  │  - DOCSYS    │    └───────┘  ││
                     │  │  - GPT       │               ││
                     │  │  - DOCSYSARC │               ││
                     │  └──────────────┘               ││
                     └──────────────────────────────────┘
```

---

## 2. Container Topology

The environment consists of three categories of services orchestrated via `docker-compose.yml`:

### A. Infrastructure Core
* **`postgres` (PostgreSQL 15)**: Stores the state machine logs in the `job_states` table and caches tokens in the `openid_tokens` table.
* **`kafka` (Apache Kafka via KRaft)**: Single-node Kafka broker handling event distribution.
* **`minio` (S3 Compatible)**: Emulates AWS S3 for object storage.

### B. Setup Helpers
* **`kafka-setup`**: A transient container that waits for Kafka health checks and creates the required input/output topics.
* **`minio-setup`**: A transient container that sets up client configuration (`mc`) and pre-creates the `ocr-documents` bucket.

### C. Downstream API Mocks (`mocks/` Directory)
Four lightweight Python FastAPI containers serve as drop-in replacements for real systems:
1. **`mock-oidc` (Port `8081`)**: Implements standard OAuth2 token generation routes.
2. **`mock-docsys` (Port `8082`)**: Simulates document storage; provides download links.
3. **`mock-gpt` (Port `8083`)**: Emulates OCR extraction (`/api/v1/ocr`) and OpenAI chat completion endpoints (`/api/v1/chat/completions`).
4. **`mock-docsysarc` (Port `8084`)**: Receives the final report and logs archival confirmation.

---

## 3. Configuration Management

The environment relies on [`.env.compose`](file:///c:/KK/Workspace/AntigravityProject/python-ocr-summary-worker-openshift/.env.compose) to bind ports and assign resource URLs.

* **DB Init**: The worker automatically invokes `await db.init_db()` upon startup, creating the schema directly in the empty database without needing schema files.
* **VPCE S3 Endpoint**: By configuring `S3_ENDPOINT_URL=http://minio:9000` and assigning dummy keys, `boto3` resolves storage transactions to the MinIO container instead of AWS endpoints.
* **Authentication**: Service-specific token credentials (e.g. `GPT_OIDC_TOKEN_URL`, `GPT_OIDC_CLIENT_ID`, `GPT_OIDC_CLIENT_SECRET`) direct requests to the OIDC mock to generate temporary access keys, storing them securely in PostgreSQL (`openid_tokens` cache).

---

## 4. End-to-End Execution Flow

When a trigger is executed locally, the message flows as follows:

```
[Trigger Ingress JSON] ──► (Kafka Ingest Loop) 
                             │
                             ▼ (DB: RECEIVED state)
                           (Claim Loop)
                             │
                             ▼ (DB: PROCESSING state)
                           (Download doc bytes from Mock DOCSYS)
                             │
                             ▼ (DB: DOWNLOADED)
                           (Upload raw bytes to MinIO S3)
                             │
                             ▼ (DB: UPLOADED_S3)
                           (Post doc bytes to Mock GPT /api/v1/ocr)
                             │
                             ▼ (DB: OCR_COMPLETED)
                           (Post OCR text to Mock GPT chat endpoint)
                             │
                             ▼ (DB: SUMMARY_COMPLETED)
                           (Post report to Mock DOCSYSARC)
                             │
                             ▼ (DB: ARCHIVED)
                           (Publish JSON result payload to Kafka)
                             │
                             ▼ (DB: SUCCESS state)
```

---

## 5. Visual Management Interfaces (Web Dashboards)

To easily inspect application state, message queues, and object storage local assets, the environment bundles three web dashboards:

* **PostgreSQL UI (Adminer)**: `http://localhost:8086`
  - **System**: `PostgreSQL`
  - **Server**: `postgres`
  - **Username**: `ocruser`
  - **Password**: `ocrpass`
  - **Database**: `ocrdb`
* **Kafdrop (Kafka Web UI)**: `http://localhost:8085`
  - Enables viewing cluster info, broker configurations, topics, partitions, consumer groups, and browsing individual messages.
* **MinIO Console (Object Storage UI)**: `http://localhost:9001`
  - **Username**: `minioadmin`
  - **Password**: `minioadmin`
  - Enables viewing the `ocr-documents` bucket and downloading intermediate files.

---

## 6. Verification Commands

### Start Environment
```bash
docker compose --env-file .env.compose up --build
```

### Inject Test Event
```bash
docker exec -i ocr-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic doc-processing-requests <<< '{"docid": "test-doc-123", "fileid": "test-file-456"}'
```

### Consume Results Topic
```bash
docker exec -it ocr-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic doc-processing-results --from-beginning --max-messages 1
```

---

## 6. Flexible Mock Replacement

To test code modifications against real APIs:
1. Open the [`.env.compose`](file:///c:/KK/Workspace/AntigravityProject/python-ocr-summary-worker-openshift/.env.compose) file.
2. Locate the service you wish to test (e.g. `mock-gpt`).
3. Replace the mock URL and client variables with real staging credentials:
   ```properties
   GPT_BASE_URL=https://real-staging-gpt.company.com
   GPT_OIDC_TOKEN_URL=https://real-keycloak.company.com/...
   GPT_OIDC_CLIENT_ID=my-real-client-id
   GPT_OIDC_CLIENT_SECRET=my-real-secret
   ```
4. Comment out the corresponding mock service definition under `docker-compose.yml` to save resources.
5. Re-run `docker compose up --build`.
