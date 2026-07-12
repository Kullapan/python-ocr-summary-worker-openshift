# Decoupled Async Worker Pool — Multi-Pod Scaling Design

To handle jobs taking an average of **5 minutes**, we decouple message consumption from pipeline execution. This design details how to scale the worker across many OpenShift pods while ensuring **no two pods ever process the same job concurrently**.

---

## 1. Core Architecture

Instead of processing sequentially inside the Kafka consumer loop, the loop acts as an **Ingress Gate**. It immediately persists incoming requests and dispatches them to a distributed task pool backed by PostgreSQL.

```
                  ┌──────────────────┐
                  │ Kafka Input Topic│
                  └────────┬─────────┘
                           │ (Consume & Commit Immediately)
                           ▼
                  ┌──────────────────┐
                  │   worker.py      │
                  └────────┬─────────┘
                           │ (Write RECEIVED State)
                           ▼
                 ┌───────────────────┐
                 │ PostgreSQL DB     │ ◄─── Heartbeat/Lease check
                 │ (State Machine)   │
                 └─────────┬─────────┘
      ┌────────────────────┼────────────────────┐
      │                    │                    │ (Claim via SKIP LOCKED)
      ▼                    ▼                    ▼
┌───────────┐        ┌───────────┐        ┌───────────┐
│   Pod 1   │        │   Pod 2   │        │   Pod N   │
│ (Worker)  │        │ (Worker)  │        │ (Worker)  │
└───────────┘        └───────────┘        └───────────┘
```

---

## 2. Preventing Duplicate Processing: The PostgreSQL Queue Pattern

To prevent race conditions where multiple scaled pods attempt to grab the same document request at the same instant, we utilize PostgreSQL’s row-level locking feature: **`FOR UPDATE SKIP LOCKED`**.

### The Schema Additions
We add three columns to the `job_states` table:
* `worker_id`: Unique identifier for the claiming pod (e.g., Pod hostname from `os.environ["HOSTNAME"]`).
* `locked_at`: Dynamic timestamp when the job was claimed.
* `leased_until`: Expiration time for the claim lease (to handle pod crashes).

### The Atomic "Claim" Query
When a pod has free processing capacity, it runs this atomic SQL query inside a database transaction:

```sql
UPDATE job_states
SET 
    state = 'PROCESSING',
    worker_id = $1,
    locked_at = NOW(),
    leased_until = NOW() + INTERVAL '15 minutes',
    updated_at = NOW()
WHERE id = (
    SELECT id 
    FROM job_states
    WHERE state IN ('RECEIVED', 'FAILED')
       OR (state = 'PROCESSING' AND leased_until < NOW()) -- Reclaim orphaned jobs
    ORDER BY created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, docid, fileid, state;
```

### How `SKIP LOCKED` Guarantees Safety:
1. **Locking (`FOR UPDATE`)**: The inner `SELECT` locks the candidate row immediately. No other transaction can modify it.
2. **Concurrency (`SKIP LOCKED`)**: If Pod 1 is currently locking Row A to write `PROCESSING`, and Pod 2 executes the same query at the same millisecond, Pod 2 will **skip** Row A entirely without blocking and claim Row B instead.
3. **No Duplication**: The query is atomic. Once the transaction commits, the state becomes `PROCESSING`, removing it from the eligible pool for other pods.

---

## 3. Handling Pod Crashes (Lease Renewal & Heartbeats)

Since jobs take 5 minutes, if a pod dies (due to an OpenShift node failure, OOM kill, or eviction), we must ensure the job is not stuck in `PROCESSING` forever.

### The Lease Pattern
* When a pod claims a job, it sets `leased_until` to `NOW() + 15 minutes` (adjust based on max time).
* **Worker Loop**: If a pod crashes, the database lock is released. When the lease expires (`leased_until < NOW()`), the job becomes eligible for reclamation by other active pods (as defined in the `WHERE` clause of the claim query).
* **Heartbeat Task**: For extremely long-running tasks, the pod runs a background coroutine that updates `leased_until = NOW() + INTERVAL '10 minutes'` every 5 minutes to renew its lease while the job is still active.

---

## 4. Internal Pod Concurrency Control (Semaphores)

Each pod limits its own concurrent execution using an `asyncio.Semaphore` to prevent running out of memory (OOM) or CPU.

```python
class PodWorkerPool:
    def __init__(self, max_concurrent_jobs: int = 5):
        self.semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self.worker_id = os.environ.get("HOSTNAME", "local-worker")

    async def start(self):
        while True:
            # Check if we have free slots in this pod
            if self.semaphore.locked():
                await asyncio.sleep(1)
                continue
                
            # Attempt to claim a job from the DB
            job = await self.db.claim_job(self.worker_id)
            if job:
                # Spawn execution in the background
                asyncio.create_task(self._run_job_with_semaphore(job))
            else:
                # No jobs available, sleep before checking again
                await asyncio.sleep(5)

    async def _run_job_with_semaphore(self, job):
        async with self.semaphore:
            await self.processor.process(job["docid"], job["fileid"])
```

---

## 5. Deployment Scaling

* **Kafka Partition Independence**: Because Kafka offsets are committed immediately upon receiving and persisting the event, you do **not** need a large number of Kafka partitions to scale. You can have a single partition and scale to **50+ pods** on OpenShift.
* **Auto-Scaling (HPA)**: You can configure an OpenShift Horizontal Pod Autoscaler (HPA) targeting CPU/Memory usage, or custom metrics (like database queue lag: `SELECT count(*) FROM job_states WHERE state='RECEIVED'`).
