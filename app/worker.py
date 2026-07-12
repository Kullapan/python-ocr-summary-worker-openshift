"""OCR Summary Worker — Kafka consumer entry point.

Runs an infinite ``aiokafka`` consumer loop that reads document-processing
requests, registers them in PostgreSQL (acting as a distributed task queue),
commits offsets immediately, and processes tasks concurrently using a
decoupled async worker pool.

Usage::

    python -m app.worker
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer

from app.config import Settings, get_settings
from app.core.database import DatabaseManager
from app.core.s3_client import S3Client
from app.core.kafka_producer import KafkaStatusProducer
from app.services.docsys_service import DocsysService
from app.services.gpt_service import SecureGptService
from app.pipeline.processor import DocumentProcessor

# ---------------------------------------------------------------------------
# Structured logging — configure once at module level
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger("app.worker")


# ---------------------------------------------------------------------------
# Consumer factory
# ---------------------------------------------------------------------------

def _build_consumer(settings: Settings) -> AIOKafkaConsumer:
    """Create an ``AIOKafkaConsumer`` from application settings.

    SASL authentication is configured automatically when
    ``security_protocol`` is not ``PLAINTEXT``.
    """
    kwargs: dict[str, Any] = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "group_id": settings.kafka_group_id,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
    }

    if settings.kafka_security_protocol and settings.kafka_security_protocol != "PLAINTEXT":
        kwargs["security_protocol"] = settings.kafka_security_protocol
        if settings.kafka_sasl_mechanism:
            kwargs["sasl_mechanism"] = settings.kafka_sasl_mechanism
        if settings.kafka_sasl_username:
            kwargs["sasl_plain_username"] = settings.kafka_sasl_username
        if settings.kafka_sasl_password:
            kwargs["sasl_plain_password"] = settings.kafka_sasl_password

    return AIOKafkaConsumer(settings.kafka_input_topic, **kwargs)


# ---------------------------------------------------------------------------
# Decoupled Queue Worker Loops
# ---------------------------------------------------------------------------

async def kafka_consumer_loop(
    consumer: AIOKafkaConsumer,
    db: DatabaseManager,
    shutdown_event: asyncio.Event,
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Read document processing requests and register them in the DB queue."""
    try:
        async for msg in consumer:
            if shutdown_event.is_set():
                break

            value: dict[str, Any] = msg.value
            docid: str = value.get("docid", "")
            fileid: str = value.get("fileid", "")
            msg_log = log.bind(
                docid=docid,
                fileid=fileid,
                topic=msg.topic,
                partition=msg.partition,
                offset=msg.offset,
            )

            if not docid or not fileid:
                msg_log.warning("worker.invalid_message", raw_value=value)
                await consumer.commit()
                continue

            msg_log.info("worker.message_received")

            # Register/retrieve the job state in PostgreSQL
            existing = await db.get_job_by_document(docid, fileid)
            if existing is None:
                job_id = await db.create_job(docid, fileid)
                msg_log.info("worker.job_registered", job_id=job_id)
            else:
                msg_log.info(
                    "worker.job_already_exists",
                    job_id=str(existing["id"]),
                    state=existing["state"],
                )

            # Immediately commit the offset to Kafka to prevent lag and timeout
            await consumer.commit()
            msg_log.info("worker.offset_committed")

    except asyncio.CancelledError:
        log.info("worker.consumer_loop_cancelled")
    except Exception:
        log.exception("worker.consumer_loop_error")
        shutdown_event.set()


async def claim_loop(
    db: DatabaseManager,
    processor: DocumentProcessor,
    settings: Settings,
    shutdown_event: asyncio.Event,
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Periodically claim and execute pending jobs using FOR UPDATE SKIP LOCKED."""
    worker_id = os.environ.get("HOSTNAME", socket.gethostname())
    semaphore = asyncio.Semaphore(settings.worker_concurrency)
    active_tasks: set[asyncio.Task] = set()

    log.info(
        "worker.claim_loop_started",
        worker_id=worker_id,
        concurrency=settings.worker_concurrency,
    )

    async def _run_job(job: dict[str, Any]) -> None:
        job_id = str(job["id"])
        docid = job["docid"]
        fileid = job["fileid"]
        job_log = log.bind(job_id=job_id, docid=docid, fileid=fileid)

        async with semaphore:
            try:
                await processor.process(docid, fileid)
                job_log.info("worker.job_execution_completed")
            except Exception:
                job_log.exception("worker.job_execution_failed")

    try:
        while not shutdown_event.is_set():
            # If the pod concurrency limit is reached, wait for a slot
            if semaphore.locked():
                await asyncio.sleep(0.5)
                continue

            # Attempt to claim a job with a 15-minute lease (900 seconds)
            job = await db.claim_job(worker_id, lease_seconds=900)
            if job:
                task = asyncio.create_task(_run_job(job))
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)
            else:
                # Sleep briefly if no work is available
                await asyncio.sleep(2.0)

    except asyncio.CancelledError:
        log.info("worker.claim_loop_cancelled")
    finally:
        # Wait for all running tasks in this pod to finish before shutting down
        if active_tasks:
            log.info("worker.waiting_for_active_tasks", count=len(active_tasks))
            await asyncio.gather(*active_tasks, return_exceptions=True)
            log.info("worker.all_active_tasks_finished")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def main() -> None:
    """Bootstrap all components and enter the Kafka consumer loop.

    Lifecycle
    ~~~~~~~~~
    1. Load settings, create infrastructure clients.
    2. Connect to PostgreSQL, start Kafka producer, ensure DB schema.
    3. Build the :class:`DocumentProcessor`.
    4. Spawn Kafka consumer and CLAIM loops concurrently.
    5. Tear down gracefully on SIGTERM/SIGINT.
    """
    settings: Settings = get_settings()
    log = logger.bind(component="main")
    log.info("worker.starting", kafka_input_topic=settings.kafka_input_topic)

    # ---- infrastructure -------------------------------------------------
    db = DatabaseManager(settings)
    s3 = S3Client(settings)
    kafka_producer = KafkaStatusProducer(settings)
    docsys = DocsysService(settings)
    gpt = SecureGptService(settings)

    # ---- connect / initialise -------------------------------------------
    await db.connect()
    await kafka_producer.start()
    await db.init_db()

    log.info("worker.infrastructure_ready")

    # ---- processor ------------------------------------------------------
    processor = DocumentProcessor(
        db=db,
        s3=s3,
        kafka_producer=kafka_producer,
        docsys=docsys,
        gpt=gpt,
        settings=settings,
    )

    # ---- Kafka consumer -------------------------------------------------
    consumer: AIOKafkaConsumer = _build_consumer(settings)
    await consumer.start()

    log.info(
        "worker.consumer_started",
        group_id=settings.kafka_group_id,
        topic=settings.kafka_input_topic,
    )

    # ---- graceful shutdown via signal handlers --------------------------
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig: signal.Signals) -> None:
        log.info("worker.shutdown_requested", signal=sig.name)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_shutdown, sig)
        except NotImplementedError:
            # Windows fallback
            signal.signal(sig, lambda s, _f: _request_shutdown(signal.Signals(s)))

    # ---- spawn concurrent tasks ----------------------------------------
    consumer_task = asyncio.create_task(
        kafka_consumer_loop(consumer, db, shutdown_event, log)
    )
    claim_task = asyncio.create_task(
        claim_loop(db, processor, settings, shutdown_event, log)
    )

    # Wait until shutdown is requested
    await shutdown_event.wait()
    log.info("worker.initiating_teardown")

    # Cancel consumer loop first to prevent receiving new events
    consumer_task.cancel()
    await asyncio.gather(consumer_task, return_exceptions=True)

    # Wait for claimant worker loop and its active jobs to finish
    claim_task.cancel()
    await asyncio.gather(claim_task, return_exceptions=True)

    # ---- teardown ---------------------------------------------------
    log.info("worker.teardown_start")
    await consumer.stop()
    await kafka_producer.stop()
    await db.disconnect()
    log.info("worker.teardown_complete")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(main())
