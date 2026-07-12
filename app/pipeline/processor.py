"""Document processing pipeline — 7-step orchestrator with state-machine recovery.

Executes: RECEIVED → DOWNLOADED → UPLOADED_S3 → OCR_COMPLETED
          → SUMMARY_COMPLETED → ARCHIVED → SUCCESS

S3 object keys are generated deterministically using the job ID. The database
only persists the raw document's filename to preserve the file extension.
"""
from __future__ import annotations

import time
from typing import Any

import structlog
import structlog.contextvars

from app.config import Settings
from app.core.database import DatabaseManager
from app.core.s3_client import S3Client
from app.core.kafka_producer import KafkaStatusProducer
from app.services.docsys_service import DocsysService
from app.services.gpt_service import SecureGptService

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Job state constants
# ---------------------------------------------------------------------------

class JobState:
    """Possible states for a pipeline job."""

    RECEIVED: str = "RECEIVED"
    DOWNLOADED: str = "DOWNLOADED"
    UPLOADED_S3: str = "UPLOADED_S3"
    OCR_COMPLETED: str = "OCR_COMPLETED"
    SUMMARY_COMPLETED: str = "SUMMARY_COMPLETED"
    ARCHIVED: str = "ARCHIVED"
    SUCCESS: str = "SUCCESS"


# Ordered list used for recovery — index indicates completion rank.
STATE_ORDER: list[str] = [
    JobState.RECEIVED,
    JobState.DOWNLOADED,
    JobState.UPLOADED_S3,
    JobState.OCR_COMPLETED,
    JobState.SUMMARY_COMPLETED,
    JobState.ARCHIVED,
    JobState.SUCCESS,
]


def _state_index(state: str) -> int:
    """Return the ordinal position of *state* in STATE_ORDER."""
    try:
        return STATE_ORDER.index(state)
    except ValueError:
        return -1


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

class DocumentProcessor:
    """Executes the full OCR-summary pipeline for a single document.

    All infrastructure dependencies are injected via the constructor so the
    class remains testable without touching real services.
    """

    def __init__(
        self,
        db: DatabaseManager,
        s3: S3Client,
        kafka_producer: KafkaStatusProducer,
        docsys: DocsysService,
        gpt: SecureGptService,
        settings: Settings,
    ) -> None:
        self._db = db
        self._s3 = s3
        self._kafka = kafka_producer
        self._docsys = docsys
        self._gpt = gpt
        self._settings = settings

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def process(self, job_id: str, docid: str, fileid: str) -> dict[str, Any]:
        """Run (or resume) the pipeline for a single document.

        Args:
            job_id: Client-provided unique correlation/job ID.
            docid: Document identifier in DOCSYS.
            fileid: File identifier within the document.

        Returns:
            A dict with ``job_id``, ``status``, and processing metadata.
        """
        # Clear context variables and bind job_id/docid/fileid globally for distributed tracing
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(job_id=job_id, docid=docid, fileid=fileid)

        log = logger.bind(docid=docid, fileid=fileid, job_id=job_id)
        start_ts = time.monotonic()

        # ---- recover or create job ------------------------------------
        job_id, current_state = await self._resolve_job(job_id, docid, fileid, log)
        resume_idx = _state_index(current_state)

        log.info(
            "pipeline.start",
            current_state=current_state,
            resume_index=resume_idx,
        )

        # Retrieve job details to fetch filename in case we are resuming
        job = await self._db.get_job(job_id)
        filename_db = job.get("filename") if job else ""

        # ---- transient context shared across steps --------------------
        ctx: dict[str, Any] = {
            "file_bytes": b"",
            "content_type": "",
            "filename": filename_db or "",
        }

        # ---- execute steps, skipping completed ones -------------------
        try:
            if resume_idx < _state_index(JobState.DOWNLOADED):
                file_bytes, content_type, filename = await self._step_download(
                    job_id, docid, fileid, log,
                )
                ctx["file_bytes"] = file_bytes
                ctx["content_type"] = content_type
                ctx["filename"] = filename

            if resume_idx < _state_index(JobState.UPLOADED_S3):
                # If we skipped download (resume), re-download to get bytes
                if not ctx["file_bytes"]:
                    ctx["file_bytes"], ctx["content_type"], ctx["filename"] = (
                        await self._step_download(job_id, docid, fileid, log)
                    )
                await self._step_upload_s3(
                    job_id,
                    ctx["file_bytes"],
                    ctx["content_type"],
                    ctx["filename"],
                    log,
                )

            if resume_idx < _state_index(JobState.OCR_COMPLETED):
                if not ctx["file_bytes"]:
                    ctx["file_bytes"], ctx["content_type"], ctx["filename"] = (
                        await self._step_download(job_id, docid, fileid, log)
                    )
                await self._step_ocr(
                    job_id,
                    ctx["file_bytes"],
                    ctx["filename"],
                    log,
                )

            if resume_idx < _state_index(JobState.SUMMARY_COMPLETED):
                await self._step_summarize(job_id, log)

            if resume_idx < _state_index(JobState.ARCHIVED):
                await self._step_archive(job_id, docid, fileid, log)

            if resume_idx < _state_index(JobState.SUCCESS):
                await self._step_publish(job_id, docid, fileid, log)

        except Exception:
            elapsed = time.monotonic() - start_ts
            log.error("pipeline.failed", elapsed_s=round(elapsed, 3))
            raise

        elapsed = time.monotonic() - start_ts
        log.info("pipeline.completed", elapsed_s=round(elapsed, 3))

        return {
            "job_id": job_id,
            "status": JobState.SUCCESS,
            "docid": docid,
            "fileid": fileid,
            "filename": ctx.get("filename"),
            "elapsed_s": round(elapsed, 3),
        }

    # ------------------------------------------------------------------
    # Job resolution / recovery
    # ------------------------------------------------------------------

    async def _resolve_job(
        self,
        job_id: str,
        docid: str,
        fileid: str,
        log: structlog.stdlib.BoundLogger,
    ) -> tuple[str, str]:
        """Find an existing job or create a new one.

        Recovery rules
        ~~~~~~~~~~~~~~
        * No existing job → create with state RECEIVED.
        * Existing job in SUCCESS → return as-is (caller can short-circuit).
        * Existing job with error_message → clear error and resume from last good state.
        * Existing job in-progress → resume from current state.

        Returns:
            ``(job_id, current_state)``
        """
        existing = await self._db.get_job(job_id)

        if existing is None:
            await self._db.create_job(job_id, docid, fileid)
            log.info("job.created", job_id=job_id, state=JobState.RECEIVED)
            return job_id, JobState.RECEIVED

        state: str = existing["state"]

        if existing.get("error_message") is not None:
            # Job failed previously, clear error and resume from its last completed state
            await self._db.update_job_state(job_id, state, error_message=None)
            log.warning(
                "job.recovery_from_failure",
                job_id=job_id,
                failed_at_state=state,
                error=existing["error_message"],
            )
            return job_id, state

        log.info("job.resumed", job_id=job_id, state=state)
        return job_id, state

    # ------------------------------------------------------------------
    # Pipeline steps
    # ------------------------------------------------------------------

    async def _step_download(
        self,
        job_id: str,
        docid: str,
        fileid: str,
        log: structlog.stdlib.BoundLogger,
    ) -> tuple[bytes, str, str]:
        """Step 1 — download file from DOCSYS."""
        step = "download"
        log = log.bind(step=step)
        log.info("step.start")

        try:
            file_bytes, content_type, filename = await self._docsys.download_document(
                docid, fileid,
            )
            await self._db.update_job_state(job_id, JobState.DOWNLOADED, filename=filename)
            log.info(
                "step.completed",
                size_bytes=len(file_bytes),
                content_type=content_type,
                filename=filename,
            )
            return file_bytes, content_type, filename

        except Exception as exc:
            # Clear lock details on failure to allow reclamation by other pods
            await self._db.update_job_state(
                job_id,
                JobState.RECEIVED,
                error_message=f"[{step}] {exc!r}",
                worker_id=None,
                locked_at=None,
                leased_until=None,
            )
            log.error("step.failed", error=str(exc), exc_info=True)
            raise

    async def _step_upload_s3(
        self,
        job_id: str,
        file_bytes: bytes,
        content_type: str,
        filename: str,
        log: structlog.stdlib.BoundLogger,
    ) -> None:
        """Step 2 — upload raw document to S3."""
        step = "upload_s3"
        log = log.bind(step=step)
        log.info("step.start", size_bytes=len(file_bytes))

        try:
            ext = filename.rsplit(".", 1)[-1] if "." in filename else "bin"
            s3_key = self._s3.generate_key(job_id, "raw", ext)

            self._s3.upload_bytes(s3_key, file_bytes, content_type)
            await self._db.update_job_state(job_id, JobState.UPLOADED_S3)
            log.info("step.completed", s3_key=s3_key)

        except Exception as exc:
            # Clear lock details on failure to allow reclamation by other pods
            await self._db.update_job_state(
                job_id,
                JobState.DOWNLOADED,
                error_message=f"[{step}] {exc!r}",
                worker_id=None,
                locked_at=None,
                leased_until=None,
            )
            log.error("step.failed", error=str(exc), exc_info=True)
            raise

    async def _step_ocr(
        self,
        job_id: str,
        file_bytes: bytes,
        filename: str,
        log: structlog.stdlib.BoundLogger,
    ) -> None:
        """Step 3 — perform OCR via Secure GPT and persist text to S3."""
        step = "ocr"
        log = log.bind(step=step)
        log.info("step.start", filename=filename)

        try:
            ocr_text: str = await self._gpt.perform_ocr(file_bytes, filename)

            s3_ocr_key = self._s3.generate_key(job_id, "ocr", "txt")
            self._s3.upload_bytes(
                s3_ocr_key, ocr_text.encode("utf-8"), "text/plain",
            )

            await self._db.update_job_state(job_id, JobState.OCR_COMPLETED)
            log.info(
                "step.completed",
                s3_ocr_key=s3_ocr_key,
                ocr_text_length=len(ocr_text),
            )

        except Exception as exc:
            # Clear lock details on failure to allow reclamation by other pods
            await self._db.update_job_state(
                job_id,
                JobState.UPLOADED_S3,
                error_message=f"[{step}] {exc!r}",
                worker_id=None,
                locked_at=None,
                leased_until=None,
            )
            log.error("step.failed", error=str(exc), exc_info=True)
            raise

    async def _step_summarize(
        self,
        job_id: str,
        log: structlog.stdlib.BoundLogger,
    ) -> None:
        """Step 4 — download OCR text from S3, summarise via GPT, persist."""
        step = "summarize"
        log = log.bind(step=step)
        log.info("step.start")

        try:
            # Reconstruct the deterministic OCR key
            s3_ocr_key = self._s3.generate_key(job_id, "ocr", "txt")

            ocr_bytes: bytes = self._s3.download_bytes(s3_ocr_key)
            ocr_text = ocr_bytes.decode("utf-8")

            report: str = await self._gpt.generate_summary(ocr_text)

            s3_report_key = self._s3.generate_key(job_id, "report", "txt")
            self._s3.upload_bytes(
                s3_report_key, report.encode("utf-8"), "text/plain",
            )

            await self._db.update_job_state(job_id, JobState.SUMMARY_COMPLETED)
            log.info(
                "step.completed",
                s3_report_key=s3_report_key,
                report_length=len(report),
            )

        except Exception as exc:
            # Clear lock details on failure to allow reclamation by other pods
            await self._db.update_job_state(
                job_id,
                JobState.OCR_COMPLETED,
                error_message=f"[{step}] {exc!r}",
                worker_id=None,
                locked_at=None,
                leased_until=None,
            )
            log.error("step.failed", error=str(exc), exc_info=True)
            raise

    async def _step_archive(
        self,
        job_id: str,
        docid: str,
        fileid: str,
        log: structlog.stdlib.BoundLogger,
    ) -> None:
        """Step 5 — download report from S3, upload to DOCSYSARC."""
        step = "archive"
        log = log.bind(step=step)
        log.info("step.start")

        try:
            # Reconstruct deterministic report key
            s3_report_key = self._s3.generate_key(job_id, "report", "txt")

            report_bytes: bytes = self._s3.download_bytes(s3_report_key)

            await self._docsys.archive_document(
                docid,
                fileid,
                report_bytes,
                f"{docid}_{fileid}_report.txt",
            )

            await self._db.update_job_state(job_id, JobState.ARCHIVED)
            log.info("step.completed", report_size=len(report_bytes))

        except Exception as exc:
            # Clear lock details on failure to allow reclamation by other pods
            await self._db.update_job_state(
                job_id,
                JobState.SUMMARY_COMPLETED,
                error_message=f"[{step}] {exc!r}",
                worker_id=None,
                locked_at=None,
                leased_until=None,
            )
            log.error("step.failed", error=str(exc), exc_info=True)
            raise

    async def _step_publish(
        self,
        job_id: str,
        docid: str,
        fileid: str,
        log: structlog.stdlib.BoundLogger,
    ) -> None:
        """Step 6 — publish SUCCESS event to Kafka output topic."""
        step = "publish"
        log = log.bind(step=step)
        log.info("step.start")

        try:
            payload: dict[str, Any] = {
                "job_id": job_id,
                "docid": docid,
                "fileid": fileid,
                "status": JobState.SUCCESS,
            }
            await self._kafka.publish_status(payload)

            # Clear lock details on successful completion
            await self._db.update_job_state(
                job_id, JobState.SUCCESS,
                worker_id=None, locked_at=None, leased_until=None
            )
            log.info("step.completed", topic=self._settings.kafka_output_topic)

        except Exception as exc:
            # Clear lock details on failure to allow reclamation by other pods
            await self._db.update_job_state(
                job_id,
                JobState.ARCHIVED,
                error_message=f"[{step}] {exc!r}",
                worker_id=None,
                locked_at=None,
                leased_until=None,
            )
            log.error("step.failed", error=str(exc), exc_info=True)
            raise
