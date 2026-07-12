"""S3-compatible object-storage client with VPC Endpoint support.

Wraps ``boto3`` to provide upload/download helpers used throughout
the OCR Summary Worker pipeline.
"""

from __future__ import annotations

from io import BytesIO
from typing import Optional

import boto3
import structlog

from app.config import Settings

logger = structlog.get_logger(__name__)


class S3Client:
    """Thin wrapper around a ``boto3`` S3 client.

    Supports optional VPC Endpoint URL and explicit credentials.
    When credentials are not provided the standard AWS credential
    chain (env vars, instance profile, etc.) is used instead.
    """

    def __init__(self, settings: Settings) -> None:
        client_kwargs: dict[str, Optional[str]] = {
            "region_name": settings.s3_region_name,
        }

        if settings.s3_endpoint_url:
            client_kwargs["endpoint_url"] = settings.s3_endpoint_url

        if settings.s3_access_key_id and settings.s3_secret_access_key:
            client_kwargs["aws_access_key_id"] = settings.s3_access_key_id
            client_kwargs["aws_secret_access_key"] = settings.s3_secret_access_key

        self.client = boto3.client("s3", **client_kwargs)  # type: ignore[arg-type]
        self.bucket = settings.s3_bucket_name

        logger.info(
            "s3.client_initialised",
            bucket=self.bucket,
            endpoint_url=settings.s3_endpoint_url,
            region=settings.s3_region_name,
        )

    # ── Upload ──────────────────────────────────────────────────────

    def upload_fileobj(
        self,
        key: str,
        fileobj: BytesIO,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload a file-like object to S3.

        Args:
            key: The S3 object key (path).
            fileobj: A file-like object positioned at the start.
            content_type: MIME type stored as ``ContentType`` metadata.
        """
        try:
            self.client.upload_fileobj(
                Fileobj=fileobj,
                Bucket=self.bucket,
                Key=key,
                ExtraArgs={"ContentType": content_type},
            )
            logger.info("s3.uploaded", key=key, content_type=content_type)
        except Exception:
            logger.exception("s3.upload_failed", key=key)
            raise

    def upload_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload raw bytes to S3.

        Convenience wrapper that converts *data* to a ``BytesIO`` stream
        and delegates to :meth:`upload_fileobj`.

        Args:
            key: The S3 object key (path).
            data: Raw bytes to upload.
            content_type: MIME type stored as ``ContentType`` metadata.
        """
        self.upload_fileobj(key, BytesIO(data), content_type)

    # ── Download ────────────────────────────────────────────────────

    def download_fileobj(self, key: str) -> BytesIO:
        """Download an S3 object into a ``BytesIO`` buffer.

        Args:
            key: The S3 object key (path).

        Returns:
            A ``BytesIO`` buffer rewound to position 0.
        """
        buf = BytesIO()
        try:
            self.client.download_fileobj(
                Bucket=self.bucket,
                Key=key,
                Fileobj=buf,
            )
            buf.seek(0)
            logger.info("s3.downloaded", key=key, size=buf.getbuffer().nbytes)
        except Exception:
            logger.exception("s3.download_failed", key=key)
            raise
        return buf

    def download_bytes(self, key: str) -> bytes:
        """Download an S3 object as raw bytes.

        Args:
            key: The S3 object key (path).

        Returns:
            The full object content as ``bytes``.
        """
        return self.download_fileobj(key).read()

    # ── Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def generate_key(job_id: str, stage: str, ext: str) -> str:
        """Build a deterministic S3 key for a pipeline artefact.

        Convention: ``jobs/<job_id>/<stage>.<ext>``

        Args:
            job_id: UUID of the processing job.
            stage: Pipeline stage name (e.g. ``'raw'``, ``'ocr'``).
            ext: File extension without the leading dot (e.g. ``'pdf'``).

        Returns:
            The formatted S3 object key.
        """
        return f"jobs/{job_id}/{stage}.{ext}"
