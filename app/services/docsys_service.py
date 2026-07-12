"""DOCSYS and DOCSYSARC API client for document download and archival.

Provides async HTTP operations against DOCSYS (document download) and
DOCSYSARC (document archival) using httpx streaming for memory efficiency.
"""

from __future__ import annotations

import re
from io import BytesIO

import httpx
import structlog

from app.config import Settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

_DEFAULT_TIMEOUT_SECONDS: float = 60.0
_DEFAULT_CONTENT_TYPE: str = "application/octet-stream"


class DocsysService:
    """HTTP client for DOCSYS document download and DOCSYSARC archival APIs."""

    def __init__(self, settings: Settings) -> None:
        """Initialise the service with connection details from settings.

        Args:
            settings: Application settings containing base URLs and auth tokens.
        """
        self._docsys_base_url: str = settings.docsys_base_url.rstrip("/")
        self._docsysarc_base_url: str = settings.docsysarc_base_url.rstrip("/")
        self._docsys_token: str = settings.docsys_api_token
        self._docsysarc_token: str = settings.docsysarc_api_token
        self._timeout: float = _DEFAULT_TIMEOUT_SECONDS

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def download_document(
        self,
        docid: str,
        fileid: str,
    ) -> tuple[bytes, str, str]:
        """Download a document file from DOCSYS using streaming.

        Args:
            docid:  The document identifier.
            fileid: The file identifier within the document.

        Returns:
            A tuple of ``(file_bytes, content_type, filename)``.

        Raises:
            httpx.HTTPStatusError: If the server responds with a non-2xx status.
        """
        url = (
            f"{self._docsys_base_url}/api/v1/documents/{docid}"
            f"/files/{fileid}/download"
        )
        headers = {"Authorization": f"Bearer {self._docsys_token}"}

        logger.info(
            "docsys.download.start",
            docid=docid,
            fileid=fileid,
            url=url,
        )

        async with httpx.AsyncClient(timeout=httpx.Timeout(self._timeout)) as client:
            async with client.stream("GET", url, headers=headers) as response:
                response.raise_for_status()

                buffer = BytesIO()
                async for chunk in response.aiter_bytes():
                    buffer.write(chunk)

                file_bytes = buffer.getvalue()

                content_type = response.headers.get(
                    "content-type",
                    _DEFAULT_CONTENT_TYPE,
                )
                filename = _parse_filename(
                    response.headers.get("content-disposition"),
                    fallback=f"{docid}_{fileid}",
                )

        logger.info(
            "docsys.download.complete",
            docid=docid,
            fileid=fileid,
            size_bytes=len(file_bytes),
            content_type=content_type,
            filename=filename,
        )

        return file_bytes, content_type, filename

    async def archive_document(
        self,
        docid: str,
        fileid: str,
        report_data: bytes,
        filename: str,
    ) -> dict:
        """Archive a processed report to DOCSYSARC via multipart upload.

        Args:
            docid:       The document identifier.
            fileid:      The file identifier within the document.
            report_data: Raw bytes of the generated PDF report.
            filename:    Filename to use for the uploaded file.

        Returns:
            The JSON response body from the archival endpoint.

        Raises:
            httpx.HTTPStatusError: If the server responds with a non-2xx status.
        """
        url = (
            f"{self._docsysarc_base_url}/api/v1/documents/{docid}"
            f"/files/{fileid}/archive"
        )
        headers = {"Authorization": f"Bearer {self._docsysarc_token}"}

        logger.info(
            "docsysarc.archive.start",
            docid=docid,
            fileid=fileid,
            filename=filename,
            size_bytes=len(report_data),
        )

        async with httpx.AsyncClient(timeout=httpx.Timeout(self._timeout)) as client:
            response = await client.post(
                url,
                headers=headers,
                files={"file": (filename, report_data, "application/pdf")},
            )
            response.raise_for_status()
            result: dict = response.json()

        logger.info(
            "docsysarc.archive.complete",
            docid=docid,
            fileid=fileid,
            response=result,
        )

        return result


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _parse_filename(disposition: str | None, *, fallback: str) -> str:
    """Extract the filename from a ``Content-Disposition`` header value.

    Handles both ``filename="quoted"`` and ``filename=unquoted`` forms.

    Args:
        disposition: Raw header value (may be *None*).
        fallback:    Value to return when the header is absent or unparseable.

    Returns:
        The extracted filename or the *fallback*.
    """
    if not disposition:
        return fallback

    match = re.search(r'filename\*?=["\']?([^"\';]+)', disposition)
    if match:
        return match.group(1).strip()

    return fallback
