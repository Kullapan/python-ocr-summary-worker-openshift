"""Secure GPT API client for OCR and summarization with tenacity retries.

Wraps the GPT service endpoints for OCR extraction and chat-based
summarization.  All outbound HTTP calls are protected by exponential-backoff
retries via tenacity, covering transient network errors, timeouts, and
server-side rate limiting (HTTP 429).
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx
import structlog
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.config import Settings
from app.core.oidc_service import OidcTokenService

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

# stdlib logger required by tenacity's ``before_sleep_log`` helper
_tenacity_logger: logging.Logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SECONDS: float = 300.0
_DEFAULT_MAX_RETRIES: int = 3


class SecureGptService:
    """Async client for the Secure GPT OCR and summarization APIs."""

    def __init__(self, settings: Settings, oidc_service: Optional[OidcTokenService] = None) -> None:
        """Initialise the service from application settings.

        Args:
            settings: Application settings containing GPT connection details.
            oidc_service: Optional service to dynamically retrieve OIDC tokens.
        """
        self._base_url: str = settings.gpt_base_url.rstrip("/")
        self._api_key: str = settings.gpt_api_key
        self._model: str = settings.gpt_model
        self._timeout: float = float(settings.gpt_timeout_seconds)
        self._max_retries: int = settings.gpt_max_retries
        self._oidc_service = oidc_service

    async def _get_gpt_token(self) -> str:
        """Resolve GPT OIDC token or fall back to static API key."""
        if self._oidc_service:
            token = await self._oidc_service.get_token("gpt")
            if token:
                return token
        return self._api_key

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def perform_ocr(self, document_data: bytes, filename: str) -> str:
        """Send a document to the GPT OCR endpoint and return extracted text.

        Args:
            document_data: Raw bytes of the document file.
            filename:      Original filename (used in multipart metadata).

        Returns:
            The OCR-extracted text content.

        Raises:
            httpx.HTTPStatusError: On a non-retryable server error.
            httpx.TimeoutException: If all retry attempts are exhausted.
        """
        url = f"{self._base_url}/api/v1/ocr"

        logger.info(
            "gpt.ocr.start",
            filename=filename,
            size_bytes=len(document_data),
            model=self._model,
        )

        response_json = await self._make_request_with_retry(
            method="POST",
            url=url,
            files={"file": (filename, document_data)},
            data={"model": self._model},
        )

        # The OCR endpoint may return the text under ``text`` or ``content``.
        ocr_text: str = response_json.get("text") or response_json.get("content", "")

        logger.info(
            "gpt.ocr.complete",
            filename=filename,
            text_length=len(ocr_text),
        )

        return ocr_text

    async def generate_summary(self, ocr_text: str) -> str:
        """Generate a structured summary of OCR text via the GPT chat endpoint.

        Args:
            ocr_text: The raw OCR-extracted text to summarize.

        Returns:
            The generated summary string.

        Raises:
            httpx.HTTPStatusError: On a non-retryable server error.
            httpx.TimeoutException: If all retry attempts are exhausted.
        """
        url = f"{self._base_url}/api/v1/chat/completions"

        logger.info(
            "gpt.summary.start",
            text_length=len(ocr_text),
            model=self._model,
        )

        payload: dict[str, Any] = {
            "model": self._model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a document analyst. Summarize the following "
                        "OCR text into a clear, structured report."
                    ),
                },
                {
                    "role": "user",
                    "content": ocr_text,
                },
            ],
        }

        response_json = await self._make_request_with_retry(
            method="POST",
            url=url,
            json_body=payload,
        )

        # Standard OpenAI-compatible response structure.
        summary = _extract_chat_content(response_json)

        logger.info(
            "gpt.summary.complete",
            summary_length=len(summary),
        )

        return summary

    # ------------------------------------------------------------------
    # Internal retry wrapper
    # ------------------------------------------------------------------

    async def _make_request_with_retry(
        self,
        *,
        method: str,
        url: str,
        files: dict[str, tuple[str, bytes]] | None = None,
        data: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute an HTTP request with tenacity exponential-backoff retries.

        The retry policy covers transient errors:
        * ``httpx.TimeoutException``  — request/read timeouts
        * ``httpx.ConnectError``      — TCP connection failures
        * ``httpx.HTTPStatusError``   — server errors including 429 rate limits

        Args:
            method:    HTTP method (``GET``, ``POST``, …).
            url:       Fully-qualified request URL.
            files:     Optional multipart file mapping.
            data:      Optional form-data mapping.
            json_body: Optional JSON request body.

        Returns:
            Parsed JSON response as a dictionary.

        Raises:
            httpx.HTTPStatusError: After all retries are exhausted.
            httpx.TimeoutException: After all retries are exhausted.
        """

        @retry(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=60),
            retry=retry_if_exception_type(
                (
                    httpx.TimeoutException,
                    httpx.ConnectError,
                    httpx.HTTPStatusError,
                )
            ),
            before_sleep=before_sleep_log(_tenacity_logger, logging.WARNING),
            reraise=True,
        )
        async def _do_request() -> dict[str, Any]:
            token = await self._get_gpt_token()
            headers: dict[str, str] = {
                "Authorization": f"Bearer {token}",
            }

            if json_body is not None:
                headers["Content-Type"] = "application/json"

            logger.debug(
                "gpt.request.attempt",
                method=method,
                url=url,
            )

            async with httpx.AsyncClient(
                timeout=httpx.Timeout(self._timeout),
            ) as client:
                response = await client.request(
                    method,
                    url,
                    headers=headers,
                    files=files,
                    data=data,
                    json=json_body,
                )
                response.raise_for_status()
                result: dict[str, Any] = response.json()

            return result

        return await _do_request()


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _extract_chat_content(response_json: dict[str, Any]) -> str:
    """Extract the assistant message content from an OpenAI-compatible response.

    Supports the standard ``choices[0].message.content`` structure as well as
    a flat ``content`` key for simpler APIs.

    Args:
        response_json: The parsed JSON response from the chat endpoint.

    Returns:
        The extracted summary text.
    """
    choices: list[dict[str, Any]] | None = response_json.get("choices")
    if choices:
        message = choices[0].get("message", {})
        content = message.get("content")
        if content:
            return str(content)

    # Fallback for non-standard response shapes.
    return str(response_json.get("content", ""))
