"""Async Kafka producer for publishing job-status messages.

Uses ``aiokafka`` and serialises payloads as UTF-8 JSON. Supports
optional SASL authentication when the security protocol requires it.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

import structlog
from aiokafka import AIOKafkaProducer

from app.config import Settings

logger = structlog.get_logger(__name__)


class KafkaStatusProducer:
    """Publishes status updates to the Kafka output topic.

    The producer is lazily initialised via :meth:`start` and must be
    cleanly shut down via :meth:`stop` on application teardown.
    """

    def __init__(self, settings: Settings) -> None:
        """Store settings for deferred producer creation.

        Args:
            settings: Application :class:`Settings` instance.
        """
        self._settings = settings
        self._producer: Optional[AIOKafkaProducer] = None

    # ── Lifecycle ───────────────────────────────────────────────────

    async def start(self) -> None:
        """Create and start the underlying ``AIOKafkaProducer``."""
        kwargs: Dict[str, Any] = {
            "bootstrap_servers": self._settings.kafka_bootstrap_servers,
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
        }

        # Attach SASL configuration when protocol is not PLAINTEXT ----------
        if self._settings.kafka_security_protocol.upper() != "PLAINTEXT":
            kwargs["security_protocol"] = self._settings.kafka_security_protocol
            if self._settings.kafka_sasl_mechanism:
                kwargs["sasl_mechanism"] = self._settings.kafka_sasl_mechanism
            if self._settings.kafka_sasl_username:
                kwargs["sasl_plain_username"] = self._settings.kafka_sasl_username
            if self._settings.kafka_sasl_password:
                kwargs["sasl_plain_password"] = self._settings.kafka_sasl_password

        try:
            self._producer = AIOKafkaProducer(**kwargs)
            await self._producer.start()
            logger.info(
                "kafka.producer_started",
                topic=self._settings.kafka_output_topic,
                bootstrap=self._settings.kafka_bootstrap_servers,
            )
        except Exception:
            logger.exception("kafka.producer_start_failed")
            raise

    async def stop(self) -> None:
        """Stop the producer and release resources."""
        if self._producer is not None:
            await self._producer.stop()
            logger.info("kafka.producer_stopped")
            self._producer = None

    # ── Publishing ──────────────────────────────────────────────────

    async def publish_status(self, payload: Dict[str, Any]) -> None:
        """Send a status message to the configured output topic.

        The message key is set to the ``docid`` value inside *payload*
        so that all messages for the same document land on the same
        partition, preserving ordering.

        Args:
            payload: JSON-serialisable dictionary to publish.

        Raises:
            RuntimeError: If the producer has not been started.
        """
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started. Call start() first.")

        docid: str = payload.get("docid", "")
        topic = self._settings.kafka_output_topic

        try:
            await self._producer.send_and_wait(
                topic=topic,
                value=payload,
                key=docid,
            )
            logger.info(
                "kafka.status_published",
                topic=topic,
                docid=docid,
            )
        except Exception:
            logger.exception(
                "kafka.publish_failed",
                topic=topic,
                docid=docid,
            )
            raise
