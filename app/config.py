"""Application configuration loaded from environment variables.

Uses pydantic-settings to validate and parse all configuration values
from environment variables with sensible defaults for local development.
"""

from __future__ import annotations

import functools
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central configuration for the OCR Summary Worker.

    All values are read from environment variables (case-insensitive).
    Field names are lowercase; the corresponding env vars are matched
    case-insensitively (e.g. ``kafka_bootstrap_servers`` reads from
    ``KAFKA_BOOTSTRAP_SERVERS``).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Kafka ───────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_input_topic: str = "doc-processing-requests"
    kafka_output_topic: str = "doc-processing-results"
    kafka_group_id: str = "ocr-summary-worker"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: Optional[str] = None
    kafka_sasl_username: Optional[str] = None
    kafka_sasl_password: Optional[str] = None

    # ── Database ────────────────────────────────────────────────────
    database_url: str = "postgresql://user:pass@localhost:5432/ocrdb"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10

    # ── Worker Pool Concurrency ──────────────────────────────────────
    worker_concurrency: int = 5

    # ── S3 / Object Storage ─────────────────────────────────────────
    s3_bucket_name: str = ""
    s3_endpoint_url: Optional[str] = None
    s3_region_name: str = "ap-southeast-1"
    s3_access_key_id: Optional[str] = None
    s3_secret_access_key: Optional[str] = None

    # ── DocSys API ──────────────────────────────────────────────────
    docsys_base_url: str = ""
    docsys_api_token: str = ""

    # ── DocSys Archive API ──────────────────────────────────────────
    docsysarc_base_url: str = ""
    docsysarc_api_token: str = ""

    # ── GPT / LLM ──────────────────────────────────────────────────
    gpt_base_url: str = ""
    gpt_api_key: str = ""
    gpt_model: str = "gpt-4o"
    gpt_timeout_seconds: int = 300
    gpt_max_retries: int = 3

    # ── Logging ─────────────────────────────────────────────────────
    log_level: str = "INFO"


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton of the application settings.

    The first call parses environment variables; subsequent calls
    return the same instance without re-parsing.
    """
    return Settings()
