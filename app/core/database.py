"""Async PostgreSQL database manager using asyncpg.

Provides connection-pool lifecycle management, schema initialisation,
and CRUD helpers for the ``job_states`` table.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import asyncpg
import structlog

from app.config import Settings

logger = structlog.get_logger(__name__)


class DatabaseManager:
    """Manages an asyncpg connection pool and job-state persistence."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.pool: Optional[asyncpg.Pool] = None

    # ── Lifecycle ───────────────────────────────────────────────────

    async def connect(self) -> None:
        """Create the asyncpg connection pool from ``DATABASE_URL``."""
        log = logger.bind(dsn=self._settings.database_url)
        log.info("database.connecting")

        try:
            self.pool = await asyncpg.create_pool(
                dsn=self._settings.database_url,
                min_size=self._settings.db_pool_min_size,
                max_size=self._settings.db_pool_max_size,
            )
            log.info("database.connected")
        except Exception:
            log.exception("database.connect_failed")
            raise

    async def disconnect(self) -> None:
        """Gracefully close the connection pool."""
        if self.pool is not None:
            await self.pool.close()
            logger.info("database.disconnected")
            self.pool = None

    # ── Schema ──────────────────────────────────────────────────────

    async def init_db(self) -> None:
        """Create the ``job_states`` table and indexes if they do not exist."""
        if self.pool is None:
            raise RuntimeError("Database pool is not initialised. Call connect() first.")

        # Updated schema: removed s3_raw_key, s3_ocr_key, s3_report_key columns.
        # Added filename column to track the raw document filename for S3 extension parsing.
        # Added openid_tokens table to store cached M2M tokens securely for all pods.
        ddl = """
        CREATE TABLE IF NOT EXISTS job_states (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            docid VARCHAR(255) NOT NULL,
            fileid VARCHAR(255) NOT NULL,
            state VARCHAR(50) NOT NULL DEFAULT 'RECEIVED',
            filename VARCHAR(255),
            error_message TEXT,
            worker_id VARCHAR(255),
            locked_at TIMESTAMPTZ,
            leased_until TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_job_states_docid ON job_states(docid);
        CREATE INDEX IF NOT EXISTS idx_job_states_state ON job_states(state);

        CREATE TABLE IF NOT EXISTS openid_tokens (
            service_name VARCHAR(50) PRIMARY KEY,
            access_token TEXT NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """

        async with self.pool.acquire() as conn:
            await conn.execute(ddl)

        logger.info("database.schema_initialised")

    # ── CRUD ────────────────────────────────────────────────────────

    async def create_job(self, docid: str, fileid: str) -> str:
        """Insert a new job row and return its UUID as a string.

        Args:
            docid: The document identifier.
            fileid: The file identifier within the document.

        Returns:
            The generated UUID of the new job row (as a string).
        """
        if self.pool is None:
            raise RuntimeError("Database pool is not initialised. Call connect() first.")

        query = """
        INSERT INTO job_states (docid, fileid)
        VALUES ($1, $2)
        RETURNING id;
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, docid, fileid)

        job_id = str(row["id"])  # type: ignore[index]
        logger.info("database.job_created", job_id=job_id, docid=docid, fileid=fileid)
        return job_id

    async def claim_job(self, worker_id: str, lease_seconds: int = 900) -> Optional[Dict[str, Any]]:
        """Atomic select-and-claim operation utilizing SKIP LOCKED.

        Claims a job that is not complete (state != SUCCESS) and is currently
        unlocked (worker_id IS NULL) or has its lease expired (leased_until < NOW()).

        Args:
            worker_id: Hostname/ID of the claiming container pod.
            lease_seconds: Number of seconds before the lease expires.

        Returns:
            A dict representing the claimed job row, or None if no jobs are available.
        """
        if self.pool is None:
            raise RuntimeError("Database pool is not initialised. Call connect() first.")

        query = """
        UPDATE job_states
        SET 
            worker_id = $1,
            locked_at = NOW(),
            leased_until = NOW() + $2 * INTERVAL '1 second',
            updated_at = NOW()
        WHERE id = (
            SELECT id 
            FROM job_states
            WHERE state != 'SUCCESS'
              AND (worker_id IS NULL OR leased_until < NOW())
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, docid, fileid, state;
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, worker_id, lease_seconds)

        if row is None:
            return None

        logger.info(
            "database.job_claimed",
            job_id=str(row["id"]),
            docid=row["docid"],
            fileid=row["fileid"],
            state=row["state"],
            worker_id=worker_id,
        )
        return dict(row)

    async def update_job_state(self, job_id: str, state: str, **kwargs: Any) -> None:
        """Update a job's state and any extra columns.

        ``kwargs`` may contain optional column updates such as
        ``filename``, ``error_message``, ``worker_id``, ``locked_at``, or
        ``leased_until``. The ``updated_at`` column is always set to ``NOW()``.

        Args:
            job_id: UUID string of the job to update.
            state: New state value (e.g. ``'DOWNLOADED'``, ``'SUCCESS'``).
            **kwargs: Additional column=value pairs to set.
        """
        if self.pool is None:
            raise RuntimeError("Database pool is not initialised. Call connect() first.")

        allowed_columns = {
            "filename", "error_message", "worker_id", "locked_at", "leased_until"
        }
        extra_cols = {k: v for k, v in kwargs.items() if k in allowed_columns}

        # Build SET clause dynamically -----------------------------------------
        set_parts: list[str] = ["state = $2", "updated_at = NOW()"]
        params: list[Any] = [job_id, state]

        idx = 3
        for col, val in extra_cols.items():
            set_parts.append(f"{col} = ${idx}")
            params.append(val)
            idx += 1

        set_clause = ", ".join(set_parts)
        query = f"UPDATE job_states SET {set_clause} WHERE id = $1::uuid;"

        async with self.pool.acquire() as conn:
            await conn.execute(query, *params)

        logger.info(
            "database.job_state_updated",
            job_id=job_id,
            state=state,
            extra=extra_cols,
        )

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single job row by its UUID.

        Args:
            job_id: UUID string of the job to retrieve.

        Returns:
            A dict representation of the row, or ``None`` if not found.
        """
        if self.pool is None:
            raise RuntimeError("Database pool is not initialised. Call connect() first.")

        query = "SELECT * FROM job_states WHERE id = $1::uuid;"

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, job_id)

        if row is None:
            logger.warning("database.job_not_found", job_id=job_id)
            return None

        return dict(row)

    async def get_job_by_document(
        self, docid: str, fileid: str,
    ) -> Optional[Dict[str, Any]]:
        """Find the most recent job for a given document/file pair.

        Used by the processor for recovery — if a job already exists for
        the same ``(docid, fileid)`` pair it can be resumed rather than
        creating a duplicate.

        Args:
            docid: The document identifier.
            fileid: The file identifier within the document.

        Returns:
            A dict of the most recent matching row, or ``None``.
        """
        if self.pool is None:
            raise RuntimeError("Database pool is not initialised. Call connect() first.")

        query = """
        SELECT * FROM job_states
        WHERE docid = $1 AND fileid = $2
        ORDER BY created_at DESC
        LIMIT 1;
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, docid, fileid)

        if row is None:
            return None

        return dict(row)
