"""OIDC M2M Token authentication and caching manager.

Uses PostgreSQL-backed caching for dynamic token reuse across multiple pods.
Coordinating token refreshes with row-level locks (SELECT FOR UPDATE) prevents
concurrent duplicate calls to token endpoints.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
import structlog

from app.config import Settings
from app.core.database import DatabaseManager

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class OidcTokenService:
    """Manages dynamically retrieving and caching M2M OAuth2/OIDC tokens."""

    def __init__(self, db: DatabaseManager, settings: Settings) -> None:
        """Initialize the OidcTokenService.

        Args:
            db: DatabaseManager instance for token cache queries and locks.
            settings: Settings object containing OAuth configurations.
        """
        self._db = db
        self._settings = settings

    def _get_config(self, service_name: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Get the OIDC configuration for a specific service, falling back to default OIDC configuration.

        Returns:
            A tuple of (token_url, client_id, client_secret, scope, audience).
        """
        if service_name == "docsys":
            token_url = self._settings.docsys_oidc_token_url or self._settings.oidc_token_url
            client_id = self._settings.docsys_oidc_client_id or self._settings.oidc_client_id
            client_secret = self._settings.docsys_oidc_client_secret or self._settings.oidc_client_secret
            scope = self._settings.docsys_oidc_scope or self._settings.oidc_scope
            audience = self._settings.docsys_oidc_audience or self._settings.oidc_audience
        elif service_name == "docsysarc":
            token_url = self._settings.docsysarc_oidc_token_url or self._settings.oidc_token_url
            client_id = self._settings.docsysarc_oidc_client_id or self._settings.oidc_client_id
            client_secret = self._settings.docsysarc_oidc_client_secret or self._settings.oidc_client_secret
            scope = self._settings.docsysarc_oidc_scope or self._settings.oidc_scope
            audience = self._settings.docsysarc_oidc_audience or self._settings.oidc_audience
        elif service_name == "gpt":
            token_url = self._settings.gpt_oidc_token_url or self._settings.oidc_token_url
            client_id = self._settings.gpt_oidc_client_id or self._settings.oidc_client_id
            client_secret = self._settings.gpt_oidc_client_secret or self._settings.oidc_client_secret
            scope = self._settings.gpt_oidc_scope or self._settings.oidc_scope
            audience = self._settings.gpt_oidc_audience or self._settings.oidc_audience
        else:
            raise ValueError(f"Unknown service name: {service_name}")

        return token_url, client_id, client_secret, scope, audience

    async def get_token(self, service_name: str) -> Optional[str]:
        """Fetch a valid, unexpired OIDC access token for the given service.

        Uses a database-backed cache (table `openid_tokens`) with row-level locks
        (`SELECT FOR UPDATE`) to ensure only one pod requests a new token when
        it expires.

        Args:
            service_name: Name of the service to authenticate ('docsys', 'docsysarc', or 'gpt').

        Returns:
            The raw token string, or None if OIDC is not configured for the service.
        """
        token_url, client_id, client_secret, scope, audience = self._get_config(service_name)

        if not token_url or not client_id or not client_secret:
            logger.debug(
                "oidc.not_configured",
                service_name=service_name,
                has_token_url=bool(token_url),
                has_client_id=bool(client_id),
                has_client_secret=bool(client_secret),
            )
            return None

        # 1. Fast read path: Check DB cache without transactions or locks
        cached = await self._read_valid_token(service_name)
        if cached:
            return cached

        # 2. Lock & Refresh path: Lock the service's token row and refresh if still expired
        if self._db.pool is None:
            raise RuntimeError("Database pool is not initialised.")

        logger.info("oidc.token_refresh_lock_attempt", service_name=service_name)

        async with self._db.pool.acquire() as conn:
            async with conn.transaction():
                # Ensure the row exists
                await conn.execute(
                    """
                    INSERT INTO openid_tokens (service_name, access_token, expires_at)
                    VALUES ($1, '', NOW() - INTERVAL '1 day')
                    ON CONFLICT (service_name) DO NOTHING
                    """,
                    service_name
                )

                # Lock the row for update
                row = await conn.fetchrow(
                    """
                    SELECT access_token, expires_at FROM openid_tokens
                    WHERE service_name = $1
                    FOR UPDATE
                    """,
                    service_name
                )

                if row:
                    expires_at = row["expires_at"]
                    # If another pod refreshed the token while we were waiting for the lock, reuse it.
                    # We use a 30-second buffer to handle clock drift.
                    now = datetime.now(timezone.utc)
                    if expires_at > now + timedelta(seconds=30):
                        logger.info(
                            "oidc.token_reused_after_lock",
                            service_name=service_name,
                            expires_at=expires_at.isoformat(),
                        )
                        return row["access_token"]  # type: ignore[no-any-return]

                # Token is definitely expired or missing. Fetch a new one.
                logger.info(
                    "oidc.token_fetching_from_provider",
                    service_name=service_name,
                    token_url=token_url,
                )
                token, expires_in = await self._fetch_token(
                    token_url, client_id, client_secret, scope, audience
                )

                expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

                await conn.execute(
                    """
                    UPDATE openid_tokens
                    SET access_token = $2, expires_at = $3, updated_at = NOW()
                    WHERE service_name = $1
                    """,
                    service_name,
                    token,
                    expires_at,
                )
                logger.info(
                    "oidc.token_refreshed",
                    service_name=service_name,
                    expires_at=expires_at.isoformat(),
                )
                return token

    async def _read_valid_token(self, service_name: str) -> Optional[str]:
        if self._db.pool is None:
            return None
        query = """
        SELECT access_token, expires_at FROM openid_tokens
        WHERE service_name = $1
        """
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(query, service_name)

        if row:
            expires_at = row["expires_at"]
            now = datetime.now(timezone.utc)
            if expires_at > now + timedelta(seconds=30):
                logger.debug(
                    "oidc.cache_hit",
                    service_name=service_name,
                    expires_at=expires_at.isoformat(),
                )
                return row["access_token"]  # type: ignore[no-any-return]
        return None

    async def _fetch_token(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: Optional[str] = None,
        audience: Optional[str] = None,
    ) -> tuple[str, int]:
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        if scope:
            data["scope"] = scope
        if audience:
            data["audience"] = audience

        async with httpx.AsyncClient() as client:
            response = await client.post(
                token_url,
                data=data,
                timeout=15.0,
            )
            response.raise_for_status()
            res_json = response.json()

        access_token = res_json.get("access_token")
        if not access_token:
            raise ValueError(f"OIDC response missing 'access_token': {res_json}")

        expires_in = res_json.get("expires_in", 3600)
        return access_token, int(expires_in)
