"""
AI Audit Client - Fire-and-forget audit trail integration.

Posts audit events to the Audit Service (port 8007) for HIPAA compliance.
All methods are non-blocking: 5s timeout, errors logged not raised.
Uses AUDIT_SERVICE_URL env var for ECS/container service discovery.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4

import httpx

logger = logging.getLogger(__name__)

# Default audit service URL (ECS service discovery or localhost for dev)
DEFAULT_AUDIT_URL = "http://localhost:8007"
AUDIT_EVENTS_PATH = "/api/v1/audit/events"
TIMEOUT_SECONDS = 5.0


class AIAuditClient:
    """
    Async HTTP client for posting audit events to the Audit Service.

    Fire-and-forget pattern: all methods catch exceptions and log errors.
    AI responses are never blocked by audit failures.
    """

    def __init__(self):
        self._base_url = os.getenv("AUDIT_SERVICE_URL", DEFAULT_AUDIT_URL)
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        """Lazy-init the httpx async client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=httpx.Timeout(TIMEOUT_SECONDS),
                headers={"Content-Type": "application/json"},
            )
        return self._client

    async def log_ai_interaction(
        self,
        tenant_id: UUID,
        user_id: UUID,
        conversation_id: UUID,
        agent_type: str,
        action: str = "ai_chat",
        message_preview: Optional[str] = None,
        contained_phi: bool = False,
        phi_was_masked: bool = False,
        phi_types: Optional[List[str]] = None,
        actions_taken: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log an AI interaction audit event.

        Args:
            tenant_id: Tenant ID
            user_id: User who initiated the interaction
            conversation_id: Conversation ID
            agent_type: Which AI agent handled the request
            action: Action type (default: "ai_chat")
            message_preview: First 50 chars of user message (no PHI)
            contained_phi: Whether the message contained PHI
            phi_was_masked: Whether PHI was masked before AI processing
            phi_types: Types of PHI detected (e.g., ["SSN", "PATIENT_NAME"])
            actions_taken: Actions the AI executed (e.g., ["create_flow"])
            metadata: Additional metadata
        """
        try:
            event = {
                "id": str(uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "tenant_id": str(tenant_id),
                "user_id": str(user_id),
                "action": action,
                "resource_type": "ai_conversation",
                "resource_id": str(conversation_id),
                "details": {
                    "agent_type": agent_type,
                    "contained_phi": contained_phi,
                    "phi_was_masked": phi_was_masked,
                    "phi_types": phi_types or [],
                    "actions_taken": actions_taken or [],
                    "message_preview": message_preview,
                    **(metadata or {}),
                },
            }

            client = self._get_client()
            response = await client.post(AUDIT_EVENTS_PATH, json=event)

            if response.status_code >= 400:
                logger.warning(
                    f"Audit service returned {response.status_code}: {response.text[:200]}"
                )
        except httpx.TimeoutException:
            logger.warning("Audit service timeout - event not recorded")
        except httpx.ConnectError:
            logger.debug("Audit service unavailable - running in degraded mode")
        except Exception as e:
            logger.warning(f"Failed to log AI interaction audit: {e}")

    async def log_phi_access(
        self,
        tenant_id: UUID,
        user_id: UUID,
        conversation_id: UUID,
        phi_types: List[str],
        was_masked: bool = True,
        access_purpose: str = "ai_chat_processing",
    ) -> None:
        """
        Log PHI access for HIPAA compliance audit trail.

        This is a separate event from the general AI interaction log,
        specifically for PHI access tracking per HIPAA requirements.

        Args:
            tenant_id: Tenant ID
            user_id: User whose message contained PHI
            conversation_id: Conversation where PHI was detected
            phi_types: Types of PHI detected
            was_masked: Whether PHI was masked before AI processing
            access_purpose: Purpose of PHI access
        """
        try:
            event = {
                "id": str(uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "tenant_id": str(tenant_id),
                "user_id": str(user_id),
                "action": "phi_access",
                "resource_type": "ai_conversation",
                "resource_id": str(conversation_id),
                "severity": "high",
                "details": {
                    "phi_types": phi_types,
                    "was_masked": was_masked,
                    "access_purpose": access_purpose,
                    "hipaa_relevant": True,
                },
            }

            client = self._get_client()
            response = await client.post(AUDIT_EVENTS_PATH, json=event)

            if response.status_code >= 400:
                logger.warning(
                    f"Audit service PHI event returned {response.status_code}: {response.text[:200]}"
                )
        except httpx.TimeoutException:
            logger.warning("Audit service timeout for PHI access event")
        except httpx.ConnectError:
            logger.debug("Audit service unavailable for PHI access event - degraded mode")
        except Exception as e:
            logger.warning(f"Failed to log PHI access audit: {e}")

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None


# Singleton
_audit_client: Optional[AIAuditClient] = None


def get_ai_audit_client() -> AIAuditClient:
    """Get singleton AIAuditClient instance."""
    global _audit_client
    if _audit_client is None:
        _audit_client = AIAuditClient()
    return _audit_client
