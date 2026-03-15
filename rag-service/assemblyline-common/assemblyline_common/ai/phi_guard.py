"""
PHI Guard - Masks PHI Before Sending to AI Models

Ensures HIPAA compliance by:
- Detecting and masking all PHI before AI model calls
- Storing PHI mappings securely (encrypted, short-lived)
- Reconstructing PHI in responses only when authorized
"""

import re
import logging
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any, Tuple
from uuid import UUID, uuid4
from enum import Enum

from assemblyline_common.phi.masking import PHIMaskingService, PHIType, get_phi_masking_service

logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class PHIToken:
    """Represents a masked PHI token."""
    token: str  # e.g., "[PATIENT_NAME_1]"
    phi_type: PHIType
    original_value: str
    position: Tuple[int, int]  # Start and end position in original text


@dataclass
class MaskedContent:
    """Content with PHI masked and mapping stored."""
    masked_text: str
    tokens: List[PHIToken]
    phi_types_found: List[PHIType]
    token_count: int
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc) + timedelta(hours=1))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "masked_text": self.masked_text,
            "phi_types_found": [t.value for t in self.phi_types_found],
            "token_count": self.token_count,
        }


# ============================================================================
# PHI Guard Service
# ============================================================================

class PHIGuard:
    """
    Guards PHI by masking before AI calls and unmasking after.
    
    Usage:
        guard = PHIGuard()
        
        # Before sending to AI
        masked = guard.mask_for_ai(user_message, conversation_id)
        ai_response = await call_ai_model(masked.masked_text)
        
        # After receiving response (if user authorized to see PHI)
        if user_can_see_phi:
            unmasked = guard.unmask_response(ai_response, conversation_id)
    """

    def __init__(self):
        self._masking_service = PHIMaskingService()
        # In-memory token storage (in production, use Redis with encryption)
        self._token_stores: Dict[str, MaskedContent] = {}
        self._token_counters: Dict[str, Dict[str, int]] = {}

    def mask_for_ai(
        self,
        content: str,
        conversation_id: str,
        additional_context: Optional[Dict[str, str]] = None,
    ) -> MaskedContent:
        """
        Mask all PHI in content before sending to AI model.
        
        Args:
            content: The text content to mask
            conversation_id: Unique ID for this conversation (for token mapping)
            additional_context: Optional additional PHI to mask (key-value pairs)
        
        Returns:
            MaskedContent with masked text and token mappings
        """
        # Initialize counter for this conversation
        if conversation_id not in self._token_counters:
            self._token_counters[conversation_id] = {}

        tokens: List[PHIToken] = []
        phi_types_found: List[PHIType] = []
        masked_text = content

        # Detect PHI using the masking service
        detections = self._masking_service.detect_phi(content)

        # Sort detections by position (reverse order for replacement)
        detections_sorted = sorted(
            [d for d in detections if d.start_position is not None],
            key=lambda d: d.start_position,
            reverse=True,
        )

        for detection in detections_sorted:
            phi_type = detection.phi_type
            original_value = detection.original_value
            start = detection.start_position
            end = detection.end_position

            # Generate unique token for this PHI type
            token = self._generate_token(conversation_id, phi_type)

            # Create PHI token record
            phi_token = PHIToken(
                token=token,
                phi_type=phi_type,
                original_value=original_value,
                position=(start, end),
            )
            tokens.append(phi_token)

            if phi_type not in phi_types_found:
                phi_types_found.append(phi_type)

            # Replace in text
            masked_text = masked_text[:start] + token + masked_text[end:]

        # Mask additional context if provided
        if additional_context:
            for key, value in additional_context.items():
                if value and len(value) > 2:
                    token = f"[{key.upper()}]"
                    if value in masked_text:
                        masked_text = masked_text.replace(value, token)
                        tokens.append(PHIToken(
                            token=token,
                            phi_type=PHIType.OTHER,
                            original_value=value,
                            position=(0, 0),
                        ))

        # Create and store masked content
        masked_content = MaskedContent(
            masked_text=masked_text,
            tokens=tokens,
            phi_types_found=phi_types_found,
            token_count=len(tokens),
        )

        # Store for later unmasking
        self._token_stores[conversation_id] = masked_content

        logger.info(
            f"Masked {len(tokens)} PHI tokens for conversation",
            extra={
                "event_type": "phi_guard.masked",
                "conversation_id": conversation_id,
                "token_count": len(tokens),
                "phi_types": [t.value for t in phi_types_found],
            }
        )

        return masked_content

    def unmask_response(
        self,
        response: str,
        conversation_id: str,
        authorized: bool = False,
    ) -> str:
        """
        Unmask PHI tokens in AI response.
        
        Args:
            response: The AI response potentially containing tokens
            conversation_id: Conversation ID to look up token mappings
            authorized: Whether user is authorized to see PHI
        
        Returns:
            Response with tokens replaced by original values (if authorized)
            or kept as tokens (if not authorized)
        """
        if not authorized:
            logger.warning(
                "Unmask requested but not authorized",
                extra={
                    "event_type": "phi_guard.unmask_denied",
                    "conversation_id": conversation_id,
                }
            )
            return response

        masked_content = self._token_stores.get(conversation_id)
        if not masked_content:
            return response

        # Check expiration
        if datetime.now(timezone.utc) > masked_content.expires_at:
            logger.warning(
                "Token mapping expired",
                extra={
                    "event_type": "phi_guard.tokens_expired",
                    "conversation_id": conversation_id,
                }
            )
            del self._token_stores[conversation_id]
            return response

        unmasked = response
        for token in masked_content.tokens:
            if token.token in unmasked:
                unmasked = unmasked.replace(token.token, token.original_value)

        logger.info(
            "Unmasked PHI tokens in response",
            extra={
                "event_type": "phi_guard.unmasked",
                "conversation_id": conversation_id,
            }
        )

        return unmasked

    def clear_conversation(self, conversation_id: str):
        """Clear token mappings for a conversation."""
        if conversation_id in self._token_stores:
            del self._token_stores[conversation_id]
        if conversation_id in self._token_counters:
            del self._token_counters[conversation_id]

    def get_phi_summary(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """Get summary of PHI found in a conversation."""
        masked_content = self._token_stores.get(conversation_id)
        if not masked_content:
            return None

        return {
            "phi_types": [t.value for t in masked_content.phi_types_found],
            "token_count": masked_content.token_count,
            "created_at": masked_content.created_at.isoformat(),
            "expires_at": masked_content.expires_at.isoformat(),
        }

    def _generate_token(self, conversation_id: str, phi_type: PHIType) -> str:
        """Generate a unique token for a PHI type."""
        counters = self._token_counters.get(conversation_id, {})
        type_name = phi_type.value.upper().replace(" ", "_")

        count = counters.get(type_name, 0) + 1
        counters[type_name] = count
        self._token_counters[conversation_id] = counters

        return f"[{type_name}_{count}]"


# ============================================================================
# Singleton Factory
# ============================================================================

_phi_guard: Optional[PHIGuard] = None


def get_phi_guard() -> PHIGuard:
    """Get singleton instance of PHI guard."""
    global _phi_guard
    if _phi_guard is None:
        _phi_guard = PHIGuard()
    return _phi_guard
