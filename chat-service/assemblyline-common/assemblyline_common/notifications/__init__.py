"""
Notifications module.

Provides email sending via SMTP and Microsoft Graph API.
"""

from assemblyline_common.notifications.email_service import (
    EmailConfig,
    EmailProvider,
    SMTPProvider,
    MicrosoftGraphProvider,
    EmailService,
    get_email_service,
    send_invite_email,
    is_email_configured,
)

__all__ = [
    "EmailConfig",
    "EmailProvider",
    "SMTPProvider",
    "MicrosoftGraphProvider",
    "EmailService",
    "get_email_service",
    "send_invite_email",
    "is_email_configured",
]
