"""
DriveSentinel Email Service
Supports SMTP and Microsoft Graph API for sending emails
Enterprise tier only
"""

import os
import ssl
import logging
import smtplib
import asyncio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# Check if httpx is available for Microsoft Graph
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    logger.warning("httpx package not available, Microsoft Graph email will be disabled")


# =============================================================================
# EMAIL TEMPLATES
# =============================================================================

INVITE_EMAIL_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background: #2563eb; color: white; padding: 20px; text-align: center; }}
        .content {{ padding: 20px; background: #f9fafb; }}
        .credentials {{ background: #fff; border: 1px solid #e5e7eb; padding: 15px; margin: 15px 0; border-radius: 4px; }}
        .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
        .button {{ display: inline-block; background: #2563eb; color: white; padding: 12px 24px; text-decoration: none; border-radius: 4px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Welcome to DriveSentinel</h1>
        </div>
        <div class="content">
            <p>Hello,</p>
            <p>You have been invited to join <strong>{organization_name}</strong> on DriveSentinel.</p>

            <div class="credentials">
                <p><strong>Your login credentials:</strong></p>
                <p>Email: <code>{email}</code></p>
                <p>Temporary Password: <code>{temp_password}</code></p>
            </div>

            <p>Please log in and change your password immediately.</p>

            <p style="text-align: center; margin: 30px 0;">
                <a href="{login_url}" class="button">Log In Now</a>
            </p>

            <p><small>If the button doesn't work, copy this URL: {login_url}</small></p>
        </div>
        <div class="footer">
            <p>This email was sent by DriveSentinel on behalf of {organization_name}.</p>
            <p>If you did not expect this invitation, please ignore this email.</p>
        </div>
    </div>
</body>
</html>
"""

INVITE_EMAIL_TEXT = """
Welcome to DriveSentinel

You have been invited to join {organization_name} on DriveSentinel.

Your login credentials:
Email: {email}
Temporary Password: {temp_password}

Please log in at {login_url} and change your password immediately.

This email was sent by DriveSentinel on behalf of {organization_name}.
If you did not expect this invitation, please ignore this email.
"""


# =============================================================================
# EMAIL PROVIDER BASE CLASS
# =============================================================================

@dataclass
class EmailConfig:
    """Email configuration from database"""
    provider: str
    from_address: str
    from_name: str = "DriveSentinel"

    # SMTP settings
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    smtp_use_tls: bool = True

    # Microsoft Graph settings
    ms_tenant_id: Optional[str] = None
    ms_client_id: Optional[str] = None
    ms_client_secret: Optional[str] = None


class EmailProvider(ABC):
    """Base class for email providers"""

    @abstractmethod
    async def send(
        self,
        config: EmailConfig,
        to: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Send an email.

        Returns:
            Tuple of (success: bool, message: str)
        """
        pass

    @abstractmethod
    async def test_connection(self, config: EmailConfig) -> Tuple[bool, str]:
        """
        Test the connection to the email provider.

        Returns:
            Tuple of (success: bool, message: str)
        """
        pass


# =============================================================================
# SMTP PROVIDER
# =============================================================================

class SMTPProvider(EmailProvider):
    """SMTP email provider"""

    async def send(
        self,
        config: EmailConfig,
        to: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None
    ) -> Tuple[bool, str]:
        """Send email via SMTP"""
        try:
            # Create message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{config.from_name} <{config.from_address}>"
            msg["To"] = to

            # Add text and HTML parts
            if text_body:
                msg.attach(MIMEText(text_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            # Send in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._send_sync,
                config,
                to,
                msg
            )

            logger.info(f"Email sent via SMTP to {to}")
            return True, "Email sent successfully"

        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP authentication failed: {e}")
            return False, "Authentication failed. Check username and password."
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error: {e}")
            return False, f"SMTP error: {str(e)}"
        except Exception as e:
            logger.error(f"Failed to send email via SMTP: {e}")
            return False, f"Failed to send email: {str(e)}"

    def _send_sync(self, config: EmailConfig, to: str, msg: MIMEMultipart):
        """Synchronous SMTP send (runs in thread pool)"""
        if config.smtp_use_tls:
            # Use STARTTLS
            with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=30) as server:
                server.starttls(context=ssl.create_default_context())
                if config.smtp_username and config.smtp_password:
                    server.login(config.smtp_username, config.smtp_password)
                server.sendmail(config.from_address, [to], msg.as_string())
        else:
            # No TLS (not recommended)
            with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=30) as server:
                if config.smtp_username and config.smtp_password:
                    server.login(config.smtp_username, config.smtp_password)
                server.sendmail(config.from_address, [to], msg.as_string())

    async def test_connection(self, config: EmailConfig) -> Tuple[bool, str]:
        """Test SMTP connection"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._test_sync,
                config
            )
            return True, "SMTP connection successful"
        except smtplib.SMTPAuthenticationError:
            return False, "Authentication failed. Check username and password."
        except smtplib.SMTPConnectError:
            return False, f"Could not connect to {config.smtp_host}:{config.smtp_port}"
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"

    def _test_sync(self, config: EmailConfig):
        """Synchronous SMTP connection test"""
        with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=10) as server:
            if config.smtp_use_tls:
                server.starttls(context=ssl.create_default_context())
            if config.smtp_username and config.smtp_password:
                server.login(config.smtp_username, config.smtp_password)
            # NOOP to verify connection is valid
            server.noop()


# =============================================================================
# MICROSOFT GRAPH PROVIDER
# =============================================================================

class MicrosoftGraphProvider(EmailProvider):
    """Microsoft Graph API email provider"""

    TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    SEND_MAIL_URL = "https://graph.microsoft.com/v1.0/users/{user_id}/sendMail"

    async def _get_access_token(self, config: EmailConfig) -> Tuple[Optional[str], Optional[str]]:
        """Get OAuth2 access token from Azure AD"""
        if not HTTPX_AVAILABLE:
            return None, "httpx package not installed"

        token_url = self.TOKEN_URL.format(tenant_id=config.ms_tenant_id)

        data = {
            "client_id": config.ms_client_id,
            "client_secret": config.ms_client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials"
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(token_url, data=data, timeout=30)

                if response.status_code == 200:
                    token_data = response.json()
                    return token_data.get("access_token"), None
                else:
                    error_data = response.json()
                    error_msg = error_data.get("error_description", "Unknown error")
                    return None, f"Token error: {error_msg}"

        except Exception as e:
            return None, f"Failed to get access token: {str(e)}"

    async def send(
        self,
        config: EmailConfig,
        to: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None
    ) -> Tuple[bool, str]:
        """Send email via Microsoft Graph API"""
        if not HTTPX_AVAILABLE:
            return False, "httpx package not installed. Install with: pip install httpx"

        # Get access token
        access_token, error = await self._get_access_token(config)
        if not access_token:
            return False, error or "Failed to get access token"

        # Build email message
        email_message = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": html_body
                },
                "toRecipients": [
                    {
                        "emailAddress": {
                            "address": to
                        }
                    }
                ],
                "from": {
                    "emailAddress": {
                        "address": config.from_address,
                        "name": config.from_name
                    }
                }
            },
            "saveToSentItems": "true"
        }

        # Send email
        send_url = self.SEND_MAIL_URL.format(user_id=config.from_address)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    send_url,
                    json=email_message,
                    headers=headers,
                    timeout=30
                )

                if response.status_code == 202:
                    logger.info(f"Email sent via Microsoft Graph to {to}")
                    return True, "Email sent successfully"
                else:
                    error_data = response.json()
                    error_msg = error_data.get("error", {}).get("message", "Unknown error")
                    return False, f"Graph API error: {error_msg}"

        except Exception as e:
            logger.error(f"Failed to send email via Microsoft Graph: {e}")
            return False, f"Failed to send email: {str(e)}"

    async def test_connection(self, config: EmailConfig) -> Tuple[bool, str]:
        """Test Microsoft Graph connection by getting an access token"""
        if not HTTPX_AVAILABLE:
            return False, "httpx package not installed. Install with: pip install httpx"

        access_token, error = await self._get_access_token(config)
        if access_token:
            return True, "Microsoft Graph authentication successful"
        else:
            return False, error or "Failed to authenticate with Microsoft Graph"


# =============================================================================
# EMAIL SERVICE
# =============================================================================

class EmailService:
    """
    Email service for sending emails using configured providers.

    Usage:
        service = EmailService()

        # Send a generic email
        success, msg = await service.send_email(org_id, to, subject, body)

        # Send an invite email
        success, msg = await service.send_invite_email(org_id, email, temp_password, org_name)
    """

    def __init__(self):
        self._providers = {
            "smtp": SMTPProvider(),
            "microsoft_graph": MicrosoftGraphProvider()
        }
        self._db_available = False
        self._initialize_db()

    def _initialize_db(self):
        """Check if database is available"""
        try:
            from assemblyline_common.database import get_connection
            self._db_available = True
        except ImportError:
            logger.warning("Database not available for email service")
            self._db_available = False

    async def get_email_config(self, org_id: int) -> Optional[EmailConfig]:
        """Get email configuration for an organization"""
        if not self._db_available:
            return None

        try:
            # Import encryption utilities (AES-256-GCM with legacy base64 fallback)
            from assemblyline_common.encryption import decrypt_value

            # Import database connection
            from assemblyline_common.database import get_connection

            async with get_connection() as conn:
                row = await conn.fetchrow("""
                    SELECT
                        provider,
                        smtp_host,
                        smtp_port,
                        smtp_username,
                        smtp_password_encrypted,
                        smtp_use_tls,
                        ms_tenant_id,
                        ms_client_id,
                        ms_client_secret_encrypted,
                        from_address,
                        from_name,
                        is_verified
                    FROM org_email_config
                    WHERE organization_id = $1
                """, org_id)

                if not row or row["provider"] == "disabled":
                    return None

                # Decrypt sensitive fields
                smtp_password = None
                ms_client_secret = None

                if row["smtp_password_encrypted"]:
                    smtp_password = decrypt_value(row["smtp_password_encrypted"])

                if row["ms_client_secret_encrypted"]:
                    ms_client_secret = decrypt_value(row["ms_client_secret_encrypted"])

                return EmailConfig(
                    provider=row["provider"],
                    from_address=row["from_address"] or "",
                    from_name=row["from_name"] or "DriveSentinel",
                    smtp_host=row["smtp_host"],
                    smtp_port=row["smtp_port"] or 587,
                    smtp_username=row["smtp_username"],
                    smtp_password=smtp_password,
                    smtp_use_tls=row["smtp_use_tls"] if row["smtp_use_tls"] is not None else True,
                    ms_tenant_id=row["ms_tenant_id"],
                    ms_client_id=row["ms_client_id"],
                    ms_client_secret=ms_client_secret
                )

        except Exception as e:
            logger.error(f"Failed to get email config for org {org_id}: {e}")
            return None

    async def send_email(
        self,
        org_id: int,
        to: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Send an email using the organization's configured email provider.

        Args:
            org_id: Organization ID
            to: Recipient email address
            subject: Email subject
            html_body: HTML email body
            text_body: Optional plain text body

        Returns:
            Tuple of (success: bool, message: str)
        """
        config = await self.get_email_config(org_id)

        if not config:
            return False, "Email not configured for this organization"

        provider = self._providers.get(config.provider)
        if not provider:
            return False, f"Unknown email provider: {config.provider}"

        return await provider.send(config, to, subject, html_body, text_body)

    async def send_invite_email(
        self,
        org_id: int,
        email: str,
        temp_password: str,
        organization_name: str,
        login_url: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Send a user invite email.

        Args:
            org_id: Organization ID
            email: Invited user's email address
            temp_password: Temporary password for the user
            organization_name: Name of the organization
            login_url: Optional login URL (defaults to environment variable or localhost)

        Returns:
            Tuple of (success: bool, message: str)
        """
        if not login_url:
            login_url = os.getenv("APP_URL", "http://localhost:5173")

        # Format email templates
        html_body = INVITE_EMAIL_TEMPLATE.format(
            organization_name=organization_name,
            email=email,
            temp_password=temp_password,
            login_url=login_url
        )

        text_body = INVITE_EMAIL_TEXT.format(
            organization_name=organization_name,
            email=email,
            temp_password=temp_password,
            login_url=login_url
        )

        subject = f"You've been invited to join {organization_name} on DriveSentinel"

        return await self.send_email(org_id, email, subject, html_body, text_body)

    async def test_email_config(
        self,
        org_id: int,
        test_email: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Test the email configuration for an organization.

        Args:
            org_id: Organization ID
            test_email: Optional email address to send test email to

        Returns:
            Tuple of (success: bool, message: str)
        """
        config = await self.get_email_config(org_id)

        if not config:
            return False, "Email not configured for this organization"

        provider = self._providers.get(config.provider)
        if not provider:
            return False, f"Unknown email provider: {config.provider}"

        # First test the connection
        success, message = await provider.test_connection(config)
        if not success:
            return False, message

        # If test email provided, send a test message
        if test_email:
            html_body = """
            <html>
            <body>
                <h1>DriveSentinel Email Test</h1>
                <p>This is a test email from DriveSentinel.</p>
                <p>If you received this email, your email configuration is working correctly.</p>
            </body>
            </html>
            """
            success, message = await provider.send(
                config,
                test_email,
                "DriveSentinel Email Configuration Test",
                html_body,
                "This is a test email from DriveSentinel. Your email configuration is working correctly."
            )
            if success:
                return True, f"Test email sent successfully to {test_email}"
            else:
                return False, f"Connection OK but failed to send test email: {message}"

        return True, message


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_email_service_instance: Optional[EmailService] = None


def get_email_service() -> EmailService:
    """Get singleton email service instance"""
    global _email_service_instance
    if _email_service_instance is None:
        _email_service_instance = EmailService()
    return _email_service_instance


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

async def send_invite_email(
    org_id: int,
    email: str,
    temp_password: str,
    organization_name: str
) -> Tuple[bool, str]:
    """Send a user invite email"""
    service = get_email_service()
    return await service.send_invite_email(org_id, email, temp_password, organization_name)


async def is_email_configured(org_id: int) -> bool:
    """Check if email is configured for an organization"""
    service = get_email_service()
    config = await service.get_email_config(org_id)
    return config is not None
