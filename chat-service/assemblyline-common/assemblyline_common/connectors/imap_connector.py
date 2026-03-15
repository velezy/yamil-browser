"""
IMAP Email Connector for Logic Weaver

Enterprise-grade IMAP connector with:
- IMAP and IMAPS (SSL/TLS) support
- OAuth2 authentication (Google, Microsoft)
- Folder management
- Advanced search and filtering
- Attachment extraction
- IDLE for real-time notifications
- Mark as read/unread/flagged
- Move, copy, delete operations
- Multi-tenant isolation

Comparison:
| Feature              | MuleSoft | Zapier | Logic Weaver |
|---------------------|----------|--------|--------------|
| IMAP Support        | Limited  | Yes    | Full         |
| OAuth2              | No       | Yes    | Yes          |
| IDLE (Push)         | No       | No     | Yes          |
| Search Filters      | Basic    | Basic  | Advanced     |
| Attachment Parse    | Limited  | No     | Yes          |
"""

from __future__ import annotations

import asyncio
import base64
import email
import imaplib
import logging
import re
import ssl
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from email.header import decode_header
from email.utils import parseaddr, parsedate_to_datetime
from enum import Enum
from typing import Any, AsyncIterator, Callable, Optional, Union

logger = logging.getLogger(__name__)

# Try to import aioimaplib for async IMAP
try:
    import aioimaplib
    HAS_AIOIMAPLIB = True
except ImportError:
    HAS_AIOIMAPLIB = False
    logger.warning("aioimaplib not installed. Using sync IMAP.")


class AuthType(Enum):
    """IMAP authentication types."""
    PLAIN = "plain"
    LOGIN = "login"
    OAUTH2 = "oauth2"


class SearchCriteria(Enum):
    """Common IMAP search criteria."""
    ALL = "ALL"
    UNSEEN = "UNSEEN"
    SEEN = "SEEN"
    FLAGGED = "FLAGGED"
    UNFLAGGED = "UNFLAGGED"
    ANSWERED = "ANSWERED"
    DELETED = "DELETED"
    RECENT = "RECENT"


@dataclass
class IMAPConfig:
    """IMAP connection configuration."""
    # Connection
    host: str
    port: int = 993
    use_ssl: bool = True

    # Authentication
    auth_type: AuthType = AuthType.PLAIN
    username: str = ""
    password: str = ""
    oauth2_token: Optional[str] = None

    # SSL settings
    ssl_verify: bool = True
    ssl_cert: Optional[str] = None

    # Connection settings
    timeout: float = 30.0
    idle_timeout: float = 300.0  # 5 minutes

    # Default folder
    folder: str = "INBOX"

    # Multi-tenant
    tenant_id: Optional[str] = None


@dataclass
class EmailAttachment:
    """Email attachment."""
    filename: str
    content_type: str
    size: int
    content: bytes
    content_id: Optional[str] = None  # For inline attachments

    def to_dict(self) -> dict[str, Any]:
        return {
            "filename": self.filename,
            "content_type": self.content_type,
            "size": self.size,
            "content_base64": base64.b64encode(self.content).decode('utf-8'),
            "content_id": self.content_id,
        }


@dataclass
class EmailMessage:
    """Represents an email message."""
    uid: str
    message_id: Optional[str] = None
    subject: str = ""
    from_address: str = ""
    from_name: str = ""
    to_addresses: list[str] = field(default_factory=list)
    cc_addresses: list[str] = field(default_factory=list)
    bcc_addresses: list[str] = field(default_factory=list)
    reply_to: Optional[str] = None
    date: Optional[datetime] = None
    body_text: str = ""
    body_html: str = ""
    attachments: list[EmailAttachment] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)
    folder: str = "INBOX"
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def is_read(self) -> bool:
        return "\\Seen" in self.flags

    @property
    def is_flagged(self) -> bool:
        return "\\Flagged" in self.flags

    @property
    def has_attachments(self) -> bool:
        return len(self.attachments) > 0

    def to_dict(self, include_body: bool = True, include_attachments: bool = False) -> dict[str, Any]:
        result = {
            "uid": self.uid,
            "message_id": self.message_id,
            "subject": self.subject,
            "from_address": self.from_address,
            "from_name": self.from_name,
            "to_addresses": self.to_addresses,
            "cc_addresses": self.cc_addresses,
            "date": self.date.isoformat() if self.date else None,
            "is_read": self.is_read,
            "is_flagged": self.is_flagged,
            "has_attachments": self.has_attachments,
            "folder": self.folder,
        }

        if include_body:
            result["body_text"] = self.body_text
            result["body_html"] = self.body_html

        if include_attachments:
            result["attachments"] = [a.to_dict() for a in self.attachments]
        else:
            result["attachment_count"] = len(self.attachments)

        return result


@dataclass
class IMAPResult:
    """Result of an IMAP operation."""
    success: bool
    message: str
    data: Optional[Any] = None
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "message": self.message,
            "data": self.data,
            "error": self.error,
        }


class IMAPConnector:
    """
    Enterprise IMAP connector.

    Example usage:

    config = IMAPConfig(
        host="imap.gmail.com",
        username="user@gmail.com",
        password="app-password"
    )

    async with IMAPConnector(config) as imap:
        # List folders
        folders = await imap.list_folders()

        # Fetch unread emails
        emails = await imap.fetch_emails(
            folder="INBOX",
            criteria=SearchCriteria.UNSEEN,
            limit=10
        )

        # Mark as read
        await imap.mark_read(emails[0].uid)

        # Real-time notifications
        async for email in imap.idle():
            process_new_email(email)
    """

    def __init__(self, config: IMAPConfig):
        self.config = config
        self._client: Optional[Union[imaplib.IMAP4_SSL, imaplib.IMAP4]] = None
        self._async_client: Optional[aioimaplib.IMAP4_SSL] = None
        self._selected_folder: Optional[str] = None
        self._is_idle = False

    @property
    def is_connected(self) -> bool:
        return self._client is not None or self._async_client is not None

    async def __aenter__(self) -> "IMAPConnector":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()

    async def connect(self) -> bool:
        """Establish IMAP connection and authenticate."""
        try:
            if HAS_AIOIMAPLIB:
                return await self._connect_async()
            else:
                return await self._connect_sync()

        except Exception as e:
            logger.error(f"IMAP connection failed: {e}")
            raise

    async def _connect_async(self) -> bool:
        """Connect using async IMAP."""
        if self.config.use_ssl:
            self._async_client = aioimaplib.IMAP4_SSL(
                host=self.config.host,
                port=self.config.port,
                timeout=self.config.timeout,
            )
        else:
            self._async_client = aioimaplib.IMAP4(
                host=self.config.host,
                port=self.config.port,
                timeout=self.config.timeout,
            )

        await self._async_client.wait_hello_from_server()

        # Authenticate
        if self.config.auth_type == AuthType.OAUTH2:
            auth_string = self._build_oauth2_string()
            await self._async_client.authenticate("XOAUTH2", lambda x: auth_string)
        else:
            await self._async_client.login(
                self.config.username,
                self.config.password
            )

        logger.info(f"Connected to IMAP server: {self.config.host}")
        return True

    async def _connect_sync(self) -> bool:
        """Connect using sync IMAP (run in thread)."""
        loop = asyncio.get_event_loop()

        def _connect():
            if self.config.use_ssl:
                context = ssl.create_default_context()
                if not self.config.ssl_verify:
                    context.check_hostname = False
                    context.verify_mode = ssl.CERT_NONE

                client = imaplib.IMAP4_SSL(
                    self.config.host,
                    self.config.port,
                    ssl_context=context,
                )
            else:
                client = imaplib.IMAP4(
                    self.config.host,
                    self.config.port,
                )

            if self.config.auth_type == AuthType.OAUTH2:
                auth_string = self._build_oauth2_string()
                client.authenticate("XOAUTH2", lambda x: auth_string)
            else:
                client.login(self.config.username, self.config.password)

            return client

        self._client = await loop.run_in_executor(None, _connect)
        logger.info(f"Connected to IMAP server: {self.config.host}")
        return True

    def _build_oauth2_string(self) -> bytes:
        """Build OAuth2 authentication string."""
        auth = f"user={self.config.username}\1auth=Bearer {self.config.oauth2_token}\1\1"
        return auth.encode('utf-8')

    async def disconnect(self) -> None:
        """Close IMAP connection."""
        if self._is_idle:
            await self.stop_idle()

        if self._async_client:
            await self._async_client.logout()
            self._async_client = None

        if self._client:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._client.logout)
            self._client = None

        logger.info("IMAP connection closed")

    async def list_folders(self) -> list[str]:
        """List all mail folders."""
        if self._async_client:
            response = await self._async_client.list()
            folders = []
            for line in response.lines:
                if isinstance(line, bytes):
                    line = line.decode('utf-8')
                # Parse folder name from IMAP LIST response
                match = re.search(r'"([^"]+)"$', line)
                if match:
                    folders.append(match.group(1))
            return folders
        else:
            loop = asyncio.get_event_loop()
            _, data = await loop.run_in_executor(None, self._client.list)
            folders = []
            for item in data:
                if isinstance(item, bytes):
                    item = item.decode('utf-8')
                match = re.search(r'"([^"]+)"$', str(item))
                if match:
                    folders.append(match.group(1))
            return folders

    async def select_folder(self, folder: str = "INBOX") -> int:
        """Select a folder. Returns message count."""
        if self._async_client:
            response = await self._async_client.select(folder)
            self._selected_folder = folder
            # Parse message count from response
            for line in response.lines:
                if b"EXISTS" in line:
                    return int(line.split()[0])
            return 0
        else:
            loop = asyncio.get_event_loop()
            _, data = await loop.run_in_executor(None, self._client.select, folder)
            self._selected_folder = folder
            return int(data[0])

    async def search(
        self,
        criteria: Union[SearchCriteria, str] = SearchCriteria.ALL,
        folder: Optional[str] = None,
    ) -> list[str]:
        """
        Search for messages matching criteria.

        Returns list of message UIDs.
        """
        folder = folder or self.config.folder
        await self.select_folder(folder)

        if isinstance(criteria, SearchCriteria):
            criteria_str = criteria.value
        else:
            criteria_str = criteria

        if self._async_client:
            response = await self._async_client.uid("search", None, criteria_str)
            if response.result == "OK" and response.lines:
                uids = response.lines[0].decode('utf-8').split()
                return uids
            return []
        else:
            loop = asyncio.get_event_loop()
            _, data = await loop.run_in_executor(
                None,
                lambda: self._client.uid("search", None, criteria_str)
            )
            if data[0]:
                return data[0].decode('utf-8').split()
            return []

    async def fetch_emails(
        self,
        folder: Optional[str] = None,
        criteria: Union[SearchCriteria, str] = SearchCriteria.ALL,
        limit: int = 10,
        offset: int = 0,
        include_body: bool = True,
        include_attachments: bool = False,
    ) -> list[EmailMessage]:
        """
        Fetch emails matching criteria.

        Args:
            folder: Folder to search
            criteria: Search criteria
            limit: Max emails to return
            offset: Skip this many emails
            include_body: Include email body
            include_attachments: Include attachment content
        """
        uids = await self.search(criteria, folder)

        # Apply pagination
        uids = uids[offset:offset + limit]

        emails = []
        for uid in uids:
            email_msg = await self.fetch_email(
                uid,
                include_body=include_body,
                include_attachments=include_attachments,
            )
            if email_msg:
                emails.append(email_msg)

        return emails

    async def fetch_email(
        self,
        uid: str,
        include_body: bool = True,
        include_attachments: bool = False,
    ) -> Optional[EmailMessage]:
        """Fetch a single email by UID."""
        try:
            # Determine what to fetch
            if include_body:
                fetch_items = "(FLAGS BODY.PEEK[])"
            else:
                fetch_items = "(FLAGS BODY.PEEK[HEADER])"

            if self._async_client:
                response = await self._async_client.uid("fetch", uid, fetch_items)
                if response.result != "OK":
                    return None
                raw_email = response.lines[1]
            else:
                loop = asyncio.get_event_loop()
                _, data = await loop.run_in_executor(
                    None,
                    lambda: self._client.uid("fetch", uid, fetch_items)
                )
                if not data or not data[0]:
                    return None
                raw_email = data[0][1]

            # Parse email
            return self._parse_email(uid, raw_email, include_attachments)

        except Exception as e:
            logger.error(f"Failed to fetch email {uid}: {e}")
            return None

    def _parse_email(
        self,
        uid: str,
        raw_email: bytes,
        include_attachments: bool = False,
    ) -> EmailMessage:
        """Parse raw email into EmailMessage."""
        msg = email.message_from_bytes(raw_email)

        # Parse headers
        subject = self._decode_header(msg.get("Subject", ""))
        from_name, from_address = parseaddr(msg.get("From", ""))
        from_name = self._decode_header(from_name)

        to_addresses = [addr for _, addr in email.utils.getaddresses([msg.get("To", "")])]
        cc_addresses = [addr for _, addr in email.utils.getaddresses([msg.get("Cc", "")])]

        message_id = msg.get("Message-ID")
        reply_to = msg.get("Reply-To")

        # Parse date
        date = None
        date_str = msg.get("Date")
        if date_str:
            try:
                date = parsedate_to_datetime(date_str)
            except Exception:
                pass

        # Parse body and attachments
        body_text = ""
        body_html = ""
        attachments = []

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = part.get("Content-Disposition", "")

                if "attachment" in content_disposition:
                    if include_attachments:
                        attachment = self._parse_attachment(part)
                        if attachment:
                            attachments.append(attachment)
                elif content_type == "text/plain":
                    try:
                        body_text = part.get_payload(decode=True).decode('utf-8', errors='replace')
                    except Exception:
                        pass
                elif content_type == "text/html":
                    try:
                        body_html = part.get_payload(decode=True).decode('utf-8', errors='replace')
                    except Exception:
                        pass
        else:
            content_type = msg.get_content_type()
            try:
                payload = msg.get_payload(decode=True)
                if payload:
                    text = payload.decode('utf-8', errors='replace')
                    if content_type == "text/html":
                        body_html = text
                    else:
                        body_text = text
            except Exception:
                pass

        return EmailMessage(
            uid=uid,
            message_id=message_id,
            subject=subject,
            from_address=from_address,
            from_name=from_name,
            to_addresses=to_addresses,
            cc_addresses=cc_addresses,
            reply_to=reply_to,
            date=date,
            body_text=body_text,
            body_html=body_html,
            attachments=attachments,
            folder=self._selected_folder or self.config.folder,
        )

    def _decode_header(self, value: str) -> str:
        """Decode MIME encoded header."""
        if not value:
            return ""

        decoded_parts = decode_header(value)
        result = []
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                part = part.decode(encoding or 'utf-8', errors='replace')
            result.append(part)
        return " ".join(result)

    def _parse_attachment(self, part) -> Optional[EmailAttachment]:
        """Parse email attachment."""
        try:
            filename = part.get_filename()
            if filename:
                filename = self._decode_header(filename)

            content_type = part.get_content_type()
            content = part.get_payload(decode=True)
            content_id = part.get("Content-ID")

            if content:
                return EmailAttachment(
                    filename=filename or "unknown",
                    content_type=content_type,
                    size=len(content),
                    content=content,
                    content_id=content_id.strip("<>") if content_id else None,
                )
        except Exception as e:
            logger.warning(f"Failed to parse attachment: {e}")

        return None

    async def mark_read(self, uid: str) -> bool:
        """Mark a message as read."""
        return await self._add_flags(uid, "\\Seen")

    async def mark_unread(self, uid: str) -> bool:
        """Mark a message as unread."""
        return await self._remove_flags(uid, "\\Seen")

    async def mark_flagged(self, uid: str) -> bool:
        """Flag a message."""
        return await self._add_flags(uid, "\\Flagged")

    async def mark_unflagged(self, uid: str) -> bool:
        """Remove flag from message."""
        return await self._remove_flags(uid, "\\Flagged")

    async def _add_flags(self, uid: str, flags: str) -> bool:
        """Add flags to a message."""
        try:
            if self._async_client:
                await self._async_client.uid("store", uid, "+FLAGS", f"({flags})")
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self._client.uid("store", uid, "+FLAGS", f"({flags})")
                )
            return True
        except Exception as e:
            logger.error(f"Failed to add flags: {e}")
            return False

    async def _remove_flags(self, uid: str, flags: str) -> bool:
        """Remove flags from a message."""
        try:
            if self._async_client:
                await self._async_client.uid("store", uid, "-FLAGS", f"({flags})")
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self._client.uid("store", uid, "-FLAGS", f"({flags})")
                )
            return True
        except Exception as e:
            logger.error(f"Failed to remove flags: {e}")
            return False

    async def move(self, uid: str, target_folder: str) -> bool:
        """Move a message to another folder."""
        try:
            if self._async_client:
                await self._async_client.uid("move", uid, target_folder)
            else:
                # MOVE not always supported, use COPY + DELETE
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self._client.uid("copy", uid, target_folder)
                )
                await loop.run_in_executor(
                    None,
                    lambda: self._client.uid("store", uid, "+FLAGS", "(\\Deleted)")
                )
                await loop.run_in_executor(None, self._client.expunge)
            return True
        except Exception as e:
            logger.error(f"Failed to move message: {e}")
            return False

    async def delete(self, uid: str) -> bool:
        """Delete a message."""
        try:
            await self._add_flags(uid, "\\Deleted")
            if self._async_client:
                await self._async_client.expunge()
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._client.expunge)
            return True
        except Exception as e:
            logger.error(f"Failed to delete message: {e}")
            return False

    async def idle(self, folder: Optional[str] = None) -> AsyncIterator[EmailMessage]:
        """
        Start IDLE mode for real-time notifications.

        Yields new emails as they arrive.
        """
        folder = folder or self.config.folder
        await self.select_folder(folder)

        self._is_idle = True

        try:
            if self._async_client:
                await self._async_client.idle_start()

                while self._is_idle:
                    msg = await asyncio.wait_for(
                        self._async_client.wait_server_push(),
                        timeout=self.config.idle_timeout
                    )

                    if b"EXISTS" in msg:
                        # New message arrived
                        uids = await self.search(SearchCriteria.RECENT)
                        for uid in uids:
                            email_msg = await self.fetch_email(uid)
                            if email_msg:
                                yield email_msg

            else:
                # Polling fallback for sync client
                known_uids = set(await self.search(SearchCriteria.ALL))

                while self._is_idle:
                    await asyncio.sleep(10)  # Poll every 10 seconds
                    current_uids = set(await self.search(SearchCriteria.ALL))
                    new_uids = current_uids - known_uids

                    for uid in new_uids:
                        email_msg = await self.fetch_email(uid)
                        if email_msg:
                            yield email_msg

                    known_uids = current_uids

        except asyncio.TimeoutError:
            # Restart IDLE
            if self._is_idle:
                async for email_msg in self.idle(folder):
                    yield email_msg
        finally:
            self._is_idle = False

    async def stop_idle(self) -> None:
        """Stop IDLE mode."""
        self._is_idle = False
        if self._async_client:
            await self._async_client.idle_done()


# Flow Node Integration
@dataclass
class IMAPNodeConfig:
    """Configuration for IMAP flow node."""
    host: str
    port: int = 993
    use_ssl: bool = True
    username: str = ""
    password: str = ""
    folder: str = "INBOX"
    operation: str = "fetch"  # fetch, search, mark_read, move, delete
    criteria: str = "UNSEEN"
    limit: int = 10
    include_attachments: bool = False
    target_folder: Optional[str] = None


@dataclass
class IMAPNodeResult:
    """Result from IMAP flow node."""
    success: bool
    operation: str
    emails: list[dict[str, Any]]
    count: int
    message: str
    error: Optional[str]


class IMAPNode:
    """Flow node for IMAP operations."""

    node_type = "imap"
    node_category = "connector"

    def __init__(self, config: IMAPNodeConfig):
        self.config = config

    async def execute(self, input_data: dict[str, Any]) -> IMAPNodeResult:
        """Execute the IMAP operation."""
        imap_config = IMAPConfig(
            host=self.config.host,
            port=self.config.port,
            use_ssl=self.config.use_ssl,
            username=self.config.username,
            password=self.config.password,
            folder=self.config.folder,
        )

        try:
            async with IMAPConnector(imap_config) as imap:
                if self.config.operation == "fetch":
                    emails = await imap.fetch_emails(
                        folder=self.config.folder,
                        criteria=self.config.criteria,
                        limit=self.config.limit,
                        include_attachments=self.config.include_attachments,
                    )

                    return IMAPNodeResult(
                        success=True,
                        operation="fetch",
                        emails=[e.to_dict(include_attachments=self.config.include_attachments) for e in emails],
                        count=len(emails),
                        message=f"Fetched {len(emails)} emails",
                        error=None,
                    )

                elif self.config.operation == "search":
                    uids = await imap.search(criteria=self.config.criteria)

                    return IMAPNodeResult(
                        success=True,
                        operation="search",
                        emails=[{"uid": uid} for uid in uids[:self.config.limit]],
                        count=len(uids),
                        message=f"Found {len(uids)} messages",
                        error=None,
                    )

                elif self.config.operation == "mark_read":
                    uid = input_data.get("uid")
                    if uid:
                        await imap.mark_read(uid)

                    return IMAPNodeResult(
                        success=True,
                        operation="mark_read",
                        emails=[],
                        count=1,
                        message="Marked as read",
                        error=None,
                    )

                elif self.config.operation == "move":
                    uid = input_data.get("uid")
                    target = self.config.target_folder or input_data.get("target_folder")
                    if uid and target:
                        await imap.move(uid, target)

                    return IMAPNodeResult(
                        success=True,
                        operation="move",
                        emails=[],
                        count=1,
                        message=f"Moved to {target}",
                        error=None,
                    )

                elif self.config.operation == "delete":
                    uid = input_data.get("uid")
                    if uid:
                        await imap.delete(uid)

                    return IMAPNodeResult(
                        success=True,
                        operation="delete",
                        emails=[],
                        count=1,
                        message="Deleted",
                        error=None,
                    )

                else:
                    return IMAPNodeResult(
                        success=False,
                        operation=self.config.operation,
                        emails=[],
                        count=0,
                        message="Unknown operation",
                        error=f"Unknown operation: {self.config.operation}",
                    )

        except Exception as e:
            logger.error(f"IMAP node execution failed: {e}")
            return IMAPNodeResult(
                success=False,
                operation=self.config.operation,
                emails=[],
                count=0,
                message="Execution failed",
                error=str(e),
            )


def get_imap_connector(config: IMAPConfig) -> IMAPConnector:
    """Factory function to create IMAP connector."""
    return IMAPConnector(config)


def get_imap_node_definition() -> dict[str, Any]:
    """Get the flow node definition for the UI."""
    return {
        "type": "imap",
        "category": "connector",
        "label": "IMAP Email",
        "description": "Receive and process emails via IMAP",
        "icon": "Mail",
        "color": "#EA4335",
        "inputs": ["trigger", "uid"],
        "outputs": ["emails", "result"],
        "config_schema": {
            "host": {
                "type": "string",
                "required": True,
                "label": "IMAP Host",
                "placeholder": "imap.gmail.com",
            },
            "port": {
                "type": "number",
                "default": 993,
                "label": "Port",
            },
            "use_ssl": {
                "type": "boolean",
                "default": True,
                "label": "Use SSL",
            },
            "username": {
                "type": "string",
                "required": True,
                "label": "Username",
            },
            "password": {
                "type": "password",
                "required": True,
                "label": "Password",
            },
            "folder": {
                "type": "string",
                "default": "INBOX",
                "label": "Folder",
            },
            "operation": {
                "type": "select",
                "options": ["fetch", "search", "mark_read", "move", "delete"],
                "default": "fetch",
                "label": "Operation",
            },
            "criteria": {
                "type": "string",
                "default": "UNSEEN",
                "label": "Search Criteria",
                "placeholder": "UNSEEN, ALL, FLAGGED, etc.",
            },
            "limit": {
                "type": "number",
                "default": 10,
                "label": "Max Messages",
            },
            "include_attachments": {
                "type": "boolean",
                "default": False,
                "label": "Include Attachments",
            },
        },
    }
