"""
Token Scanner for Leaked API Key Detection

Scans logs, repositories, and external sources for exposed Logic Weaver API keys.
Automatically revokes leaked keys and notifies administrators.
"""

import re
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, AsyncIterator
from pathlib import Path
import asyncio

logger = logging.getLogger(__name__)


@dataclass
class LeakedKeyAlert:
    """Alert for a leaked API key"""
    key_id: str
    key_prefix: str
    key_name: Optional[str]
    tenant_id: str
    source: str  # e.g., "github:repo/name", "log:/var/log/app.log", "manual"
    detected_at: datetime
    context: str  # Surrounding text where key was found
    auto_revoked: bool = False
    notified: bool = False


@dataclass
class ScanResult:
    """Result of a token scan operation"""
    scanned_files: int = 0
    scanned_lines: int = 0
    keys_found: int = 0
    alerts: list[LeakedKeyAlert] = field(default_factory=list)
    duration_seconds: float = 0.0


class TokenScanner:
    """
    Scans for leaked Logic Weaver API keys.

    Features:
    - Regex pattern matching for API key format
    - File system scanning (logs, config files)
    - Git repository scanning
    - Automatic revocation of leaked keys
    - Admin notifications

    Usage:
        scanner = TokenScanner(db_pool)
        result = await scanner.scan_directory("/var/log")
        for alert in result.alerts:
            print(f"Found leaked key: {alert.key_prefix}")
    """

    # Logic Weaver API key pattern: mw_{env}_{4hex}_{32hex}
    KEY_PATTERN = re.compile(
        r'mw_(?:dev|development|staging|production|test)_[a-f0-9]{4}_[a-f0-9]{32}',
        re.IGNORECASE
    )

    # Patterns that indicate a key is in a safe context (e.g., documentation examples)
    SAFE_CONTEXT_PATTERNS = [
        r'example',
        r'sample',
        r'placeholder',
        r'your[_-]?api[_-]?key',
        r'<api[_-]?key>',
        r'\$\{.*\}',  # Environment variable placeholder
        r'xxxxxxxx',
        r'••••••••',  # Masked
    ]

    # File patterns to skip
    SKIP_PATTERNS = [
        r'\.git/',
        r'node_modules/',
        r'__pycache__/',
        r'\.pyc$',
        r'\.min\.js$',
        r'\.map$',
        r'\.lock$',
        r'package-lock\.json$',
    ]

    def __init__(self, db_pool=None, auto_revoke: bool = True):
        """
        Initialize the token scanner.

        Args:
            db_pool: AsyncPG connection pool for database operations
            auto_revoke: Whether to automatically revoke leaked keys
        """
        self.pool = db_pool
        self.auto_revoke = auto_revoke
        self._safe_patterns = [re.compile(p, re.IGNORECASE) for p in self.SAFE_CONTEXT_PATTERNS]
        self._skip_patterns = [re.compile(p) for p in self.SKIP_PATTERNS]

    def _should_skip_file(self, filepath: str) -> bool:
        """Check if file should be skipped based on patterns"""
        for pattern in self._skip_patterns:
            if pattern.search(filepath):
                return True
        return False

    def _is_safe_context(self, line: str, match: str) -> bool:
        """Check if the key appears in a safe context (documentation, examples)"""
        # Get surrounding context
        context_start = max(0, line.find(match) - 50)
        context_end = min(len(line), line.find(match) + len(match) + 50)
        context = line[context_start:context_end].lower()

        for pattern in self._safe_patterns:
            if pattern.search(context):
                return True

        return False

    def _extract_context(self, line: str, match: str, context_chars: int = 100) -> str:
        """Extract surrounding context for a match, masking the key"""
        pos = line.find(match)
        start = max(0, pos - context_chars)
        end = min(len(line), pos + len(match) + context_chars)

        context = line[start:end]
        # Mask the key in context
        masked_key = match[:20] + "••••••••••••"
        context = context.replace(match, masked_key)

        return context.strip()

    async def scan_text(self, text: str, source: str = "unknown") -> list[str]:
        """
        Scan text for API keys.

        Args:
            text: Text content to scan
            source: Source identifier for logging

        Returns:
            List of found API key strings
        """
        found_keys = []

        for line in text.split('\n'):
            for match in self.KEY_PATTERN.findall(line):
                if not self._is_safe_context(line, match):
                    found_keys.append(match)
                    logger.warning(f"Potential leaked key found in {source}: {match[:20]}...")

        return found_keys

    async def scan_file(self, filepath: Path) -> list[tuple[str, int, str]]:
        """
        Scan a single file for API keys.

        Returns:
            List of (key, line_number, context) tuples
        """
        if self._should_skip_file(str(filepath)):
            return []

        findings = []

        try:
            # Try to read as text
            content = filepath.read_text(encoding='utf-8', errors='ignore')

            for line_num, line in enumerate(content.split('\n'), 1):
                for match in self.KEY_PATTERN.findall(line):
                    if not self._is_safe_context(line, match):
                        context = self._extract_context(line, match)
                        findings.append((match, line_num, context))
                        logger.warning(
                            f"Leaked key found: {filepath}:{line_num} - {match[:20]}..."
                        )

        except Exception as e:
            logger.debug(f"Could not scan {filepath}: {e}")

        return findings

    async def scan_directory(
        self,
        directory: str,
        recursive: bool = True,
        file_extensions: Optional[list[str]] = None,
    ) -> ScanResult:
        """
        Scan a directory for leaked API keys.

        Args:
            directory: Directory path to scan
            recursive: Whether to scan subdirectories
            file_extensions: List of extensions to scan (e.g., ['.py', '.js', '.log'])
                           If None, scans all text files

        Returns:
            ScanResult with findings
        """
        start_time = datetime.utcnow()
        result = ScanResult()
        dir_path = Path(directory)

        if not dir_path.exists():
            logger.error(f"Directory not found: {directory}")
            return result

        # Default extensions for source code and config files
        if file_extensions is None:
            file_extensions = [
                '.py', '.js', '.ts', '.jsx', '.tsx', '.json', '.yaml', '.yml',
                '.env', '.conf', '.cfg', '.ini', '.log', '.txt', '.md',
                '.sh', '.bash', '.zsh', '.sql', '.html', '.xml'
            ]

        pattern = '**/*' if recursive else '*'

        for filepath in dir_path.glob(pattern):
            if not filepath.is_file():
                continue

            if file_extensions and filepath.suffix.lower() not in file_extensions:
                continue

            if self._should_skip_file(str(filepath)):
                continue

            result.scanned_files += 1

            findings = await self.scan_file(filepath)

            for key, line_num, context in findings:
                result.keys_found += 1

                # Look up key in database if pool available
                key_info = await self._lookup_key(key) if self.pool else None

                alert = LeakedKeyAlert(
                    key_id=key_info['id'] if key_info else 'unknown',
                    key_prefix=key[:20],
                    key_name=key_info['name'] if key_info else None,
                    tenant_id=key_info['tenant_id'] if key_info else 'unknown',
                    source=f"file:{filepath}:{line_num}",
                    detected_at=datetime.utcnow(),
                    context=context,
                )

                # Auto-revoke if enabled and key exists
                if self.auto_revoke and key_info and key_info['status'] == 'active':
                    await self._revoke_key(key_info['id'], alert.source)
                    alert.auto_revoked = True

                result.alerts.append(alert)

        result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()

        logger.info(
            f"Scan complete: {result.scanned_files} files, "
            f"{result.keys_found} keys found in {result.duration_seconds:.2f}s"
        )

        return result

    async def scan_git_history(self, repo_path: str, max_commits: int = 100) -> ScanResult:
        """
        Scan git history for leaked keys in past commits.

        Args:
            repo_path: Path to git repository
            max_commits: Maximum number of commits to scan

        Returns:
            ScanResult with findings
        """
        import subprocess

        start_time = datetime.utcnow()
        result = ScanResult()

        try:
            # Get list of commits
            proc = subprocess.run(
                ['git', 'log', '--oneline', f'-{max_commits}', '--format=%H'],
                cwd=repo_path,
                capture_output=True,
                text=True
            )

            if proc.returncode != 0:
                logger.error(f"Git log failed: {proc.stderr}")
                return result

            commits = proc.stdout.strip().split('\n')

            for commit_hash in commits:
                if not commit_hash:
                    continue

                # Get diff for this commit
                proc = subprocess.run(
                    ['git', 'show', commit_hash, '--format='],
                    cwd=repo_path,
                    capture_output=True,
                    text=True
                )

                if proc.returncode == 0:
                    for match in self.KEY_PATTERN.findall(proc.stdout):
                        result.keys_found += 1

                        alert = LeakedKeyAlert(
                            key_id='unknown',
                            key_prefix=match[:20],
                            key_name=None,
                            tenant_id='unknown',
                            source=f"git:{repo_path}:{commit_hash[:8]}",
                            detected_at=datetime.utcnow(),
                            context=f"Found in commit {commit_hash[:8]}",
                        )
                        result.alerts.append(alert)

        except FileNotFoundError:
            logger.error("Git not found. Install git to scan repositories.")
        except Exception as e:
            logger.error(f"Git scan error: {e}")

        result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
        return result

    async def _lookup_key(self, key_value: str) -> Optional[dict]:
        """Look up API key in database by its value"""
        if not self.pool:
            return None

        # Extract prefix for lookup
        key_prefix = key_value[:20]

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, name, tenant_id, status, key_prefix
                FROM common.api_keys
                WHERE key_prefix = $1
                """,
                key_prefix
            )
            return dict(row) if row else None

    async def _revoke_key(self, key_id: str, source: str) -> None:
        """Revoke a leaked API key"""
        if not self.pool:
            return

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE common.api_keys
                SET status = 'revoked',
                    revoked_at = NOW(),
                    revocation_reason = $2
                WHERE id = $1 AND status = 'active'
                """,
                key_id,
                f"Auto-revoked: Leaked key detected at {source}"
            )

            logger.warning(f"Auto-revoked leaked key {key_id}")

    async def watch_directory(
        self,
        directory: str,
        interval_seconds: int = 300,
    ) -> AsyncIterator[LeakedKeyAlert]:
        """
        Continuously watch a directory for new leaked keys.

        Args:
            directory: Directory to watch
            interval_seconds: Scan interval in seconds

        Yields:
            LeakedKeyAlert for each new finding
        """
        seen_keys = set()

        while True:
            result = await self.scan_directory(directory)

            for alert in result.alerts:
                # Only yield new findings
                alert_key = (alert.key_prefix, alert.source)
                if alert_key not in seen_keys:
                    seen_keys.add(alert_key)
                    yield alert

            await asyncio.sleep(interval_seconds)


# Utility functions for standalone use
def scan_string_for_keys(text: str) -> list[str]:
    """
    Synchronous utility to scan a string for API keys.

    Usage:
        keys = scan_string_for_keys(log_content)
        if keys:
            print(f"Found {len(keys)} potential leaked keys!")
    """
    pattern = re.compile(
        r'mw_(?:dev|development|staging|production|test)_[a-f0-9]{4}_[a-f0-9]{32}',
        re.IGNORECASE
    )
    return pattern.findall(text)


def mask_keys_in_text(text: str) -> str:
    """
    Mask any API keys found in text (for safe logging).

    Usage:
        safe_log = mask_keys_in_text(potentially_sensitive_log)
    """
    pattern = re.compile(
        r'(mw_(?:dev|development|staging|production|test)_[a-f0-9]{4}_)[a-f0-9]{32}',
        re.IGNORECASE
    )
    return pattern.sub(r'\1••••••••••••••••••••••••••••••••', text)
