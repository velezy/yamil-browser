"""
Response Post-Processor - Rule-based LLM Output Cleaning
=========================================================

Fast, deterministic regex-based processors for cleaning and normalizing
LLM responses. No LLM calls - pure pattern matching for < 5ms latency.

Processors:
- MarkdownCleaner: Strip markdown formatting
- HeaderRemover: Remove unwanted section headers
- WhitespaceCleaner: Normalize whitespace and newlines
- CodeBlockProcessor: Extract or format code blocks
- LinkCleaner: Handle URLs and markdown links
- ResponseFormatter: Apply multiple processors in sequence
"""

import re
import html
from typing import List, Optional, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum


class CleanerType(str, Enum):
    """Types of cleaning operations"""
    MARKDOWN = "markdown"
    HEADERS = "headers"
    WHITESPACE = "whitespace"
    CODE_BLOCKS = "code_blocks"
    LINKS = "links"
    LISTS = "lists"
    TABLES = "tables"
    EMOJI = "emoji"
    HTML = "html"
    CITATIONS = "citations"


@dataclass
class CleanerResult:
    """Result from a cleaning operation"""
    original: str
    cleaned: str
    changes_made: List[str] = field(default_factory=list)
    items_removed: int = 0

    @property
    def was_modified(self) -> bool:
        return self.original != self.cleaned


# =============================================================================
# MARKDOWN CLEANER
# =============================================================================

class MarkdownCleaner:
    """Remove or convert markdown formatting to plain text"""

    # Patterns for markdown elements
    BOLD_PATTERN = re.compile(r'\*\*(.+?)\*\*|\*(.+?)\*')
    ITALIC_PATTERN = re.compile(r'_(.+?)_|__(.+?)__')
    STRIKETHROUGH_PATTERN = re.compile(r'~~(.+?)~~')
    INLINE_CODE_PATTERN = re.compile(r'`([^`]+)`')
    HEADER_PATTERN = re.compile(r'^#{1,6}\s+(.+)$', re.MULTILINE)
    BLOCKQUOTE_PATTERN = re.compile(r'^>\s*(.+)$', re.MULTILINE)
    HORIZONTAL_RULE_PATTERN = re.compile(r'^[-*_]{3,}\s*$', re.MULTILINE)
    IMAGE_PATTERN = re.compile(r'!\[([^\]]*)\]\([^)]+\)')
    LINK_PATTERN = re.compile(r'\[([^\]]+)\]\([^)]+\)')

    @classmethod
    def clean(cls, text: str, preserve_structure: bool = False) -> CleanerResult:
        """
        Remove markdown formatting from text.

        Args:
            text: Input text with markdown
            preserve_structure: If True, keep newlines and basic structure

        Returns:
            CleanerResult with cleaned text
        """
        original = text
        changes = []

        # Remove images (replace with alt text or nothing)
        if cls.IMAGE_PATTERN.search(text):
            text = cls.IMAGE_PATTERN.sub(r'\1', text)
            changes.append("removed images")

        # Convert links to just the link text
        if cls.LINK_PATTERN.search(text):
            text = cls.LINK_PATTERN.sub(r'\1', text)
            changes.append("simplified links")

        # Remove bold/italic
        text = cls.BOLD_PATTERN.sub(r'\1\2', text)
        text = cls.ITALIC_PATTERN.sub(r'\1\2', text)
        text = cls.STRIKETHROUGH_PATTERN.sub(r'\1', text)

        # Remove inline code backticks but keep content
        text = cls.INLINE_CODE_PATTERN.sub(r'\1', text)

        # Remove headers markers but keep text
        if cls.HEADER_PATTERN.search(text):
            text = cls.HEADER_PATTERN.sub(r'\1', text)
            changes.append("removed header markers")

        # Remove blockquote markers
        if cls.BLOCKQUOTE_PATTERN.search(text):
            text = cls.BLOCKQUOTE_PATTERN.sub(r'\1', text)
            changes.append("removed blockquotes")

        # Remove horizontal rules
        if cls.HORIZONTAL_RULE_PATTERN.search(text):
            text = cls.HORIZONTAL_RULE_PATTERN.sub('', text)
            changes.append("removed horizontal rules")

        if not preserve_structure:
            # Collapse multiple newlines
            text = re.sub(r'\n{3,}', '\n\n', text)

        return CleanerResult(
            original=original,
            cleaned=text.strip(),
            changes_made=changes
        )


# =============================================================================
# HEADER REMOVER
# =============================================================================

class HeaderRemover:
    """Remove unwanted LLM-generated section headers"""

    # Common LLM-generated headers to remove
    DEFAULT_HEADERS = [
        r'^#+\s*(Summary|Overview|Introduction|Conclusion|Key (Points|Takeaways|Findings|Metrics|Insights)):?\s*$',
        r'^#+\s*(Analysis|Results|Details|Background|Context):?\s*$',
        r'^#+\s*(Answer|Response|Explanation|Solution):?\s*$',
        r'^\*\*(Summary|Overview|Key Points|Analysis|Conclusion)\*\*:?\s*$',
        r'^(Summary|Overview|Key Points|Analysis|Conclusion):?\s*$',
    ]

    def __init__(self, custom_headers: Optional[List[str]] = None):
        """
        Initialize with optional custom header patterns.

        Args:
            custom_headers: Additional regex patterns to match headers
        """
        patterns = self.DEFAULT_HEADERS.copy()
        if custom_headers:
            patterns.extend(custom_headers)

        self.header_pattern = re.compile(
            '|'.join(f'({p})' for p in patterns),
            re.MULTILINE | re.IGNORECASE
        )

    def clean(self, text: str) -> CleanerResult:
        """Remove matching headers from text."""
        original = text
        matches = list(self.header_pattern.finditer(text))

        if matches:
            text = self.header_pattern.sub('', text)
            # Clean up extra newlines left behind
            text = re.sub(r'\n{3,}', '\n\n', text)

        return CleanerResult(
            original=original,
            cleaned=text.strip(),
            changes_made=[f"removed {len(matches)} headers"] if matches else [],
            items_removed=len(matches)
        )


# =============================================================================
# WHITESPACE CLEANER
# =============================================================================

class WhitespaceCleaner:
    """Normalize whitespace and newlines"""

    @classmethod
    def clean(
        cls,
        text: str,
        max_consecutive_newlines: int = 2,
        trim_lines: bool = True,
        remove_trailing_spaces: bool = True
    ) -> CleanerResult:
        """
        Normalize whitespace in text.

        Args:
            text: Input text
            max_consecutive_newlines: Maximum allowed consecutive newlines
            trim_lines: Remove leading/trailing whitespace from each line
            remove_trailing_spaces: Remove trailing spaces from lines
        """
        original = text
        changes = []

        # Remove trailing spaces from lines
        if remove_trailing_spaces:
            text = re.sub(r'[ \t]+$', '', text, flags=re.MULTILINE)
            changes.append("removed trailing spaces")

        # Trim each line
        if trim_lines:
            lines = text.split('\n')
            text = '\n'.join(line.strip() for line in lines)

        # Normalize consecutive newlines
        pattern = r'\n{' + str(max_consecutive_newlines + 1) + r',}'
        if re.search(pattern, text):
            text = re.sub(pattern, '\n' * max_consecutive_newlines, text)
            changes.append(f"normalized newlines (max {max_consecutive_newlines})")

        # Remove leading/trailing whitespace
        text = text.strip()

        return CleanerResult(
            original=original,
            cleaned=text,
            changes_made=changes
        )


# =============================================================================
# CODE BLOCK PROCESSOR
# =============================================================================

class CodeBlockProcessor:
    """Process code blocks in LLM responses"""

    FENCED_CODE_PATTERN = re.compile(
        r'```(\w*)\n(.*?)```',
        re.DOTALL
    )

    @classmethod
    def extract_code_blocks(cls, text: str) -> List[Dict[str, str]]:
        """
        Extract all code blocks from text.

        Returns:
            List of dicts with 'language' and 'code' keys
        """
        blocks = []
        for match in cls.FENCED_CODE_PATTERN.finditer(text):
            blocks.append({
                'language': match.group(1) or 'text',
                'code': match.group(2).strip()
            })
        return blocks

    @classmethod
    def remove_code_blocks(cls, text: str, placeholder: str = "[code]") -> CleanerResult:
        """Remove code blocks, optionally replacing with placeholder."""
        original = text
        count = len(cls.FENCED_CODE_PATTERN.findall(text))

        if placeholder:
            text = cls.FENCED_CODE_PATTERN.sub(placeholder, text)
        else:
            text = cls.FENCED_CODE_PATTERN.sub('', text)

        return CleanerResult(
            original=original,
            cleaned=text.strip(),
            changes_made=[f"removed {count} code blocks"] if count else [],
            items_removed=count
        )

    @classmethod
    def unwrap_code_blocks(cls, text: str) -> CleanerResult:
        """Remove code block markers but keep the code content."""
        original = text
        count = len(cls.FENCED_CODE_PATTERN.findall(text))

        text = cls.FENCED_CODE_PATTERN.sub(r'\2', text)

        return CleanerResult(
            original=original,
            cleaned=text.strip(),
            changes_made=[f"unwrapped {count} code blocks"] if count else [],
            items_removed=0
        )


# =============================================================================
# LINK CLEANER
# =============================================================================

class LinkCleaner:
    """Process URLs and links in LLM responses"""

    URL_PATTERN = re.compile(
        r'https?://[^\s<>"{}|\\^`\[\]]+',
        re.IGNORECASE
    )
    MARKDOWN_LINK_PATTERN = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')

    @classmethod
    def extract_urls(cls, text: str) -> List[str]:
        """Extract all URLs from text."""
        return cls.URL_PATTERN.findall(text)

    @classmethod
    def remove_urls(cls, text: str) -> CleanerResult:
        """Remove all URLs from text."""
        original = text
        urls = cls.URL_PATTERN.findall(text)
        text = cls.URL_PATTERN.sub('', text)

        return CleanerResult(
            original=original,
            cleaned=text.strip(),
            changes_made=[f"removed {len(urls)} URLs"] if urls else [],
            items_removed=len(urls)
        )

    @classmethod
    def simplify_markdown_links(cls, text: str) -> CleanerResult:
        """Convert markdown links to just the link text."""
        original = text
        count = len(cls.MARKDOWN_LINK_PATTERN.findall(text))
        text = cls.MARKDOWN_LINK_PATTERN.sub(r'\1', text)

        return CleanerResult(
            original=original,
            cleaned=text,
            changes_made=[f"simplified {count} markdown links"] if count else [],
            items_removed=0
        )


# =============================================================================
# LIST PROCESSOR
# =============================================================================

class ListProcessor:
    """Process markdown lists"""

    BULLET_PATTERN = re.compile(r'^[\s]*[-*+]\s+', re.MULTILINE)
    NUMBERED_PATTERN = re.compile(r'^[\s]*\d+\.\s+', re.MULTILINE)

    @classmethod
    def remove_bullets(cls, text: str) -> CleanerResult:
        """Remove bullet points from lists."""
        original = text
        bullet_count = len(cls.BULLET_PATTERN.findall(text))
        numbered_count = len(cls.NUMBERED_PATTERN.findall(text))

        text = cls.BULLET_PATTERN.sub('', text)
        text = cls.NUMBERED_PATTERN.sub('', text)

        total = bullet_count + numbered_count
        return CleanerResult(
            original=original,
            cleaned=text,
            changes_made=[f"removed {total} list markers"] if total else [],
            items_removed=total
        )

    @classmethod
    def convert_to_sentences(cls, text: str) -> CleanerResult:
        """Convert bullet lists to comma-separated sentences."""
        original = text
        lines = text.split('\n')
        result_lines = []
        list_items = []

        for line in lines:
            # Check if line is a list item
            bullet_match = cls.BULLET_PATTERN.match(line)
            numbered_match = cls.NUMBERED_PATTERN.match(line)

            if bullet_match or numbered_match:
                # Extract the content after the bullet/number
                content = cls.BULLET_PATTERN.sub('', line)
                content = cls.NUMBERED_PATTERN.sub('', content).strip()
                if content:
                    list_items.append(content)
            else:
                # Flush accumulated list items
                if list_items:
                    result_lines.append(', '.join(list_items) + '.')
                    list_items = []
                result_lines.append(line)

        # Flush any remaining list items
        if list_items:
            result_lines.append(', '.join(list_items) + '.')

        cleaned = '\n'.join(result_lines)

        return CleanerResult(
            original=original,
            cleaned=cleaned,
            changes_made=["converted lists to sentences"] if original != cleaned else []
        )


# =============================================================================
# HTML CLEANER
# =============================================================================

class HTMLCleaner:
    """Remove HTML tags and entities"""

    TAG_PATTERN = re.compile(r'<[^>]+>')

    @classmethod
    def clean(cls, text: str, decode_entities: bool = True) -> CleanerResult:
        """
        Remove HTML tags and optionally decode entities.

        Args:
            text: Input text with HTML
            decode_entities: If True, decode &amp; etc.
        """
        original = text
        changes = []

        # Remove HTML tags
        if cls.TAG_PATTERN.search(text):
            text = cls.TAG_PATTERN.sub('', text)
            changes.append("removed HTML tags")

        # Decode HTML entities
        if decode_entities:
            decoded = html.unescape(text)
            if decoded != text:
                text = decoded
                changes.append("decoded HTML entities")

        return CleanerResult(
            original=original,
            cleaned=text,
            changes_made=changes
        )


# =============================================================================
# EMOJI CLEANER
# =============================================================================

class EmojiCleaner:
    """Remove or replace emojis"""

    # Pattern to match most emojis
    EMOJI_PATTERN = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map
        "\U0001F700-\U0001F77F"  # alchemical
        "\U0001F780-\U0001F7FF"  # geometric
        "\U0001F800-\U0001F8FF"  # supplemental arrows
        "\U0001F900-\U0001F9FF"  # supplemental symbols
        "\U0001FA00-\U0001FA6F"  # chess
        "\U0001FA70-\U0001FAFF"  # symbols
        "\U00002702-\U000027B0"  # dingbats
        "\U000024C2-\U0001F251"  # misc
        "]+",
        flags=re.UNICODE
    )

    @classmethod
    def remove(cls, text: str) -> CleanerResult:
        """Remove all emojis from text."""
        original = text
        emojis = cls.EMOJI_PATTERN.findall(text)
        text = cls.EMOJI_PATTERN.sub('', text)

        return CleanerResult(
            original=original,
            cleaned=text,
            changes_made=[f"removed {len(emojis)} emojis"] if emojis else [],
            items_removed=len(emojis)
        )


# =============================================================================
# CITATION CLEANER
# =============================================================================

class CitationCleaner:
    """Process citation markers in LLM responses"""

    # Common citation patterns
    BRACKET_CITATION = re.compile(r'\[(\d+)\]')
    SUPERSCRIPT_CITATION = re.compile(r'[\u00B9\u00B2\u00B3\u2074-\u2079]+')
    SOURCE_PREFIX = re.compile(r'^(Source|Reference|Citation|From):\s*', re.MULTILINE | re.IGNORECASE)

    @classmethod
    def remove_citations(cls, text: str) -> CleanerResult:
        """Remove citation markers like [1], [2], etc."""
        original = text
        count = len(cls.BRACKET_CITATION.findall(text))
        text = cls.BRACKET_CITATION.sub('', text)
        text = cls.SUPERSCRIPT_CITATION.sub('', text)

        return CleanerResult(
            original=original,
            cleaned=text,
            changes_made=[f"removed {count} citation markers"] if count else [],
            items_removed=count
        )

    @classmethod
    def extract_citations(cls, text: str) -> List[str]:
        """Extract citation numbers from text."""
        return cls.BRACKET_CITATION.findall(text)


# =============================================================================
# RESPONSE FORMATTER (Pipeline)
# =============================================================================

class ResponseFormatter:
    """
    Apply multiple post-processors in sequence.

    Usage:
        formatter = ResponseFormatter()
        formatter.add(CleanerType.MARKDOWN)
        formatter.add(CleanerType.HEADERS)
        formatter.add(CleanerType.WHITESPACE)

        result = formatter.process(llm_response)
    """

    # Map cleaner types to their implementations
    CLEANERS: Dict[CleanerType, Callable[[str], CleanerResult]] = {
        CleanerType.MARKDOWN: MarkdownCleaner.clean,
        CleanerType.HEADERS: HeaderRemover().clean,
        CleanerType.WHITESPACE: WhitespaceCleaner.clean,
        CleanerType.CODE_BLOCKS: CodeBlockProcessor.remove_code_blocks,
        CleanerType.LINKS: LinkCleaner.remove_urls,
        CleanerType.LISTS: ListProcessor.remove_bullets,
        CleanerType.HTML: HTMLCleaner.clean,
        CleanerType.EMOJI: EmojiCleaner.remove,
        CleanerType.CITATIONS: CitationCleaner.remove_citations,
    }

    def __init__(self):
        self.pipeline: List[CleanerType] = []
        self.custom_cleaners: Dict[str, Callable[[str], CleanerResult]] = {}

    def add(self, cleaner_type: CleanerType) -> 'ResponseFormatter':
        """Add a cleaner to the pipeline."""
        self.pipeline.append(cleaner_type)
        return self

    def add_custom(self, name: str, cleaner: Callable[[str], CleanerResult]) -> 'ResponseFormatter':
        """Add a custom cleaner function."""
        self.custom_cleaners[name] = cleaner
        return self

    def process(self, text: str) -> Dict[str, Any]:
        """
        Run all cleaners in the pipeline.

        Returns:
            Dict with 'result', 'original', 'changes', 'cleaners_applied'
        """
        original = text
        all_changes = []
        cleaners_applied = []

        for cleaner_type in self.pipeline:
            cleaner = self.CLEANERS.get(cleaner_type)
            if cleaner:
                result = cleaner(text)
                if result.was_modified:
                    text = result.cleaned
                    all_changes.extend(result.changes_made)
                    cleaners_applied.append(cleaner_type.value)

        # Run custom cleaners
        for name, cleaner in self.custom_cleaners.items():
            result = cleaner(text)
            if result.was_modified:
                text = result.cleaned
                all_changes.extend(result.changes_made)
                cleaners_applied.append(name)

        return {
            'result': text,
            'original': original,
            'changes': all_changes,
            'cleaners_applied': cleaners_applied,
            'was_modified': original != text
        }


# =============================================================================
# PRESET FORMATTERS
# =============================================================================

def create_tts_formatter() -> ResponseFormatter:
    """Create a formatter optimized for TTS output."""
    formatter = ResponseFormatter()
    formatter.add(CleanerType.MARKDOWN)
    formatter.add(CleanerType.CODE_BLOCKS)
    formatter.add(CleanerType.LINKS)
    formatter.add(CleanerType.EMOJI)
    formatter.add(CleanerType.CITATIONS)
    formatter.add(CleanerType.WHITESPACE)
    return formatter


def create_plain_text_formatter() -> ResponseFormatter:
    """Create a formatter for plain text output."""
    formatter = ResponseFormatter()
    formatter.add(CleanerType.MARKDOWN)
    formatter.add(CleanerType.HTML)
    formatter.add(CleanerType.HEADERS)
    formatter.add(CleanerType.WHITESPACE)
    return formatter


def create_chat_formatter() -> ResponseFormatter:
    """Create a formatter for chat responses (light cleaning)."""
    formatter = ResponseFormatter()
    formatter.add(CleanerType.HEADERS)
    formatter.add(CleanerType.WHITESPACE)
    return formatter


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def clean_response(
    text: str,
    remove_markdown: bool = False,
    remove_headers: bool = True,
    remove_code_blocks: bool = False,
    remove_urls: bool = False,
    remove_emojis: bool = False,
    normalize_whitespace: bool = True
) -> str:
    """
    Clean an LLM response with configurable options.

    Args:
        text: The LLM response text
        remove_markdown: Strip all markdown formatting
        remove_headers: Remove section headers like "Summary:"
        remove_code_blocks: Remove code blocks
        remove_urls: Remove URLs
        remove_emojis: Remove emoji characters
        normalize_whitespace: Normalize newlines and spaces

    Returns:
        Cleaned text string
    """
    formatter = ResponseFormatter()

    if remove_markdown:
        formatter.add(CleanerType.MARKDOWN)
    if remove_headers:
        formatter.add(CleanerType.HEADERS)
    if remove_code_blocks:
        formatter.add(CleanerType.CODE_BLOCKS)
    if remove_urls:
        formatter.add(CleanerType.LINKS)
    if remove_emojis:
        formatter.add(CleanerType.EMOJI)
    if normalize_whitespace:
        formatter.add(CleanerType.WHITESPACE)

    result = formatter.process(text)
    return result['result']


def clean_for_tts(text: str) -> str:
    """Clean text for text-to-speech output."""
    formatter = create_tts_formatter()
    return formatter.process(text)['result']


def clean_for_display(text: str) -> str:
    """Light cleaning for display (remove headers, normalize whitespace)."""
    formatter = create_chat_formatter()
    return formatter.process(text)['result']
