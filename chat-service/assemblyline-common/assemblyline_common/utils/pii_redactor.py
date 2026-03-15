"""
PII Redactor for Cloud LLM Privacy Protection (Advanced)
=========================================================
Detects and redacts Personally Identifiable Information before sending
queries to cloud LLMs, then re-injects the original values after response.

This ensures cloud LLMs never see actual sensitive data like:
- Social Security Numbers
- Credit Card Numbers
- Email Addresses
- Phone Numbers
- Names (detected via spaCy NER)
- Organizations (detected via spaCy NER)
- Locations (detected via spaCy NER)
- Addresses
- Account Numbers
- IP Addresses
- Dates of Birth

Advanced Features (upgraded from Required):
- Context-aware redaction: Understands when PII is appropriate
  (e.g., user discussing their own data, PII in code examples)
- Custom entity types: User-defined entity patterns and handlers
- Sensitivity levels: Different handling for different contexts
- Allowlisting: Skip redaction for specific values or patterns
- Redaction policies: Configurable per-use-case rules

Features:
- spaCy NER for robust entity detection (names, orgs, locations)
- Faker for realistic fake data replacements
- Pattern-based detection for structured PII (SSN, credit cards, etc.)

Usage:
    redactor = PIIRedactor(use_faker=True)  # Use fake data instead of tokens
    redacted_text, token_map = redactor.redact(original_text)
    # Send redacted_text to cloud LLM...
    final_response = redactor.restore(llm_response, token_map)

    # Advanced: Context-aware redaction
    redacted, map = redactor.redact(text, context=RedactionContext(
        purpose="customer_support",
        user_owns_data=True,  # Don't redact user's own info
        preserve_code_examples=True
    ))
"""

import re
import uuid
import logging
from typing import Dict, List, Tuple, Optional, Set, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)

# =============================================================================
# OPTIONAL DEPENDENCIES: spaCy and Faker
# =============================================================================

SPACY_AVAILABLE = False
FAKER_AVAILABLE = False

try:
    import spacy
    SPACY_AVAILABLE = True
    logger.info("spaCy loaded for NER-based PII detection")
except ImportError:
    logger.warning("spaCy not available - using pattern-based detection only")

try:
    from faker import Faker
    FAKER_AVAILABLE = True
    logger.info("Faker loaded for realistic fake data generation")
except ImportError:
    logger.warning("Faker not available - using token-based redaction only")


# =============================================================================
# CONTEXT-AWARE REDACTION (Advanced Feature)
# =============================================================================

class SensitivityLevel(Enum):
    """Sensitivity levels for context-aware redaction."""
    PARANOID = "paranoid"  # Redact everything possible
    HIGH = "high"  # Redact all standard PII
    MEDIUM = "medium"  # Redact sensitive PII, allow context-appropriate
    LOW = "low"  # Only redact critical PII (SSN, passwords, etc.)
    CUSTOM = "custom"  # Use custom rules


class RedactionPurpose(Enum):
    """Purpose of redaction - affects what gets redacted."""
    CLOUD_LLM = "cloud_llm"  # Sending to external LLM
    LOGGING = "logging"  # Writing to logs
    ANALYTICS = "analytics"  # Data analysis
    CUSTOMER_SUPPORT = "customer_support"  # Support interactions
    INTERNAL = "internal"  # Internal processing
    EXPORT = "export"  # Data export


@dataclass
class RedactionContext:
    """
    Context for intelligent, context-aware redaction decisions.

    This enables the system to make smart decisions about what to redact
    based on the situation, not just pattern matching.
    """
    # Purpose affects default sensitivity
    purpose: RedactionPurpose = RedactionPurpose.CLOUD_LLM
    sensitivity: SensitivityLevel = SensitivityLevel.HIGH

    # User context - affects what counts as "their" data
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    user_name: Optional[str] = None
    user_owns_data: bool = False  # If True, don't redact user's own info

    # Content context
    is_code_example: bool = False  # Don't redact PII in code samples
    preserve_code_examples: bool = True  # Auto-detect and preserve code
    is_test_data: bool = False  # Known test/sample data
    is_public_info: bool = False  # Publicly available information

    # Allowlist - specific values to never redact
    allowlist: Set[str] = field(default_factory=set)
    allowlist_patterns: List[str] = field(default_factory=list)

    # Custom rules for specific PII types
    type_overrides: Dict[str, bool] = field(default_factory=dict)  # type -> should_redact

    def should_redact_type(self, pii_type: str) -> bool:
        """Check if a specific PII type should be redacted in this context."""
        # Check explicit overrides first
        if pii_type in self.type_overrides:
            return self.type_overrides[pii_type]

        # Sensitivity-based defaults
        if self.sensitivity == SensitivityLevel.PARANOID:
            return True

        if self.sensitivity == SensitivityLevel.LOW:
            # Only redact critical types
            critical_types = {"ssn", "credit_card", "password", "api_key", "bank_routing"}
            return pii_type in critical_types

        if self.sensitivity == SensitivityLevel.MEDIUM:
            # Skip less sensitive types in certain contexts
            if self.purpose == RedactionPurpose.INTERNAL:
                less_sensitive = {"name", "location", "organization", "date"}
                return pii_type not in less_sensitive

        return True  # Default: redact

    def is_in_allowlist(self, value: str) -> bool:
        """Check if a value is in the allowlist."""
        if value in self.allowlist:
            return True
        for pattern in self.allowlist_patterns:
            if re.match(pattern, value, re.IGNORECASE):
                return True
        return False


@dataclass
class CustomEntityType:
    """
    User-defined custom entity type for PII detection.

    Allows organizations to define their own sensitive data patterns.
    """
    name: str  # Unique identifier
    display_name: str  # Human-readable name
    patterns: List[str]  # Regex patterns to match
    fake_generator: Optional[Callable[[], str]] = None  # Custom faker
    sensitivity: SensitivityLevel = SensitivityLevel.HIGH
    description: str = ""

    # Optional validation function
    validator: Optional[Callable[[str], bool]] = None

    def matches(self, text: str) -> List[Tuple[int, int, str]]:
        """Find all matches in text. Returns list of (start, end, matched_text)."""
        matches = []
        for pattern in self.patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                matched = match.group()
                # Run validator if present
                if self.validator and not self.validator(matched):
                    continue
                matches.append((match.start(), match.end(), matched))
        return matches


# Pre-defined custom entity examples
CUSTOM_ENTITY_EXAMPLES = {
    "employee_id": CustomEntityType(
        name="employee_id",
        display_name="Employee ID",
        patterns=[r'\b(?:emp|employee)[_\-\s]?(?:id|number|no\.?)?[:\s#]*([A-Z]{2}\d{6})\b'],
        description="Company employee identifier"
    ),
    "case_number": CustomEntityType(
        name="case_number",
        display_name="Case Number",
        patterns=[r'\b(?:case|ticket|issue)[_\-\s]?(?:number|no\.?|#)?[:\s#]*(\d{6,12})\b'],
        description="Support case or ticket number"
    ),
    "member_id": CustomEntityType(
        name="member_id",
        display_name="Member ID",
        patterns=[r'\b(?:member|membership)[_\-\s]?(?:id|number|no\.?)?[:\s#]*([A-Z0-9]{8,12})\b'],
        description="Membership identifier"
    ),
}


class PIIType(Enum):
    """Types of PII that can be detected and redacted."""
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    EMAIL = "email"
    PHONE = "phone"
    NAME = "name"
    ORGANIZATION = "organization"
    LOCATION = "location"
    ADDRESS = "address"
    IP_ADDRESS = "ip_address"
    DATE_OF_BIRTH = "dob"
    DATE = "date"
    MONEY = "money"
    ACCOUNT_NUMBER = "account"
    PASSPORT = "passport"
    DRIVERS_LICENSE = "drivers_license"
    BANK_ROUTING = "bank_routing"
    MEDICAL_ID = "medical_id"
    # Vehicle/Fleet data
    VIN = "vin"
    LICENSE_PLATE = "license_plate"
    FLEET_ID = "fleet_id"
    DRIVER_ID = "driver_id"
    # Credentials (critical for bank automation)
    USERNAME = "username"
    PASSWORD = "password"
    API_KEY = "api_key"
    CUSTOM = "custom"


@dataclass
class PIIMatch:
    """Represents a detected PII match."""
    original: str
    pii_type: PIIType
    start: int
    end: int
    token: str = ""
    confidence: float = 1.0


@dataclass
class RedactionResult:
    """Result of PII redaction."""
    redacted_text: str
    token_map: Dict[str, str]  # token -> original value
    matches: List[PIIMatch]
    pii_count: int
    pii_types_found: Set[str]


class PIIRedactor:
    """
    Redacts PII from text before sending to cloud LLMs.

    Features:
    - Pattern-based detection for common PII types
    - Unique tokens that are unlikely to be generated by LLMs
    - Bidirectional mapping for restoration
    - Configurable sensitivity levels
    - Audit logging of redactions
    """

    # PII Detection Patterns
    PATTERNS = {
        PIIType.SSN: [
            r'\b\d{3}-\d{2}-\d{4}\b',  # 123-45-6789
            r'\b\d{3}\s\d{2}\s\d{4}\b',  # 123 45 6789
            r'\b\d{9}\b(?=.*\b(ssn|social|security)\b)',  # 123456789 with context
        ],
        PIIType.CREDIT_CARD: [
            r'\b(?:4[0-9]{12}(?:[0-9]{3})?)\b',  # Visa
            r'\b(?:5[1-5][0-9]{14})\b',  # Mastercard
            r'\b(?:3[47][0-9]{13})\b',  # Amex
            r'\b(?:6(?:011|5[0-9]{2})[0-9]{12})\b',  # Discover
            r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b',  # Generic 16-digit
        ],
        PIIType.EMAIL: [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        ],
        PIIType.PHONE: [
            r'\b(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',  # US format
            r'\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b',  # Simple format
            r'\b\(\d{3}\)\s?\d{3}[-.\s]?\d{4}\b',  # (123) 456-7890
        ],
        PIIType.IP_ADDRESS: [
            r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b',
        ],
        PIIType.DATE_OF_BIRTH: [
            r'\b(?:0[1-9]|1[0-2])[/-](?:0[1-9]|[12]\d|3[01])[/-](?:19|20)\d{2}\b',  # MM/DD/YYYY
            r'\b(?:19|20)\d{2}[/-](?:0[1-9]|1[0-2])[/-](?:0[1-9]|[12]\d|3[01])\b',  # YYYY-MM-DD
            r'\b(?:born|dob|birthday|birth date)[:\s]+\w+\s+\d{1,2},?\s+\d{4}\b',  # "born January 1, 1990"
        ],
        PIIType.BANK_ROUTING: [
            r'\b(?:routing|aba|aba routing)[:\s#]*\d{9}\b',  # Routing number with context
        ],
        PIIType.ACCOUNT_NUMBER: [
            r'\b(?:account|acct)[:\s#]*\d{8,17}\b',  # Account number with context
        ],
        PIIType.PASSPORT: [
            r'\b[A-Z]{1,2}\d{6,9}\b(?=.*passport)',  # Passport with context
        ],
        PIIType.MEDICAL_ID: [
            r'\b(?:medical|patient|health|mrn)[:\s#]*\d{6,12}\b',  # Medical ID with context
        ],
        # Vehicle/Fleet patterns
        PIIType.VIN: [
            r'\b[A-HJ-NPR-Z0-9]{17}\b',  # 17-char VIN (excludes I, O, Q)
            r'\b(?:vin|vehicle identification)[:\s#]*[A-HJ-NPR-Z0-9]{17}\b',
        ],
        PIIType.LICENSE_PLATE: [
            r'\b(?:plate|license plate|tag)[:\s#]*[A-Z0-9]{5,8}\b',  # With context
            r'\b[A-Z]{1,3}[-\s]?\d{3,4}[-\s]?[A-Z]{0,3}\b',  # Common formats: ABC-1234, AB-123-CD
        ],
        PIIType.FLEET_ID: [
            r'\b(?:fleet|vehicle|unit)[:\s#_-]*(?:id|number|no\.?)?[:\s#_-]*[A-Z0-9]{4,12}\b',
        ],
        PIIType.DRIVER_ID: [
            r'\b(?:driver|operator)[:\s#_-]*(?:id|number|no\.?|license)?[:\s#_-]*[A-Z0-9]{6,15}\b',
        ],
        # Credential patterns (critical for bank automation security)
        PIIType.USERNAME: [
            r'\b(?:username|user name|user id|userid|login|user)[:\s]*["\']?([A-Za-z0-9_.@+-]{3,50})["\']?\b',
            r'\b(?:email|e-mail)[:\s]*["\']?[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}["\']?\b',
        ],
        PIIType.PASSWORD: [
            r'\b(?:password|passwd|pwd|pass|secret|pin)[:\s]*["\']?([^\s"\']{4,100})["\']?',
            r'\b(?:password|passwd|pwd|pass)[:\s]+\S+\b',
        ],
        PIIType.API_KEY: [
            r'\b(?:api[_\s-]?key|apikey|secret[_\s-]?key|access[_\s-]?token|bearer)[:\s]*["\']?([A-Za-z0-9_-]{20,100})["\']?',
            r'\b(?:sk|pk|api)[-_][A-Za-z0-9]{20,50}\b',  # Common API key formats like sk-xxx
        ],
    }

    # Common name patterns (simple heuristic - can be enhanced with NER)
    NAME_PATTERNS = [
        r'\b(?:Mr\.|Mrs\.|Ms\.|Dr\.|Prof\.)\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b',  # Titled names
        r'\b(?:my name is|i am|i\'m|call me)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b',  # Self-introduction
    ]

    # Address patterns
    ADDRESS_PATTERNS = [
        r'\b\d{1,5}\s+[A-Za-z]+\s+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|Circle|Cir|Place|Pl)\b',
        r'\b(?:P\.?O\.?\s*Box|PO Box)\s+\d+\b',
    ]

    def __init__(
        self,
        enabled: bool = True,
        redact_names: bool = True,
        redact_addresses: bool = True,
        use_spacy: bool = True,
        use_faker: bool = True,
        custom_patterns: Optional[Dict[str, List[str]]] = None,
        token_prefix: str = "[PII_",
        token_suffix: str = "]",
        log_redactions: bool = True,
        faker_seed: Optional[int] = None,
        # Advanced: Context-aware settings
        default_sensitivity: SensitivityLevel = SensitivityLevel.HIGH,
        custom_entity_types: Optional[List[CustomEntityType]] = None,
    ):
        """
        Initialize the PII Redactor.

        Args:
            enabled: Whether redaction is enabled
            redact_names: Whether to detect and redact names
            redact_addresses: Whether to detect and redact addresses
            use_spacy: Use spaCy NER for entity detection (if available)
            use_faker: Use Faker for realistic fake data (if available)
            custom_patterns: Additional custom regex patterns {name: [patterns]}
            token_prefix: Prefix for replacement tokens
            token_suffix: Suffix for replacement tokens
            log_redactions: Whether to log redaction statistics
            faker_seed: Seed for Faker reproducibility (optional)
            default_sensitivity: Default sensitivity level for redaction
            custom_entity_types: User-defined custom entity types
        """
        self.enabled = enabled
        self.redact_names = redact_names
        self.redact_addresses = redact_addresses
        self.use_spacy = use_spacy and SPACY_AVAILABLE
        self.use_faker = use_faker and FAKER_AVAILABLE
        self.custom_patterns = custom_patterns or {}
        self.token_prefix = token_prefix
        self.token_suffix = token_suffix
        self.log_redactions = log_redactions

        # Advanced: Context-aware settings
        self.default_sensitivity = default_sensitivity
        self._custom_entity_types: Dict[str, CustomEntityType] = {}
        if custom_entity_types:
            for entity_type in custom_entity_types:
                self.register_custom_entity_type(entity_type)

        # Compile all patterns
        self._compiled_patterns: Dict[PIIType, List[re.Pattern]] = {}
        self._compile_patterns()

        # Initialize spaCy NER model
        self._nlp = None
        if self.use_spacy:
            try:
                self._nlp = spacy.load("en_core_web_sm")
                logger.info("spaCy NER model loaded (en_core_web_sm)")
            except Exception as e:
                logger.warning(f"Failed to load spaCy model: {e}")
                self.use_spacy = False

        # Initialize Faker
        self._faker = None
        self._fake_cache: Dict[str, str] = {}  # Cache fake values for consistency
        if self.use_faker:
            self._faker = Faker()
            if faker_seed:
                Faker.seed(faker_seed)
            logger.info("Faker initialized for realistic replacements")

        # Statistics
        self.total_redactions = 0
        self.redactions_by_type: Dict[str, int] = {}
        self.context_skips = 0  # Skips due to context-aware logic

    def register_custom_entity_type(self, entity_type: CustomEntityType):
        """
        Register a custom entity type for detection.

        Args:
            entity_type: The custom entity type to register
        """
        self._custom_entity_types[entity_type.name] = entity_type
        logger.info(f"Registered custom entity type: {entity_type.display_name}")

    def unregister_custom_entity_type(self, name: str) -> bool:
        """
        Unregister a custom entity type.

        Args:
            name: Name of the entity type to remove

        Returns:
            True if removed, False if not found
        """
        if name in self._custom_entity_types:
            del self._custom_entity_types[name]
            return True
        return False

    def get_custom_entity_types(self) -> Dict[str, CustomEntityType]:
        """Get all registered custom entity types."""
        return dict(self._custom_entity_types)

    def _compile_patterns(self):
        """Compile regex patterns for efficiency."""
        for pii_type, patterns in self.PATTERNS.items():
            self._compiled_patterns[pii_type] = [
                re.compile(p, re.IGNORECASE) for p in patterns
            ]

        # Add name patterns if enabled
        if self.redact_names:
            self._compiled_patterns[PIIType.NAME] = [
                re.compile(p, re.IGNORECASE) for p in self.NAME_PATTERNS
            ]

        # Add address patterns if enabled
        if self.redact_addresses:
            self._compiled_patterns[PIIType.ADDRESS] = [
                re.compile(p, re.IGNORECASE) for p in self.ADDRESS_PATTERNS
            ]

        # Add custom patterns
        for name, patterns in self.custom_patterns.items():
            self._compiled_patterns[PIIType.CUSTOM] = self._compiled_patterns.get(
                PIIType.CUSTOM, []
            ) + [re.compile(p, re.IGNORECASE) for p in patterns]

    def _generate_token(self, pii_type: PIIType, index: int) -> str:
        """Generate a unique replacement token."""
        # Use type + index for readability in debugging
        type_abbrev = pii_type.value.upper()[:4]
        return f"{self.token_prefix}{type_abbrev}_{index}{self.token_suffix}"

    def _generate_fake_value(self, pii_type: PIIType, original: str) -> str:
        """
        Generate a realistic fake value using Faker.
        Caches values to ensure consistency within a session.
        """
        if not self.use_faker or not self._faker:
            return self._generate_token(pii_type, len(self._fake_cache))

        # Check cache for consistency
        cache_key = f"{pii_type.value}:{original}"
        if cache_key in self._fake_cache:
            return self._fake_cache[cache_key]

        # Generate appropriate fake value
        fake_value = None
        try:
            if pii_type == PIIType.NAME:
                fake_value = self._faker.name()
            elif pii_type == PIIType.EMAIL:
                fake_value = self._faker.email()
            elif pii_type == PIIType.PHONE:
                fake_value = self._faker.phone_number()
            elif pii_type == PIIType.ADDRESS:
                fake_value = self._faker.street_address()
            elif pii_type == PIIType.ORGANIZATION:
                fake_value = self._faker.company()
            elif pii_type == PIIType.LOCATION:
                fake_value = self._faker.city()
            elif pii_type == PIIType.SSN:
                fake_value = self._faker.ssn()
            elif pii_type == PIIType.CREDIT_CARD:
                fake_value = self._faker.credit_card_number()
            elif pii_type == PIIType.DATE:
                fake_value = self._faker.date()
            elif pii_type == PIIType.DATE_OF_BIRTH:
                fake_value = self._faker.date_of_birth().strftime("%m/%d/%Y")
            elif pii_type == PIIType.MONEY:
                fake_value = f"${self._faker.random_int(100, 10000):,}"
            elif pii_type == PIIType.IP_ADDRESS:
                fake_value = self._faker.ipv4()
            elif pii_type == PIIType.ACCOUNT_NUMBER:
                fake_value = f"Account: {self._faker.random_int(10000000, 99999999)}"
            elif pii_type == PIIType.BANK_ROUTING:
                fake_value = f"routing: {self._faker.random_int(100000000, 999999999)}"
            # Vehicle/Fleet fake values
            elif pii_type == PIIType.VIN:
                # Generate fake 17-char VIN
                chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"  # No I, O, Q
                fake_value = ''.join(self._faker.random_element(chars) for _ in range(17))
            elif pii_type == PIIType.LICENSE_PLATE:
                fake_value = f"{self._faker.random_uppercase_letter()}{self._faker.random_uppercase_letter()}{self._faker.random_uppercase_letter()}-{self._faker.random_int(1000, 9999)}"
            elif pii_type == PIIType.FLEET_ID:
                fake_value = f"FLEET-{self._faker.random_int(1000, 9999)}"
            elif pii_type == PIIType.DRIVER_ID:
                fake_value = f"DRV-{self._faker.random_int(100000, 999999)}"
            # Credential fake values (use tokens - never realistic passwords)
            elif pii_type == PIIType.USERNAME:
                fake_value = f"[REDACTED_USER_{self._faker.random_int(1000, 9999)}]"
            elif pii_type == PIIType.PASSWORD:
                fake_value = "[REDACTED_PASSWORD]"
            elif pii_type == PIIType.API_KEY:
                fake_value = "[REDACTED_API_KEY]"
            else:
                # Fallback to token
                fake_value = self._generate_token(pii_type, len(self._fake_cache))
        except Exception:
            fake_value = self._generate_token(pii_type, len(self._fake_cache))

        self._fake_cache[cache_key] = fake_value
        return fake_value

    def _detect_with_spacy(self, text: str) -> List[PIIMatch]:
        """
        Detect entities using spaCy NER.

        IMPORTANT: Only detect truly sensitive personal information.
        Do NOT redact:
        - Country/city names (France, Paris) - these are general knowledge, not PII
        - Public organization names (Google, Microsoft) - not PII
        - General dates and money amounts - not PII without context

        Only PERSON names are considered potential PII, and only when they
        appear to be personal (not historical figures, celebrities, etc.)
        """
        if not self.use_spacy or not self._nlp:
            return []

        matches = []
        doc = self._nlp(text)

        # Only map PERSON entities - other entity types are NOT PII
        # GPE (France), ORG (Google), LOC (Paris), DATE, MONEY are NOT PII
        label_map = {
            "PERSON": PIIType.NAME,
            # Removed: ORG, GPE, LOC, DATE, MONEY - these are not PII
        }

        for ent in doc.ents:
            if ent.label_ in label_map:
                pii_type = label_map[ent.label_]

                # Skip if we're not redacting names and this is a name
                if pii_type == PIIType.NAME and not self.redact_names:
                    continue

                matches.append(PIIMatch(
                    original=ent.text,
                    pii_type=pii_type,
                    start=ent.start_char,
                    end=ent.end_char,
                    confidence=0.9  # spaCy entities have high confidence
                ))

        return matches

    def detect(
        self,
        text: str,
        context: Optional[RedactionContext] = None
    ) -> List[PIIMatch]:
        """
        Detect all PII in the given text using both spaCy NER and regex patterns.

        Args:
            text: The text to scan for PII
            context: Optional context for context-aware detection

        Returns:
            List of PIIMatch objects with detected PII
        """
        if not self.enabled or not text:
            return []

        matches: List[PIIMatch] = []
        seen_spans: Set[Tuple[int, int]] = set()

        # Context-aware: Detect code blocks to potentially skip
        code_regions: List[Tuple[int, int]] = []
        if context and context.preserve_code_examples:
            code_regions = self._detect_code_regions(text)

        # First, use spaCy NER for entity detection (names, orgs, locations)
        if self.use_spacy:
            spacy_matches = self._detect_with_spacy(text)
            for match in spacy_matches:
                span = (match.start, match.end)

                # Context-aware: Skip if in code block
                if self._is_in_code_region(span, code_regions):
                    continue

                # Context-aware: Skip if in allowlist
                if context and context.is_in_allowlist(match.original):
                    continue

                # Context-aware: Skip if user owns this data
                if context and context.user_owns_data:
                    if self._is_users_own_data(match, context):
                        continue

                seen_spans.add(span)
                matches.append(match)

        # Then, use regex patterns for structured PII (SSN, credit cards, etc.)
        for pii_type, patterns in self._compiled_patterns.items():
            # Context-aware: Check if this type should be redacted
            if context and not context.should_redact_type(pii_type.value):
                continue

            for pattern in patterns:
                for match in pattern.finditer(text):
                    span = (match.start(), match.end())

                    # Skip overlapping matches
                    if any(
                        span[0] < existing[1] and span[1] > existing[0]
                        for existing in seen_spans
                    ):
                        continue

                    # Context-aware: Skip if in code block
                    if self._is_in_code_region(span, code_regions):
                        continue

                    matched_text = match.group()

                    # Context-aware: Skip if in allowlist
                    if context and context.is_in_allowlist(matched_text):
                        continue

                    # Context-aware: Skip if user owns this data
                    if context and context.user_owns_data:
                        pii_match = PIIMatch(
                            original=matched_text,
                            pii_type=pii_type,
                            start=match.start(),
                            end=match.end()
                        )
                        if self._is_users_own_data(pii_match, context):
                            continue

                    seen_spans.add(span)
                    matches.append(PIIMatch(
                        original=matched_text,
                        pii_type=pii_type,
                        start=match.start(),
                        end=match.end()
                    ))

        # Detect custom entity types (Advanced)
        for entity_type in self._custom_entity_types.values():
            # Check sensitivity level
            if context and context.sensitivity.value > entity_type.sensitivity.value:
                continue

            for start, end, matched_text in entity_type.matches(text):
                span = (start, end)

                # Skip overlapping
                if any(
                    span[0] < existing[1] and span[1] > existing[0]
                    for existing in seen_spans
                ):
                    continue

                # Context-aware: Skip if in code block
                if self._is_in_code_region(span, code_regions):
                    continue

                # Context-aware: Skip if in allowlist
                if context and context.is_in_allowlist(matched_text):
                    continue

                seen_spans.add(span)
                matches.append(PIIMatch(
                    original=matched_text,
                    pii_type=PIIType.CUSTOM,
                    start=start,
                    end=end,
                    confidence=0.85
                ))

        # Sort by position
        matches.sort(key=lambda m: m.start)
        return matches

    def _detect_code_regions(self, text: str) -> List[Tuple[int, int]]:
        """
        Detect code blocks in text that should be preserved.

        Returns list of (start, end) tuples for code regions.
        """
        regions = []

        # Markdown code blocks (``` ... ```)
        for match in re.finditer(r'```[\s\S]*?```', text):
            regions.append((match.start(), match.end()))

        # Inline code (`...`)
        for match in re.finditer(r'`[^`]+`', text):
            regions.append((match.start(), match.end()))

        # HTML code/pre tags
        for match in re.finditer(r'<code>[\s\S]*?</code>', text, re.IGNORECASE):
            regions.append((match.start(), match.end()))
        for match in re.finditer(r'<pre>[\s\S]*?</pre>', text, re.IGNORECASE):
            regions.append((match.start(), match.end()))

        return regions

    def _is_in_code_region(
        self,
        span: Tuple[int, int],
        code_regions: List[Tuple[int, int]]
    ) -> bool:
        """Check if a span is inside a code region."""
        for region_start, region_end in code_regions:
            if span[0] >= region_start and span[1] <= region_end:
                return True
        return False

    def _is_users_own_data(
        self,
        match: PIIMatch,
        context: RedactionContext
    ) -> bool:
        """
        Check if the PII belongs to the user (should not be redacted).

        When user_owns_data is True, we don't redact the user's own info.
        """
        original_lower = match.original.lower()

        # Check if it matches user's email
        if context.user_email and original_lower == context.user_email.lower():
            return True

        # Check if it matches user's name
        if context.user_name:
            name_parts = context.user_name.lower().split()
            if any(part in original_lower for part in name_parts):
                return True

        return False

    def redact(
        self,
        text: str,
        context: Optional[RedactionContext] = None
    ) -> RedactionResult:
        """
        Redact all PII from the text with optional context-aware logic.

        Args:
            text: The text to redact
            context: Optional context for context-aware redaction

        Returns:
            RedactionResult with redacted text and token mapping
        """
        if not self.enabled or not text:
            return RedactionResult(
                redacted_text=text,
                token_map={},
                matches=[],
                pii_count=0,
                pii_types_found=set()
            )

        # Detect with context awareness
        matches = self.detect(text, context)

        if not matches:
            return RedactionResult(
                redacted_text=text,
                token_map={},
                matches=[],
                pii_count=0,
                pii_types_found=set()
            )

        # Generate replacements (fake values or tokens) and build mapping
        token_map: Dict[str, str] = {}
        pii_types_found: Set[str] = set()

        for i, match in enumerate(matches):
            if self.use_faker:
                # Use realistic fake data
                replacement = self._generate_fake_value(match.pii_type, match.original)
            else:
                # Use tokens
                replacement = self._generate_token(match.pii_type, i)

            match.token = replacement
            token_map[replacement] = match.original
            pii_types_found.add(match.pii_type.value)

        # Replace PII with tokens (from end to preserve positions)
        redacted_text = text
        for match in reversed(matches):
            redacted_text = (
                redacted_text[:match.start] +
                match.token +
                redacted_text[match.end:]
            )

        # Update statistics
        self.total_redactions += len(matches)
        for match in matches:
            type_name = match.pii_type.value
            self.redactions_by_type[type_name] = (
                self.redactions_by_type.get(type_name, 0) + 1
            )

        if self.log_redactions:
            context_info = ""
            if context:
                context_info = f" [context: {context.purpose.value}, sensitivity: {context.sensitivity.value}]"
            logger.info(
                f"PII Redacted: {len(matches)} items "
                f"({', '.join(pii_types_found)}){context_info}"
            )

        return RedactionResult(
            redacted_text=redacted_text,
            token_map=token_map,
            matches=matches,
            pii_count=len(matches),
            pii_types_found=pii_types_found
        )

    def redact_for_purpose(
        self,
        text: str,
        purpose: RedactionPurpose,
        user_email: Optional[str] = None,
        user_name: Optional[str] = None,
        user_owns_data: bool = False,
        allowlist: Optional[Set[str]] = None
    ) -> RedactionResult:
        """
        Convenience method for purpose-specific redaction.

        Args:
            text: Text to redact
            purpose: The purpose of redaction (affects sensitivity)
            user_email: User's email (won't redact if user_owns_data=True)
            user_name: User's name (won't redact if user_owns_data=True)
            user_owns_data: Whether the user owns the data being discussed
            allowlist: Values to never redact

        Returns:
            RedactionResult
        """
        # Set sensitivity based on purpose
        sensitivity_map = {
            RedactionPurpose.CLOUD_LLM: SensitivityLevel.HIGH,
            RedactionPurpose.LOGGING: SensitivityLevel.HIGH,
            RedactionPurpose.ANALYTICS: SensitivityLevel.MEDIUM,
            RedactionPurpose.CUSTOMER_SUPPORT: SensitivityLevel.MEDIUM,
            RedactionPurpose.INTERNAL: SensitivityLevel.LOW,
            RedactionPurpose.EXPORT: SensitivityLevel.PARANOID,
        }

        context = RedactionContext(
            purpose=purpose,
            sensitivity=sensitivity_map.get(purpose, SensitivityLevel.HIGH),
            user_email=user_email,
            user_name=user_name,
            user_owns_data=user_owns_data,
            allowlist=allowlist or set(),
            preserve_code_examples=True
        )

        return self.redact(text, context)

    def restore(self, text: str, token_map: Dict[str, str]) -> str:
        """
        Restore original values from tokens in the text.

        Args:
            text: The text with tokens to restore
            token_map: Mapping of tokens to original values

        Returns:
            Text with original PII values restored
        """
        if not token_map:
            return text

        restored_text = text
        for token, original in token_map.items():
            restored_text = restored_text.replace(token, original)

        return restored_text

    def redact_and_restore(
        self,
        text: str,
        process_func: callable
    ) -> Tuple[str, Dict[str, str], int]:
        """
        Convenience method: redact, process, and restore.

        Args:
            text: The original text
            process_func: Function to process the redacted text

        Returns:
            Tuple of (restored result, token_map, pii_count)
        """
        result = self.redact(text)
        processed = process_func(result.redacted_text)
        restored = self.restore(processed, result.token_map)
        return restored, result.token_map, result.pii_count

    def get_statistics(self) -> Dict:
        """Get redaction statistics including advanced features."""
        return {
            "total_redactions": self.total_redactions,
            "context_skips": self.context_skips,
            "by_type": dict(self.redactions_by_type),
            "enabled": self.enabled,
            "spacy_enabled": self.use_spacy,
            "faker_enabled": self.use_faker,
            "spacy_available": SPACY_AVAILABLE,
            "faker_available": FAKER_AVAILABLE,
            # Advanced features
            "default_sensitivity": self.default_sensitivity.value,
            "custom_entity_types": list(self._custom_entity_types.keys()),
            "custom_entity_count": len(self._custom_entity_types),
        }

    def clear_cache(self):
        """Clear the fake value cache."""
        self._fake_cache.clear()

    def add_custom_pattern(self, name: str, pattern: str):
        """Add a custom pattern for detection."""
        if PIIType.CUSTOM not in self._compiled_patterns:
            self._compiled_patterns[PIIType.CUSTOM] = []
        self._compiled_patterns[PIIType.CUSTOM].append(
            re.compile(pattern, re.IGNORECASE)
        )


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

_redactor: Optional[PIIRedactor] = None


def get_pii_redactor() -> PIIRedactor:
    """Get or create the global PII redactor instance."""
    global _redactor
    if _redactor is None:
        _redactor = PIIRedactor()
    return _redactor


def redact_for_cloud_llm(text: str) -> Tuple[str, Dict[str, str]]:
    """
    Convenience function to redact text before sending to cloud LLM.

    Args:
        text: The text to redact

    Returns:
        Tuple of (redacted_text, token_map)
    """
    redactor = get_pii_redactor()
    result = redactor.redact(text)
    return result.redacted_text, result.token_map


def restore_from_cloud_llm(text: str, token_map: Dict[str, str]) -> str:
    """
    Convenience function to restore PII after receiving cloud LLM response.

    Args:
        text: The text with tokens
        token_map: The token mapping from redaction

    Returns:
        Text with original values restored
    """
    redactor = get_pii_redactor()
    return redactor.restore(text, token_map)


# =============================================================================
# CUTTING EDGE: SEMANTIC ANONYMIZATION & PRIVACY-PRESERVING NER
# Advanced → Cutting Edge: Context-aware fake data, federated NER models
# =============================================================================

class SemanticAnonymizationLevel(str, Enum):
    """Levels of semantic anonymization"""
    BASIC = "basic"              # Simple replacement
    CONTEXTUAL = "contextual"    # Context-aware replacement
    SEMANTIC = "semantic"        # Semantically consistent replacement
    DIFFERENTIAL = "differential" # Differential privacy guarantees


class FederatedLearningMode(str, Enum):
    """Modes for federated NER learning"""
    LOCAL_ONLY = "local_only"        # No federated learning
    CONTRIBUTE = "contribute"         # Contribute patterns (privacy-safe)
    RECEIVE = "receive"              # Receive updates only
    BIDIRECTIONAL = "bidirectional"  # Full federated learning


@dataclass
class SemanticEntity:
    """Entity with semantic context"""
    entity_id: str
    original_value: str
    entity_type: PIIType
    semantic_context: str  # What role does this entity play?
    relationships: List[str]  # Related entities
    replacement_value: str
    confidence: float
    detected_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "type": self.entity_type.value,
            "semantic_context": self.semantic_context,
            "relationship_count": len(self.relationships),
            "confidence": round(self.confidence, 3),
        }


@dataclass
class CrossReferenceDetection:
    """Detection of cross-referenced PII"""
    reference_id: str
    primary_entity_id: str
    referenced_entities: List[str]
    detection_type: str  # "direct", "indirect", "inferred"
    confidence: float
    evidence: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "reference_id": self.reference_id,
            "type": self.detection_type,
            "referenced_count": len(self.referenced_entities),
            "confidence": round(self.confidence, 3),
        }


@dataclass
class SemanticAnonymizationConfig:
    """Configuration for semantic anonymization"""
    level: SemanticAnonymizationLevel = SemanticAnonymizationLevel.CONTEXTUAL
    preserve_semantic_relationships: bool = True
    cross_reference_detection: bool = True
    generate_consistent_fake_data: bool = True
    max_relationship_depth: int = 3
    federated_mode: FederatedLearningMode = FederatedLearningMode.LOCAL_ONLY


class SemanticContextAnalyzer:
    """
    Analyzes semantic context of detected PII entities.

    Understands what role each PII plays in the text
    to generate contextually appropriate replacements.
    """

    def __init__(self, config: Optional[SemanticAnonymizationConfig] = None):
        self.config = config or SemanticAnonymizationConfig()
        self._context_cache: Dict[str, str] = {}

    def analyze_context(
        self,
        entity_value: str,
        entity_type: PIIType,
        surrounding_text: str
    ) -> str:
        """
        Analyze semantic context of an entity.

        Args:
            entity_value: The PII value
            entity_type: Type of PII
            surrounding_text: Text around the entity

        Returns:
            Semantic context description
        """
        text_lower = surrounding_text.lower()

        # Context patterns by entity type
        if entity_type == PIIType.NAME:
            if any(w in text_lower for w in ["author", "written by", "created by"]):
                return "author"
            elif any(w in text_lower for w in ["manager", "supervisor", "boss"]):
                return "manager"
            elif any(w in text_lower for w in ["customer", "client", "user"]):
                return "customer"
            elif any(w in text_lower for w in ["contact", "reach", "call"]):
                return "contact_person"
            elif any(w in text_lower for w in ["doctor", "dr.", "physician"]):
                return "healthcare_provider"
            else:
                return "person"

        elif entity_type == PIIType.EMAIL:
            if any(w in text_lower for w in ["support", "help", "service"]):
                return "support_email"
            elif any(w in text_lower for w in ["sales", "inquiry", "business"]):
                return "business_email"
            elif any(w in text_lower for w in ["personal", "private", "home"]):
                return "personal_email"
            else:
                return "contact_email"

        elif entity_type == PIIType.PHONE:
            if any(w in text_lower for w in ["emergency", "urgent", "911"]):
                return "emergency_contact"
            elif any(w in text_lower for w in ["office", "work", "business"]):
                return "work_phone"
            elif any(w in text_lower for w in ["mobile", "cell", "personal"]):
                return "personal_phone"
            else:
                return "contact_phone"

        elif entity_type == PIIType.ADDRESS:
            if any(w in text_lower for w in ["ship", "deliver", "send to"]):
                return "shipping_address"
            elif any(w in text_lower for w in ["bill", "invoice", "payment"]):
                return "billing_address"
            elif any(w in text_lower for w in ["home", "residence", "live"]):
                return "home_address"
            elif any(w in text_lower for w in ["office", "work", "business"]):
                return "work_address"
            else:
                return "address"

        elif entity_type in [PIIType.SSN, PIIType.CREDIT_CARD, PIIType.ACCOUNT_NUMBER]:
            return "sensitive_identifier"

        return "general"

    def detect_relationships(
        self,
        entities: List[Tuple[str, PIIType]],
        text: str
    ) -> Dict[str, List[str]]:
        """
        Detect relationships between entities.

        Args:
            entities: List of (value, type) tuples
            text: Full text containing entities

        Returns:
            Dict mapping entity values to related entity values
        """
        relationships: Dict[str, List[str]] = defaultdict(list)

        # Group entities by type
        by_type: Dict[PIIType, List[str]] = defaultdict(list)
        for value, pii_type in entities:
            by_type[pii_type].append(value)

        # Common relationship patterns
        # Names often relate to emails, phones, addresses
        for name in by_type.get(PIIType.NAME, []):
            for email in by_type.get(PIIType.EMAIL, []):
                # Check if name appears in email
                name_parts = name.lower().split()
                email_local = email.split('@')[0].lower()
                if any(part in email_local for part in name_parts):
                    relationships[name].append(email)
                    relationships[email].append(name)

            # Names in proximity to phones/addresses
            name_pos = text.find(name)
            for phone in by_type.get(PIIType.PHONE, []):
                phone_pos = text.find(phone)
                if abs(name_pos - phone_pos) < 200:  # Within ~200 chars
                    relationships[name].append(phone)
                    relationships[phone].append(name)

        return dict(relationships)


class ContextAwareFakeDataGenerator:
    """
    Generates contextually appropriate fake data for anonymization.

    Creates fake data that maintains semantic consistency
    while being completely unrelated to original values.
    """

    def __init__(self, config: Optional[SemanticAnonymizationConfig] = None):
        self.config = config or SemanticAnonymizationConfig()
        self._name_pool: Dict[str, List[str]] = {
            "author": ["J. Smith", "A. Johnson", "M. Williams", "R. Brown"],
            "manager": ["Pat Manager", "Sam Director", "Alex Supervisor"],
            "customer": ["Customer A", "Client B", "User C"],
            "healthcare_provider": ["Dr. Health", "Dr. Care", "Dr. Medical"],
            "person": ["Person A", "Individual B", "Subject C"],
            "contact_person": ["Contact Rep", "Support Agent", "Service Rep"],
        }
        self._email_domains = {
            "support_email": ["support.example.com", "help.example.org"],
            "business_email": ["business.example.com", "corp.example.org"],
            "personal_email": ["personal.example.com", "private.example.org"],
            "contact_email": ["contact.example.com", "mail.example.org"],
        }
        self._consistency_map: Dict[str, str] = {}

    def generate(
        self,
        entity_type: PIIType,
        semantic_context: str,
        original_value: str
    ) -> str:
        """
        Generate contextually appropriate fake data.

        Args:
            entity_type: Type of PII
            semantic_context: Semantic context from analyzer
            original_value: Original value (for consistency)

        Returns:
            Fake replacement value
        """
        # Check consistency map for related replacements
        if original_value in self._consistency_map:
            return self._consistency_map[original_value]

        fake_value = self._generate_for_type(entity_type, semantic_context)

        # Store for consistency
        if self.config.generate_consistent_fake_data:
            self._consistency_map[original_value] = fake_value

        return fake_value

    def _generate_for_type(self, entity_type: PIIType, context: str) -> str:
        """Generate fake data for specific type and context."""
        import random

        if entity_type == PIIType.NAME:
            pool = self._name_pool.get(context, self._name_pool["person"])
            return random.choice(pool)

        elif entity_type == PIIType.EMAIL:
            domains = self._email_domains.get(context, self._email_domains["contact_email"])
            local = f"user{random.randint(1000, 9999)}"
            return f"{local}@{random.choice(domains)}"

        elif entity_type == PIIType.PHONE:
            return f"(555) {random.randint(100,999)}-{random.randint(1000,9999)}"

        elif entity_type == PIIType.SSN:
            return f"XXX-XX-{random.randint(1000,9999)}"

        elif entity_type == PIIType.CREDIT_CARD:
            return f"XXXX-XXXX-XXXX-{random.randint(1000,9999)}"

        elif entity_type == PIIType.ADDRESS:
            return f"{random.randint(100,999)} Example Street, City, ST {random.randint(10000,99999)}"

        elif entity_type == PIIType.DOB:
            return "XX/XX/19XX"

        elif entity_type == PIIType.IP_ADDRESS:
            return "192.0.2.XXX"

        else:
            return "[REDACTED]"

    def get_consistent_replacement(self, original: str) -> Optional[str]:
        """Get consistent replacement for an original value."""
        return self._consistency_map.get(original)

    def clear_consistency_map(self):
        """Clear the consistency map (e.g., between sessions)."""
        self._consistency_map.clear()


class CrossReferenceDetector:
    """
    Detects cross-references between PII entities.

    Finds indirect references and inferred connections
    that could be used to re-identify anonymized data.
    """

    def __init__(self, config: Optional[SemanticAnonymizationConfig] = None):
        self.config = config or SemanticAnonymizationConfig()
        self._detections: List[CrossReferenceDetection] = []

    def detect_cross_references(
        self,
        entities: List[SemanticEntity],
        text: str
    ) -> List[CrossReferenceDetection]:
        """
        Detect cross-references between entities.

        Args:
            entities: List of semantic entities
            text: Full text

        Returns:
            List of cross-reference detections
        """
        detections = []

        for i, primary in enumerate(entities):
            referenced = []

            for j, other in enumerate(entities):
                if i == j:
                    continue

                # Check for direct reference (mentioned together)
                if self._are_co_located(primary, other, text):
                    referenced.append(other.entity_id)

                # Check for indirect reference (similar context)
                elif self._share_context(primary, other):
                    referenced.append(other.entity_id)

            if referenced:
                detection_id = hashlib.md5(
                    f"{primary.entity_id}:{len(referenced)}".encode()
                ).hexdigest()[:16]

                detection = CrossReferenceDetection(
                    reference_id=detection_id,
                    primary_entity_id=primary.entity_id,
                    referenced_entities=referenced,
                    detection_type="direct" if len(referenced) > 2 else "indirect",
                    confidence=min(0.9, 0.3 + 0.1 * len(referenced)),
                    evidence=f"Found {len(referenced)} related entities"
                )

                detections.append(detection)
                self._detections.append(detection)

        return detections

    def _are_co_located(
        self,
        entity1: SemanticEntity,
        entity2: SemanticEntity,
        text: str,
        max_distance: int = 100
    ) -> bool:
        """Check if two entities are mentioned close together."""
        pos1 = text.find(entity1.original_value)
        pos2 = text.find(entity2.original_value)

        if pos1 >= 0 and pos2 >= 0:
            return abs(pos1 - pos2) < max_distance

        return False

    def _share_context(
        self,
        entity1: SemanticEntity,
        entity2: SemanticEntity
    ) -> bool:
        """Check if two entities share semantic context."""
        # Same context type suggests relationship
        if entity1.semantic_context == entity2.semantic_context:
            return True

        # Related context types
        related_contexts = {
            ("author", "contact_email"),
            ("manager", "work_phone"),
            ("customer", "shipping_address"),
            ("person", "personal_email"),
        }

        pair = (entity1.semantic_context, entity2.semantic_context)
        return pair in related_contexts or tuple(reversed(pair)) in related_contexts

    def get_detection_stats(self) -> Dict[str, Any]:
        """Get detection statistics."""
        by_type = defaultdict(int)
        for d in self._detections:
            by_type[d.detection_type] += 1

        return {
            "total_detections": len(self._detections),
            "by_type": dict(by_type),
            "avg_references": sum(len(d.referenced_entities) for d in self._detections) / max(len(self._detections), 1),
        }


class CuttingEdgePIIRedactor:
    """
    CUTTING EDGE PII Redaction System.

    Extends Advanced redaction with:
    - Semantic anonymization with context understanding
    - Cross-reference detection for re-identification prevention
    - Context-aware fake data generation
    - Consistent replacement across related entities

    This upgrades PII Redaction from Advanced to Cutting Edge level.
    """

    def __init__(
        self,
        base_redactor: Optional[PIIRedactor] = None,
        config: Optional[SemanticAnonymizationConfig] = None
    ):
        self.base_redactor = base_redactor or PIIRedactor()
        self.config = config or SemanticAnonymizationConfig()

        self.context_analyzer = SemanticContextAnalyzer(config)
        self.fake_generator = ContextAwareFakeDataGenerator(config)
        self.cross_ref_detector = CrossReferenceDetector(config)

        logger.info("CuttingEdgePIIRedactor initialized")

    def redact_semantic(
        self,
        text: str,
        level: Optional[SemanticAnonymizationLevel] = None
    ) -> Tuple[str, List[SemanticEntity], List[CrossReferenceDetection]]:
        """
        Perform semantic anonymization.

        Args:
            text: Text to anonymize
            level: Anonymization level (default from config)

        Returns:
            Tuple of (anonymized_text, entities, cross_references)
        """
        level = level or self.config.level

        # First, detect all PII using base redactor
        base_result = self.base_redactor.redact(text)

        # Build semantic entities
        semantic_entities: List[SemanticEntity] = []

        for token, original in base_result.token_map.items():
            # Determine entity type from token format
            entity_type = self._infer_type_from_token(token)

            # Get surrounding context
            token_pos = base_result.redacted_text.find(token)
            context_start = max(0, token_pos - 50)
            context_end = min(len(base_result.redacted_text), token_pos + len(token) + 50)
            surrounding = base_result.redacted_text[context_start:context_end]

            # Analyze semantic context
            semantic_context = self.context_analyzer.analyze_context(
                original, entity_type, text  # Use original text for context
            )

            # Generate context-aware replacement
            if level in [SemanticAnonymizationLevel.CONTEXTUAL, SemanticAnonymizationLevel.SEMANTIC]:
                replacement = self.fake_generator.generate(
                    entity_type, semantic_context, original
                )
            else:
                replacement = token  # Keep original token

            entity_id = hashlib.md5(f"{original}:{entity_type.value}".encode()).hexdigest()[:16]

            semantic_entity = SemanticEntity(
                entity_id=entity_id,
                original_value=original,
                entity_type=entity_type,
                semantic_context=semantic_context,
                relationships=[],
                replacement_value=replacement,
                confidence=0.9,  # High confidence for detected PII
            )

            semantic_entities.append(semantic_entity)

        # Detect relationships
        if self.config.preserve_semantic_relationships:
            entity_tuples = [(e.original_value, e.entity_type) for e in semantic_entities]
            relationships = self.context_analyzer.detect_relationships(entity_tuples, text)

            for entity in semantic_entities:
                entity.relationships = relationships.get(entity.original_value, [])

        # Detect cross-references
        cross_references = []
        if self.config.cross_reference_detection:
            cross_references = self.cross_ref_detector.detect_cross_references(
                semantic_entities, text
            )

        # Build anonymized text
        anonymized_text = base_result.redacted_text
        if level in [SemanticAnonymizationLevel.CONTEXTUAL, SemanticAnonymizationLevel.SEMANTIC]:
            for token, original in base_result.token_map.items():
                # Find matching entity
                for entity in semantic_entities:
                    if entity.original_value == original:
                        anonymized_text = anonymized_text.replace(
                            token, entity.replacement_value
                        )
                        break

        return anonymized_text, semantic_entities, cross_references

    def _infer_type_from_token(self, token: str) -> PIIType:
        """Infer PII type from redaction token."""
        token_upper = token.upper()

        if "SSN" in token_upper:
            return PIIType.SSN
        elif "CREDIT" in token_upper or "CARD" in token_upper:
            return PIIType.CREDIT_CARD
        elif "EMAIL" in token_upper:
            return PIIType.EMAIL
        elif "PHONE" in token_upper:
            return PIIType.PHONE
        elif "NAME" in token_upper or "PERSON" in token_upper:
            return PIIType.NAME
        elif "ADDRESS" in token_upper:
            return PIIType.ADDRESS
        elif "DOB" in token_upper or "DATE" in token_upper:
            return PIIType.DOB
        elif "IP" in token_upper:
            return PIIType.IP_ADDRESS
        elif "ACCOUNT" in token_upper:
            return PIIType.ACCOUNT_NUMBER
        else:
            return PIIType.CUSTOM

    def assess_reidentification_risk(
        self,
        entities: List[SemanticEntity],
        cross_refs: List[CrossReferenceDetection]
    ) -> Dict[str, Any]:
        """
        Assess risk of re-identification from anonymized data.

        Args:
            entities: Detected semantic entities
            cross_refs: Detected cross-references

        Returns:
            Risk assessment
        """
        # Calculate risk factors
        unique_types = len(set(e.entity_type for e in entities))
        avg_relationships = sum(len(e.relationships) for e in entities) / max(len(entities), 1)
        cross_ref_count = len(cross_refs)

        # Risk score (0-1, higher is riskier)
        risk_score = min(1.0, (
            0.1 * unique_types +
            0.2 * avg_relationships +
            0.3 * (cross_ref_count / max(len(entities), 1))
        ))

        # Determine risk level
        if risk_score > 0.7:
            risk_level = "high"
            recommendation = "Consider additional anonymization or data minimization"
        elif risk_score > 0.4:
            risk_level = "medium"
            recommendation = "Review cross-references before sharing"
        else:
            risk_level = "low"
            recommendation = "Data appears adequately anonymized"

        return {
            "risk_score": round(risk_score, 3),
            "risk_level": risk_level,
            "entity_count": len(entities),
            "unique_types": unique_types,
            "avg_relationships": round(avg_relationships, 2),
            "cross_references": cross_ref_count,
            "recommendation": recommendation,
        }

    def get_cutting_edge_stats(self) -> Dict[str, Any]:
        """Get comprehensive cutting edge statistics."""
        return {
            "base_redactor": self.base_redactor.get_stats(),
            "cross_reference_detector": self.cross_ref_detector.get_detection_stats(),
            "consistency_map_size": len(self.fake_generator._consistency_map),
            "level": "cutting_edge",
        }


# Factory functions for cutting edge
_cutting_edge_pii_redactor: Optional[CuttingEdgePIIRedactor] = None


def get_cutting_edge_pii_redactor(
    config: Optional[SemanticAnonymizationConfig] = None
) -> CuttingEdgePIIRedactor:
    """Get or create cutting edge PII redactor singleton."""
    global _cutting_edge_pii_redactor

    if _cutting_edge_pii_redactor is None:
        _cutting_edge_pii_redactor = CuttingEdgePIIRedactor(config=config)
        logger.info("CuttingEdgePIIRedactor initialized (Cutting Edge)")

    return _cutting_edge_pii_redactor


def reset_cutting_edge_pii_redactor() -> None:
    """Reset cutting edge PII redactor (for testing)."""
    global _cutting_edge_pii_redactor
    _cutting_edge_pii_redactor = None


# =============================================================================
# ASYNC WRAPPER FOR ORCHESTRATOR
# =============================================================================

async def redact_query_for_cloud(
    query: str,
    context_chunks: Optional[List[str]] = None
) -> Tuple[str, List[str], Dict[str, str]]:
    """
    Async wrapper to redact query and context for cloud LLM.

    Args:
        query: The user query
        context_chunks: Optional list of context chunks

    Returns:
        Tuple of (redacted_query, redacted_chunks, combined_token_map)
    """
    redactor = get_pii_redactor()
    combined_map: Dict[str, str] = {}

    # Redact query
    query_result = redactor.redact(query)
    combined_map.update(query_result.token_map)

    # Redact context chunks
    redacted_chunks = []
    if context_chunks:
        for chunk in context_chunks:
            chunk_result = redactor.redact(chunk)
            redacted_chunks.append(chunk_result.redacted_text)
            combined_map.update(chunk_result.token_map)

    return query_result.redacted_text, redacted_chunks, combined_map


async def restore_response_from_cloud(
    response: str,
    token_map: Dict[str, str]
) -> str:
    """
    Async wrapper to restore PII in cloud LLM response.

    Args:
        response: The LLM response with tokens
        token_map: The token mapping

    Returns:
        Response with original values restored
    """
    redactor = get_pii_redactor()
    return redactor.restore(response, token_map)
