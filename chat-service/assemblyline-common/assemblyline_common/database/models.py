"""
T.A.L.O.S. Database Models
Pydantic models for database entities
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
from enum import Enum
import json


# =============================================================================
# ENUMS
# =============================================================================

class UserRole(str, Enum):
    USER = "user"
    MANAGER = "manager"
    ADMIN = "admin"
    SUPERADMIN = "superadmin"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class ChangeRequestType(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class TargetType(str, Enum):
    PROMPT = "prompt"
    GUARDRAIL = "guardrail"


class DocumentStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    NO_CONTENT = "no_content"        # Document had no extractable content
    NEEDS_REINDEX = "needs_reindex"  # Chunks created but indexing returned 0


class PromptCategory(str, Enum):
    SYSTEM = "system"      # Core AI behavior prompts
    GUARDRAILS = "guardrails"  # Safety and filtering rules
    TEMPLATES = "templates"    # User-facing templates
    RAG = "rag"            # RAG-specific prompts
    CUSTOM = "custom"      # User-created prompts


class GuardrailCategory(str, Enum):
    SAFETY = "safety"          # Harmful content prevention
    PRIVACY = "privacy"        # PII protection rules
    COMPLIANCE = "compliance"  # Industry-specific rules (HIPAA, etc.)
    BRAND = "brand"            # Tone and voice guidelines
    ACCURACY = "accuracy"      # Citation and source requirements


class GuardrailAction(str, Enum):
    BLOCK = "block"            # Block the message entirely
    WARN = "warn"              # Allow but flag for review
    MODIFY = "modify"          # Auto-modify/redact content
    LOG = "log"                # Allow but log for audit


class LicenseTier(str, Enum):
    FREE = "free"                           # Limited features, 1 user
    PRO = "pro"                             # Legacy consumer tier
    ENTERPRISE = "enterprise"               # Legacy enterprise tier
    # New tier system
    CONSUMER = "consumer"                   # Consumer (2 users, machine-locked)
    CONSUMER_PLUS = "consumer_plus"         # Consumer Plus (5 users, machine-locked)
    ENTERPRISE_S = "enterprise_s"           # Enterprise Small (100 users, domain-locked)
    ENTERPRISE_M = "enterprise_m"           # Enterprise Medium (500 users, domain-locked)
    ENTERPRISE_L = "enterprise_l"           # Enterprise Large (1000 users, domain-locked)
    ENTERPRISE_UNLIMITED = "enterprise_unlimited"  # Enterprise Unlimited
    DEVELOPER = "developer"                 # Developer (unlimited, no binding)


# =============================================================================
# ORGANIZATION MODELS (Multi-tenant support)
# =============================================================================

class Organization(BaseModel):
    """Organization model for multi-tenant support"""
    id: int
    name: str
    license_key: Optional[str] = None
    license_tier: LicenseTier = LicenseTier.PRO
    max_users: int = 2
    domain: Optional[str] = None
    settings: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator('settings', mode='before')
    @classmethod
    def parse_settings(cls, v):
        """Parse settings from JSON string if needed"""
        if isinstance(v, str):
            return json.loads(v) if v else {}
        return v if v else {}

    class Config:
        from_attributes = True


class OrganizationCreate(BaseModel):
    """Create organization request"""
    name: str
    license_tier: LicenseTier = LicenseTier.PRO
    max_users: int = 2
    settings: Dict[str, Any] = Field(default_factory=dict)


class OrganizationResponse(BaseModel):
    """Organization API response"""
    id: int
    name: str
    license_tier: str
    max_users: int
    user_count: int = 0
    settings: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


# =============================================================================
# DEPARTMENT ACCESS GRANTS (Enterprise cross-department access)
# =============================================================================

class DepartmentAccessGrant(BaseModel):
    """Grant for cross-department document access (Enterprise only)"""
    id: int
    user_id: int
    organization_id: int
    department: str
    granted_by: Optional[int] = None
    granted_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    is_active: bool = True

    class Config:
        from_attributes = True


class DepartmentAccessGrantCreate(BaseModel):
    """Create access grant request"""
    user_id: int
    department: str
    expires_at: Optional[datetime] = None


class DepartmentAccessGrantResponse(BaseModel):
    """Access grant API response"""
    id: int
    user_id: int
    user_email: Optional[str] = None
    organization_id: int
    department: str
    granted_by: Optional[int] = None
    granted_at: datetime
    expires_at: Optional[datetime] = None
    is_active: bool


# =============================================================================
# ORGANIZATION EMAIL CONFIG (Enterprise email sending)
# =============================================================================

class EmailProvider(str, Enum):
    """Supported email providers"""
    DISABLED = "disabled"
    SMTP = "smtp"
    MICROSOFT_GRAPH = "microsoft_graph"


class OrgEmailConfig(BaseModel):
    """Organization email configuration for sending invites"""
    id: int
    organization_id: int
    provider: EmailProvider = EmailProvider.DISABLED

    # SMTP Settings
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_username: Optional[str] = None
    smtp_password_encrypted: Optional[str] = None
    smtp_use_tls: bool = True

    # Microsoft Graph Settings
    ms_tenant_id: Optional[str] = None
    ms_client_id: Optional[str] = None
    ms_client_secret_encrypted: Optional[str] = None

    # Common Settings
    from_address: Optional[str] = None
    from_name: str = "DriveSentinel"

    # Metadata
    is_verified: bool = False
    last_test_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class OrgEmailConfigUpdate(BaseModel):
    """Update email configuration request"""
    provider: EmailProvider

    # SMTP Settings (optional based on provider)
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = 587
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None  # Plain text, will be encrypted
    smtp_use_tls: Optional[bool] = True

    # Microsoft Graph Settings (optional based on provider)
    ms_tenant_id: Optional[str] = None
    ms_client_id: Optional[str] = None
    ms_client_secret: Optional[str] = None  # Plain text, will be encrypted

    # Common Settings
    from_address: Optional[str] = None
    from_name: Optional[str] = "DriveSentinel"


class OrgEmailConfigResponse(BaseModel):
    """Email configuration API response (secrets masked)"""
    id: int
    organization_id: int
    provider: str

    # SMTP Settings (password masked)
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_username: Optional[str] = None
    smtp_password_set: bool = False  # Indicates if password is set, never shows actual value
    smtp_use_tls: bool = True

    # Microsoft Graph Settings (secret masked)
    ms_tenant_id: Optional[str] = None
    ms_client_id: Optional[str] = None
    ms_client_secret_set: bool = False  # Indicates if secret is set

    # Common Settings
    from_address: Optional[str] = None
    from_name: str = "DriveSentinel"

    # Status
    is_verified: bool = False
    last_test_at: Optional[datetime] = None


# =============================================================================
# USER MODELS
# =============================================================================

class User(BaseModel):
    id: int
    email: str
    name: Optional[str] = None
    hashed_password: str
    role: UserRole = UserRole.USER
    organization_id: Optional[int] = None
    department: Optional[str] = None
    is_active: bool = True
    must_change_password: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class UserCreate(BaseModel):
    email: str
    password: str
    name: Optional[str] = None
    role: UserRole = UserRole.USER
    organization_id: Optional[int] = None
    department: Optional[str] = None


class UserResponse(BaseModel):
    id: int
    email: str
    name: Optional[str] = None
    role: UserRole
    organization_id: Optional[int] = None
    department: Optional[str] = None
    is_active: bool
    is_admin: bool = False
    is_manager: bool = False
    created_at: datetime

    @classmethod
    def from_user(cls, user: User) -> "UserResponse":
        return cls(
            id=user.id,
            email=user.email,
            name=user.name,
            role=user.role,
            organization_id=user.organization_id,
            department=user.department,
            is_active=user.is_active,
            is_admin=user.role in [UserRole.ADMIN, UserRole.SUPERADMIN],
            is_manager=user.role in [UserRole.MANAGER, UserRole.ADMIN, UserRole.SUPERADMIN],
            created_at=user.created_at,
        )


# =============================================================================
# DOCUMENT MODELS
# =============================================================================

class DocumentVisibility(str, Enum):
    PRIVATE = "private"
    DEPARTMENT = "department"
    ORGANIZATION = "organization"


class Document(BaseModel):
    id: int
    filename: str
    file_hash: str
    file_size: int
    file_type: str
    file_path: Optional[str] = None
    status: DocumentStatus = DocumentStatus.PENDING
    chunk_count: int = 0
    user_id: Optional[int] = None
    organization_id: Optional[int] = None
    department: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None

    # Document flagging (Enterprise feature)
    flagged_at: Optional[datetime] = None
    flagged_by: Optional[int] = None
    flag_reason: Optional[str] = None

    # Delete protection (Enterprise feature)
    is_protected: bool = False
    protected_by: Optional[int] = None
    protected_at: Optional[datetime] = None

    # Visibility control
    visibility: DocumentVisibility = DocumentVisibility.PRIVATE

    # Folder assignment
    folder_id: Optional[str] = None

    @field_validator('folder_id', mode='before')
    @classmethod
    def parse_folder_id(cls, v):
        """Coerce UUID from asyncpg to string"""
        if v is not None:
            return str(v)
        return v

    @field_validator('metadata', mode='before')
    @classmethod
    def parse_metadata(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v or {}

    @field_validator('visibility', mode='before')
    @classmethod
    def parse_visibility(cls, v):
        if isinstance(v, str):
            return DocumentVisibility(v)
        return v or DocumentVisibility.PRIVATE

    class Config:
        from_attributes = True


class DocumentChunk(BaseModel):
    id: int
    document_id: int
    chunk_index: int
    content: str
    embedding: Optional[List[float]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    token_count: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator('metadata', mode='before')
    @classmethod
    def parse_metadata(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v or {}

    class Config:
        from_attributes = True


# =============================================================================
# CONVERSATION MODELS
# =============================================================================

class Conversation(BaseModel):
    id: int
    title: Optional[str] = None
    user_id: Optional[int] = None
    model_used: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class Message(BaseModel):
    id: int
    conversation_id: int
    role: str  # 'user' or 'assistant'
    content: str
    sources: Optional[List[Dict[str, Any]]] = None
    agent_used: Optional[str] = None
    model_used: Optional[str] = None
    processing_time_ms: Optional[int] = None
    # Quality metrics (computed by MonitorAgent)
    quality_score: Optional[float] = None  # 0.0-1.0 score
    quality_grade: Optional[str] = None  # A, B, C, D, F grade
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


# =============================================================================
# PROMPT MODELS
# =============================================================================

class Prompt(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    content: str
    category: PromptCategory = PromptCategory.CUSTOM
    is_default: bool = False
    user_id: Optional[int] = None
    created_by: Optional[int] = None
    updated_by: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class PromptCreate(BaseModel):
    name: str
    description: Optional[str] = None
    content: str
    category: PromptCategory = PromptCategory.CUSTOM


class PromptVersion(BaseModel):
    """Tracks prompt version history"""
    id: int
    prompt_id: int
    version: int
    content: str
    change_summary: Optional[str] = None
    created_by: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


# =============================================================================
# GUARDRAIL MODELS
# =============================================================================

class Guardrail(BaseModel):
    """Content filtering and safety rule"""
    id: int
    name: str
    description: Optional[str] = None
    category: GuardrailCategory
    pattern: Optional[str] = None  # Regex pattern for matching
    keywords: List[str] = Field(default_factory=list)  # Keywords to match
    action: GuardrailAction = GuardrailAction.BLOCK
    replacement_text: Optional[str] = None  # For MODIFY action
    is_active: bool = True
    priority: int = 0  # Higher priority rules evaluated first
    apply_to_input: bool = True   # Apply to user messages
    apply_to_output: bool = True  # Apply to AI responses
    created_by: Optional[int] = None
    updated_by: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class GuardrailCreate(BaseModel):
    name: str
    description: Optional[str] = None
    category: GuardrailCategory
    pattern: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    action: GuardrailAction = GuardrailAction.BLOCK
    replacement_text: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    apply_to_input: bool = True
    apply_to_output: bool = True


class GuardrailUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[GuardrailCategory] = None
    pattern: Optional[str] = None
    keywords: Optional[List[str]] = None
    action: Optional[GuardrailAction] = None
    replacement_text: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    apply_to_input: Optional[bool] = None
    apply_to_output: Optional[bool] = None


class GuardrailViolation(BaseModel):
    """Log of guardrail violations"""
    id: int
    guardrail_id: int
    user_id: Optional[int] = None
    conversation_id: Optional[int] = None
    original_content: str
    action_taken: GuardrailAction
    modified_content: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class GuardrailTestRequest(BaseModel):
    """Request to test content against guardrails"""
    content: str
    test_input: bool = True
    test_output: bool = True


class GuardrailTestResult(BaseModel):
    """Result of testing content against guardrails"""
    passed: bool
    violations: List[Dict[str, Any]] = Field(default_factory=list)
    modified_content: Optional[str] = None
    guardrails_checked: int = 0


# =============================================================================
# SETTINGS MODELS
# =============================================================================

class UserSettings(BaseModel):
    id: int
    user_id: int
    theme: str = "system"
    high_contrast: bool = False
    notifications: bool = True
    stream_responses: bool = True
    ollama_url: str = "http://localhost:11434"
    model: str = "gemma3:4b"
    embedding_model: str = "nomic-embed-text"
    temperature: float = 0.7
    max_tokens: int = 2000
    chunk_size: int = 500
    chunk_overlap: int = 50
    top_k: int = 5
    min_score: float = 0.5
    voice: str = "af_heart"
    voice_speed: float = 1.0
    voice_volume: float = 1.0
    auto_read_aloud: bool = False
    tts_provider: str = "kokoro"
    tts_streaming_enabled: bool = True
    tts_wait_for_complete: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


# =============================================================================
# USER SECRET VAULT MODELS
# =============================================================================

class SecretVaultCategory(str, Enum):
    """Categories for secret vault entries"""
    EMAIL = "email"
    OAUTH = "oauth"
    API_KEY = "api_key"
    DATABASE = "database"
    CLOUD = "cloud"
    GENERAL = "general"


class UserSecret(BaseModel):
    """A single secret in the user's vault"""
    id: int
    user_id: int
    secret_key: str  # e.g., 'email.gmail.access_token'
    encrypted_value: str  # AES-256-GCM encrypted
    encryption_metadata: Dict[str, Any]  # salt, nonce, algorithm
    category: SecretVaultCategory = SecretVaultCategory.GENERAL
    description: Optional[str] = None
    expires_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class UserSecretCreate(BaseModel):
    """Create a new secret"""
    secret_key: str
    value: str  # Plain text - will be encrypted
    category: SecretVaultCategory = SecretVaultCategory.GENERAL
    description: Optional[str] = None
    expires_at: Optional[datetime] = None


class UserSecretUpdate(BaseModel):
    """Update an existing secret"""
    value: Optional[str] = None  # Plain text - will be encrypted
    description: Optional[str] = None
    expires_at: Optional[datetime] = None


class UserSecretResponse(BaseModel):
    """Response model (never includes decrypted value)"""
    id: int
    secret_key: str
    category: SecretVaultCategory
    description: Optional[str] = None
    expires_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    is_expired: bool = False

    class Config:
        from_attributes = True


# =============================================================================
# API KEY MODELS
# =============================================================================

class APIKey(BaseModel):
    id: int
    user_id: int
    name: str
    key_hash: str
    key_prefix: str
    expires_at: Optional[datetime] = None
    last_used: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class APIKeyCreate(BaseModel):
    name: str
    expires_in_days: Optional[int] = 90


# =============================================================================
# PROMPT SUGGESTION MODELS
# =============================================================================

class PromptSuggestion(BaseModel):
    """AI-generated prompt suggestions from user queries"""
    id: int
    suggested_prompt: str
    source_query: str
    category: PromptCategory = PromptCategory.CUSTOM
    risk_level: RiskLevel = RiskLevel.LOW
    suggested_by: str = "orchestrator"
    status: ApprovalStatus = ApprovalStatus.PENDING
    reviewed_by: Optional[int] = None
    review_notes: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    reviewed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class PromptSuggestionCreate(BaseModel):
    suggested_prompt: str
    source_query: str
    category: PromptCategory = PromptCategory.CUSTOM
    risk_level: RiskLevel = RiskLevel.LOW
    suggested_by: str = "orchestrator"


class PromptSuggestionReview(BaseModel):
    status: ApprovalStatus
    review_notes: Optional[str] = None


# =============================================================================
# PROMPT CHANGE REQUEST MODELS
# =============================================================================

class PromptChangeRequest(BaseModel):
    """Approval workflow for prompt/guardrail changes"""
    id: int
    request_type: ChangeRequestType
    target_type: TargetType
    target_id: Optional[int] = None  # NULL for create requests
    proposed_content: Dict[str, Any]
    risk_level: RiskLevel = RiskLevel.LOW
    status: ApprovalStatus = ApprovalStatus.PENDING
    requested_by: int
    reviewed_by: Optional[int] = None
    review_notes: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    reviewed_at: Optional[datetime] = None

    @field_validator('proposed_content', mode='before')
    @classmethod
    def parse_proposed_content(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v or {}

    class Config:
        from_attributes = True


class PromptChangeRequestCreate(BaseModel):
    request_type: ChangeRequestType
    target_type: TargetType
    target_id: Optional[int] = None
    proposed_content: Dict[str, Any]
    risk_level: RiskLevel = RiskLevel.LOW


class PromptChangeRequestReview(BaseModel):
    status: ApprovalStatus
    review_notes: Optional[str] = None


# =============================================================================
# PROMPT TEST MODELS
# =============================================================================

class PromptTest(BaseModel):
    """Prompt testing metadata"""
    id: int
    prompt_id: Optional[int] = None
    prompt_content: str
    test_query: str
    created_by: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class PromptTestCreate(BaseModel):
    prompt_id: Optional[int] = None
    prompt_content: str
    test_query: str


class PromptTestResult(BaseModel):
    """Multi-model test results with quality scores"""
    id: int
    test_id: int
    model: str
    response: Optional[str] = None
    quality_score: Optional[int] = None  # 0-100 from MonitorAgent
    relevance_score: Optional[int] = None  # 0-100
    latency_ms: Optional[int] = None
    token_count: Optional[int] = None
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class PromptTestResultCreate(BaseModel):
    test_id: int
    model: str
    response: Optional[str] = None
    quality_score: Optional[int] = None
    relevance_score: Optional[int] = None
    latency_ms: Optional[int] = None
    token_count: Optional[int] = None
    error_message: Optional[str] = None


# =============================================================================
# PROMPT TEST REQUEST/RESPONSE MODELS
# =============================================================================

class PromptTestRequest(BaseModel):
    """Request to test a prompt against models"""
    prompt_content: str
    test_query: str
    models: List[str] = Field(default_factory=lambda: ["llama3.2:3b", "gemma3:4b", "llama3.1:8b"])
    prompt_id: Optional[int] = None


class PromptTestResponse(BaseModel):
    """Response with test results across models"""
    test_id: int
    prompt_content: str
    test_query: str
    results: List[PromptTestResult]
    best_model: Optional[str] = None
    created_at: datetime


class PromptCompareRequest(BaseModel):
    """Request to compare multiple prompts"""
    prompts: List[str]  # List of prompt contents
    test_query: str
    model: str = "gemma3:4b"


class PromptCompareResult(BaseModel):
    """Result of comparing prompts"""
    prompt_content: str
    response: Optional[str] = None
    quality_score: Optional[int] = None
    latency_ms: Optional[int] = None
    token_count: Optional[int] = None


class PromptCompareResponse(BaseModel):
    """Response with comparison results"""
    test_query: str
    model: str
    results: List[PromptCompareResult]
    best_prompt_index: Optional[int] = None


# =============================================================================
# EMAIL RAG MODELS
# =============================================================================

class EmailSyncStatus(str, Enum):
    SYNCED = "synced"
    DELETED = "deleted"
    ARCHIVED = "archived"
    ERROR = "error"


class EmailMessage(BaseModel):
    """Email message from Gmail synced for RAG"""
    id: int
    user_id: int
    organization_id: Optional[int] = None
    gmail_id: str
    thread_id: Optional[str] = None
    account_email: str
    from_address: Optional[str] = None
    from_name: Optional[str] = None
    to_addresses: List[str] = Field(default_factory=list)
    cc_addresses: List[str] = Field(default_factory=list)
    subject: Optional[str] = None
    body_text: Optional[str] = None
    body_html: Optional[str] = None
    snippet: Optional[str] = None
    labels: List[str] = Field(default_factory=list)
    is_read: bool = False
    is_starred: bool = False
    is_important: bool = False
    has_attachments: bool = False
    attachment_count: int = 0
    attachment_names: List[str] = Field(default_factory=list)
    email_date: Optional[datetime] = None
    internal_date: Optional[int] = None
    size_bytes: Optional[int] = None
    is_indexed: bool = False
    indexed_at: Optional[datetime] = None
    # AI Classification
    category: Optional[str] = None  # promotional, security, social, invoice, newsletter, personal, business, other
    category_confidence: Optional[float] = None  # 0.0 to 1.0 confidence score
    classified_at: Optional[datetime] = None
    sync_status: EmailSyncStatus = EmailSyncStatus.SYNCED
    deleted_at: Optional[datetime] = None
    deleted_from_gmail: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator('to_addresses', 'cc_addresses', 'labels', 'attachment_names', mode='before')
    @classmethod
    def parse_array(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            return json.loads(v) if v.startswith('[') else [v]
        return list(v) if v else []

    class Config:
        from_attributes = True


class EmailMessageResponse(BaseModel):
    """Email message API response"""
    id: int
    gmail_id: str
    thread_id: Optional[str] = None
    account_email: str
    from_address: Optional[str] = None
    from_name: Optional[str] = None
    to_addresses: List[str] = Field(default_factory=list)
    subject: Optional[str] = None
    snippet: Optional[str] = None
    body_text: Optional[str] = None
    labels: List[str] = Field(default_factory=list)
    is_read: bool = False
    is_starred: bool = False
    is_important: bool = False
    has_attachments: bool = False
    attachment_count: int = 0
    email_date: Optional[datetime] = None
    category: Optional[str] = None
    category_confidence: Optional[float] = None
    sync_status: str = "synced"


class EmailMessageSummary(BaseModel):
    """Compact email summary for list views"""
    id: int
    gmail_id: str
    from_address: Optional[str] = None
    from_name: Optional[str] = None
    subject: Optional[str] = None
    snippet: Optional[str] = None
    is_read: bool = False
    is_starred: bool = False
    has_attachments: bool = False
    email_date: Optional[datetime] = None
    labels: List[str] = Field(default_factory=list)
    category: Optional[str] = None


class EmailSyncState(BaseModel):
    """Email sync state for incremental sync"""
    id: int
    user_id: int
    account_email: str
    last_history_id: Optional[int] = None
    last_sync_at: Optional[datetime] = None
    total_synced: int = 0
    sync_errors: int = 0
    last_error: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        from_attributes = True


class EmailSyncRequest(BaseModel):
    """Request to sync emails"""
    account_email: str
    max_results: int = 100
    query: Optional[str] = None  # Gmail search query (e.g., "is:unread", "from:someone@example.com")
    labels: List[str] = Field(default_factory=lambda: ["INBOX"])
    include_body: bool = True
    force_full_sync: bool = False


class EmailSyncResponse(BaseModel):
    """Response from email sync"""
    synced: int
    new: int
    updated: int
    errors: int
    account_email: str
    history_id: Optional[int] = None


class EmailSearchRequest(BaseModel):
    """Search emails via RAG"""
    query: str
    account_email: Optional[str] = None
    limit: int = 20
    include_body: bool = False
    labels: Optional[List[str]] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None


class EmailSearchResult(BaseModel):
    """Email search result with relevance score"""
    email: EmailMessageSummary
    score: float = 0.0
    match_context: Optional[str] = None


class EmailActionRequest(BaseModel):
    """Request to perform action on emails"""
    email_ids: List[int]  # Database IDs
    action: str  # delete, archive, mark_read, mark_unread, star, unstar, trash
    confirm: bool = False  # For destructive actions


class EmailActionResponse(BaseModel):
    """Response from email action"""
    success: bool
    action: str
    affected: int
    gmail_synced: bool = False
    errors: List[str] = Field(default_factory=list)


class EmailAnalysisRequest(BaseModel):
    """Request AI analysis of emails"""
    email_ids: Optional[List[int]] = None  # Specific emails, or None for recent
    query: Optional[str] = None  # Natural language query
    analysis_type: str = "summary"  # summary, categorize, find_actionable, suggest_cleanup


class EmailAnalysisResponse(BaseModel):
    """AI email analysis response"""
    analysis_type: str
    result: str
    emails_analyzed: int
    suggestions: List[Dict[str, Any]] = Field(default_factory=list)
    recommended_actions: List[Dict[str, Any]] = Field(default_factory=list)
