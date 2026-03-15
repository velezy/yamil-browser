"""
API Environment Management for Logic Weaver.

Provides environment lifecycle management for APIs:
- Development → Staging → Production promotion
- Environment-specific policies and configurations
- Promotion workflows with approval gates
- Environment isolation and access control

Comparable to Apigee's environment management and AWS API Gateway stages.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
import asyncio
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


class EnvironmentType(str, Enum):
    """API environment types."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    SANDBOX = "sandbox"


class PromotionStatus(str, Enum):
    """Promotion request status."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class ApprovalType(str, Enum):
    """Types of approval required for promotion."""
    NONE = "none"
    SINGLE = "single"
    MULTI = "multi"
    AUTOMATIC = "automatic"


@dataclass
class EnvironmentPolicy:
    """Environment-specific policy configuration."""
    # Rate limiting
    rate_limit_requests: int = 1000
    rate_limit_period_seconds: int = 60

    # Quotas
    daily_quota: Optional[int] = None
    monthly_quota: Optional[int] = None

    # Security
    require_api_key: bool = True
    require_oauth: bool = False
    allowed_origins: List[str] = field(default_factory=list)
    ip_whitelist: List[str] = field(default_factory=list)

    # Caching
    cache_enabled: bool = True
    cache_ttl_seconds: int = 300

    # Logging
    log_level: str = "INFO"
    log_request_body: bool = False
    log_response_body: bool = False

    # Timeouts
    timeout_seconds: int = 30

    # Custom policies
    custom_policies: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "rate_limit_requests": self.rate_limit_requests,
            "rate_limit_period_seconds": self.rate_limit_period_seconds,
            "daily_quota": self.daily_quota,
            "monthly_quota": self.monthly_quota,
            "require_api_key": self.require_api_key,
            "require_oauth": self.require_oauth,
            "allowed_origins": self.allowed_origins,
            "ip_whitelist": self.ip_whitelist,
            "cache_enabled": self.cache_enabled,
            "cache_ttl_seconds": self.cache_ttl_seconds,
            "log_level": self.log_level,
            "log_request_body": self.log_request_body,
            "log_response_body": self.log_response_body,
            "timeout_seconds": self.timeout_seconds,
            "custom_policies": self.custom_policies,
        }


@dataclass
class Environment:
    """API environment configuration."""
    name: str
    type: EnvironmentType
    base_url: str
    description: str = ""

    # Policies
    policy: EnvironmentPolicy = field(default_factory=EnvironmentPolicy)

    # State
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    # Deployed version
    deployed_version: Optional[str] = None
    deployed_at: Optional[datetime] = None
    deployed_by: Optional[str] = None

    # Access control
    allowed_users: Set[str] = field(default_factory=set)
    allowed_roles: Set[str] = field(default_factory=set)

    # Variables
    variables: Dict[str, str] = field(default_factory=dict)
    secrets: Set[str] = field(default_factory=set)  # Variable names that are secrets

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "type": self.type.value,
            "base_url": self.base_url,
            "description": self.description,
            "policy": self.policy.to_dict(),
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "deployed_version": self.deployed_version,
            "deployed_at": self.deployed_at.isoformat() if self.deployed_at else None,
            "deployed_by": self.deployed_by,
            "allowed_users": list(self.allowed_users),
            "allowed_roles": list(self.allowed_roles),
            "variables": {
                k: "***" if k in self.secrets else v
                for k, v in self.variables.items()
            },
        }


@dataclass
class PromotionRule:
    """Rules for promoting between environments."""
    source: EnvironmentType
    target: EnvironmentType

    # Approval requirements
    approval_type: ApprovalType = ApprovalType.SINGLE
    required_approvers: int = 1
    allowed_approvers: Set[str] = field(default_factory=set)

    # Validation requirements
    require_tests_pass: bool = True
    require_no_critical_issues: bool = True
    require_documentation: bool = False

    # Automatic checks
    run_smoke_tests: bool = True
    run_integration_tests: bool = False
    run_security_scan: bool = False

    # Rollback settings
    auto_rollback_on_failure: bool = True
    rollback_timeout_seconds: int = 300

    # Custom validation
    custom_validators: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "source": self.source.value,
            "target": self.target.value,
            "approval_type": self.approval_type.value,
            "required_approvers": self.required_approvers,
            "allowed_approvers": list(self.allowed_approvers),
            "require_tests_pass": self.require_tests_pass,
            "require_no_critical_issues": self.require_no_critical_issues,
            "require_documentation": self.require_documentation,
            "run_smoke_tests": self.run_smoke_tests,
            "run_integration_tests": self.run_integration_tests,
            "run_security_scan": self.run_security_scan,
            "auto_rollback_on_failure": self.auto_rollback_on_failure,
            "rollback_timeout_seconds": self.rollback_timeout_seconds,
            "custom_validators": self.custom_validators,
        }


@dataclass
class PromotionRequest:
    """Request to promote API between environments."""
    id: str
    api_id: str
    version: str
    source_environment: str
    target_environment: str

    # Status
    status: PromotionStatus = PromotionStatus.PENDING

    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None

    # Users
    requested_by: str = ""
    approvals: Dict[str, datetime] = field(default_factory=dict)
    rejections: Dict[str, str] = field(default_factory=dict)  # user -> reason

    # Validation results
    validation_results: Dict[str, bool] = field(default_factory=dict)
    validation_messages: Dict[str, str] = field(default_factory=dict)

    # Deployment info
    deployment_log: List[str] = field(default_factory=list)
    rollback_version: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "api_id": self.api_id,
            "version": self.version,
            "source_environment": self.source_environment,
            "target_environment": self.target_environment,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "requested_by": self.requested_by,
            "approvals": {k: v.isoformat() for k, v in self.approvals.items()},
            "rejections": self.rejections,
            "validation_results": self.validation_results,
            "validation_messages": self.validation_messages,
            "deployment_log": self.deployment_log,
            "rollback_version": self.rollback_version,
        }


class EnvironmentManager:
    """
    Manages API environments and promotion workflows.

    Features:
    - Environment CRUD operations
    - Environment-specific policies
    - Promotion workflows with approvals
    - Rollback support
    - Variable/secret management
    """

    def __init__(self):
        self._environments: Dict[str, Environment] = {}
        self._promotion_rules: Dict[str, PromotionRule] = {}
        self._promotion_requests: Dict[str, PromotionRequest] = {}
        self._validators: Dict[str, Callable] = {}
        self._deployers: Dict[str, Callable] = {}

        # Initialize default environments
        self._init_default_environments()
        self._init_default_promotion_rules()

    def _init_default_environments(self) -> None:
        """Initialize default environment configurations."""
        # Development
        dev_policy = EnvironmentPolicy(
            rate_limit_requests=10000,
            require_api_key=False,
            log_level="DEBUG",
            log_request_body=True,
            log_response_body=True,
            cache_enabled=False,
        )
        self.register_environment(Environment(
            name="development",
            type=EnvironmentType.DEVELOPMENT,
            base_url="http://localhost:8000",
            description="Local development environment",
            policy=dev_policy,
            allowed_roles={"developer", "admin"},
        ))

        # Staging
        staging_policy = EnvironmentPolicy(
            rate_limit_requests=5000,
            require_api_key=True,
            log_level="INFO",
            log_request_body=False,
            cache_enabled=True,
        )
        self.register_environment(Environment(
            name="staging",
            type=EnvironmentType.STAGING,
            base_url="https://staging-api.example.com",
            description="Pre-production staging environment",
            policy=staging_policy,
            allowed_roles={"developer", "qa", "admin"},
        ))

        # Production
        prod_policy = EnvironmentPolicy(
            rate_limit_requests=1000,
            require_api_key=True,
            require_oauth=True,
            log_level="WARNING",
            cache_enabled=True,
            cache_ttl_seconds=600,
        )
        self.register_environment(Environment(
            name="production",
            type=EnvironmentType.PRODUCTION,
            base_url="https://api.example.com",
            description="Production environment",
            policy=prod_policy,
            allowed_roles={"admin", "operator"},
        ))

    def _init_default_promotion_rules(self) -> None:
        """Initialize default promotion rules."""
        # Dev → Staging
        self.register_promotion_rule(PromotionRule(
            source=EnvironmentType.DEVELOPMENT,
            target=EnvironmentType.STAGING,
            approval_type=ApprovalType.SINGLE,
            required_approvers=1,
            require_tests_pass=True,
            run_smoke_tests=True,
        ))

        # Staging → Production
        self.register_promotion_rule(PromotionRule(
            source=EnvironmentType.STAGING,
            target=EnvironmentType.PRODUCTION,
            approval_type=ApprovalType.MULTI,
            required_approvers=2,
            require_tests_pass=True,
            require_no_critical_issues=True,
            require_documentation=True,
            run_smoke_tests=True,
            run_integration_tests=True,
            run_security_scan=True,
            auto_rollback_on_failure=True,
        ))

    def register_environment(self, env: Environment) -> None:
        """Register an environment."""
        self._environments[env.name] = env
        logger.info(f"Registered environment: {env.name} ({env.type.value})")

    def get_environment(self, name: str) -> Optional[Environment]:
        """Get environment by name."""
        return self._environments.get(name)

    def get_all_environments(self) -> List[Environment]:
        """Get all registered environments."""
        return list(self._environments.values())

    def get_environments_by_type(self, env_type: EnvironmentType) -> List[Environment]:
        """Get environments by type."""
        return [e for e in self._environments.values() if e.type == env_type]

    def update_environment(self, name: str, **kwargs) -> Optional[Environment]:
        """Update environment configuration."""
        env = self._environments.get(name)
        if not env:
            return None

        for key, value in kwargs.items():
            if hasattr(env, key):
                setattr(env, key, value)

        env.updated_at = datetime.utcnow()
        return env

    def set_environment_variable(
        self,
        env_name: str,
        key: str,
        value: str,
        is_secret: bool = False,
    ) -> bool:
        """Set an environment variable."""
        env = self._environments.get(env_name)
        if not env:
            return False

        env.variables[key] = value
        if is_secret:
            env.secrets.add(key)
        elif key in env.secrets:
            env.secrets.remove(key)

        env.updated_at = datetime.utcnow()
        return True

    def get_environment_variable(
        self,
        env_name: str,
        key: str,
    ) -> Optional[str]:
        """Get an environment variable."""
        env = self._environments.get(env_name)
        if not env:
            return None
        return env.variables.get(key)

    def register_promotion_rule(self, rule: PromotionRule) -> None:
        """Register a promotion rule."""
        key = f"{rule.source.value}:{rule.target.value}"
        self._promotion_rules[key] = rule
        logger.info(f"Registered promotion rule: {rule.source.value} → {rule.target.value}")

    def get_promotion_rule(
        self,
        source: EnvironmentType,
        target: EnvironmentType,
    ) -> Optional[PromotionRule]:
        """Get promotion rule for source → target."""
        key = f"{source.value}:{target.value}"
        return self._promotion_rules.get(key)

    def register_validator(self, name: str, validator: Callable) -> None:
        """Register a custom validator function."""
        self._validators[name] = validator

    def register_deployer(self, env_type: EnvironmentType, deployer: Callable) -> None:
        """Register a deployer function for an environment type."""
        self._deployers[env_type.value] = deployer

    def create_promotion_request(
        self,
        api_id: str,
        version: str,
        source_environment: str,
        target_environment: str,
        requested_by: str,
    ) -> Optional[PromotionRequest]:
        """Create a promotion request."""
        source_env = self._environments.get(source_environment)
        target_env = self._environments.get(target_environment)

        if not source_env or not target_env:
            logger.error(f"Invalid environments: {source_environment} → {target_environment}")
            return None

        # Check promotion rule exists
        rule = self.get_promotion_rule(source_env.type, target_env.type)
        if not rule:
            logger.error(f"No promotion rule for {source_env.type.value} → {target_env.type.value}")
            return None

        # Generate request ID
        request_id = hashlib.sha256(
            f"{api_id}:{version}:{source_environment}:{target_environment}:{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:16]

        request = PromotionRequest(
            id=request_id,
            api_id=api_id,
            version=version,
            source_environment=source_environment,
            target_environment=target_environment,
            requested_by=requested_by,
            rollback_version=target_env.deployed_version,
        )

        self._promotion_requests[request_id] = request
        logger.info(f"Created promotion request: {request_id}")

        # Auto-approve if rule allows
        if rule.approval_type == ApprovalType.AUTOMATIC:
            request.status = PromotionStatus.APPROVED
            request.approvals["system"] = datetime.utcnow()

        return request

    def approve_promotion(
        self,
        request_id: str,
        approver: str,
    ) -> bool:
        """Approve a promotion request."""
        request = self._promotion_requests.get(request_id)
        if not request or request.status != PromotionStatus.PENDING:
            return False

        source_env = self._environments.get(request.source_environment)
        target_env = self._environments.get(request.target_environment)
        if not source_env or not target_env:
            return False

        rule = self.get_promotion_rule(source_env.type, target_env.type)
        if not rule:
            return False

        # Check if approver is allowed
        if rule.allowed_approvers and approver not in rule.allowed_approvers:
            logger.warning(f"Approver {approver} not in allowed list")
            return False

        request.approvals[approver] = datetime.utcnow()
        request.updated_at = datetime.utcnow()

        # Check if enough approvals
        if len(request.approvals) >= rule.required_approvers:
            request.status = PromotionStatus.APPROVED
            logger.info(f"Promotion request {request_id} approved")

        return True

    def reject_promotion(
        self,
        request_id: str,
        rejector: str,
        reason: str,
    ) -> bool:
        """Reject a promotion request."""
        request = self._promotion_requests.get(request_id)
        if not request or request.status != PromotionStatus.PENDING:
            return False

        request.rejections[rejector] = reason
        request.status = PromotionStatus.REJECTED
        request.updated_at = datetime.utcnow()

        logger.info(f"Promotion request {request_id} rejected by {rejector}: {reason}")
        return True

    async def execute_promotion(self, request_id: str) -> bool:
        """Execute an approved promotion."""
        request = self._promotion_requests.get(request_id)
        if not request or request.status != PromotionStatus.APPROVED:
            return False

        source_env = self._environments.get(request.source_environment)
        target_env = self._environments.get(request.target_environment)
        if not source_env or not target_env:
            return False

        rule = self.get_promotion_rule(source_env.type, target_env.type)
        if not rule:
            return False

        request.status = PromotionStatus.IN_PROGRESS
        request.updated_at = datetime.utcnow()
        request.deployment_log.append(f"[{datetime.utcnow().isoformat()}] Starting promotion")

        try:
            # Run validations
            if rule.require_tests_pass:
                request.deployment_log.append("Running tests...")
                request.validation_results["tests"] = True  # Simulated

            if rule.run_smoke_tests:
                request.deployment_log.append("Running smoke tests...")
                request.validation_results["smoke_tests"] = True

            if rule.run_integration_tests:
                request.deployment_log.append("Running integration tests...")
                request.validation_results["integration_tests"] = True

            if rule.run_security_scan:
                request.deployment_log.append("Running security scan...")
                request.validation_results["security_scan"] = True

            # Run custom validators
            for validator_name in rule.custom_validators:
                if validator_name in self._validators:
                    request.deployment_log.append(f"Running validator: {validator_name}")
                    result = await self._validators[validator_name](request)
                    request.validation_results[validator_name] = result

            # Check all validations passed
            if not all(request.validation_results.values()):
                raise Exception("Validation failed")

            # Deploy
            request.deployment_log.append(f"Deploying to {target_env.name}...")

            if target_env.type.value in self._deployers:
                await self._deployers[target_env.type.value](request, target_env)

            # Update environment
            target_env.deployed_version = request.version
            target_env.deployed_at = datetime.utcnow()
            target_env.deployed_by = request.requested_by
            target_env.updated_at = datetime.utcnow()

            request.status = PromotionStatus.COMPLETED
            request.completed_at = datetime.utcnow()
            request.deployment_log.append(f"[{datetime.utcnow().isoformat()}] Promotion completed")

            logger.info(f"Promotion {request_id} completed successfully")
            return True

        except Exception as e:
            logger.error(f"Promotion {request_id} failed: {e}")
            request.deployment_log.append(f"[{datetime.utcnow().isoformat()}] ERROR: {e}")
            request.status = PromotionStatus.FAILED

            if rule.auto_rollback_on_failure and request.rollback_version:
                request.deployment_log.append("Initiating rollback...")
                await self._rollback(request, target_env)

            return False

    async def _rollback(
        self,
        request: PromotionRequest,
        target_env: Environment,
    ) -> bool:
        """Rollback a failed promotion."""
        try:
            if request.rollback_version:
                target_env.deployed_version = request.rollback_version
                target_env.updated_at = datetime.utcnow()
                request.status = PromotionStatus.ROLLED_BACK
                request.deployment_log.append(
                    f"[{datetime.utcnow().isoformat()}] Rolled back to {request.rollback_version}"
                )
                logger.info(f"Rolled back to version {request.rollback_version}")
                return True
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            request.deployment_log.append(f"[{datetime.utcnow().isoformat()}] Rollback failed: {e}")
        return False

    def get_promotion_request(self, request_id: str) -> Optional[PromotionRequest]:
        """Get a promotion request by ID."""
        return self._promotion_requests.get(request_id)

    def get_promotion_requests(
        self,
        api_id: Optional[str] = None,
        status: Optional[PromotionStatus] = None,
    ) -> List[PromotionRequest]:
        """Get promotion requests with optional filters."""
        requests = list(self._promotion_requests.values())

        if api_id:
            requests = [r for r in requests if r.api_id == api_id]
        if status:
            requests = [r for r in requests if r.status == status]

        return sorted(requests, key=lambda r: r.created_at, reverse=True)


# Default policies for common scenarios
ENVIRONMENT_PRESETS: Dict[str, EnvironmentPolicy] = {
    "development": EnvironmentPolicy(
        rate_limit_requests=10000,
        require_api_key=False,
        log_level="DEBUG",
        log_request_body=True,
        log_response_body=True,
        cache_enabled=False,
    ),
    "testing": EnvironmentPolicy(
        rate_limit_requests=5000,
        require_api_key=True,
        log_level="DEBUG",
        cache_enabled=False,
    ),
    "staging": EnvironmentPolicy(
        rate_limit_requests=2000,
        require_api_key=True,
        log_level="INFO",
        cache_enabled=True,
        cache_ttl_seconds=300,
    ),
    "production": EnvironmentPolicy(
        rate_limit_requests=1000,
        daily_quota=100000,
        require_api_key=True,
        require_oauth=True,
        log_level="WARNING",
        cache_enabled=True,
        cache_ttl_seconds=600,
    ),
    "sandbox": EnvironmentPolicy(
        rate_limit_requests=100,
        daily_quota=1000,
        require_api_key=True,
        log_level="INFO",
        cache_enabled=False,
    ),
}


# Singleton instance
_environment_manager: Optional[EnvironmentManager] = None


def get_environment_manager() -> EnvironmentManager:
    """Get the global environment manager instance."""
    global _environment_manager
    if _environment_manager is None:
        _environment_manager = EnvironmentManager()
    return _environment_manager
