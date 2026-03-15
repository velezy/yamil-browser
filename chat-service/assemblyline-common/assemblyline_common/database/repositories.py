"""
T.A.L.O.S. Database Repositories
Data access layer for all entities
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import json

from .asyncpg_connection import get_connection, get_db_pool
from .models import (
    User, UserCreate, UserResponse,
    Organization, OrganizationCreate, LicenseTier,
    DepartmentAccessGrant,
    Document, DocumentChunk, DocumentStatus,
    Conversation, Message,
    Prompt, PromptCreate, PromptCategory,
    UserSettings, APIKey, APIKeyCreate,
    PromptSuggestion, PromptSuggestionCreate,
    PromptChangeRequest, PromptChangeRequestCreate,
    PromptTest, PromptTestCreate, PromptTestResult, PromptTestResultCreate,
    RiskLevel, ApprovalStatus, ChangeRequestType, TargetType,
)

logger = logging.getLogger(__name__)


# =============================================================================
# USER REPOSITORY
# =============================================================================

class UserRepository:
    """User data access operations"""

    @staticmethod
    async def create(user: UserCreate, hashed_password: str, must_change_password: bool = False) -> User:
        """Create a new user"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO users (email, full_name, password_hash, role, organization_id, department_id)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id, email, full_name as name, password_hash as hashed_password, role, organization_id,
                          department_id as department, is_active, FALSE as must_change_password, created_at, updated_at
            """, user.email, user.name, hashed_password, user.role.value, user.organization_id, user.department)

            return User(**dict(row))

    @staticmethod
    async def get_by_id(user_id: int) -> Optional[User]:
        """Get user by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT id, email, full_name as name, password_hash as hashed_password, role, organization_id,
                       department_id as department, is_active, FALSE as must_change_password, created_at, updated_at
                FROM users WHERE id = $1
            """, user_id)

            return User(**dict(row)) if row else None

    @staticmethod
    async def get_by_email(email: str) -> Optional[User]:
        """Get user by email"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT id, email, full_name as name, password_hash as hashed_password, role, organization_id,
                       department_id as department, is_active, FALSE as must_change_password, created_at, updated_at
                FROM users WHERE email = $1
            """, email)

            return User(**dict(row)) if row else None

    @staticmethod
    async def update_password(user_id: int, hashed_password: str) -> bool:
        """Update user password"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE users SET password_hash = $1, updated_at = CURRENT_TIMESTAMP
                WHERE id = $2
            """, hashed_password, user_id)
            return result == "UPDATE 1"

    @staticmethod
    async def list_all() -> List[User]:
        """List all users"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT id, email, full_name as name, password_hash as hashed_password, role, organization_id,
                       department_id as department, is_active, created_at, updated_at
                FROM users ORDER BY created_at DESC
            """)
            return [User(**dict(row)) for row in rows]

    @staticmethod
    async def list_by_organization(organization_id: int) -> List[User]:
        """List all users in an organization"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT id, email, full_name as name, password_hash as hashed_password, role, organization_id,
                       department_id as department, is_active, FALSE as must_change_password, created_at, updated_at
                FROM users WHERE organization_id = $1 ORDER BY created_at DESC
            """, organization_id)
            return [User(**dict(row)) for row in rows]

    @staticmethod
    async def count() -> int:
        """Count total users in the system"""
        async with get_connection() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM users")


# =============================================================================
# ORGANIZATION REPOSITORY (Multi-tenant support)
# =============================================================================

class OrganizationRepository:
    """Organization data access operations for multi-tenant support"""

    @staticmethod
    async def create(
        name: str,
        license_tier: LicenseTier = LicenseTier.PRO,
        max_users: int = 2,
        settings: Optional[Dict] = None,
        license_key: Optional[str] = None,
        domain: Optional[str] = None
    ) -> Organization:
        """Create a new organization"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO organizations (name, license_tier, max_users, settings, license_key, domain)
                VALUES ($1, $2, $3, $4::jsonb, $5, $6)
                RETURNING *
            """, name, license_tier.value, max_users, json.dumps(settings or {}), license_key, domain)

            return Organization(**dict(row))

    @staticmethod
    async def get_by_license_key(license_key: str) -> Optional[Organization]:
        """Get organization by license key"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM organizations WHERE license_key = $1",
                license_key
            )
            return Organization(**dict(row)) if row else None

    @staticmethod
    async def get_by_id(org_id: int) -> Optional[Organization]:
        """Get organization by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM organizations WHERE id = $1",
                org_id
            )
            return Organization(**dict(row)) if row else None

    @staticmethod
    async def update(
        org_id: int,
        name: Optional[str] = None,
        max_users: Optional[int] = None,
        settings: Optional[Dict] = None
    ) -> Optional[Organization]:
        """Update organization details"""
        async with get_connection() as conn:
            updates = []
            params = []
            param_count = 1

            if name is not None:
                updates.append(f"name = ${param_count}")
                params.append(name)
                param_count += 1

            if max_users is not None:
                updates.append(f"max_users = ${param_count}")
                params.append(max_users)
                param_count += 1

            if settings is not None:
                updates.append(f"settings = ${param_count}::jsonb")
                params.append(json.dumps(settings))
                param_count += 1

            if not updates:
                return await OrganizationRepository.get_by_id(org_id)

            updates.append("updated_at = CURRENT_TIMESTAMP")
            params.append(org_id)

            query = f"""
                UPDATE organizations
                SET {', '.join(updates)}
                WHERE id = ${param_count}
                RETURNING *
            """
            row = await conn.fetchrow(query, *params)
            return Organization(**dict(row)) if row else None

    @staticmethod
    async def get_user_count(org_id: int) -> int:
        """Get the number of users in an organization"""
        async with get_connection() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM users WHERE organization_id = $1",
                org_id
            )
            return count or 0

    @staticmethod
    async def can_add_user(org_id: int) -> bool:
        """Check if organization can add more users"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT o.max_users, COUNT(u.id) as user_count
                FROM organizations o
                LEFT JOIN users u ON u.organization_id = o.id
                WHERE o.id = $1
                GROUP BY o.id
            """, org_id)
            if not row:
                return False
            return row['user_count'] < row['max_users']

    @staticmethod
    async def list_all() -> List[Organization]:
        """List all organizations"""
        async with get_connection() as conn:
            rows = await conn.fetch(
                "SELECT * FROM organizations ORDER BY created_at DESC"
            )
            return [Organization(**dict(row)) for row in rows]

    @staticmethod
    async def delete(org_id: int) -> bool:
        """Delete an organization"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM organizations WHERE id = $1",
                org_id
            )
            return result == "DELETE 1"


# =============================================================================
# DEPARTMENT ACCESS GRANT REPOSITORY (Enterprise cross-department access)
# =============================================================================

class DepartmentAccessGrantRepository:
    """Manage cross-department access grants for enterprise tier"""

    @staticmethod
    async def create(
        user_id: int,
        organization_id: int,
        department: str,
        granted_by: int,
        expires_at: Optional[datetime] = None
    ) -> DepartmentAccessGrant:
        """Grant a user access to another department's documents"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO department_access_grants
                    (user_id, organization_id, department, granted_by, expires_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id, organization_id, department)
                DO UPDATE SET
                    is_active = TRUE,
                    granted_by = EXCLUDED.granted_by,
                    granted_at = CURRENT_TIMESTAMP,
                    expires_at = EXCLUDED.expires_at
                RETURNING *
            """, user_id, organization_id, department, granted_by, expires_at)

            return DepartmentAccessGrant(**dict(row))

    @staticmethod
    async def revoke(
        user_id: int,
        organization_id: int,
        department: str
    ) -> bool:
        """Revoke a user's access to a department"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE department_access_grants
                SET is_active = FALSE
                WHERE user_id = $1 AND organization_id = $2 AND department = $3
            """, user_id, organization_id, department)
            return "UPDATE" in result

    @staticmethod
    async def get_user_grants(user_id: int) -> List[DepartmentAccessGrant]:
        """Get all active access grants for a user"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM department_access_grants
                WHERE user_id = $1
                  AND is_active = TRUE
                  AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
                ORDER BY department
            """, user_id)
            return [DepartmentAccessGrant(**dict(row)) for row in rows]

    @staticmethod
    async def get_accessible_departments(user_id: int, organization_id: int) -> List[str]:
        """Get list of departments a user can access (including their own)"""
        async with get_connection() as conn:
            # Get user's own department
            user_dept = await conn.fetchval(
                "SELECT department FROM users WHERE id = $1",
                user_id
            )

            # Get granted departments
            rows = await conn.fetch("""
                SELECT DISTINCT department FROM department_access_grants
                WHERE user_id = $1
                  AND organization_id = $2
                  AND is_active = TRUE
                  AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            """, user_id, organization_id)

            departments = [row['department'] for row in rows]
            if user_dept and user_dept not in departments:
                departments.insert(0, user_dept)

            return departments

    @staticmethod
    async def has_department_access(
        user_id: int,
        organization_id: int,
        department: str
    ) -> bool:
        """Check if user has access to a specific department"""
        async with get_connection() as conn:
            # Check if it's user's own department
            user_dept = await conn.fetchval(
                "SELECT department FROM users WHERE id = $1 AND organization_id = $2",
                user_id, organization_id
            )
            if user_dept == department:
                return True

            # Check for grant
            grant = await conn.fetchval("""
                SELECT 1 FROM department_access_grants
                WHERE user_id = $1
                  AND organization_id = $2
                  AND department = $3
                  AND is_active = TRUE
                  AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            """, user_id, organization_id, department)

            return grant is not None

    @staticmethod
    async def list_by_organization(organization_id: int) -> List[Dict]:
        """List all access grants for an organization with user details"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT
                    dag.*,
                    u.email as user_email,
                    u.name as user_name,
                    u.department as user_department
                FROM department_access_grants dag
                JOIN users u ON u.id = dag.user_id
                WHERE dag.organization_id = $1
                ORDER BY dag.department, u.email
            """, organization_id)
            return [dict(row) for row in rows]

    @staticmethod
    async def delete(grant_id: int) -> bool:
        """Delete an access grant"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM department_access_grants WHERE id = $1",
                grant_id
            )
            return result == "DELETE 1"

    @staticmethod
    async def cleanup_expired_grants() -> int:
        """
        Deactivate all expired access grants.
        Returns the number of grants deactivated.
        """
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE department_access_grants
                SET is_active = FALSE
                WHERE is_active = TRUE
                  AND expires_at IS NOT NULL
                  AND expires_at < CURRENT_TIMESTAMP
            """)
            # Extract count from result like "UPDATE 5"
            count = int(result.split()[-1]) if result else 0
            return count

    @staticmethod
    async def get_expiring_grants(days: int = 7) -> List[Dict]:
        """
        Get grants that will expire within the specified number of days.
        Useful for sending notifications before grants expire.
        """
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT
                    dag.*,
                    u.email as user_email,
                    u.name as user_name,
                    o.name as organization_name
                FROM department_access_grants dag
                JOIN users u ON u.id = dag.user_id
                JOIN organizations o ON o.id = dag.organization_id
                WHERE dag.is_active = TRUE
                  AND dag.expires_at IS NOT NULL
                  AND dag.expires_at BETWEEN CURRENT_TIMESTAMP
                      AND CURRENT_TIMESTAMP + make_interval(days => $1)
                ORDER BY dag.expires_at ASC
            """, days)
            return [dict(row) for row in rows]


# =============================================================================
# DOCUMENT REPOSITORY
# =============================================================================

class DocumentRepository:
    """Document data access operations"""

    @staticmethod
    async def create(
        filename: str,
        file_hash: str,
        file_size: int,
        file_type: str,
        file_path: Optional[str] = None,
        user_id: Optional[int] = None,
        organization_id: Optional[int] = None,
        department: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> Document:
        """Create a new document"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO documents (filename, file_hash, file_size, file_type, file_path, user_id, organization_id, department, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                RETURNING *
            """, filename, file_hash, file_size, file_type, file_path, user_id, organization_id, department,
               json.dumps(metadata or {}))

            return Document(**dict(row))

    @staticmethod
    async def get_by_id(
        doc_id: int,
        user_id: int,
        include_deleted: bool = False
    ) -> Optional[Document]:
        """Get document by ID - enforces user ownership for security"""
        async with get_connection() as conn:
            if include_deleted:
                row = await conn.fetchrow(
                    "SELECT * FROM documents WHERE id = $1 AND user_id = $2",
                    doc_id, user_id
                )
            else:
                row = await conn.fetchrow(
                    "SELECT * FROM documents WHERE id = $1 AND user_id = $2 AND deleted_at IS NULL",
                    doc_id, user_id
                )
            return Document(**dict(row)) if row else None

    @staticmethod
    async def get_by_hash(file_hash: str, user_id: Optional[int] = None) -> Optional[Document]:
        """Get document by file hash (excludes deleted)"""
        async with get_connection() as conn:
            if user_id:
                row = await conn.fetchrow(
                    "SELECT * FROM documents WHERE file_hash = $1 AND user_id = $2 AND deleted_at IS NULL",
                    file_hash, user_id
                )
            else:
                row = await conn.fetchrow(
                    "SELECT * FROM documents WHERE file_hash = $1 AND deleted_at IS NULL",
                    file_hash
                )
            return Document(**dict(row)) if row else None

    @staticmethod
    async def update_status(
        doc_id: int,
        status: DocumentStatus,
        chunk_count: int = 0,
        error_message: Optional[str] = None
    ) -> bool:
        """Update document processing status"""
        async with get_connection() as conn:
            if status == DocumentStatus.COMPLETED:
                result = await conn.execute("""
                    UPDATE documents
                    SET status = $1, chunk_count = $2, processed_at = CURRENT_TIMESTAMP
                    WHERE id = $3
                """, status.value, chunk_count, doc_id)
            elif status == DocumentStatus.FAILED:
                result = await conn.execute("""
                    UPDATE documents
                    SET status = $1, error_message = $2
                    WHERE id = $3
                """, status.value, error_message, doc_id)
            else:
                result = await conn.execute("""
                    UPDATE documents SET status = $1 WHERE id = $2
                """, status.value, doc_id)
            return "UPDATE 1" in result

    @staticmethod
    async def list_all(
        user_id: int,
        include_deleted: bool = False,
        organization_id: Optional[int] = None,
        accessible_departments: Optional[List[str]] = None,
        is_admin: bool = False
    ) -> List[Document]:
        """List all documents for a user including cross-department access (excludes deleted by default)

        Args:
            user_id: The user's ID
            include_deleted: Whether to include soft-deleted documents
            organization_id: The user's organization ID (for department filtering)
            accessible_departments: List of departments the user can access
            is_admin: Whether the user is an admin (sees all org documents)
        """
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"list_all called: user_id={user_id}, org_id={organization_id}, is_admin={is_admin}")

        async with get_connection() as conn:
            deleted_filter = "" if include_deleted else " AND deleted_at IS NULL"

            # Admin users see ALL documents in their organization PLUS their own documents
            if is_admin and organization_id:
                logger.info(f"Using admin query: org_id={organization_id}, user_id={user_id}")
                rows = await conn.fetch(f"""
                    SELECT * FROM documents
                    WHERE (organization_id = $1 OR user_id = $2){deleted_filter}
                    ORDER BY created_at DESC
                """, organization_id, user_id)
                logger.info(f"Admin query returned {len(rows)} rows")
            # Non-admin with department access
            elif organization_id and accessible_departments:
                # Fetch documents the user owns OR documents in accessible departments
                rows = await conn.fetch(f"""
                    SELECT * FROM documents
                    WHERE (
                        user_id = $1
                        OR (organization_id = $2 AND (department = ANY($3) OR department IS NULL))
                    ){deleted_filter}
                    ORDER BY created_at DESC
                """, user_id, organization_id, accessible_departments)
            else:
                # Fallback to original behavior - only user's own documents
                rows = await conn.fetch(f"""
                    SELECT * FROM documents WHERE user_id = $1{deleted_filter} ORDER BY created_at DESC
                """, user_id)

            return [Document(**dict(row)) for row in rows]

    @staticmethod
    async def delete(doc_id: int, user_id: int) -> bool:
        """Delete a document and its chunks - enforces user ownership"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM documents WHERE id = $1 AND user_id = $2",
                doc_id, user_id
            )
            return "DELETE 1" in result

    # =========================================================================
    # DOCUMENT FLAGGING METHODS (Enterprise feature)
    # =========================================================================

    @staticmethod
    async def flag_document(
        doc_id: int,
        flagged_by: int,
        reason: Optional[str] = None
    ) -> bool:
        """Flag a document - hides it from AI and most users"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE documents
                SET flagged_at = CURRENT_TIMESTAMP,
                    flagged_by = $2,
                    flag_reason = $3
                WHERE id = $1 AND flagged_at IS NULL
            """, doc_id, flagged_by, reason)
            return "UPDATE 1" in result

    @staticmethod
    async def unflag_document(doc_id: int) -> bool:
        """Remove flag from a document"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE documents
                SET flagged_at = NULL,
                    flagged_by = NULL,
                    flag_reason = NULL
                WHERE id = $1 AND flagged_at IS NOT NULL
            """, doc_id)
            return "UPDATE 1" in result

    @staticmethod
    async def get_flagged_documents(
        organization_id: int,
        include_deleted: bool = False
    ) -> List[Document]:
        """Get all flagged documents in an organization (admin only)"""
        async with get_connection() as conn:
            deleted_filter = "" if include_deleted else " AND deleted_at IS NULL"
            rows = await conn.fetch(f"""
                SELECT * FROM documents
                WHERE organization_id = $1
                AND flagged_at IS NOT NULL
                {deleted_filter}
                ORDER BY flagged_at DESC
            """, organization_id)
            return [Document(**dict(row)) for row in rows]

    # =========================================================================
    # DOCUMENT PROTECTION METHODS (Enterprise feature)
    # =========================================================================

    @staticmethod
    async def protect_document(doc_id: int, protected_by: int) -> bool:
        """Protect a document from deletion"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE documents
                SET is_protected = TRUE,
                    protected_by = $2,
                    protected_at = CURRENT_TIMESTAMP
                WHERE id = $1 AND is_protected = FALSE
            """, doc_id, protected_by)
            return "UPDATE 1" in result

    @staticmethod
    async def unprotect_document(doc_id: int) -> bool:
        """Remove protection from a document"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE documents
                SET is_protected = FALSE,
                    protected_by = NULL,
                    protected_at = NULL
                WHERE id = $1 AND is_protected = TRUE
            """, doc_id)
            return "UPDATE 1" in result

    @staticmethod
    async def safe_delete(
        doc_id: int,
        user_id: int,
        is_admin: bool = False
    ) -> tuple[bool, str]:
        """
        Safe delete that respects protection status.
        Returns (success, message) tuple.
        """
        async with get_connection() as conn:
            # Check document exists and get protection status
            doc = await conn.fetchrow(
                "SELECT user_id, is_protected FROM documents WHERE id = $1 AND deleted_at IS NULL",
                doc_id
            )

            if not doc:
                return False, "Document not found"

            if doc['is_protected'] and not is_admin:
                return False, "Document is protected and cannot be deleted"

            if doc['user_id'] != user_id and not is_admin:
                return False, "Permission denied"

            # Soft delete
            result = await conn.execute("""
                UPDATE documents
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE id = $1 AND deleted_at IS NULL
            """, doc_id)

            if "UPDATE 1" in result:
                return True, "Document deleted successfully"
            return False, "Failed to delete document"

    # =========================================================================
    # DOCUMENT VISIBILITY METHODS
    # =========================================================================

    @staticmethod
    async def update_visibility(
        doc_id: int,
        visibility: str,
        user_id: int,
        is_admin: bool = False
    ) -> bool:
        """Update document visibility (private/department/organization)"""
        async with get_connection() as conn:
            if is_admin:
                # Admin can update any document in their org
                result = await conn.execute("""
                    UPDATE documents
                    SET visibility = $2
                    WHERE id = $1
                """, doc_id, visibility)
            else:
                # Regular users can only update their own documents
                result = await conn.execute("""
                    UPDATE documents
                    SET visibility = $2
                    WHERE id = $1 AND user_id = $3
                """, doc_id, visibility, user_id)
            return "UPDATE 1" in result

    @staticmethod
    async def get_accessible_documents(
        user_id: int,
        organization_id: Optional[int] = None,
        user_department: Optional[str] = None,
        accessible_departments: Optional[List[str]] = None,
        is_admin: bool = False,
        can_view_flagged: bool = False,
        include_deleted: bool = False
    ) -> List[Document]:
        """
        Get documents based on visibility and user access level.

        Access rules:
        - Private: Only document owner can see
        - Department: Users in same department or with department grant can see
        - Organization: All users in organization can see
        - Flagged: Only users with can_view_flagged permission can see
        """
        async with get_connection() as conn:
            conditions = ["deleted_at IS NULL"] if not include_deleted else []

            # Filter flagged documents unless user has permission
            if not can_view_flagged:
                conditions.append("flagged_at IS NULL")

            # Build access filter based on role
            if is_admin and organization_id:
                # Admin sees all documents in their organization
                conditions.append("organization_id = $1")
                params = [organization_id]
            elif organization_id:
                # Build complex visibility filter
                visibility_conditions = [
                    "user_id = $1",  # Own documents
                    "(visibility = 'organization' AND organization_id = $2)",  # Org-visible docs
                ]

                # Add department visibility if user has department
                if user_department:
                    visibility_conditions.append(
                        "(visibility = 'department' AND organization_id = $2 AND department = $3)"
                    )
                    params = [user_id, organization_id, user_department]

                    # Add accessible departments from grants
                    if accessible_departments:
                        dept_list = ", ".join(f"'{d}'" for d in accessible_departments)
                        visibility_conditions.append(
                            f"(visibility = 'department' AND organization_id = $2 AND department IN ({dept_list}))"
                        )
                else:
                    params = [user_id, organization_id]

                conditions.append(f"({' OR '.join(visibility_conditions)})")
            else:
                # No organization - only own documents
                conditions.append("user_id = $1")
                params = [user_id]

            where_clause = " AND ".join(conditions)
            query = f"""
                SELECT * FROM documents
                WHERE {where_clause}
                ORDER BY created_at DESC
            """

            rows = await conn.fetch(query, *params)
            return [Document(**dict(row)) for row in rows]

    @staticmethod
    async def get_by_id_with_access_check(
        doc_id: int,
        user_id: int,
        organization_id: Optional[int] = None,
        user_department: Optional[str] = None,
        is_admin: bool = False,
        can_view_flagged: bool = False
    ) -> Optional[Document]:
        """
        Get a document by ID with access control checks.
        Returns None if document doesn't exist or user lacks access.
        """
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM documents WHERE id = $1 AND deleted_at IS NULL",
                doc_id
            )

            if not row:
                return None

            doc = Document(**dict(row))

            # Check flagged access
            if doc.flagged_at and not can_view_flagged:
                return None

            # Check visibility access
            if doc.user_id == user_id:
                return doc  # Owner always has access

            if is_admin and doc.organization_id == organization_id:
                return doc  # Admin has access to all org docs

            if doc.visibility == "organization" and doc.organization_id == organization_id:
                return doc  # Organization-visible

            if doc.visibility == "department":
                if doc.organization_id == organization_id and doc.department == user_department:
                    return doc  # Same department

            return None  # No access


# =============================================================================
# DOCUMENT CHUNK REPOSITORY
# =============================================================================

class ChunkRepository:
    """Document chunk data access operations"""

    @staticmethod
    async def create_batch(
        document_id: int,
        chunks: List[Dict[str, Any]],
        embeddings: List[List[float]]
    ) -> int:
        """Create multiple chunks with embeddings"""
        async with get_connection() as conn:
            count = 0
            for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                await conn.execute("""
                    INSERT INTO document_chunks
                    (document_id, chunk_index, content, embedding, metadata, token_count)
                    VALUES ($1, $2, $3, $4::vector, $5::jsonb, $6)
                    ON CONFLICT (document_id, chunk_index)
                    DO UPDATE SET content = EXCLUDED.content,
                                  embedding = EXCLUDED.embedding,
                                  metadata = EXCLUDED.metadata,
                                  token_count = EXCLUDED.token_count
                """,
                    document_id,
                    i,
                    chunk.get("content", ""),
                    str(embedding),
                    json.dumps(chunk.get("metadata", {})),
                    chunk.get("token_count", len(chunk.get("content", "").split()))
                )
                count += 1
            return count

    @staticmethod
    async def search_similar(
        query_embedding: List[float],
        user_id: int,
        limit: int = 10,
        threshold: float = 0.0,
        document_ids: Optional[List[int]] = None,
        organization_id: Optional[int] = None,
        department: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar chunks - enforces user isolation for security.

        Filtering modes:
        - Default: Only user's own documents (user_id required)
        - Organization: All documents in the organization (organization_id)
        - Department: Documents from users in the same department (organization_id + department)
        """
        async with get_connection() as conn:
            # Set HNSW search parameter
            await conn.execute("SET hnsw.ef_search = 64")

            # Build the user filter based on isolation mode
            if department and organization_id:
                # Department isolation: search documents from users in the same department
                user_filter = """
                    d.user_id IN (
                        SELECT id FROM users
                        WHERE organization_id = $2 AND department = $6
                    )
                """
                extra_params = [organization_id, department]
            elif organization_id:
                # Organization-wide search (admin feature)
                user_filter = """
                    d.user_id IN (
                        SELECT id FROM users WHERE organization_id = $2
                    )
                """
                extra_params = [organization_id]
            else:
                # Default: user's own documents only
                user_filter = "d.user_id = $2"
                extra_params = []

            if document_ids:
                if extra_params:
                    # With organization/department filter
                    if department:
                        rows = await conn.fetch(f"""
                            SELECT
                                dc.id,
                                dc.document_id,
                                dc.chunk_index,
                                dc.content,
                                dc.metadata,
                                d.filename,
                                1 - (dc.embedding <=> $1::vector) as score
                            FROM document_chunks dc
                            JOIN documents d ON d.id = dc.document_id
                            WHERE {user_filter}
                              AND dc.document_id = ANY($3::int[])
                              AND d.deleted_at IS NULL
                              AND d.flagged_at IS NULL
                              AND 1 - (dc.embedding <=> $1::vector) >= $4
                            ORDER BY dc.embedding <=> $1::vector
                            LIMIT $5
                        """, str(query_embedding), organization_id, document_ids, threshold, limit, department)
                    else:
                        rows = await conn.fetch(f"""
                            SELECT
                                dc.id,
                                dc.document_id,
                                dc.chunk_index,
                                dc.content,
                                dc.metadata,
                                d.filename,
                                1 - (dc.embedding <=> $1::vector) as score
                            FROM document_chunks dc
                            JOIN documents d ON d.id = dc.document_id
                            WHERE {user_filter}
                              AND dc.document_id = ANY($3::int[])
                              AND d.deleted_at IS NULL
                              AND d.flagged_at IS NULL
                              AND 1 - (dc.embedding <=> $1::vector) >= $4
                            ORDER BY dc.embedding <=> $1::vector
                            LIMIT $5
                        """, str(query_embedding), organization_id, document_ids, threshold, limit)
                else:
                    rows = await conn.fetch("""
                        SELECT
                            dc.id,
                            dc.document_id,
                            dc.chunk_index,
                            dc.content,
                            dc.metadata,
                            d.filename,
                            1 - (dc.embedding <=> $1::vector) as score
                        FROM document_chunks dc
                        JOIN documents d ON d.id = dc.document_id
                        WHERE d.user_id = $2
                          AND dc.document_id = ANY($3::int[])
                          AND d.deleted_at IS NULL
                          AND d.flagged_at IS NULL
                          AND 1 - (dc.embedding <=> $1::vector) >= $4
                        ORDER BY dc.embedding <=> $1::vector
                        LIMIT $5
                    """, str(query_embedding), user_id, document_ids, threshold, limit)
            else:
                if department and organization_id:
                    rows = await conn.fetch(f"""
                        SELECT
                            dc.id,
                            dc.document_id,
                            dc.chunk_index,
                            dc.content,
                            dc.metadata,
                            d.filename,
                            1 - (dc.embedding <=> $1::vector) as score
                        FROM document_chunks dc
                        JOIN documents d ON d.id = dc.document_id
                        WHERE {user_filter}
                          AND d.deleted_at IS NULL
                          AND d.flagged_at IS NULL
                          AND 1 - (dc.embedding <=> $1::vector) >= $3
                        ORDER BY dc.embedding <=> $1::vector
                        LIMIT $4
                    """, str(query_embedding), organization_id, threshold, limit, department)
                elif organization_id:
                    rows = await conn.fetch(f"""
                        SELECT
                            dc.id,
                            dc.document_id,
                            dc.chunk_index,
                            dc.content,
                            dc.metadata,
                            d.filename,
                            1 - (dc.embedding <=> $1::vector) as score
                        FROM document_chunks dc
                        JOIN documents d ON d.id = dc.document_id
                        WHERE {user_filter}
                          AND d.deleted_at IS NULL
                          AND d.flagged_at IS NULL
                          AND 1 - (dc.embedding <=> $1::vector) >= $3
                        ORDER BY dc.embedding <=> $1::vector
                        LIMIT $4
                    """, str(query_embedding), organization_id, threshold, limit)
                else:
                    rows = await conn.fetch("""
                        SELECT
                            dc.id,
                            dc.document_id,
                            dc.chunk_index,
                            dc.content,
                            dc.metadata,
                            d.filename,
                            1 - (dc.embedding <=> $1::vector) as score
                        FROM document_chunks dc
                        JOIN documents d ON d.id = dc.document_id
                        WHERE d.user_id = $2
                          AND d.deleted_at IS NULL
                          AND d.flagged_at IS NULL
                          AND 1 - (dc.embedding <=> $1::vector) >= $3
                        ORDER BY dc.embedding <=> $1::vector
                        LIMIT $4
                    """, str(query_embedding), user_id, threshold, limit)

            return [dict(row) for row in rows]

    @staticmethod
    async def hybrid_search(
        query_embedding: List[float],
        query_text: str,
        user_id: int,
        limit: int = 10,
        semantic_weight: float = 0.7,
        organization_id: Optional[int] = None,
        department: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Hybrid search combining vector and text search - enforces user isolation.

        Filtering modes:
        - Default: Only user's own documents (user_id required)
        - Organization: All documents in the organization (organization_id)
        - Department: Documents from users in the same department (organization_id + department)
        """
        async with get_connection() as conn:
            await conn.execute("SET hnsw.ef_search = 64")

            # Build the user filter based on isolation mode
            if department and organization_id:
                # Department isolation
                user_filter = f"""
                    d.user_id IN (
                        SELECT id FROM users
                        WHERE organization_id = $3 AND department = $6
                    )
                """
                params = [str(query_embedding), query_text, organization_id, limit, semantic_weight, department]
            elif organization_id:
                # Organization-wide search
                user_filter = f"""
                    d.user_id IN (
                        SELECT id FROM users WHERE organization_id = $3
                    )
                """
                params = [str(query_embedding), query_text, organization_id, limit, semantic_weight]
            else:
                # Default: user's own documents only
                user_filter = "d.user_id = $3"
                params = [str(query_embedding), query_text, user_id, limit, semantic_weight]

            rows = await conn.fetch(f"""
                WITH vector_search AS (
                    SELECT
                        dc.id,
                        dc.document_id,
                        dc.content,
                        dc.metadata,
                        d.filename,
                        1 - (dc.embedding <=> $1::vector) as vector_score
                    FROM document_chunks dc
                    JOIN documents d ON d.id = dc.document_id
                    WHERE {user_filter}
                      AND d.deleted_at IS NULL
                    ORDER BY dc.embedding <=> $1::vector
                    LIMIT $4 * 2
                ),
                text_search AS (
                    SELECT
                        dc.id,
                        ts_rank(to_tsvector('english', dc.content), plainto_tsquery('english', $2)) as text_score
                    FROM document_chunks dc
                    JOIN documents d ON d.id = dc.document_id
                    WHERE {user_filter}
                      AND d.deleted_at IS NULL
                      AND to_tsvector('english', dc.content) @@ plainto_tsquery('english', $2)
                )
                SELECT
                    v.id,
                    v.document_id,
                    v.content,
                    v.metadata,
                    v.filename,
                    v.vector_score,
                    COALESCE(t.text_score, 0) as text_score,
                    (v.vector_score * $5 + COALESCE(t.text_score, 0) * (1 - $5)) as combined_score
                FROM vector_search v
                LEFT JOIN text_search t ON v.id = t.id
                ORDER BY combined_score DESC
                LIMIT $4
            """, *params)

            return [dict(row) for row in rows]

    @staticmethod
    async def get_by_document(document_id: int) -> List[Dict[str, Any]]:
        """Get all chunks for a document"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT id, document_id, chunk_index, content, metadata, token_count
                FROM document_chunks
                WHERE document_id = $1
                ORDER BY chunk_index
            """, document_id)
            return [dict(row) for row in rows]

    @staticmethod
    async def delete_by_document(document_id: int) -> int:
        """Delete all chunks for a document"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM document_chunks WHERE document_id = $1",
                document_id
            )
            return int(result.split()[-1])

    @staticmethod
    async def get_stats() -> Dict[str, Any]:
        """Get chunk statistics"""
        async with get_connection() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM document_chunks")
            doc_count = await conn.fetchval("SELECT COUNT(DISTINCT document_id) FROM document_chunks")
            return {
                "total_chunks": total,
                "total_documents": doc_count,
                "embedding_dim": 384,
                "index_type": "hnsw"
            }


# =============================================================================
# CONVERSATION REPOSITORY
# =============================================================================

class ConversationRepository:
    """Conversation data access operations"""

    @staticmethod
    async def create(title: Optional[str] = None, user_id: Optional[int] = None) -> Conversation:
        """Create a new conversation"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO conversations (title, user_id)
                VALUES ($1, $2)
                RETURNING *
            """, title, user_id)
            return Conversation(**dict(row))

    @staticmethod
    async def get_by_id(conv_id: int, user_id: int) -> Optional[Conversation]:
        """Get conversation by ID - enforces user ownership for security"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM conversations WHERE id = $1 AND user_id = $2",
                conv_id, user_id
            )
            return Conversation(**dict(row)) if row else None

    @staticmethod
    async def update_title(conv_id: int, title: str) -> bool:
        """Update conversation title"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE conversations
                SET title = $1, updated_at = CURRENT_TIMESTAMP
                WHERE id = $2
            """, title, conv_id)
            return "UPDATE 1" in result

    @staticmethod
    async def list_all(user_id: int, limit: int = 50) -> List[Conversation]:
        """List conversations for a user"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM conversations
                WHERE user_id = $1
                ORDER BY updated_at DESC
                LIMIT $2
            """, user_id, limit)
            return [Conversation(**dict(row)) for row in rows]

    @staticmethod
    async def delete(conv_id: int, user_id: int) -> bool:
        """Delete a conversation - enforces user ownership"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM conversations WHERE id = $1 AND user_id = $2",
                conv_id, user_id
            )
            return "DELETE 1" in result


# =============================================================================
# MESSAGE REPOSITORY
# =============================================================================

class MessageRepository:
    """Message data access operations"""

    @staticmethod
    async def create(
        conversation_id: int,
        role: str,
        content: str,
        sources: Optional[List[Dict]] = None,
        agent_used: Optional[str] = None,
        model_used: Optional[str] = None,
        processing_time_ms: Optional[int] = None,
        quality_score: Optional[float] = None,
        quality_grade: Optional[str] = None
    ) -> Message:
        """Create a new message"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO messages
                (conversation_id, role, content, sources, agent_used, model_used, processing_time_ms, quality_score, quality_grade)
                VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9)
                RETURNING *
            """,
                conversation_id,
                role,
                content,
                json.dumps(sources) if sources else None,
                agent_used,
                model_used,
                processing_time_ms,
                quality_score,
                quality_grade
            )

            # Update conversation timestamp
            await conn.execute("""
                UPDATE conversations SET updated_at = CURRENT_TIMESTAMP WHERE id = $1
            """, conversation_id)

            return Message(**dict(row))

    @staticmethod
    async def update_quality(
        message_id: int,
        quality_score: float,
        quality_grade: str
    ) -> Optional[Message]:
        """Update quality metrics for a message"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE messages
                SET quality_score = $2, quality_grade = $3
                WHERE id = $1
                RETURNING *
            """, message_id, quality_score, quality_grade)
            return Message(**dict(row)) if row else None

    @staticmethod
    async def get_by_conversation(conversation_id: int) -> List[Message]:
        """Get all messages for a conversation"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM messages
                WHERE conversation_id = $1
                ORDER BY created_at ASC
            """, conversation_id)
            return [Message(**dict(row)) for row in rows]

    @staticmethod
    async def get_by_id(message_id: int) -> Optional[Message]:
        """Get a message by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM messages WHERE id = $1
            """, message_id)
            return Message(**dict(row)) if row else None


# =============================================================================
# PROMPT REPOSITORY
# =============================================================================

class PromptRepository:
    """Prompt data access operations"""

    @staticmethod
    async def create(prompt: PromptCreate, user_id: Optional[int] = None, created_by: Optional[int] = None) -> Prompt:
        """Create a new prompt"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO prompts (name, description, content, category, user_id, created_by)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING *
            """, prompt.name, prompt.description, prompt.content, prompt.category.value, user_id, created_by or user_id)
            return Prompt(**dict(row))

    @staticmethod
    async def get_by_id(prompt_id: int, include_deleted: bool = False) -> Optional[Prompt]:
        """Get prompt by ID (excludes deleted by default)"""
        async with get_connection() as conn:
            if include_deleted:
                row = await conn.fetchrow("SELECT * FROM prompts WHERE id = $1", prompt_id)
            else:
                row = await conn.fetchrow(
                    "SELECT * FROM prompts WHERE id = $1 AND deleted_at IS NULL",
                    prompt_id
                )
            return Prompt(**dict(row)) if row else None

    @staticmethod
    async def update(prompt_id: int, updated_by: Optional[int] = None, **kwargs) -> Optional[Prompt]:
        """Update a prompt"""
        async with get_connection() as conn:
            # Build dynamic update
            sets = []
            values = []
            for i, (key, value) in enumerate(kwargs.items(), 1):
                if value is not None:
                    sets.append(f"{key} = ${i}")
                    values.append(value)

            if not sets:
                return await PromptRepository.get_by_id(prompt_id)

            # Add updated_by if provided
            if updated_by is not None:
                sets.append(f"updated_by = ${len(values) + 1}")
                values.append(updated_by)

            values.append(prompt_id)
            query = f"""
                UPDATE prompts
                SET {', '.join(sets)}, updated_at = CURRENT_TIMESTAMP
                WHERE id = ${len(values)}
                RETURNING *
            """
            row = await conn.fetchrow(query, *values)
            return Prompt(**dict(row)) if row else None

    @staticmethod
    async def list_all(include_deleted: bool = False) -> List[Prompt]:
        """List all prompts (excludes deleted by default)"""
        async with get_connection() as conn:
            if include_deleted:
                rows = await conn.fetch("SELECT * FROM prompts ORDER BY category, name")
            else:
                rows = await conn.fetch(
                    "SELECT * FROM prompts WHERE deleted_at IS NULL ORDER BY category, name"
                )
            return [Prompt(**dict(row)) for row in rows]

    @staticmethod
    async def get_by_category(category: str, include_deleted: bool = False) -> List[Prompt]:
        """Get all prompts in a category (excludes deleted by default)"""
        async with get_connection() as conn:
            if include_deleted:
                rows = await conn.fetch(
                    "SELECT * FROM prompts WHERE category = $1 ORDER BY name",
                    category
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM prompts WHERE category = $1 AND deleted_at IS NULL ORDER BY name",
                    category
                )
            return [Prompt(**dict(row)) for row in rows]

    @staticmethod
    async def get_by_name(name: str, category: Optional[str] = None, include_deleted: bool = False) -> Optional[Prompt]:
        """Get a prompt by name, optionally filtered by category (excludes deleted by default)"""
        async with get_connection() as conn:
            deleted_filter = "" if include_deleted else " AND deleted_at IS NULL"
            if category:
                row = await conn.fetchrow(
                    f"SELECT * FROM prompts WHERE name = $1 AND category = $2{deleted_filter}",
                    name, category
                )
            else:
                row = await conn.fetchrow(
                    f"SELECT * FROM prompts WHERE name = $1{deleted_filter}",
                    name
                )
            return Prompt(**dict(row)) if row else None

    @staticmethod
    async def get_system_prompt(name: str = "DriveSentinel System Prompt") -> Optional[str]:
        """Get the main system prompt content"""
        prompt = await PromptRepository.get_by_name(name, "system")
        return prompt.content if prompt else None

    @staticmethod
    async def get_guardrails() -> List[Prompt]:
        """Get all guardrail prompts"""
        return await PromptRepository.get_by_category("guardrails")

    @staticmethod
    async def delete(prompt_id: int) -> bool:
        """Delete a prompt"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM prompts WHERE id = $1 AND is_default = FALSE",
                prompt_id
            )
            return "DELETE 1" in result


# =============================================================================
# MODEL MANAGEMENT REPOSITORY
# =============================================================================

class ModelRepository:
    """Ollama model management operations"""

    @staticmethod
    async def list_all() -> List[Dict[str, Any]]:
        """List all configured models"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM ai_models
                ORDER BY is_default DESC, tier, name
            """)
            return [dict(row) for row in rows]

    @staticmethod
    async def get_by_id(model_id: int) -> Optional[Dict[str, Any]]:
        """Get model by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM ai_models WHERE id = $1", model_id)
            return dict(row) if row else None

    @staticmethod
    async def get_by_name(name: str) -> Optional[Dict[str, Any]]:
        """Get model by name"""
        async with get_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM ai_models WHERE name = $1", name)
            return dict(row) if row else None

    @staticmethod
    async def create(
        name: str,
        tier: str = "quality",
        role: str = "general",
        description: str = "",
        is_default: bool = False,
        parameters: Optional[Dict] = None,
        context_length: int = 4096
    ) -> Dict[str, Any]:
        """Add a new model configuration"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO ai_models (name, tier, role, description, is_default, parameters, context_length)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING *
            """, name, tier, role, description, is_default, json.dumps(parameters or {}), context_length)
            return dict(row)

    @staticmethod
    async def update(model_id: int, **kwargs) -> Optional[Dict[str, Any]]:
        """Update a model configuration"""
        async with get_connection() as conn:
            sets = []
            values = []
            for i, (key, value) in enumerate(kwargs.items(), 1):
                if value is not None:
                    if key == 'parameters':
                        value = json.dumps(value)
                    sets.append(f"{key} = ${i}")
                    values.append(value)

            if not sets:
                return await ModelRepository.get_by_id(model_id)

            values.append(model_id)
            query = f"""
                UPDATE ai_models
                SET {', '.join(sets)}, updated_at = CURRENT_TIMESTAMP
                WHERE id = ${len(values)}
                RETURNING *
            """
            row = await conn.fetchrow(query, *values)
            return dict(row) if row else None

    @staticmethod
    async def delete(model_id: int) -> bool:
        """Delete a model configuration"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM ai_models WHERE id = $1 AND is_default = FALSE",
                model_id
            )
            return "DELETE 1" in result

    @staticmethod
    async def set_default(model_id: int, tier: str) -> bool:
        """Set a model as default for a tier"""
        async with get_connection() as conn:
            # First, unset current default for tier
            await conn.execute(
                "UPDATE ai_models SET is_default = FALSE WHERE tier = $1",
                tier
            )
            # Then set the new default
            result = await conn.execute(
                "UPDATE ai_models SET is_default = TRUE WHERE id = $1",
                model_id
            )
            return "UPDATE 1" in result

    @staticmethod
    async def get_default_for_tier(tier: str) -> Optional[Dict[str, Any]]:
        """Get the default model for a tier"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM ai_models WHERE tier = $1 AND is_default = TRUE",
                tier
            )
            return dict(row) if row else None

    @staticmethod
    async def get_by_role(role: str) -> List[Dict[str, Any]]:
        """Get all models assigned to a role"""
        async with get_connection() as conn:
            rows = await conn.fetch(
                "SELECT * FROM ai_models WHERE role = $1 ORDER BY tier, name",
                role
            )
            return [dict(row) for row in rows]


# =============================================================================
# SETTINGS REPOSITORY
# =============================================================================

class SettingsRepository:
    """User settings data access operations"""

    @staticmethod
    async def get_or_create(user_id: int) -> UserSettings:
        """Get user settings or create defaults"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM user_settings WHERE user_id = $1",
                user_id
            )

            if row:
                return UserSettings(**dict(row))

            # Create default settings
            row = await conn.fetchrow("""
                INSERT INTO user_settings (user_id)
                VALUES ($1)
                RETURNING *
            """, user_id)
            return UserSettings(**dict(row))

    @staticmethod
    async def update(user_id: int, **kwargs) -> UserSettings:
        """Update user settings"""
        async with get_connection() as conn:
            # Ensure settings exist
            await SettingsRepository.get_or_create(user_id)

            # Build dynamic update
            sets = []
            values = []
            for i, (key, value) in enumerate(kwargs.items(), 1):
                if value is not None:
                    sets.append(f"{key} = ${i}")
                    values.append(value)

            if not sets:
                return await SettingsRepository.get_or_create(user_id)

            values.append(user_id)
            query = f"""
                UPDATE user_settings
                SET {', '.join(sets)}, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ${len(values)}
                RETURNING *
            """
            row = await conn.fetchrow(query, *values)
            return UserSettings(**dict(row))


# =============================================================================
# SECRET VAULT REPOSITORY
# =============================================================================

class SecretVaultRepository:
    """
    User secret vault data access operations.

    Handles encrypted storage of sensitive credentials like OAuth tokens.
    Uses AES-256-GCM encryption from the security module.
    """

    @staticmethod
    async def store_secret(
        user_id: int,
        secret_key: str,
        encrypted_value: str,
        encryption_metadata: dict,
        category: str = "general",
        description: Optional[str] = None,
        expires_at: Optional[datetime] = None
    ) -> dict:
        """
        Store or update an encrypted secret.

        Args:
            user_id: User ID
            secret_key: Unique key for the secret (e.g., 'email.gmail.access_token')
            encrypted_value: Base64-encoded encrypted value
            encryption_metadata: JSON with salt, nonce, algorithm info
            category: Category (email, oauth, api_key, etc.)
            description: Optional description
            expires_at: Optional expiration timestamp

        Returns:
            The stored secret record (without decrypted value)
        """
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO user_secret_vault
                    (user_id, secret_key, encrypted_value, encryption_metadata,
                     category, description, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (user_id, secret_key)
                DO UPDATE SET
                    encrypted_value = EXCLUDED.encrypted_value,
                    encryption_metadata = EXCLUDED.encryption_metadata,
                    description = COALESCE(EXCLUDED.description, user_secret_vault.description),
                    expires_at = EXCLUDED.expires_at,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id, user_id, secret_key, category, description,
                          expires_at, created_at, updated_at
            """, user_id, secret_key, encrypted_value,
                json.dumps(encryption_metadata), category, description, expires_at)

            return dict(row) if row else None

    @staticmethod
    async def get_secret(user_id: int, secret_key: str) -> Optional[dict]:
        """
        Get an encrypted secret by key.

        Returns the encrypted value and metadata for decryption.
        """
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT id, user_id, secret_key, encrypted_value, encryption_metadata,
                       category, description, expires_at, created_at, updated_at
                FROM user_secret_vault
                WHERE user_id = $1 AND secret_key = $2
            """, user_id, secret_key)

            if row:
                result = dict(row)
                # Parse JSON metadata
                if isinstance(result['encryption_metadata'], str):
                    result['encryption_metadata'] = json.loads(result['encryption_metadata'])
                return result
            return None

    @staticmethod
    async def list_secrets(
        user_id: int,
        category: Optional[str] = None,
        include_expired: bool = False
    ) -> list:
        """
        List all secrets for a user (metadata only, no values).

        Args:
            user_id: User ID
            category: Optional filter by category
            include_expired: Whether to include expired secrets

        Returns:
            List of secret metadata (no encrypted values)
        """
        async with get_connection() as conn:
            query = """
                SELECT id, user_id, secret_key, category, description,
                       expires_at, created_at, updated_at,
                       (expires_at IS NOT NULL AND expires_at < NOW()) as is_expired
                FROM user_secret_vault
                WHERE user_id = $1
            """
            params = [user_id]
            param_idx = 2

            if category:
                query += f" AND category = ${param_idx}"
                params.append(category)
                param_idx += 1

            if not include_expired:
                query += " AND (expires_at IS NULL OR expires_at > NOW())"

            query += " ORDER BY category, secret_key"

            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    @staticmethod
    async def delete_secret(user_id: int, secret_key: str) -> bool:
        """Delete a secret from the vault."""
        async with get_connection() as conn:
            result = await conn.execute("""
                DELETE FROM user_secret_vault
                WHERE user_id = $1 AND secret_key = $2
            """, user_id, secret_key)
            return result == "DELETE 1"

    @staticmethod
    async def delete_category(user_id: int, category: str) -> int:
        """Delete all secrets in a category."""
        async with get_connection() as conn:
            result = await conn.execute("""
                DELETE FROM user_secret_vault
                WHERE user_id = $1 AND category = $2
            """, user_id, category)
            # Parse "DELETE n" to get count
            return int(result.split()[1]) if result else 0

    @staticmethod
    async def cleanup_expired() -> int:
        """Remove all expired secrets (maintenance task)."""
        async with get_connection() as conn:
            result = await conn.execute("""
                DELETE FROM user_secret_vault
                WHERE expires_at IS NOT NULL AND expires_at < NOW()
            """)
            return int(result.split()[1]) if result else 0


# =============================================================================
# API KEY REPOSITORY
# =============================================================================

class APIKeyRepository:
    """API key data access operations"""

    @staticmethod
    async def create(
        user_id: int,
        name: str,
        key_hash: str,
        key_prefix: str,
        expires_in_days: Optional[int] = 90
    ) -> APIKey:
        """Create a new API key"""
        expires_at = None
        if expires_in_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_in_days)

        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO api_keys (user_id, name, key_hash, key_prefix, expires_at)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
            """, user_id, name, key_hash, key_prefix, expires_at)
            return APIKey(**dict(row))

    @staticmethod
    async def get_by_hash(key_hash: str) -> Optional[APIKey]:
        """Get API key by hash"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM api_keys WHERE key_hash = $1",
                key_hash
            )
            return APIKey(**dict(row)) if row else None

    @staticmethod
    async def list_by_user(user_id: int) -> List[Dict[str, Any]]:
        """List user's API keys (without hash)"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT id, name, key_prefix, expires_at, last_used, created_at
                FROM api_keys
                WHERE user_id = $1
                ORDER BY created_at DESC
            """, user_id)
            return [dict(row) for row in rows]

    @staticmethod
    async def delete(key_id: int, user_id: int) -> bool:
        """Delete an API key"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM api_keys WHERE id = $1 AND user_id = $2",
                key_id, user_id
            )
            return "DELETE 1" in result

    @staticmethod
    async def update_last_used(key_id: int) -> None:
        """Update last used timestamp"""
        async with get_connection() as conn:
            await conn.execute(
                "UPDATE api_keys SET last_used = CURRENT_TIMESTAMP WHERE id = $1",
                key_id
            )


# =============================================================================
# PROMPT VERSION REPOSITORY
# =============================================================================

class PromptVersionRepository:
    """Prompt version history operations"""

    @staticmethod
    async def create(prompt_id: int, version: int, content: str,
                     change_summary: Optional[str] = None,
                     created_by: Optional[int] = None) -> dict:
        """Create a new prompt version"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO prompt_versions (prompt_id, version, content, change_summary, created_by)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
            """, prompt_id, version, content, change_summary, created_by)
            return dict(row)

    @staticmethod
    async def get_versions(prompt_id: int) -> List[dict]:
        """Get all versions of a prompt"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT pv.*, u.name as created_by_name
                FROM prompt_versions pv
                LEFT JOIN users u ON pv.created_by = u.id
                WHERE pv.prompt_id = $1
                ORDER BY pv.version DESC
            """, prompt_id)
            return [dict(row) for row in rows]

    @staticmethod
    async def get_version(prompt_id: int, version: int) -> Optional[dict]:
        """Get a specific version of a prompt"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM prompt_versions
                WHERE prompt_id = $1 AND version = $2
            """, prompt_id, version)
            return dict(row) if row else None


# =============================================================================
# GUARDRAIL REPOSITORY
# =============================================================================

class GuardrailRepository:
    """Guardrail data access operations"""

    @staticmethod
    async def create(guardrail_data: dict) -> dict:
        """Create a new guardrail"""
        import json
        async with get_connection() as conn:
            keywords_json = json.dumps(guardrail_data.get('keywords', []))
            row = await conn.fetchrow("""
                INSERT INTO guardrails (
                    name, description, category, pattern, keywords, action,
                    replacement_text, is_active, priority, apply_to_input, apply_to_output
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING *
            """,
                guardrail_data['name'],
                guardrail_data.get('description'),
                guardrail_data['category'],
                guardrail_data.get('pattern'),
                keywords_json,
                guardrail_data.get('action', 'block'),
                guardrail_data.get('replacement_text'),
                guardrail_data.get('is_active', True),
                guardrail_data.get('priority', 0),
                guardrail_data.get('apply_to_input', True),
                guardrail_data.get('apply_to_output', True)
            )
            result = dict(row)
            # Parse keywords JSON
            if result.get('keywords'):
                result['keywords'] = json.loads(result['keywords']) if isinstance(result['keywords'], str) else result['keywords']
            return result

    @staticmethod
    async def get_by_id(guardrail_id: int, include_deleted: bool = False) -> Optional[dict]:
        """Get guardrail by ID (excludes deleted by default)"""
        import json
        async with get_connection() as conn:
            if include_deleted:
                row = await conn.fetchrow("SELECT * FROM guardrails WHERE id = $1", guardrail_id)
            else:
                row = await conn.fetchrow(
                    "SELECT * FROM guardrails WHERE id = $1 AND deleted_at IS NULL",
                    guardrail_id
                )
            if row:
                result = dict(row)
                if result.get('keywords'):
                    result['keywords'] = json.loads(result['keywords']) if isinstance(result['keywords'], str) else result['keywords']
                return result
            return None

    @staticmethod
    async def update(guardrail_id: int, **kwargs) -> Optional[dict]:
        """Update a guardrail"""
        import json
        async with get_connection() as conn:
            # Handle keywords JSON conversion
            if 'keywords' in kwargs and kwargs['keywords'] is not None:
                kwargs['keywords'] = json.dumps(kwargs['keywords'])

            sets = []
            values = []
            for i, (key, value) in enumerate(kwargs.items(), 1):
                if value is not None:
                    sets.append(f"{key} = ${i}")
                    values.append(value)

            if not sets:
                return await GuardrailRepository.get_by_id(guardrail_id)

            values.append(guardrail_id)
            query = f"""
                UPDATE guardrails
                SET {', '.join(sets)}, updated_at = CURRENT_TIMESTAMP
                WHERE id = ${len(values)}
                RETURNING *
            """
            row = await conn.fetchrow(query, *values)
            if row:
                result = dict(row)
                if result.get('keywords'):
                    result['keywords'] = json.loads(result['keywords']) if isinstance(result['keywords'], str) else result['keywords']
                return result
            return None

    @staticmethod
    async def list_all(category: Optional[str] = None, active_only: bool = False, include_deleted: bool = False) -> List[dict]:
        """List all guardrails (excludes deleted by default)"""
        import json
        async with get_connection() as conn:
            query = "SELECT * FROM guardrails WHERE 1=1"
            params = []

            if not include_deleted:
                query += " AND deleted_at IS NULL"

            if category:
                params.append(category)
                query += f" AND category = ${len(params)}"

            if active_only:
                query += " AND is_active = TRUE"

            query += " ORDER BY priority DESC, name"

            rows = await conn.fetch(query, *params)
            results = []
            for row in rows:
                result = dict(row)
                if result.get('keywords'):
                    result['keywords'] = json.loads(result['keywords']) if isinstance(result['keywords'], str) else result['keywords']
                results.append(result)
            return results

    @staticmethod
    async def delete(guardrail_id: int) -> bool:
        """Delete a guardrail"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM guardrails WHERE id = $1",
                guardrail_id
            )
            return "DELETE 1" in result

    @staticmethod
    async def log_violation(guardrail_id: int, original_content: str,
                           action_taken: str, user_id: Optional[int] = None,
                           conversation_id: Optional[int] = None,
                           modified_content: Optional[str] = None) -> dict:
        """Log a guardrail violation"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO guardrail_violations (
                    guardrail_id, user_id, conversation_id, original_content,
                    action_taken, modified_content
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING *
            """, guardrail_id, user_id, conversation_id, original_content,
                action_taken, modified_content)
            return dict(row)

    @staticmethod
    async def get_violations(guardrail_id: Optional[int] = None,
                            limit: int = 100) -> List[dict]:
        """Get guardrail violations"""
        async with get_connection() as conn:
            if guardrail_id:
                rows = await conn.fetch("""
                    SELECT v.*, g.name as guardrail_name
                    FROM guardrail_violations v
                    JOIN guardrails g ON v.guardrail_id = g.id
                    WHERE v.guardrail_id = $1
                    ORDER BY v.created_at DESC
                    LIMIT $2
                """, guardrail_id, limit)
            else:
                rows = await conn.fetch("""
                    SELECT v.*, g.name as guardrail_name
                    FROM guardrail_violations v
                    JOIN guardrails g ON v.guardrail_id = g.id
                    ORDER BY v.created_at DESC
                    LIMIT $1
                """, limit)
            return [dict(row) for row in rows]


# =============================================================================
# PROMPT SUGGESTION REPOSITORY
# =============================================================================

class PromptSuggestionRepository:
    """Prompt suggestion data access operations"""

    @staticmethod
    async def create(suggestion: PromptSuggestionCreate) -> PromptSuggestion:
        """Create a new prompt suggestion"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO prompt_suggestions (
                    suggested_prompt, source_query, category, risk_level, suggested_by
                )
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
            """,
                suggestion.suggested_prompt,
                suggestion.source_query,
                suggestion.category.value,
                suggestion.risk_level.value,
                suggestion.suggested_by
            )
            return PromptSuggestion(**dict(row))

    @staticmethod
    async def get_by_id(suggestion_id: int) -> Optional[PromptSuggestion]:
        """Get suggestion by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM prompt_suggestions WHERE id = $1",
                suggestion_id
            )
            return PromptSuggestion(**dict(row)) if row else None

    @staticmethod
    async def list_all(
        status: Optional[ApprovalStatus] = None,
        limit: int = 100
    ) -> List[PromptSuggestion]:
        """List all suggestions with optional status filter"""
        async with get_connection() as conn:
            if status:
                rows = await conn.fetch("""
                    SELECT * FROM prompt_suggestions
                    WHERE status = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                """, status.value, limit)
            else:
                rows = await conn.fetch("""
                    SELECT * FROM prompt_suggestions
                    ORDER BY created_at DESC
                    LIMIT $1
                """, limit)
            return [PromptSuggestion(**dict(row)) for row in rows]

    @staticmethod
    async def review(
        suggestion_id: int,
        status: ApprovalStatus,
        reviewed_by: int,
        review_notes: Optional[str] = None
    ) -> Optional[PromptSuggestion]:
        """Review (approve/reject) a suggestion"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE prompt_suggestions
                SET status = $1, reviewed_by = $2, review_notes = $3, reviewed_at = CURRENT_TIMESTAMP
                WHERE id = $4
                RETURNING *
            """, status.value, reviewed_by, review_notes, suggestion_id)
            return PromptSuggestion(**dict(row)) if row else None

    @staticmethod
    async def delete(suggestion_id: int) -> bool:
        """Delete a suggestion"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM prompt_suggestions WHERE id = $1",
                suggestion_id
            )
            return "DELETE 1" in result


# =============================================================================
# PROMPT CHANGE REQUEST REPOSITORY
# =============================================================================

class PromptChangeRequestRepository:
    """Prompt change request data access operations"""

    @staticmethod
    async def create(
        request: PromptChangeRequestCreate,
        requested_by: int
    ) -> PromptChangeRequest:
        """Create a new change request"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO prompt_change_requests (
                    request_type, target_type, target_id, proposed_content, risk_level, requested_by
                )
                VALUES ($1, $2, $3, $4::jsonb, $5, $6)
                RETURNING *
            """,
                request.request_type.value,
                request.target_type.value,
                request.target_id,
                json.dumps(request.proposed_content),
                request.risk_level.value,
                requested_by
            )
            return PromptChangeRequest(**dict(row))

    @staticmethod
    async def get_by_id(request_id: int) -> Optional[PromptChangeRequest]:
        """Get change request by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM prompt_change_requests WHERE id = $1",
                request_id
            )
            return PromptChangeRequest(**dict(row)) if row else None

    @staticmethod
    async def list_all(
        status: Optional[ApprovalStatus] = None,
        requested_by: Optional[int] = None,
        limit: int = 100
    ) -> List[PromptChangeRequest]:
        """List all change requests with optional filters"""
        async with get_connection() as conn:
            query = "SELECT * FROM prompt_change_requests WHERE 1=1"
            params = []

            if status:
                params.append(status.value)
                query += f" AND status = ${len(params)}"

            if requested_by:
                params.append(requested_by)
                query += f" AND requested_by = ${len(params)}"

            params.append(limit)
            query += f" ORDER BY created_at DESC LIMIT ${len(params)}"

            rows = await conn.fetch(query, *params)
            return [PromptChangeRequest(**dict(row)) for row in rows]

    @staticmethod
    async def review(
        request_id: int,
        status: ApprovalStatus,
        reviewed_by: int,
        review_notes: Optional[str] = None
    ) -> Optional[PromptChangeRequest]:
        """Review (approve/reject) a change request"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE prompt_change_requests
                SET status = $1, reviewed_by = $2, review_notes = $3, reviewed_at = CURRENT_TIMESTAMP
                WHERE id = $4
                RETURNING *
            """, status.value, reviewed_by, review_notes, request_id)
            return PromptChangeRequest(**dict(row)) if row else None

    @staticmethod
    async def delete(request_id: int) -> bool:
        """Delete a change request"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM prompt_change_requests WHERE id = $1",
                request_id
            )
            return "DELETE 1" in result


# =============================================================================
# PROMPT TEST REPOSITORY
# =============================================================================

class PromptTestRepository:
    """Prompt test data access operations"""

    @staticmethod
    async def create(
        test: PromptTestCreate,
        created_by: Optional[int] = None
    ) -> PromptTest:
        """Create a new prompt test"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO prompt_tests (prompt_id, prompt_content, test_query, created_by)
                VALUES ($1, $2, $3, $4)
                RETURNING *
            """,
                test.prompt_id,
                test.prompt_content,
                test.test_query,
                created_by
            )
            return PromptTest(**dict(row))

    @staticmethod
    async def get_by_id(test_id: int) -> Optional[PromptTest]:
        """Get test by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM prompt_tests WHERE id = $1",
                test_id
            )
            return PromptTest(**dict(row)) if row else None

    @staticmethod
    async def list_all(
        prompt_id: Optional[int] = None,
        created_by: Optional[int] = None,
        limit: int = 100
    ) -> List[PromptTest]:
        """List all tests with optional filters"""
        async with get_connection() as conn:
            query = "SELECT * FROM prompt_tests WHERE 1=1"
            params = []

            if prompt_id:
                params.append(prompt_id)
                query += f" AND prompt_id = ${len(params)}"

            if created_by:
                params.append(created_by)
                query += f" AND created_by = ${len(params)}"

            params.append(limit)
            query += f" ORDER BY created_at DESC LIMIT ${len(params)}"

            rows = await conn.fetch(query, *params)
            return [PromptTest(**dict(row)) for row in rows]

    @staticmethod
    async def add_result(result: PromptTestResultCreate) -> PromptTestResult:
        """Add a test result"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO prompt_test_results (
                    test_id, model, response, quality_score, relevance_score,
                    latency_ms, token_count, error_message
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING *
            """,
                result.test_id,
                result.model,
                result.response,
                result.quality_score,
                result.relevance_score,
                result.latency_ms,
                result.token_count,
                result.error_message
            )
            return PromptTestResult(**dict(row))

    @staticmethod
    async def get_results(test_id: int) -> List[PromptTestResult]:
        """Get all results for a test"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM prompt_test_results
                WHERE test_id = $1
                ORDER BY created_at ASC
            """, test_id)
            return [PromptTestResult(**dict(row)) for row in rows]

    @staticmethod
    async def delete(test_id: int) -> bool:
        """Delete a test and its results (cascade)"""
        async with get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM prompt_tests WHERE id = $1",
                test_id
            )
            return "DELETE 1" in result


# =============================================================================
# GENERAL AUDIT LOG REPOSITORY
# =============================================================================

class GeneralAuditLogRepository:
    """General audit log for system actions (documents, users, settings, etc.)"""

    @staticmethod
    async def log_action(
        user_id: int,
        action: str,
        resource_type: str,
        resource_id: str,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> int:
        """
        Log a general audit event.

        Args:
            user_id: User who performed the action
            action: Action performed (e.g., 'document.flagged', 'document.protected')
            resource_type: Type of resource (e.g., 'document', 'user', 'setting')
            resource_id: ID of the resource
            details: Additional details as JSON
            ip_address: Client IP address
            user_agent: Client user agent

        Returns:
            ID of the created audit log entry
        """
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO audit_log (user_id, action, resource_type, resource_id, details, ip_address, user_agent)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
                RETURNING id
            """, user_id, action, resource_type, resource_id, json.dumps(details or {}), ip_address, user_agent)
            return row['id']

    @staticmethod
    async def log_document_action(
        user_id: int,
        document_id: int,
        action: str,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None
    ) -> int:
        """
        Log a document-related audit event.

        Common actions:
        - document.flagged
        - document.unflagged
        - document.protected
        - document.unprotected
        - document.visibility_changed
        - document.deleted
        - document.delete_blocked
        """
        return await GeneralAuditLogRepository.log_action(
            user_id=user_id,
            action=action,
            resource_type="document",
            resource_id=str(document_id),
            details=details,
            ip_address=ip_address
        )

    @staticmethod
    async def get_document_history(
        document_id: int,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get audit history for a specific document."""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT al.*, u.email as user_email
                FROM audit_log al
                LEFT JOIN users u ON u.id = al.user_id
                WHERE al.resource_type = 'document' AND al.resource_id = $1
                ORDER BY al.created_at DESC
                LIMIT $2
            """, str(document_id), limit)
            return [dict(row) for row in rows]

    @staticmethod
    async def get_user_actions(
        user_id: int,
        action_filter: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get audit history for a specific user's actions."""
        async with get_connection() as conn:
            if action_filter:
                rows = await conn.fetch("""
                    SELECT * FROM audit_log
                    WHERE user_id = $1 AND action LIKE $2
                    ORDER BY created_at DESC
                    LIMIT $3
                """, user_id, f"{action_filter}%", limit)
            else:
                rows = await conn.fetch("""
                    SELECT * FROM audit_log
                    WHERE user_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                """, user_id, limit)
            return [dict(row) for row in rows]


# =============================================================================
# AGENT AUDIT LOG REPOSITORY
# =============================================================================

class AuditLogRepository:
    """Agent audit log data access operations (Industry-standard agentic AI observability)"""

    @staticmethod
    async def log_agent_request(
        user_id: Optional[int],
        query: str,
        request_type: str = "chat",
        conversation_id: Optional[int] = None,
        agent_used: Optional[str] = None,
        model_tier: Optional[str] = None,
        model_used: Optional[str] = None,
        complexity: Optional[str] = None,
        response: Optional[str] = None,
        success: bool = True,
        error_message: Optional[str] = None,
        quality_score: Optional[int] = None,
        quality_grade: Optional[str] = None,
        issues: Optional[List[str]] = None,
        context_used: bool = False,
        context_quality: Optional[float] = None,
        sources: Optional[List[Dict]] = None,
        tool_used: Optional[str] = None,
        tool_result: Optional[Dict] = None,
        query_tokens: Optional[int] = None,
        context_tokens: Optional[int] = None,
        response_tokens: Optional[int] = None,
        efficiency_grade: Optional[str] = None,
        total_latency_ms: Optional[int] = None,
        rag_latency_ms: Optional[int] = None,
        generation_latency_ms: Optional[int] = None,
        quality_latency_ms: Optional[int] = None,
        regeneration_count: int = 0,
        reflection_improved: bool = False,
        memory_learned: bool = False,
        personalization_applied: bool = False,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Log an agent request/response with full metrics"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO agent_audit_log (
                    user_id, conversation_id, query, request_type,
                    agent_used, model_tier, model_used, complexity,
                    response, success, error_message,
                    quality_score, quality_grade, issues,
                    context_used, context_quality, sources,
                    tool_used, tool_result,
                    query_tokens, context_tokens, response_tokens, efficiency_grade,
                    total_latency_ms, rag_latency_ms, generation_latency_ms, quality_latency_ms,
                    regeneration_count, reflection_improved,
                    memory_learned, personalization_applied,
                    ip_address, user_agent, session_id
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14::jsonb, $15, $16, $17::jsonb,
                    $18, $19::jsonb, $20, $21, $22, $23, $24, $25, $26, $27,
                    $28, $29, $30, $31, $32, $33, $34
                )
                RETURNING *
            """,
                user_id, conversation_id, query, request_type,
                agent_used, model_tier, model_used, complexity,
                response, success, error_message,
                quality_score, quality_grade, json.dumps(issues) if issues else '[]',
                context_used, context_quality, json.dumps(sources) if sources else '[]',
                tool_used, json.dumps(tool_result) if tool_result else None,
                query_tokens, context_tokens, response_tokens, efficiency_grade,
                total_latency_ms, rag_latency_ms, generation_latency_ms, quality_latency_ms,
                regeneration_count, reflection_improved,
                memory_learned, personalization_applied,
                ip_address, user_agent, session_id
            )
            return dict(row) if row else {}

    @staticmethod
    async def get_by_id(audit_id: int) -> Optional[Dict[str, Any]]:
        """Get audit log by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agent_audit_log WHERE id = $1",
                audit_id
            )
            return dict(row) if row else None

    @staticmethod
    async def list_all(
        user_id: Optional[int] = None,
        agent_used: Optional[str] = None,
        success: Optional[bool] = None,
        quality_grade: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        search_query: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List audit logs with filters"""
        async with get_connection() as conn:
            query = "SELECT * FROM agent_audit_log WHERE 1=1"
            params = []

            if user_id is not None:
                params.append(user_id)
                query += f" AND user_id = ${len(params)}"

            if agent_used:
                params.append(agent_used)
                query += f" AND agent_used = ${len(params)}"

            if success is not None:
                params.append(success)
                query += f" AND success = ${len(params)}"

            if quality_grade:
                params.append(quality_grade)
                query += f" AND quality_grade = ${len(params)}"

            if start_date:
                params.append(start_date)
                query += f" AND created_at >= ${len(params)}"

            if end_date:
                params.append(end_date)
                query += f" AND created_at <= ${len(params)}"

            if search_query:
                params.append(f"%{search_query}%")
                query += f" AND (query ILIKE ${len(params)} OR response ILIKE ${len(params)})"

            query += " ORDER BY created_at DESC"

            params.append(limit)
            query += f" LIMIT ${len(params)}"

            params.append(offset)
            query += f" OFFSET ${len(params)}"

            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    @staticmethod
    async def get_stats(
        user_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get aggregate statistics for audit logs"""
        async with get_connection() as conn:
            where_clause = "WHERE 1=1"
            params = []

            if user_id is not None:
                params.append(user_id)
                where_clause += f" AND user_id = ${len(params)}"

            if start_date:
                params.append(start_date)
                where_clause += f" AND created_at >= ${len(params)}"

            if end_date:
                params.append(end_date)
                where_clause += f" AND created_at <= ${len(params)}"

            # Main stats
            main_query = f"""
                SELECT
                    COUNT(*) as total_requests,
                    COUNT(*) FILTER (WHERE success = TRUE) as successful_requests,
                    COUNT(*) FILTER (WHERE success = FALSE) as failed_requests,
                    ROUND(AVG(quality_score)::numeric, 1) as avg_quality_score,
                    ROUND(AVG(total_latency_ms)::numeric, 0) as avg_latency_ms,
                    COUNT(*) FILTER (WHERE memory_learned = TRUE) as memory_learned_count,
                    COUNT(*) FILTER (WHERE context_used = TRUE) as context_used_count
                FROM agent_audit_log
                {where_clause}
            """
            main_stats = await conn.fetchrow(main_query, *params)

            # Agent distribution
            agent_query = f"""
                SELECT agent_used, COUNT(*) as count
                FROM agent_audit_log
                {where_clause}
                GROUP BY agent_used
                ORDER BY count DESC
            """
            agent_rows = await conn.fetch(agent_query, *params)

            # Quality distribution
            quality_query = f"""
                SELECT quality_grade, COUNT(*) as count
                FROM agent_audit_log
                {where_clause} AND quality_grade IS NOT NULL
                GROUP BY quality_grade
                ORDER BY quality_grade
            """
            quality_rows = await conn.fetch(quality_query, *params)

            return {
                "total_requests": main_stats["total_requests"] or 0,
                "successful_requests": main_stats["successful_requests"] or 0,
                "failed_requests": main_stats["failed_requests"] or 0,
                "success_rate": round(
                    (main_stats["successful_requests"] or 0) /
                    max(main_stats["total_requests"] or 1, 1) * 100, 1
                ),
                "avg_quality_score": float(main_stats["avg_quality_score"] or 0),
                "avg_latency_ms": int(main_stats["avg_latency_ms"] or 0),
                "memory_learned_count": main_stats["memory_learned_count"] or 0,
                "context_used_count": main_stats["context_used_count"] or 0,
                "by_agent": {row["agent_used"]: row["count"] for row in agent_rows if row["agent_used"]},
                "by_quality": {row["quality_grade"]: row["count"] for row in quality_rows if row["quality_grade"]}
            }

    @staticmethod
    async def count(
        user_id: Optional[int] = None,
        success: Optional[bool] = None
    ) -> int:
        """Get total count of audit logs"""
        async with get_connection() as conn:
            query = "SELECT COUNT(*) FROM agent_audit_log WHERE 1=1"
            params = []

            if user_id is not None:
                params.append(user_id)
                query += f" AND user_id = ${len(params)}"

            if success is not None:
                params.append(success)
                query += f" AND success = ${len(params)}"

            return await conn.fetchval(query, *params) or 0


# =============================================================================
# SYSTEM ERROR LOG REPOSITORY
# =============================================================================

class ErrorLogRepository:
    """System error log data access operations"""

    @staticmethod
    async def log_error(
        error_type: str,
        error_message: str,
        user_id: Optional[int] = None,
        agent_audit_id: Optional[int] = None,
        stack_trace: Optional[str] = None,
        service_name: Optional[str] = None,
        agent_name: Optional[str] = None,
        model_used: Optional[str] = None,
        request_context: Optional[Dict] = None,
        severity: str = "error"
    ) -> Dict[str, Any]:
        """Log a system error"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO system_error_log (
                    user_id, agent_audit_id,
                    error_type, error_message, stack_trace,
                    service_name, agent_name, model_used,
                    request_context, severity
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
                RETURNING *
            """,
                user_id, agent_audit_id,
                error_type, error_message, stack_trace,
                service_name, agent_name, model_used,
                json.dumps(request_context) if request_context else None,
                severity
            )
            return dict(row) if row else {}

    @staticmethod
    async def get_by_id(error_id: int) -> Optional[Dict[str, Any]]:
        """Get error log by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM system_error_log WHERE id = $1",
                error_id
            )
            return dict(row) if row else None

    @staticmethod
    async def list_all(
        user_id: Optional[int] = None,
        service_name: Optional[str] = None,
        severity: Optional[str] = None,
        resolved: Optional[bool] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List error logs with filters"""
        async with get_connection() as conn:
            query = "SELECT * FROM system_error_log WHERE 1=1"
            params = []

            if user_id is not None:
                params.append(user_id)
                query += f" AND user_id = ${len(params)}"

            if service_name:
                params.append(service_name)
                query += f" AND service_name = ${len(params)}"

            if severity:
                params.append(severity)
                query += f" AND severity = ${len(params)}"

            if resolved is not None:
                params.append(resolved)
                query += f" AND resolved = ${len(params)}"

            if start_date:
                params.append(start_date)
                query += f" AND created_at >= ${len(params)}"

            if end_date:
                params.append(end_date)
                query += f" AND created_at <= ${len(params)}"

            query += " ORDER BY created_at DESC"

            params.append(limit)
            query += f" LIMIT ${len(params)}"

            params.append(offset)
            query += f" OFFSET ${len(params)}"

            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    @staticmethod
    async def resolve_error(
        error_id: int,
        resolved_by: int,
        resolution_notes: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Mark an error as resolved"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE system_error_log
                SET resolved = TRUE,
                    resolution_notes = $1,
                    resolved_at = CURRENT_TIMESTAMP,
                    resolved_by = $2
                WHERE id = $3
                RETURNING *
            """, resolution_notes, resolved_by, error_id)
            return dict(row) if row else None

    @staticmethod
    async def get_stats(
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get error statistics"""
        async with get_connection() as conn:
            where_clause = "WHERE 1=1"
            params = []

            if start_date:
                params.append(start_date)
                where_clause += f" AND created_at >= ${len(params)}"

            if end_date:
                params.append(end_date)
                where_clause += f" AND created_at <= ${len(params)}"

            # Main stats
            main_query = f"""
                SELECT
                    COUNT(*) as total_errors,
                    COUNT(*) FILTER (WHERE resolved = TRUE) as resolved_count,
                    COUNT(*) FILTER (WHERE resolved = FALSE) as unresolved_count,
                    COUNT(*) FILTER (WHERE severity = 'critical') as critical_count,
                    COUNT(*) FILTER (WHERE severity = 'error') as error_count,
                    COUNT(*) FILTER (WHERE severity = 'warning') as warning_count
                FROM system_error_log
                {where_clause}
            """
            stats = await conn.fetchrow(main_query, *params)

            # By service
            service_query = f"""
                SELECT service_name, COUNT(*) as count
                FROM system_error_log
                {where_clause}
                GROUP BY service_name
                ORDER BY count DESC
            """
            service_rows = await conn.fetch(service_query, *params)

            # By error type
            type_query = f"""
                SELECT error_type, COUNT(*) as count
                FROM system_error_log
                {where_clause}
                GROUP BY error_type
                ORDER BY count DESC
                LIMIT 10
            """
            type_rows = await conn.fetch(type_query, *params)

            return {
                "total_errors": stats["total_errors"] or 0,
                "resolved_count": stats["resolved_count"] or 0,
                "unresolved_count": stats["unresolved_count"] or 0,
                "critical_count": stats["critical_count"] or 0,
                "error_count": stats["error_count"] or 0,
                "warning_count": stats["warning_count"] or 0,
                "by_service": {row["service_name"]: row["count"] for row in service_rows if row["service_name"]},
                "by_type": {row["error_type"]: row["count"] for row in type_rows if row["error_type"]}
            }

    @staticmethod
    async def count(
        resolved: Optional[bool] = None,
        severity: Optional[str] = None
    ) -> int:
        """Get total count of error logs"""
        async with get_connection() as conn:
            query = "SELECT COUNT(*) FROM system_error_log WHERE 1=1"
            params = []

            if resolved is not None:
                params.append(resolved)
                query += f" AND resolved = ${len(params)}"

            if severity:
                params.append(severity)
                query += f" AND severity = ${len(params)}"

            return await conn.fetchval(query, *params) or 0


# =============================================================================
# KNOWLEDGE GRAPH REPOSITORY
# =============================================================================

class KnowledgeGraphRepository:
    """Repository for knowledge graph entities and relationships."""

    @staticmethod
    async def add_entity(
        name: str,
        entity_type: str,
        importance: float = 0.5,
        description: Optional[str] = None,
        document_id: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Add or update an entity in the knowledge graph."""
        normalized_name = name.lower().strip()
        async with get_connection() as conn:
            # Try to insert or update on conflict
            row = await conn.fetchrow("""
                INSERT INTO kg_entities (name, normalized_name, entity_type, importance, description, document_ids, metadata)
                VALUES ($1, $2, $3, $4, $5,
                        CASE WHEN $6::integer IS NOT NULL THEN ARRAY[$6::integer] ELSE ARRAY[]::integer[] END,
                        $7::jsonb)
                ON CONFLICT (normalized_name) DO UPDATE SET
                    importance = GREATEST(kg_entities.importance, EXCLUDED.importance),
                    description = COALESCE(EXCLUDED.description, kg_entities.description),
                    document_ids = CASE
                        WHEN $6::integer IS NOT NULL AND NOT ($6::integer = ANY(kg_entities.document_ids))
                        THEN array_append(kg_entities.document_ids, $6::integer)
                        ELSE kg_entities.document_ids
                    END,
                    mention_count = kg_entities.mention_count + 1,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING *
            """, name, normalized_name, entity_type, importance, description,
                document_id, json.dumps(metadata) if metadata else '{}')
            return dict(row) if row else None

    @staticmethod
    async def add_relationship(
        source_name: str,
        target_name: str,
        relationship_type: str,
        weight: float = 1.0,
        document_id: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Add a relationship between two entities."""
        async with get_connection() as conn:
            # Get entity IDs
            source_row = await conn.fetchrow(
                "SELECT id FROM kg_entities WHERE normalized_name = $1",
                source_name.lower().strip()
            )
            target_row = await conn.fetchrow(
                "SELECT id FROM kg_entities WHERE normalized_name = $1",
                target_name.lower().strip()
            )

            if not source_row or not target_row:
                return None

            source_id = source_row['id']
            target_id = target_row['id']

            # Insert or update relationship
            row = await conn.fetchrow("""
                INSERT INTO kg_relationships (source_entity_id, target_entity_id, relationship_type, weight, document_ids, metadata)
                VALUES ($1, $2, $3, $4,
                        CASE WHEN $5::integer IS NOT NULL THEN ARRAY[$5::integer] ELSE ARRAY[]::integer[] END,
                        $6::jsonb)
                ON CONFLICT (source_entity_id, target_entity_id, relationship_type) DO UPDATE SET
                    weight = kg_relationships.weight + EXCLUDED.weight,
                    document_ids = CASE
                        WHEN $5::integer IS NOT NULL AND NOT ($5::integer = ANY(kg_relationships.document_ids))
                        THEN array_append(kg_relationships.document_ids, $5::integer)
                        ELSE kg_relationships.document_ids
                    END
                RETURNING *
            """, source_id, target_id, relationship_type, weight, document_id,
                json.dumps(metadata) if metadata else '{}')
            return dict(row) if row else None

    @staticmethod
    async def get_all_entities() -> List[Dict[str, Any]]:
        """Get all entities from the knowledge graph."""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM kg_entities ORDER BY importance DESC, mention_count DESC
            """)
            return [dict(row) for row in rows]

    @staticmethod
    async def get_all_relationships() -> List[Dict[str, Any]]:
        """Get all relationships with entity names."""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT r.*,
                       s.name as source_name, s.normalized_name as source_normalized,
                       t.name as target_name, t.normalized_name as target_normalized
                FROM kg_relationships r
                JOIN kg_entities s ON r.source_entity_id = s.id
                JOIN kg_entities t ON r.target_entity_id = t.id
                ORDER BY r.weight DESC
            """)
            return [dict(row) for row in rows]

    @staticmethod
    async def get_entity_by_name(name: str) -> Optional[Dict[str, Any]]:
        """Get an entity by name."""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM kg_entities WHERE normalized_name = $1",
                name.lower().strip()
            )
            return dict(row) if row else None

    @staticmethod
    async def get_related_entities(name: str) -> List[Dict[str, Any]]:
        """Get entities related to the given entity."""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT e.*, r.relationship_type, r.weight
                FROM kg_entities e
                JOIN kg_relationships r ON (
                    (r.target_entity_id = e.id AND r.source_entity_id = (
                        SELECT id FROM kg_entities WHERE normalized_name = $1
                    ))
                    OR
                    (r.source_entity_id = e.id AND r.target_entity_id = (
                        SELECT id FROM kg_entities WHERE normalized_name = $1
                    ))
                )
                WHERE e.normalized_name != $1
                ORDER BY r.weight DESC
            """, name.lower().strip())
            return [dict(row) for row in rows]

    @staticmethod
    async def get_stats() -> Dict[str, Any]:
        """Get knowledge graph statistics."""
        async with get_connection() as conn:
            entity_count = await conn.fetchval("SELECT COUNT(*) FROM kg_entities") or 0
            relationship_count = await conn.fetchval("SELECT COUNT(*) FROM kg_relationships") or 0

            # Entity types distribution
            type_rows = await conn.fetch("""
                SELECT entity_type, COUNT(*) as count
                FROM kg_entities
                GROUP BY entity_type
                ORDER BY count DESC
            """)
            entity_types = {row['entity_type']: row['count'] for row in type_rows}

            # Relationship types distribution
            rel_rows = await conn.fetch("""
                SELECT relationship_type, COUNT(*) as count
                FROM kg_relationships
                GROUP BY relationship_type
                ORDER BY count DESC
            """)
            relationship_types = {row['relationship_type']: row['count'] for row in rel_rows}

            return {
                "entity_count": entity_count,
                "relationship_count": relationship_count,
                "entity_types": entity_types,
                "relationship_types": relationship_types
            }

    @staticmethod
    async def search_entities(
        query: str,
        entity_type: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Search entities by name or description."""
        async with get_connection() as conn:
            params = [f"%{query.lower()}%", limit]
            sql = """
                SELECT * FROM kg_entities
                WHERE (normalized_name LIKE $1 OR description ILIKE $1)
            """
            if entity_type:
                params.insert(1, entity_type)
                sql += " AND entity_type = $2"
                sql += f" ORDER BY importance DESC LIMIT ${len(params)}"
            else:
                sql += " ORDER BY importance DESC LIMIT $2"

            rows = await conn.fetch(sql, *params)
            return [dict(row) for row in rows]

    @staticmethod
    async def delete_by_document(document_id: int) -> int:
        """Remove entities and relationships for a deleted document."""
        async with get_connection() as conn:
            # Remove document from entity arrays and delete orphaned entities
            await conn.execute("""
                UPDATE kg_entities
                SET document_ids = array_remove(document_ids, $1)
                WHERE $1 = ANY(document_ids)
            """, document_id)

            # Delete entities with no document references
            deleted = await conn.execute("""
                DELETE FROM kg_entities
                WHERE document_ids = '{}'
            """)

            return deleted.split()[-1] if deleted else 0

    @staticmethod
    async def clear_all() -> None:
        """Clear all entities and relationships."""
        async with get_connection() as conn:
            await conn.execute("TRUNCATE kg_relationships CASCADE")
            await conn.execute("TRUNCATE kg_entities CASCADE")


# =============================================================================
# REASONING TRACE REPOSITORY
# =============================================================================

class ReasoningTraceRepository:
    """Repository for storing LRM thinking traces for self-learning."""

    @staticmethod
    async def save_trace(
        query: str,
        reasoning_steps: List[Dict[str, Any]],
        sub_questions: List[Dict[str, Any]],
        final_confidence: float,
        detected_intent: str,
        retrieval_attempts: int = 1,
        backtrack_count: int = 0,
        total_reasoning_time_ms: int = 0,
        was_successful: bool = True,
        failure_reason: Optional[str] = None,
        conversation_id: Optional[int] = None,
        user_id: Optional[int] = None
    ) -> Optional[int]:
        """Save a reasoning trace to the database."""
        async with get_connection() as conn:
            trace_id = await conn.fetchval("""
                INSERT INTO reasoning_traces (
                    conversation_id, user_id, query, reasoning_steps, sub_questions,
                    retrieval_attempts, final_confidence, backtrack_count,
                    total_reasoning_time_ms, was_successful, failure_reason, detected_intent
                ) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8, $9, $10, $11, $12)
                RETURNING id
            """,
                conversation_id, user_id, query,
                json.dumps(reasoning_steps), json.dumps(sub_questions),
                retrieval_attempts, final_confidence, backtrack_count,
                total_reasoning_time_ms, was_successful, failure_reason, detected_intent
            )
            return trace_id

    @staticmethod
    async def get_recent_traces(
        user_id: Optional[int] = None,
        limit: int = 100,
        successful_only: bool = False
    ) -> List[Dict[str, Any]]:
        """Get recent reasoning traces for analysis."""
        async with get_connection() as conn:
            conditions = []
            params = []

            if user_id:
                params.append(user_id)
                conditions.append(f"user_id = ${len(params)}")

            if successful_only:
                conditions.append("was_successful = TRUE")

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            params.append(limit)

            rows = await conn.fetch(f"""
                SELECT * FROM reasoning_traces
                {where_clause}
                ORDER BY created_at DESC
                LIMIT ${len(params)}
            """, *params)
            return [dict(row) for row in rows]

    @staticmethod
    async def get_failed_traces(limit: int = 50) -> List[Dict[str, Any]]:
        """Get failed reasoning traces for debugging."""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM reasoning_traces
                WHERE was_successful = FALSE
                ORDER BY created_at DESC
                LIMIT $1
            """, limit)
            return [dict(row) for row in rows]

    @staticmethod
    async def get_stats() -> Dict[str, Any]:
        """Get reasoning trace statistics."""
        async with get_connection() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM reasoning_traces") or 0
            successful = await conn.fetchval(
                "SELECT COUNT(*) FROM reasoning_traces WHERE was_successful = TRUE"
            ) or 0
            avg_confidence = await conn.fetchval(
                "SELECT AVG(final_confidence) FROM reasoning_traces"
            ) or 0.0
            avg_time = await conn.fetchval(
                "SELECT AVG(total_reasoning_time_ms) FROM reasoning_traces"
            ) or 0

            return {
                "total_traces": total,
                "successful_traces": successful,
                "success_rate": successful / total if total > 0 else 0,
                "avg_confidence": round(float(avg_confidence), 3),
                "avg_reasoning_time_ms": round(float(avg_time), 1)
            }


# =============================================================================
# SEMANTIC CACHE REPOSITORY
# =============================================================================

class SemanticCacheRepository:
    """Repository for semantic caching with embedding similarity."""

    @staticmethod
    async def ensure_table() -> None:
        """Ensure semantic cache table exists."""
        async with get_connection() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS semantic_cache (
                    id SERIAL PRIMARY KEY,
                    query_hash VARCHAR(64) NOT NULL,
                    query_text TEXT NOT NULL,
                    query_embedding vector(384),
                    response TEXT NOT NULL,
                    metadata JSONB DEFAULT '{}',
                    hit_count INTEGER DEFAULT 0,
                    ttl_seconds INTEGER DEFAULT 3600,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_semantic_cache_hash ON semantic_cache(query_hash);
                CREATE INDEX IF NOT EXISTS idx_semantic_cache_expires ON semantic_cache(expires_at);
                CREATE INDEX IF NOT EXISTS idx_semantic_cache_embedding
                    ON semantic_cache USING hnsw (query_embedding vector_cosine_ops)
                    WITH (m = 16, ef_construction = 64);
            """)

    @staticmethod
    async def get_by_similarity(
        query_embedding: List[float],
        similarity_threshold: float = 0.92,
        limit: int = 1
    ) -> Optional[Dict[str, Any]]:
        """Find cached response by embedding similarity."""
        async with get_connection() as conn:
            # Find similar queries using cosine similarity
            row = await conn.fetchrow("""
                SELECT
                    id, query_text, response, metadata, hit_count,
                    1 - (query_embedding <=> $1::vector) as similarity
                FROM semantic_cache
                WHERE expires_at IS NULL OR expires_at > NOW()
                ORDER BY query_embedding <=> $1::vector
                LIMIT 1
            """, str(query_embedding))

            if row and row['similarity'] >= similarity_threshold:
                # Update hit count
                await conn.execute("""
                    UPDATE semantic_cache
                    SET hit_count = hit_count + 1, last_accessed = NOW()
                    WHERE id = $1
                """, row['id'])

                return {
                    "query_text": row['query_text'],
                    "response": row['response'],
                    "metadata": row['metadata'],
                    "similarity": float(row['similarity']),
                    "hit_count": row['hit_count'] + 1
                }
            return None

    @staticmethod
    async def save(
        query_text: str,
        query_embedding: List[float],
        response: str,
        metadata: Optional[Dict[str, Any]] = None,
        ttl_seconds: int = 3600
    ) -> int:
        """Save response to semantic cache."""
        import hashlib
        query_hash = hashlib.sha256(query_text.encode()).hexdigest()

        async with get_connection() as conn:
            cache_id = await conn.fetchval("""
                INSERT INTO semantic_cache (
                    query_hash, query_text, query_embedding, response,
                    metadata, ttl_seconds, expires_at
                ) VALUES ($1, $2, $3::vector, $4, $5::jsonb, $6, NOW() + INTERVAL '1 second' * $6)
                ON CONFLICT (query_hash) DO UPDATE SET
                    response = EXCLUDED.response,
                    metadata = EXCLUDED.metadata,
                    last_accessed = NOW(),
                    expires_at = NOW() + INTERVAL '1 second' * EXCLUDED.ttl_seconds
                RETURNING id
            """, query_hash, query_text, str(query_embedding), response,
                json.dumps(metadata or {}), ttl_seconds)
            return cache_id

    @staticmethod
    async def invalidate_expired() -> int:
        """Remove expired cache entries."""
        async with get_connection() as conn:
            result = await conn.execute("""
                DELETE FROM semantic_cache
                WHERE expires_at IS NOT NULL AND expires_at < NOW()
            """)
            return int(result.split()[-1]) if result else 0

    @staticmethod
    async def get_stats() -> Dict[str, Any]:
        """Get cache statistics."""
        async with get_connection() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM semantic_cache") or 0
            total_hits = await conn.fetchval(
                "SELECT SUM(hit_count) FROM semantic_cache"
            ) or 0
            avg_hits = await conn.fetchval(
                "SELECT AVG(hit_count) FROM semantic_cache"
            ) or 0

            return {
                "total_entries": total,
                "total_hits": int(total_hits),
                "avg_hits_per_entry": round(float(avg_hits), 2)
            }


# =============================================================================
# LEARNING EVENT REPOSITORY
# =============================================================================

class LearningEventRepository:
    """Repository for tracking system learning events."""

    @staticmethod
    async def record_event(
        event_type: str,
        source_agent: str,
        description: str,
        before_state: Optional[Dict[str, Any]] = None,
        after_state: Optional[Dict[str, Any]] = None,
        impact_score: Optional[float] = None
    ) -> int:
        """Record a learning event."""
        async with get_connection() as conn:
            event_id = await conn.fetchval("""
                INSERT INTO learning_events (
                    event_type, source_agent, description,
                    before_state, after_state, impact_score
                ) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)
                RETURNING id
            """,
                event_type, source_agent, description,
                json.dumps(before_state) if before_state else None,
                json.dumps(after_state) if after_state else None,
                impact_score
            )
            return event_id

    @staticmethod
    async def get_recent_events(
        event_type: Optional[str] = None,
        source_agent: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get recent learning events."""
        async with get_connection() as conn:
            conditions = []
            params = []

            if event_type:
                params.append(event_type)
                conditions.append(f"event_type = ${len(params)}")

            if source_agent:
                params.append(source_agent)
                conditions.append(f"source_agent = ${len(params)}")

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            params.append(limit)

            rows = await conn.fetch(f"""
                SELECT * FROM learning_events
                {where_clause}
                ORDER BY created_at DESC
                LIMIT ${len(params)}
            """, *params)
            return [dict(row) for row in rows]

    @staticmethod
    async def get_stats() -> Dict[str, Any]:
        """Get learning event statistics."""
        async with get_connection() as conn:
            # Event type distribution
            type_rows = await conn.fetch("""
                SELECT event_type, COUNT(*) as count
                FROM learning_events
                GROUP BY event_type
                ORDER BY count DESC
            """)
            by_type = {row['event_type']: row['count'] for row in type_rows}

            # Agent distribution
            agent_rows = await conn.fetch("""
                SELECT source_agent, COUNT(*) as count
                FROM learning_events
                GROUP BY source_agent
                ORDER BY count DESC
            """)
            by_agent = {row['source_agent']: row['count'] for row in agent_rows}

            total = await conn.fetchval("SELECT COUNT(*) FROM learning_events") or 0
            avg_impact = await conn.fetchval(
                "SELECT AVG(impact_score) FROM learning_events WHERE impact_score IS NOT NULL"
            ) or 0.0

            return {
                "total_events": total,
                "by_event_type": by_type,
                "by_source_agent": by_agent,
                "avg_impact_score": round(float(avg_impact), 3)
            }


# =============================================================================
# TRASH REPOSITORY
# =============================================================================

class TrashRepository:
    """
    Unified trash bin repository for soft-deleted items.

    Handles restore and permanent delete operations for:
    - Documents
    - Guardrails
    - System Prompts
    - Memories
    """

    # Mapping of item types to their table and key columns
    ITEM_TYPES = {
        'document': {
            'table': 'documents',
            'name_column': 'filename',
            'user_column': 'user_id',
            'metadata_columns': ['file_size', 'file_type']
        },
        'guardrail': {
            'table': 'guardrails',
            'name_column': 'name',
            'user_column': 'created_by',  # guardrails uses created_by instead of user_id
            'metadata_columns': ['category', 'action']
        },
        'prompt': {
            'table': 'prompts',
            'name_column': 'name',
            'user_column': 'user_id',
            'metadata_columns': ['category']
        },
        'memory': {
            'table': 'user_memories',
            'name_column': 'content',
            'user_column': 'user_id',
            'metadata_columns': ['source', 'category']
        },
        'email': {
            'table': 'email_messages',
            'name_column': 'subject',
            'user_column': 'user_id',
            'metadata_columns': ['from_address', 'from_name', 'email_date'],
            'use_sync_status': True  # Emails use sync_status='deleted' instead of deleted_at
        }
    }

    @staticmethod
    async def list_trash_items(
        user_id: int,
        item_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        List all items in trash for a user.

        Args:
            user_id: The user's ID
            item_type: Optional filter by item type (document, guardrail, prompt, memory, email)
            limit: Maximum items to return
            offset: Number of items to skip

        Returns:
            List of trash items with metadata
        """
        async with get_connection() as conn:
            if item_type and item_type in TrashRepository.ITEM_TYPES:
                # Query single type
                type_info = TrashRepository.ITEM_TYPES[item_type]
                table = type_info['table']
                name_col = type_info['name_column']
                user_col = type_info.get('user_column', 'user_id')
                meta_cols = type_info['metadata_columns']

                # Build metadata jsonb
                meta_json = ', '.join([f"'{col}', {col}" for col in meta_cols])

                # Emails use sync_status='deleted' instead of deleted_at
                if type_info.get('use_sync_status'):
                    query = f"""
                        SELECT
                            id,
                            '{item_type}' as item_type,
                            COALESCE({name_col}, 'No Subject') as name,
                            {user_col} as user_id,
                            COALESCE(deleted_at, updated_at) as deleted_at,
                            jsonb_build_object({meta_json}) as metadata
                        FROM {table}
                        WHERE sync_status = 'deleted' AND ({user_col} = $1 OR {user_col} IS NULL)
                        ORDER BY COALESCE(deleted_at, updated_at) DESC
                        LIMIT $2 OFFSET $3
                    """
                else:
                    query = f"""
                        SELECT
                            id,
                            '{item_type}' as item_type,
                            {name_col} as name,
                            {user_col} as user_id,
                            deleted_at,
                            jsonb_build_object({meta_json}) as metadata
                        FROM {table}
                        WHERE deleted_at IS NOT NULL AND ({user_col} = $1 OR {user_col} IS NULL)
                        ORDER BY deleted_at DESC
                        LIMIT $2 OFFSET $3
                    """
                rows = await conn.fetch(query, user_id, limit, offset)
            else:
                # Query using the trash_items view (handle NULL user_id)
                rows = await conn.fetch("""
                    SELECT * FROM trash_items
                    WHERE (user_id = $1 OR user_id IS NULL)
                    ORDER BY deleted_at DESC
                    LIMIT $2 OFFSET $3
                """, user_id, limit, offset)

            return [dict(row) for row in rows]

    @staticmethod
    async def count_trash_items(
        user_id: int,
        item_type: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Count items in trash by type.

        Args:
            user_id: The user's ID
            item_type: Optional filter by item type

        Returns:
            Dict with counts by type and total
        """
        async with get_connection() as conn:
            counts = {}

            for type_name, type_info in TrashRepository.ITEM_TYPES.items():
                if item_type and item_type != type_name:
                    continue

                table = type_info['table']
                user_col = type_info.get('user_column', 'user_id')

                # Emails use sync_status='deleted' instead of deleted_at
                if type_info.get('use_sync_status'):
                    count = await conn.fetchval(f"""
                        SELECT COUNT(*) FROM {table}
                        WHERE sync_status = 'deleted' AND ({user_col} = $1 OR {user_col} IS NULL)
                    """, user_id)
                else:
                    count = await conn.fetchval(f"""
                        SELECT COUNT(*) FROM {table}
                        WHERE deleted_at IS NOT NULL AND ({user_col} = $1 OR {user_col} IS NULL)
                    """, user_id)
                counts[type_name] = count or 0

            counts['total'] = sum(counts.values())
            return counts

    @staticmethod
    async def restore_item(
        item_type: str,
        item_id: int,
        user_id: int
    ) -> bool:
        """
        Restore an item from trash.

        Args:
            item_type: Type of item (document, guardrail, prompt, memory, email)
            item_id: ID of the item
            user_id: User ID (for authorization)

        Returns:
            True if restored successfully
        """
        if item_type not in TrashRepository.ITEM_TYPES:
            raise ValueError(f"Unknown item type: {item_type}")

        type_info = TrashRepository.ITEM_TYPES[item_type]
        table = type_info['table']
        user_col = type_info.get('user_column', 'user_id')

        async with get_connection() as conn:
            # Emails use sync_status='deleted' instead of deleted_at
            if type_info.get('use_sync_status'):
                result = await conn.execute(f"""
                    UPDATE {table}
                    SET sync_status = 'synced', deleted_at = NULL
                    WHERE id = $1 AND ({user_col} = $2 OR {user_col} IS NULL) AND sync_status = 'deleted'
                """, item_id, user_id)
            else:
                # Handle NULL user_id and use correct user column
                result = await conn.execute(f"""
                    UPDATE {table}
                    SET deleted_at = NULL
                    WHERE id = $1 AND ({user_col} = $2 OR {user_col} IS NULL) AND deleted_at IS NOT NULL
                """, item_id, user_id)

            return "UPDATE 1" in result

    @staticmethod
    async def permanent_delete(
        item_type: str,
        item_id: int,
        user_id: int
    ) -> bool:
        """
        Permanently delete an item from trash.

        Args:
            item_type: Type of item (document, guardrail, prompt, memory, email)
            item_id: ID of the item
            user_id: User ID (for authorization)

        Returns:
            True if deleted successfully
        """
        if item_type not in TrashRepository.ITEM_TYPES:
            raise ValueError(f"Unknown item type: {item_type}")

        type_info = TrashRepository.ITEM_TYPES[item_type]
        table = type_info['table']
        user_col = type_info.get('user_column', 'user_id')

        async with get_connection() as conn:
            # Emails use sync_status='deleted' instead of deleted_at
            if type_info.get('use_sync_status'):
                result = await conn.execute(f"""
                    DELETE FROM {table}
                    WHERE id = $1 AND ({user_col} = $2 OR {user_col} IS NULL) AND sync_status = 'deleted'
                """, item_id, user_id)
            else:
                # Only delete if already in trash (deleted_at is not null)
                # Handle NULL user_id and use correct user column
                result = await conn.execute(f"""
                    DELETE FROM {table}
                    WHERE id = $1 AND ({user_col} = $2 OR {user_col} IS NULL) AND deleted_at IS NOT NULL
                """, item_id, user_id)

            return "DELETE 1" in result

    @staticmethod
    async def empty_trash(
        user_id: int,
        item_type: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Permanently delete all items in trash.

        Args:
            user_id: The user's ID
            item_type: Optional filter by item type (delete only that type)

        Returns:
            Dict with count of items deleted by type
        """
        deleted_counts = {}

        async with get_connection() as conn:
            for type_name, type_info in TrashRepository.ITEM_TYPES.items():
                if item_type and item_type != type_name:
                    continue

                table = type_info['table']
                user_col = type_info.get('user_column', 'user_id')

                # Emails use sync_status='deleted' instead of deleted_at
                if type_info.get('use_sync_status'):
                    result = await conn.execute(f"""
                        DELETE FROM {table}
                        WHERE sync_status = 'deleted' AND {user_col} = $1
                    """, user_id)
                else:
                    result = await conn.execute(f"""
                        DELETE FROM {table}
                        WHERE deleted_at IS NOT NULL AND {user_col} = $1
                    """, user_id)

                # Parse count from result like "DELETE 5"
                try:
                    count = int(result.split()[-1])
                except Exception:
                    count = 0
                deleted_counts[type_name] = count

            deleted_counts['total'] = sum(deleted_counts.values())
            return deleted_counts

    @staticmethod
    async def soft_delete_document(doc_id: int, user_id: int = None) -> bool:
        """Soft delete a document (move to trash)"""
        async with get_connection() as conn:
            # Handle NULL user_id - allow deletion if user_id matches or is NULL
            result = await conn.execute("""
                UPDATE documents
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE id = $1 AND (user_id = $2 OR user_id IS NULL) AND deleted_at IS NULL
            """, doc_id, user_id)
            return "UPDATE 1" in result

    @staticmethod
    async def soft_delete_guardrail(guardrail_id: int, user_id: int = None) -> bool:
        """Soft delete a guardrail (move to trash)"""
        async with get_connection() as conn:
            # Guardrails use created_by instead of user_id
            result = await conn.execute("""
                UPDATE guardrails
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE id = $1 AND (created_by = $2 OR created_by IS NULL) AND deleted_at IS NULL
            """, guardrail_id, user_id)
            return "UPDATE 1" in result

    @staticmethod
    async def soft_delete_prompt(prompt_id: int, user_id: int) -> bool:
        """Soft delete a prompt (move to trash)"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE prompts
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE id = $1 AND (user_id = $2 OR user_id IS NULL) AND deleted_at IS NULL
            """, prompt_id, user_id)
            return "UPDATE 1" in result

    @staticmethod
    async def soft_delete_memory(memory_id: int, user_id: int) -> bool:
        """Soft delete a memory (move to trash)"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE user_memories
                SET deleted_at = CURRENT_TIMESTAMP
                WHERE id = $1 AND user_id = $2 AND deleted_at IS NULL
            """, memory_id, user_id)
            return "UPDATE 1" in result

    @staticmethod
    async def get_item_details(
        item_type: str,
        item_id: int,
        user_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get details of a specific trash item.

        Args:
            item_type: Type of item
            item_id: ID of the item
            user_id: User ID (for authorization)

        Returns:
            Item details or None if not found
        """
        if item_type not in TrashRepository.ITEM_TYPES:
            return None

        table = TrashRepository.ITEM_TYPES[item_type]['table']

        async with get_connection() as conn:
            row = await conn.fetchrow(f"""
                SELECT * FROM {table}
                WHERE id = $1 AND user_id = $2 AND deleted_at IS NOT NULL
            """, item_id, user_id)

            return dict(row) if row else None


# =============================================================================
# EMAIL RAG REPOSITORIES
# =============================================================================

class EmailMessageRepository:
    """Email message data access for RAG integration"""

    @staticmethod
    async def create(
        user_id: int,
        gmail_id: str,
        account_email: str,
        organization_id: Optional[int] = None,
        thread_id: Optional[str] = None,
        from_address: Optional[str] = None,
        from_name: Optional[str] = None,
        to_addresses: Optional[List[str]] = None,
        cc_addresses: Optional[List[str]] = None,
        subject: Optional[str] = None,
        body_text: Optional[str] = None,
        body_html: Optional[str] = None,
        snippet: Optional[str] = None,
        labels: Optional[List[str]] = None,
        is_read: bool = False,
        is_starred: bool = False,
        has_attachments: bool = False,
        email_date: Optional[datetime] = None,
        internal_date: Optional[int] = None,
        raw_headers: Optional[Dict] = None
    ) -> Dict:
        """Create or update an email message"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO email_messages (
                    user_id, organization_id, gmail_id, thread_id, account_email,
                    from_address, from_name, to_addresses, cc_addresses,
                    subject, body_text, body_html, snippet, labels,
                    is_read, is_starred, has_attachments, email_date,
                    internal_date, raw_headers, sync_status
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20::jsonb, 'synced')
                ON CONFLICT (user_id, gmail_id) DO UPDATE SET
                    thread_id = EXCLUDED.thread_id,
                    from_address = EXCLUDED.from_address,
                    from_name = EXCLUDED.from_name,
                    to_addresses = EXCLUDED.to_addresses,
                    cc_addresses = EXCLUDED.cc_addresses,
                    subject = EXCLUDED.subject,
                    body_text = EXCLUDED.body_text,
                    body_html = EXCLUDED.body_html,
                    snippet = EXCLUDED.snippet,
                    labels = EXCLUDED.labels,
                    is_read = EXCLUDED.is_read,
                    is_starred = EXCLUDED.is_starred,
                    has_attachments = EXCLUDED.has_attachments,
                    email_date = EXCLUDED.email_date,
                    updated_at = NOW()
                RETURNING *
            """,
                user_id, organization_id, gmail_id, thread_id, account_email,
                from_address, from_name, to_addresses or [], cc_addresses or [],
                subject, body_text, body_html, snippet, labels or [],
                is_read, is_starred, has_attachments, email_date,
                internal_date, json.dumps(raw_headers or {})
            )
            return dict(row)

    @staticmethod
    async def get_by_id(email_id: int) -> Optional[Dict]:
        """Get email by database ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM email_messages WHERE id = $1
            """, email_id)
            return dict(row) if row else None

    @staticmethod
    async def get_by_gmail_id(user_id: int, gmail_id: str) -> Optional[Dict]:
        """Get email by Gmail message ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM email_messages WHERE user_id = $1 AND gmail_id = $2
            """, user_id, gmail_id)
            return dict(row) if row else None

    @staticmethod
    async def list_by_user(
        user_id: int,
        account_email: Optional[str] = None,
        labels: Optional[List[str]] = None,
        is_read: Optional[bool] = None,
        is_starred: Optional[bool] = None,
        is_indexed: Optional[bool] = None,
        sync_status: Optional[str] = None,
        category: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict]:
        """List emails for a user with filters"""
        async with get_connection() as conn:
            conditions = ["user_id = $1", "sync_status != 'deleted'"]
            params = [user_id]
            param_idx = 2

            if account_email:
                conditions.append(f"account_email = ${param_idx}")
                params.append(account_email)
                param_idx += 1

            if labels:
                conditions.append(f"labels && ${param_idx}")
                params.append(labels)
                param_idx += 1

            if is_read is not None:
                conditions.append(f"is_read = ${param_idx}")
                params.append(is_read)
                param_idx += 1

            if is_starred is not None:
                conditions.append(f"is_starred = ${param_idx}")
                params.append(is_starred)
                param_idx += 1

            if is_indexed is not None:
                conditions.append(f"is_indexed = ${param_idx}")
                params.append(is_indexed)
                param_idx += 1

            if sync_status:
                conditions.append(f"sync_status = ${param_idx}")
                params.append(sync_status)
                param_idx += 1

            if category:
                conditions.append(f"category = ${param_idx}")
                params.append(category)
                param_idx += 1

            params.extend([limit, offset])
            query = f"""
                SELECT id, gmail_id, thread_id, account_email, from_address, from_name,
                       to_addresses, subject, snippet, labels, is_read, is_starred,
                       has_attachments, email_date, is_indexed, sync_status, created_at,
                       category, category_confidence
                FROM email_messages
                WHERE {' AND '.join(conditions)}
                ORDER BY email_date DESC NULLS LAST
                LIMIT ${param_idx} OFFSET ${param_idx + 1}
            """
            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    @staticmethod
    async def count_by_user(
        user_id: int,
        account_email: Optional[str] = None,
        labels: Optional[List[str]] = None,
        sync_status: Optional[str] = None
    ) -> int:
        """Count emails for a user"""
        async with get_connection() as conn:
            conditions = ["user_id = $1", "sync_status != 'deleted'"]
            params = [user_id]
            param_idx = 2

            if account_email:
                conditions.append(f"account_email = ${param_idx}")
                params.append(account_email)
                param_idx += 1

            if labels:
                conditions.append(f"labels && ${param_idx}")
                params.append(labels)
                param_idx += 1

            if sync_status:
                conditions.append(f"sync_status = ${param_idx}")
                params.append(sync_status)
                param_idx += 1

            query = f"SELECT COUNT(*) FROM email_messages WHERE {' AND '.join(conditions)}"
            return await conn.fetchval(query, *params)

    @staticmethod
    async def search_text(
        user_id: int,
        search_query: str,
        account_email: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict]:
        """Full-text search emails"""
        async with get_connection() as conn:
            conditions = ["user_id = $1", "sync_status != 'deleted'"]
            params = [user_id, f"%{search_query}%"]
            param_idx = 3

            if account_email:
                conditions.append(f"account_email = ${param_idx}")
                params.append(account_email)
                param_idx += 1

            params.append(limit)
            query = f"""
                SELECT id, gmail_id, thread_id, account_email, from_address, from_name,
                       to_addresses, subject, snippet, labels, is_read, is_starred,
                       email_date, is_indexed
                FROM email_messages
                WHERE {' AND '.join(conditions)}
                  AND (subject ILIKE $2 OR body_text ILIKE $2 OR from_address ILIKE $2 OR from_name ILIKE $2)
                ORDER BY email_date DESC
                LIMIT ${param_idx}
            """
            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    @staticmethod
    async def get_unindexed(user_id: int, limit: int = 100) -> List[Dict]:
        """Get emails not yet indexed for RAG"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM email_messages
                WHERE user_id = $1 AND is_indexed = FALSE AND sync_status = 'synced'
                ORDER BY email_date DESC
                LIMIT $2
            """, user_id, limit)
            return [dict(row) for row in rows]

    @staticmethod
    async def mark_indexed(email_ids: List[int], embedding: Optional[List[float]] = None) -> int:
        """Mark emails as indexed, optionally storing embedding"""
        if not email_ids:
            return 0
        async with get_connection() as conn:
            if embedding:
                result = await conn.execute("""
                    UPDATE email_messages
                    SET is_indexed = TRUE, embedding = $2, indexed_at = NOW()
                    WHERE id = ANY($1)
                """, email_ids, embedding)
            else:
                result = await conn.execute("""
                    UPDATE email_messages
                    SET is_indexed = TRUE, indexed_at = NOW()
                    WHERE id = ANY($1)
                """, email_ids)
            return int(result.split()[-1])

    @staticmethod
    async def update_sync_status(email_ids: List[int], status: str) -> int:
        """Update sync status for multiple emails"""
        if not email_ids:
            return 0
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE email_messages
                SET sync_status = $2, updated_at = NOW()
                WHERE id = ANY($1)
            """, email_ids, status)
            return int(result.split()[-1])

    @staticmethod
    async def delete_by_ids(email_ids: List[int]) -> int:
        """Soft delete emails (mark as deleted)"""
        if not email_ids:
            return 0
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE email_messages
                SET sync_status = 'deleted', updated_at = NOW()
                WHERE id = ANY($1)
            """, email_ids)
            return int(result.split()[-1])

    @staticmethod
    async def hard_delete_by_ids(email_ids: List[int]) -> int:
        """Permanently delete emails from database"""
        if not email_ids:
            return 0
        async with get_connection() as conn:
            result = await conn.execute("""
                DELETE FROM email_messages WHERE id = ANY($1)
            """, email_ids)
            return int(result.split()[-1])

    @staticmethod
    async def get_stats(user_id: int, account_email: Optional[str] = None) -> Dict:
        """Get email statistics for a user"""
        async with get_connection() as conn:
            conditions = ["user_id = $1"]
            params = [user_id]
            param_idx = 2

            if account_email:
                conditions.append(f"account_email = ${param_idx}")
                params.append(account_email)

            where_clause = ' AND '.join(conditions)
            row = await conn.fetchrow(f"""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE sync_status = 'synced') as synced,
                    COUNT(*) FILTER (WHERE sync_status = 'deleted') as deleted,
                    COUNT(*) FILTER (WHERE sync_status = 'archived') as archived,
                    COUNT(*) FILTER (WHERE is_read = FALSE AND sync_status = 'synced') as unread,
                    COUNT(*) FILTER (WHERE is_starred = TRUE AND sync_status = 'synced') as starred,
                    COUNT(*) FILTER (WHERE is_indexed = TRUE) as indexed,
                    COUNT(*) FILTER (WHERE is_indexed = FALSE AND sync_status = 'synced') as pending_index,
                    MIN(email_date) as oldest,
                    MAX(email_date) as newest
                FROM email_messages
                WHERE {where_clause}
            """, *params)
            return dict(row) if row else {}

    @staticmethod
    async def update_embedding(email_id: int, embedding: List[float]) -> bool:
        """Store embedding for a single email"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE email_messages
                SET embedding = $2::vector, is_indexed = TRUE, indexed_at = NOW()
                WHERE id = $1
            """, email_id, str(embedding))
            return result == "UPDATE 1"

    @staticmethod
    async def semantic_search(
        user_id: int,
        query_embedding: List[float],
        account_email: Optional[str] = None,
        limit: int = 10,
        threshold: float = 0.0,
        include_body: bool = False
    ) -> List[Dict]:
        """
        Semantic search emails using pgvector similarity.

        Args:
            user_id: User ID for multi-tenant isolation
            query_embedding: Query vector from embedding model
            account_email: Optional filter by email account
            limit: Maximum results
            threshold: Minimum similarity score (0-1 for cosine)
            include_body: Whether to include full body text

        Returns:
            List of emails with similarity scores
        """
        async with get_connection() as conn:
            conditions = [
                "user_id = $1",
                "sync_status = 'synced'",
                "is_indexed = TRUE",
                "embedding IS NOT NULL"
            ]
            params = [user_id, str(query_embedding)]
            param_idx = 3

            if account_email:
                conditions.append(f"account_email = ${param_idx}")
                params.append(account_email)
                param_idx += 1

            # Cosine similarity: 1 - cosine_distance
            similarity_sql = "1 - (embedding <=> $2::vector)"

            if threshold > 0:
                conditions.append(f"{similarity_sql} >= ${param_idx}")
                params.append(threshold)
                param_idx += 1

            params.append(limit)

            body_field = ", body_text" if include_body else ""
            query = f"""
                SELECT
                    id, gmail_id, thread_id, account_email, from_address, from_name,
                    to_addresses, cc_addresses, subject, snippet, labels,
                    is_read, is_starred, has_attachments, email_date{body_field},
                    {similarity_sql} as similarity_score
                FROM email_messages
                WHERE {' AND '.join(conditions)}
                ORDER BY embedding <=> $2::vector
                LIMIT ${param_idx}
            """

            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    @staticmethod
    async def hybrid_search(
        user_id: int,
        text_query: str,
        query_embedding: List[float],
        account_email: Optional[str] = None,
        limit: int = 10,
        semantic_weight: float = 0.7
    ) -> List[Dict]:
        """
        Hybrid search combining text and semantic search.

        Args:
            user_id: User ID
            text_query: Text search query
            query_embedding: Query vector
            account_email: Optional filter
            limit: Maximum results
            semantic_weight: Weight for semantic vs text (0-1)

        Returns:
            List of emails ranked by combined score
        """
        async with get_connection() as conn:
            conditions = ["user_id = $1", "sync_status = 'synced'"]
            params = [user_id, str(query_embedding), f"%{text_query}%"]
            param_idx = 4

            if account_email:
                conditions.append(f"account_email = ${param_idx}")
                params.append(account_email)
                param_idx += 1

            params.append(limit)

            # Combined scoring: semantic similarity + text match bonus
            query = f"""
                WITH scored AS (
                    SELECT
                        id, gmail_id, thread_id, account_email, from_address, from_name,
                        to_addresses, cc_addresses, subject, snippet, labels,
                        is_read, is_starred, has_attachments, email_date,
                        CASE WHEN embedding IS NOT NULL
                             THEN (1 - (embedding <=> $2::vector))
                             ELSE 0
                        END as semantic_score,
                        CASE WHEN subject ILIKE $3 OR body_text ILIKE $3
                             THEN 1.0
                             ELSE 0
                        END as text_score
                    FROM email_messages
                    WHERE {' AND '.join(conditions)}
                )
                SELECT *,
                       (semantic_score * {semantic_weight} + text_score * {1 - semantic_weight}) as combined_score
                FROM scored
                WHERE semantic_score > 0 OR text_score > 0
                ORDER BY combined_score DESC
                LIMIT ${param_idx}
            """

            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    @staticmethod
    async def list_by_category(
        user_id: int,
        category: str,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict]:
        """List emails by category"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT id, gmail_id, from_address, from_name, subject, snippet,
                       is_read, is_starred, has_attachments, email_date, labels,
                       category, category_confidence
                FROM email_messages
                WHERE user_id = $1 AND category = $2 AND sync_status = 'synced'
                ORDER BY email_date DESC
                LIMIT $3 OFFSET $4
            """, user_id, category, limit, offset)
            return [dict(row) for row in rows]

    @staticmethod
    async def count_by_category(user_id: int, category: str) -> int:
        """Count emails by category"""
        async with get_connection() as conn:
            return await conn.fetchval("""
                SELECT COUNT(*) FROM email_messages
                WHERE user_id = $1 AND category = $2 AND sync_status = 'synced'
            """, user_id, category)

    @staticmethod
    async def get_category_stats(user_id: int) -> Dict[str, int]:
        """Get email counts by category"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT COALESCE(category, 'unclassified') as category, COUNT(*) as count
                FROM email_messages
                WHERE user_id = $1 AND sync_status = 'synced'
                GROUP BY category
                ORDER BY count DESC
            """, user_id)
            return {row['category']: row['count'] for row in rows}

    @staticmethod
    async def classify_email(
        email_id: int,
        category: str,
        confidence: float
    ) -> bool:
        """Classify a single email"""
        async with get_connection() as conn:
            result = await conn.execute("""
                UPDATE email_messages
                SET category = $2, category_confidence = $3,
                    classified_at = NOW(), updated_at = NOW()
                WHERE id = $1
            """, email_id, category, confidence)
            return result == "UPDATE 1"

    @staticmethod
    async def get_unclassified(user_id: int, limit: int = 500) -> List[Dict]:
        """Get emails without category"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT id, from_address, from_name, subject, snippet, body_text, labels
                FROM email_messages
                WHERE user_id = $1 AND category IS NULL AND sync_status = 'synced'
                LIMIT $2
            """, user_id, limit)
            return [dict(row) for row in rows]

    @staticmethod
    async def bulk_delete_by_category(user_id: int, category: str, permanent: bool = False) -> List[int]:
        """Delete all emails in a category"""
        async with get_connection() as conn:
            if permanent:
                rows = await conn.fetch("""
                    DELETE FROM email_messages
                    WHERE user_id = $1 AND category = $2
                    RETURNING id
                """, user_id, category)
            else:
                rows = await conn.fetch("""
                    UPDATE email_messages
                    SET sync_status = 'deleted', deleted_at = NOW(), updated_at = NOW()
                    WHERE user_id = $1 AND category = $2 AND sync_status = 'synced'
                    RETURNING id
                """, user_id, category)
            return [row['id'] for row in rows]

    @staticmethod
    async def bulk_delete_by_ids(user_id: int, email_ids: List[int], permanent: bool = False) -> List[int]:
        """Delete emails by their IDs"""
        async with get_connection() as conn:
            if permanent:
                rows = await conn.fetch("""
                    DELETE FROM email_messages
                    WHERE user_id = $1 AND id = ANY($2)
                    RETURNING id
                """, user_id, email_ids)
            else:
                rows = await conn.fetch("""
                    UPDATE email_messages
                    SET sync_status = 'deleted', deleted_at = NOW(), updated_at = NOW()
                    WHERE user_id = $1 AND id = ANY($2) AND sync_status = 'synced'
                    RETURNING id
                """, user_id, email_ids)
            return [row['id'] for row in rows]


class EmailSyncStateRepository:
    """Email sync state tracking for incremental sync"""

    @staticmethod
    async def get_or_create(user_id: int, account_email: str) -> Dict:
        """Get or create sync state for an account"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO email_sync_state (user_id, account_email)
                VALUES ($1, $2)
                ON CONFLICT (user_id, account_email) DO UPDATE SET
                    updated_at = NOW()
                RETURNING *
            """, user_id, account_email)
            return dict(row)

    @staticmethod
    async def get(user_id: int, account_email: str) -> Optional[Dict]:
        """Get sync state for an account"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM email_sync_state
                WHERE user_id = $1 AND account_email = $2
            """, user_id, account_email)
            return dict(row) if row else None

    @staticmethod
    async def update_history_id(user_id: int, account_email: str, history_id: int) -> Dict:
        """Update last history ID for incremental sync"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE email_sync_state
                SET last_history_id = $3, last_sync_at = NOW(), updated_at = NOW()
                WHERE user_id = $1 AND account_email = $2
                RETURNING *
            """, user_id, account_email, history_id)
            return dict(row) if row else {}

    @staticmethod
    async def update_full_sync(user_id: int, account_email: str) -> Dict:
        """Mark full sync completed"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE email_sync_state
                SET last_sync_at = NOW(), updated_at = NOW()
                WHERE user_id = $1 AND account_email = $2
                RETURNING *
            """, user_id, account_email)
            return dict(row) if row else {}

    @staticmethod
    async def record_error(user_id: int, account_email: str, error: str) -> Dict:
        """Record sync error"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE email_sync_state
                SET sync_errors = sync_errors + 1,
                    last_error = $3,
                    updated_at = NOW()
                WHERE user_id = $1 AND account_email = $2
                RETURNING *
            """, user_id, account_email, error)
            return dict(row) if row else {}

    @staticmethod
    async def clear_errors(user_id: int, account_email: str) -> Dict:
        """Clear error state after successful sync"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE email_sync_state
                SET sync_errors = 0, last_error = NULL, updated_at = NOW()
                WHERE user_id = $1 AND account_email = $2
                RETURNING *
            """, user_id, account_email)
            return dict(row) if row else {}

    @staticmethod
    async def list_by_user(user_id: int) -> List[Dict]:
        """List all sync states for a user"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM email_sync_state
                WHERE user_id = $1
                ORDER BY last_sync_at DESC NULLS LAST
            """, user_id)
            return [dict(row) for row in rows]

    @staticmethod
    async def delete(user_id: int, account_email: str) -> bool:
        """Delete sync state for an account"""
        async with get_connection() as conn:
            result = await conn.execute("""
                DELETE FROM email_sync_state
                WHERE user_id = $1 AND account_email = $2
            """, user_id, account_email)
            return result.split()[-1] != '0'


# =============================================================================
# EMAIL ACCOUNT REPOSITORY - Product Ready OAuth Storage
# =============================================================================

class EmailAccountRepository:
    """Email account management with secure token storage"""

    @staticmethod
    async def create_oauth_state(
        state_token: str,
        user_id: int,
        provider: str,
        email_hint: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        code_verifier: Optional[str] = None,
    ) -> Dict:
        """Create OAuth state for authorization flow"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO email_oauth_states
                (state_token, user_id, provider, email_hint, redirect_uri, code_verifier)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING *
            """, state_token, user_id, provider, email_hint, redirect_uri, code_verifier)
            return dict(row)

    @staticmethod
    async def get_oauth_state(state_token: str) -> Optional[Dict]:
        """Get OAuth state by token (validates not expired)"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM email_oauth_states
                WHERE state_token = $1 AND expires_at > NOW()
            """, state_token)
            return dict(row) if row else None

    @staticmethod
    async def delete_oauth_state(state_token: str) -> bool:
        """Delete OAuth state after use"""
        async with get_connection() as conn:
            result = await conn.execute("""
                DELETE FROM email_oauth_states WHERE state_token = $1
            """, state_token)
            return result.split()[-1] != '0'

    @staticmethod
    async def cleanup_expired_oauth_states() -> int:
        """Clean up expired OAuth states"""
        async with get_connection() as conn:
            result = await conn.execute("""
                DELETE FROM email_oauth_states WHERE expires_at < NOW()
            """)
            return int(result.split()[-1])

    @staticmethod
    async def upsert_account(
        user_id: int,
        email_address: str,
        provider: str,
        access_token_encrypted: Optional[str] = None,
        refresh_token_encrypted: Optional[str] = None,
        token_expiry: Optional[datetime] = None,
        token_scope: Optional[str] = None,
        organization_id: Optional[int] = None,
        display_name: Optional[str] = None,
    ) -> Dict:
        """Create or update email account (OAuth flow)"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO email_accounts
                (user_id, email_address, provider, access_token_encrypted,
                 refresh_token_encrypted, token_expiry, token_scope,
                 organization_id, display_name, is_active)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, TRUE)
                ON CONFLICT (user_id, email_address) DO UPDATE SET
                    access_token_encrypted = EXCLUDED.access_token_encrypted,
                    refresh_token_encrypted = EXCLUDED.refresh_token_encrypted,
                    token_expiry = EXCLUDED.token_expiry,
                    token_scope = EXCLUDED.token_scope,
                    is_active = TRUE,
                    sync_error = NULL,
                    sync_error_count = 0,
                    updated_at = NOW()
                RETURNING *
            """, user_id, email_address, provider, access_token_encrypted,
                refresh_token_encrypted, token_expiry, token_scope,
                organization_id, display_name)
            return dict(row)

    @staticmethod
    async def create_imap_account(
        user_id: int,
        email_address: str,
        imap_host: str,
        imap_port: int,
        imap_use_ssl: bool,
        imap_username_encrypted: str,
        imap_password_encrypted: str,
        organization_id: Optional[int] = None,
        display_name: Optional[str] = None,
    ) -> Dict:
        """Create IMAP email account"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO email_accounts
                (user_id, email_address, provider, imap_host, imap_port,
                 imap_use_ssl, imap_username_encrypted, imap_password_encrypted,
                 organization_id, display_name, is_active)
                VALUES ($1, $2, 'imap', $3, $4, $5, $6, $7, $8, $9, TRUE)
                ON CONFLICT (user_id, email_address) DO UPDATE SET
                    imap_host = EXCLUDED.imap_host,
                    imap_port = EXCLUDED.imap_port,
                    imap_use_ssl = EXCLUDED.imap_use_ssl,
                    imap_username_encrypted = EXCLUDED.imap_username_encrypted,
                    imap_password_encrypted = EXCLUDED.imap_password_encrypted,
                    display_name = EXCLUDED.display_name,
                    is_active = TRUE,
                    sync_error = NULL,
                    sync_error_count = 0,
                    updated_at = NOW()
                RETURNING *
            """, user_id, email_address, imap_host, imap_port,
                imap_use_ssl, imap_username_encrypted, imap_password_encrypted,
                organization_id, display_name)
            return dict(row)

    @staticmethod
    async def get_by_id(account_id: int) -> Optional[Dict]:
        """Get account by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM email_accounts WHERE id = $1
            """, account_id)
            return dict(row) if row else None

    @staticmethod
    async def get_by_email(user_id: int, email_address: str) -> Optional[Dict]:
        """Get account by user ID and email"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM email_accounts
                WHERE user_id = $1 AND email_address = $2
            """, user_id, email_address)
            return dict(row) if row else None

    @staticmethod
    async def list_by_user(user_id: int) -> List[Dict]:
        """List all accounts for a user"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM email_accounts
                WHERE user_id = $1
                ORDER BY created_at DESC
            """, user_id)
            return [dict(row) for row in rows]

    @staticmethod
    async def list_active_for_sync() -> List[Dict]:
        """List all active accounts due for sync"""
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM email_accounts
                WHERE is_active = TRUE
                  AND sync_enabled = TRUE
                  AND (
                    last_sync_at IS NULL
                    OR last_sync_at < NOW() - (sync_frequency_minutes * INTERVAL '1 minute')
                  )
                ORDER BY last_sync_at ASC NULLS FIRST
                LIMIT 100
            """)
            return [dict(row) for row in rows]

    @staticmethod
    async def update(account_id: int, **kwargs) -> Dict:
        """Update account fields"""
        if not kwargs:
            return {}

        # Build dynamic update query
        set_clauses = []
        values = []
        param_idx = 1

        for key, value in kwargs.items():
            set_clauses.append(f"{key} = ${param_idx}")
            values.append(value)
            param_idx += 1

        values.append(account_id)

        async with get_connection() as conn:
            row = await conn.fetchrow(f"""
                UPDATE email_accounts
                SET {', '.join(set_clauses)}, updated_at = NOW()
                WHERE id = ${param_idx}
                RETURNING *
            """, *values)
            return dict(row) if row else {}

    @staticmethod
    async def update_tokens(
        account_id: int,
        access_token_encrypted: str,
        refresh_token_encrypted: str,
        token_expiry: datetime,
    ) -> Dict:
        """Update OAuth tokens"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE email_accounts
                SET access_token_encrypted = $2,
                    refresh_token_encrypted = $3,
                    token_expiry = $4,
                    updated_at = NOW()
                WHERE id = $1
                RETURNING *
            """, account_id, access_token_encrypted, refresh_token_encrypted, token_expiry)
            return dict(row) if row else {}

    @staticmethod
    async def update_sync_status(
        account_id: int,
        synced_count: int = 0,
        history_id: Optional[int] = None,
    ) -> Dict:
        """Update sync status after successful sync"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE email_accounts
                SET last_sync_at = NOW(),
                    total_synced = total_synced + $2,
                    last_history_id = COALESCE($3, last_history_id),
                    sync_error = NULL,
                    sync_error_count = 0,
                    updated_at = NOW()
                WHERE id = $1
                RETURNING *
            """, account_id, synced_count, history_id)
            return dict(row) if row else {}

    @staticmethod
    async def record_error(account_id: int, error: str) -> Dict:
        """Record sync error"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE email_accounts
                SET sync_error = $2,
                    sync_error_count = sync_error_count + 1,
                    last_error_at = NOW(),
                    updated_at = NOW()
                WHERE id = $1
                RETURNING *
            """, account_id, error)
            return dict(row) if row else {}

    @staticmethod
    async def delete(account_id: int) -> bool:
        """Delete account and all associated emails"""
        async with get_connection() as conn:
            result = await conn.execute("""
                DELETE FROM email_accounts WHERE id = $1
            """, account_id)
            return result.split()[-1] != '0'

    # -------------------------------------------------------------------------
    # OAuth Credentials (mcp_oauth_credentials table)
    # -------------------------------------------------------------------------

    @staticmethod
    async def get_oauth_credential(oauth_cred_id: int) -> Optional[Dict]:
        """Get OAuth credential by ID"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM mcp_oauth_credentials WHERE id = $1
            """, oauth_cred_id)
            return dict(row) if row else None

    @staticmethod
    async def upsert_oauth_credential(
        user_id: int,
        provider_id: str,
        account_identifier: str,
        access_token: str,
        refresh_token: str,
        token_expiry: datetime,
        scopes: List[str],
    ) -> Dict:
        """Create or update OAuth credential"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                INSERT INTO mcp_oauth_credentials
                (user_id, provider_id, account_identifier, access_token,
                 refresh_token, token_expiry, scopes)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (user_id, provider_id, account_identifier) DO UPDATE SET
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    token_expiry = EXCLUDED.token_expiry,
                    scopes = EXCLUDED.scopes,
                    updated_at = NOW()
                RETURNING *
            """, user_id, provider_id, account_identifier, access_token,
                refresh_token, token_expiry, scopes)
            return dict(row)

    @staticmethod
    async def update_oauth_credential(
        oauth_cred_id: int,
        access_token: str,
        refresh_token: str,
        token_expiry: datetime,
    ) -> Dict:
        """Update OAuth credential tokens"""
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                UPDATE mcp_oauth_credentials
                SET access_token = $2,
                    refresh_token = $3,
                    token_expiry = $4,
                    updated_at = NOW()
                WHERE id = $1
                RETURNING *
            """, oauth_cred_id, access_token, refresh_token, token_expiry)
            return dict(row) if row else {}

    @staticmethod
    async def upsert_account_with_oauth(
        user_id: int,
        email_address: str,
        provider: str,
        access_token: str,
        refresh_token: str,
        token_expiry: datetime,
        scopes: List[str],
        organization_id: Optional[int] = None,
        display_name: Optional[str] = None,
    ) -> Dict:
        """Create or update email account with OAuth credentials"""
        async with get_connection() as conn:
            # First, upsert OAuth credential
            oauth_row = await conn.fetchrow("""
                INSERT INTO mcp_oauth_credentials
                (user_id, provider_id, account_identifier, access_token,
                 refresh_token, token_expiry, scopes)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (user_id, provider_id, account_identifier) DO UPDATE SET
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    token_expiry = EXCLUDED.token_expiry,
                    scopes = EXCLUDED.scopes,
                    updated_at = NOW()
                RETURNING id
            """, user_id, provider, email_address, access_token,
                refresh_token, token_expiry, scopes)

            oauth_cred_id = oauth_row["id"]

            # Then, upsert email account
            account_row = await conn.fetchrow("""
                INSERT INTO email_accounts
                (user_id, email_address, provider, oauth_credential_id,
                 display_name, organization_id, is_active)
                VALUES ($1, $2, $3, $4, $5, $6, TRUE)
                ON CONFLICT (user_id, email_address) DO UPDATE SET
                    oauth_credential_id = EXCLUDED.oauth_credential_id,
                    display_name = COALESCE(EXCLUDED.display_name, email_accounts.display_name),
                    is_active = TRUE,
                    sync_error = NULL,
                    sync_error_count = 0,
                    updated_at = NOW()
                RETURNING *
            """, user_id, email_address, provider, oauth_cred_id,
                display_name, organization_id)

            return dict(account_row)
