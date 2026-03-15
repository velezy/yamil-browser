"""
Learn UI Tool

Writes AI's UI discoveries to a draft orchestrator layer.
The AI can save page descriptions, UI patterns, flow templates,
and node behaviors it discovers through browsing.

All learnings start as 'draft' and must be reviewed/approved by
a user before they become part of the orchestrator's system prompt.
"""

import logging
from typing import Any, Dict, List, Optional
from uuid import uuid4

from assemblyline_common.ai.authorization import AuthorizationContext, Permission
from assemblyline_common.ai.tools.base import Tool, ToolDefinition, ToolResult

logger = logging.getLogger(__name__)

# Valid categories for AI learnings
LEARNING_CATEGORIES = [
    "page_description",   # What a page looks like and does
    "ui_pattern",         # Reusable UI interaction patterns
    "flow_template",      # Common flow patterns the AI discovered
    "node_behavior",      # How specific node types behave
]


class LearnUITool(Tool):
    """Write UI discoveries to the draft orchestrator layer."""

    def get_definition(self) -> ToolDefinition:
        return ToolDefinition(
            name="learn_ui",
            description=(
                "Save UI discoveries and learned patterns to the draft orchestrator layer. "
                "Learnings are saved as drafts and must be approved by a user before they "
                "become part of your knowledge base. Use this after browsing pages with "
                "canvas_vision to record what you learned."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["save_learning", "suggest_prompt", "list_learnings", "query_learnings"],
                        "description": (
                            "'save_learning' — write a new UI discovery; "
                            "'suggest_prompt' — create a draft AI prompt entry; "
                            "'list_learnings' — show pending drafts for review; "
                            "'query_learnings' — search approved knowledge by category/route (like git log)"
                        ),
                    },
                    "category": {
                        "type": "string",
                        "enum": LEARNING_CATEGORIES,
                        "description": "Category of the learning (for save_learning)",
                    },
                    "page_route": {
                        "type": "string",
                        "description": "The page route this learning applies to, e.g. '/flows', '/settings' (also used as filter for query_learnings)",
                    },
                    "status_filter": {
                        "type": "string",
                        "enum": ["draft", "approved", "rejected"],
                        "description": "Filter by status (for query_learnings, default: 'approved')",
                    },
                    "title": {
                        "type": "string",
                        "description": "Short descriptive title for the learning",
                    },
                    "content": {
                        "type": "string",
                        "description": "The learned knowledge in prompt format — write as if giving instructions to another AI",
                    },
                    "prompt_key": {
                        "type": "string",
                        "description": "Key for the draft AI prompt (for suggest_prompt action)",
                    },
                    "prompt_content": {
                        "type": "string",
                        "description": "Content for the draft AI prompt (for suggest_prompt action)",
                    },
                },
                "required": ["action"],
            },
            required_permission=Permission.READ,
        )

    async def execute(
        self,
        auth_context: AuthorizationContext,
        **kwargs,
    ) -> ToolResult:
        action = kwargs.get("action", "")
        db = kwargs.get("_db")

        if not db:
            return ToolResult(success=False, error="Database connection not available")

        try:
            if action == "save_learning":
                return await self._save_learning(db, auth_context, kwargs)
            elif action == "suggest_prompt":
                return await self._suggest_prompt(db, auth_context, kwargs)
            elif action == "list_learnings":
                return await self._list_learnings(db, auth_context)
            elif action == "query_learnings":
                return await self._query_learnings(db, auth_context, kwargs)
            else:
                return ToolResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            logger.exception(f"learn_ui action '{action}' failed")
            # Rollback to prevent PendingRollbackError cascading to chat_history
            try:
                await db.rollback()
            except Exception:
                pass
            return ToolResult(success=False, error=f"Learn UI action failed: {str(e)}")

    async def _save_learning(
        self, db, auth_context: AuthorizationContext, kwargs: Dict[str, Any]
    ) -> ToolResult:
        """Save a new AI learning to the draft layer."""
        from sqlalchemy import text

        category = kwargs.get("category", "")
        title = kwargs.get("title", "")
        content = kwargs.get("content", "")
        page_route = kwargs.get("page_route", "")

        if not category or category not in LEARNING_CATEGORIES:
            return ToolResult(
                success=False,
                error=f"Invalid category. Must be one of: {', '.join(LEARNING_CATEGORIES)}",
            )
        if not title:
            return ToolResult(success=False, error="title is required")
        if not content:
            return ToolResult(success=False, error="content is required")

        # Limit content size
        if len(content) > 10000:
            content = content[:10000]

        learning_id = str(uuid4())
        tenant_id = str(auth_context.tenant_id)

        await db.execute(
            text("""
                INSERT INTO common.ai_learnings (id, tenant_id, category, page_route, title, content, source, status)
                VALUES (:id, :tenant_id, :category, :page_route, :title, :content, :source, :status)
            """),
            {
                "id": learning_id, "tenant_id": tenant_id, "category": category,
                "page_route": page_route, "title": title[:255], "content": content,
                "source": "browser_exploration", "status": "draft",
            },
        )
        await db.commit()

        logger.info(f"AI learning saved: {title} (category={category}, id={learning_id})")

        return ToolResult(
            success=True,
            message=f"Learning saved as draft: '{title}'. A user must approve it before it becomes active.",
            data={
                "learning_id": learning_id,
                "category": category,
                "title": title,
                "status": "draft",
            },
        )

    async def _suggest_prompt(
        self, db, auth_context: AuthorizationContext, kwargs: Dict[str, Any]
    ) -> ToolResult:
        """Create a draft AI prompt entry."""
        from sqlalchemy import text

        prompt_key = kwargs.get("prompt_key", "")
        prompt_content = kwargs.get("prompt_content", "")
        title = kwargs.get("title", prompt_key)

        if not prompt_key:
            return ToolResult(success=False, error="prompt_key is required")
        if not prompt_content:
            return ToolResult(success=False, error="prompt_content is required")

        tenant_id = str(auth_context.tenant_id)
        prompt_id = str(uuid4())

        # Check if prompt key already exists
        existing = await db.execute(
            text("SELECT id, status FROM common.ai_prompts WHERE key = :key AND tenant_id = :tid"),
            {"key": prompt_key, "tid": tenant_id},
        )
        row = existing.first()

        if row:
            return ToolResult(
                success=False,
                error=f"AI prompt with key '{prompt_key}' already exists (status: {row[1]}). "
                      "Use a different key or update the existing prompt.",
            )

        await db.execute(
            text("""
                INSERT INTO common.ai_prompts (id, tenant_id, key, name, system_prompt, status, is_active, is_default)
                VALUES (:id, :tid, :key, :name, :content, :status, :active, :default)
            """),
            {
                "id": prompt_id, "tid": tenant_id, "key": prompt_key,
                "name": title[:255], "content": prompt_content,
                "status": "draft", "active": False, "default": False,
            },
        )
        await db.commit()

        logger.info(f"Draft AI prompt suggested: {prompt_key} (id={prompt_id})")

        return ToolResult(
            success=True,
            message=f"Draft AI prompt created: '{prompt_key}'. A user must approve it before it becomes active.",
            data={
                "prompt_id": prompt_id,
                "key": prompt_key,
                "status": "draft",
            },
        )

    async def _list_learnings(
        self, db, auth_context: AuthorizationContext
    ) -> ToolResult:
        """List pending draft learnings for review."""
        from sqlalchemy import text

        tenant_id = str(auth_context.tenant_id)

        result = await db.execute(
            text("""
                SELECT id, category, page_route, title, content, source, status, created_at
                FROM common.ai_learnings
                WHERE tenant_id = :tid AND status = 'draft'
                ORDER BY created_at DESC
                LIMIT 20
            """),
            {"tid": tenant_id},
        )
        rows = result.fetchall()

        learnings = []
        for row in rows:
            content_val = row[4] or ""
            learnings.append({
                "id": str(row[0]),
                "category": row[1],
                "page_route": row[2] or "",
                "title": row[3],
                "content": content_val[:200] + "..." if len(content_val) > 200 else content_val,
                "source": row[5],
                "created_at": row[7].isoformat() if row[7] else "",
            })

        # Create a data_table ui_block for the learnings list
        ui_blocks = []
        if learnings:
            ui_blocks.append({
                "type": "data_table",
                "title": f"Pending AI Learnings ({len(learnings)})",
                "columns": ["Title", "Category", "Page", "Source"],
                "rows": [
                    {
                        "Title": lr["title"],
                        "Category": lr["category"],
                        "Page": lr["page_route"],
                        "Source": lr["source"],
                    }
                    for lr in learnings
                ],
                "total": len(learnings),
            })

        return ToolResult(
            success=True,
            message=f"Found {len(learnings)} pending draft learning(s)",
            data={
                "learnings": learnings,
                "count": len(learnings),
                "ui_blocks": ui_blocks,
            },
        )

    async def _query_learnings(
        self, db, auth_context: AuthorizationContext, kwargs: Dict[str, Any]
    ) -> ToolResult:
        """Query approved learnings by category and/or page_route (like git log)."""
        from sqlalchemy import text

        tenant_id = str(auth_context.tenant_id)
        category = kwargs.get("category", "")
        page_route = kwargs.get("page_route", "")
        status_filter = kwargs.get("status_filter", "approved")

        # Build dynamic query with optional filters
        conditions = ["tenant_id = :tid", "status = :status"]
        params: Dict[str, Any] = {"tid": tenant_id, "status": status_filter}

        if category and category in LEARNING_CATEGORIES:
            conditions.append("category = :category")
            params["category"] = category

        if page_route:
            conditions.append("page_route = :page_route")
            params["page_route"] = page_route

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT id, category, page_route, title, content, source, status, created_at, reviewed_at
            FROM common.ai_learnings
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT 25
        """

        result = await db.execute(text(query), params)
        rows = result.fetchall()

        learnings = []
        for row in rows:
            content_val = row[4] or ""
            learnings.append({
                "id": str(row[0]),
                "category": row[1],
                "page_route": row[2] or "",
                "title": row[3],
                "content": content_val,
                "source": row[5],
                "status": row[6],
                "created_at": row[7].isoformat() if row[7] else "",
                "reviewed_at": row[8].isoformat() if row[8] else "",
            })

        filter_desc = []
        if category:
            filter_desc.append(f"category={category}")
        if page_route:
            filter_desc.append(f"route={page_route}")
        filter_str = f" (filters: {', '.join(filter_desc)})" if filter_desc else ""

        return ToolResult(
            success=True,
            message=f"Found {len(learnings)} {status_filter} learning(s){filter_str}",
            data={
                "learnings": learnings,
                "count": len(learnings),
                "filters": {
                    "category": category or None,
                    "page_route": page_route or None,
                    "status": status_filter,
                },
            },
        )


# ============================================================================
# Tool Registration Helper
# ============================================================================

class LearnUITools:
    """Container for learn UI tool registration."""

    @staticmethod
    def get_all_tools() -> List[Tool]:
        return [LearnUITool()]
