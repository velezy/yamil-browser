"""
Text2SQL Service using HuggingFace Model

Uses yasserrmd/Text2SQL-1.5B model for natural language to SQL conversion.
Based on Qwen2.5-Coder with 4-bit quantization for efficient inference.

Features:
- Natural language to SQL conversion
- Schema-aware query generation
- JSON response format for easy parsing
- Async-compatible interface
- Fallback to pattern matching if model unavailable
"""

import asyncio
import json
import logging
import re
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from functools import lru_cache

logger = logging.getLogger(__name__)

# Try to import transformers
try:
    from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
    import torch
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    logger.warning("transformers not available. Text2SQL will use fallback mode.")


@dataclass
class Text2SQLResult:
    """Result from Text2SQL conversion"""
    query: str
    explanation: str
    success: bool
    error: Optional[str] = None
    model_used: str = "pattern_matching"


class Text2SQLService:
    """
    Text2SQL service using HuggingFace yasserrmd/Text2SQL-1.5B model.

    The model converts natural language questions to SQL queries
    given a database schema.
    """

    MODEL_ID = "yasserrmd/Text2SQL-1.5B"

    # System instruction for JSON output format
    SYSTEM_INSTRUCTION = """Always separate SQL code and explanation. Return SQL queries in a JSON format containing two keys: 'query' and 'explanation'. The response should strictly follow the structure: {"query": "SQL_QUERY_HERE", "explanation": "EXPLANATION_HERE"}. The 'query' key should contain only the SQL statement, and the 'explanation' key should provide a plain-text explanation of the query."""

    def __init__(
        self,
        model_id: str = None,
        device: str = "auto",
        load_in_4bit: bool = True
    ):
        """
        Initialize Text2SQL service.

        Args:
            model_id: HuggingFace model ID (default: yasserrmd/Text2SQL-1.5B)
            device: Device to run on ('auto', 'cuda', 'cpu', 'mps')
            load_in_4bit: Whether to use 4-bit quantization (saves memory)
        """
        self.model_id = model_id or self.MODEL_ID
        self.device = device
        self.load_in_4bit = load_in_4bit
        self._pipeline = None
        self._loaded = False

    async def initialize(self) -> bool:
        """
        Load the Text2SQL model.

        Returns:
            True if model loaded successfully
        """
        if self._loaded:
            return True

        if not TRANSFORMERS_AVAILABLE:
            logger.warning("Transformers not available, using fallback mode")
            return False

        try:
            # Run model loading in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._load_model)
            self._loaded = True
            logger.info(f"✅ Text2SQL model loaded: {self.model_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to load Text2SQL model: {e}")
            return False

    def _load_model(self):
        """Load model synchronously (called from thread pool)"""
        try:
            # Determine device
            if self.device == "auto":
                if torch.cuda.is_available():
                    device = "cuda"
                elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
                    device = "mps"
                else:
                    device = "cpu"
            else:
                device = self.device

            logger.info(f"Loading Text2SQL model on {device}...")

            # Load tokenizer
            tokenizer = AutoTokenizer.from_pretrained(self.model_id)

            # Load model with quantization if available
            model_kwargs = {}
            if self.load_in_4bit and device == "cuda":
                try:
                    import bitsandbytes
                    model_kwargs["load_in_4bit"] = True
                    model_kwargs["device_map"] = "auto"
                    logger.info("Using 4-bit quantization")
                except ImportError:
                    logger.warning("bitsandbytes not available, loading full precision")
                    model_kwargs["device_map"] = device

            else:
                if device != "cpu":
                    model_kwargs["device_map"] = device
                else:
                    model_kwargs["device_map"] = "cpu"
                    # Use float32 on CPU for compatibility
                    model_kwargs["torch_dtype"] = torch.float32

            model = AutoModelForCausalLM.from_pretrained(
                self.model_id,
                trust_remote_code=True,
                **model_kwargs
            )

            # Create pipeline
            self._pipeline = pipeline(
                "text-generation",
                model=model,
                tokenizer=tokenizer,
                max_new_tokens=512,
                do_sample=False,
                return_full_text=False
            )

        except Exception as e:
            logger.error(f"Error loading model: {e}")
            raise

    async def generate_sql(
        self,
        question: str,
        schema: str,
        max_tokens: int = 256
    ) -> Text2SQLResult:
        """
        Convert natural language question to SQL.

        Args:
            question: Natural language question
            schema: Database schema (CREATE TABLE statements)
            max_tokens: Maximum tokens to generate

        Returns:
            Text2SQLResult with query and explanation
        """
        if not self._loaded or self._pipeline is None:
            # Try fallback
            return await self._fallback_generate(question, schema)

        try:
            # Prepare messages
            messages = [
                {"role": "system", "content": self.SYSTEM_INSTRUCTION},
                {"role": "user", "content": f"{question}\n\n{schema}"}
            ]

            # Generate in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self._pipeline(messages, max_new_tokens=max_tokens)
            )

            # Parse response
            generated_text = result[0]["generated_text"]
            return self._parse_response(generated_text)

        except Exception as e:
            logger.error(f"Text2SQL generation error: {e}")
            return Text2SQLResult(
                query="",
                explanation="",
                success=False,
                error=str(e),
                model_used="error"
            )

    def _parse_response(self, response: str) -> Text2SQLResult:
        """Parse the model response to extract SQL query and explanation"""
        try:
            # Try to parse as JSON
            # Find JSON in response
            json_match = re.search(r'\{[^}]+\}', response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                return Text2SQLResult(
                    query=data.get("query", "").strip(),
                    explanation=data.get("explanation", "").strip(),
                    success=True,
                    model_used="text2sql-1.5b"
                )

            # Fallback: try to extract SQL from code blocks
            sql_match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
            if sql_match:
                return Text2SQLResult(
                    query=sql_match.group(1).strip(),
                    explanation="Query generated by Text2SQL model",
                    success=True,
                    model_used="text2sql-1.5b"
                )

            # Last resort: assume the whole response is SQL
            if response.strip().upper().startswith("SELECT"):
                return Text2SQLResult(
                    query=response.strip(),
                    explanation="",
                    success=True,
                    model_used="text2sql-1.5b"
                )

            return Text2SQLResult(
                query="",
                explanation="",
                success=False,
                error="Could not parse SQL from response",
                model_used="text2sql-1.5b"
            )

        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse error: {e}")
            return Text2SQLResult(
                query="",
                explanation="",
                success=False,
                error=f"JSON parse error: {e}",
                model_used="text2sql-1.5b"
            )

    async def _fallback_generate(
        self,
        question: str,
        schema: str
    ) -> Text2SQLResult:
        """
        Fallback SQL generation using pattern matching.
        Used when the HuggingFace model is not available.
        """
        query_lower = question.lower()

        # Extract table names from schema
        tables = re.findall(r'CREATE\s+TABLE\s+(\w+)', schema, re.IGNORECASE)

        # Simple pattern matching for common queries
        if 'count' in query_lower or 'how many' in query_lower:
            if tables:
                return Text2SQLResult(
                    query=f"SELECT COUNT(*) FROM {tables[0]}",
                    explanation=f"Count all records in {tables[0]}",
                    success=True,
                    model_used="pattern_matching"
                )

        if 'all' in query_lower or 'list' in query_lower or 'show' in query_lower:
            if tables:
                return Text2SQLResult(
                    query=f"SELECT * FROM {tables[0]} LIMIT 100",
                    explanation=f"List all records from {tables[0]} (limited to 100)",
                    success=True,
                    model_used="pattern_matching"
                )

        if 'total' in query_lower or 'sum' in query_lower:
            # Try to find a numeric column
            amount_cols = re.findall(r'(\w+)\s+(?:INTEGER|NUMERIC|DECIMAL|FLOAT|REAL)', schema, re.IGNORECASE)
            if tables and amount_cols:
                return Text2SQLResult(
                    query=f"SELECT SUM({amount_cols[0]}) FROM {tables[0]}",
                    explanation=f"Sum of {amount_cols[0]} from {tables[0]}",
                    success=True,
                    model_used="pattern_matching"
                )

        if 'average' in query_lower or 'avg' in query_lower:
            amount_cols = re.findall(r'(\w+)\s+(?:INTEGER|NUMERIC|DECIMAL|FLOAT|REAL)', schema, re.IGNORECASE)
            if tables and amount_cols:
                return Text2SQLResult(
                    query=f"SELECT AVG({amount_cols[0]}) FROM {tables[0]}",
                    explanation=f"Average of {amount_cols[0]} from {tables[0]}",
                    success=True,
                    model_used="pattern_matching"
                )

        if 'recent' in query_lower or 'latest' in query_lower:
            date_cols = re.findall(r'(\w+)\s+(?:TIMESTAMP|DATE|DATETIME)', schema, re.IGNORECASE)
            if tables:
                order_col = date_cols[0] if date_cols else 'id'
                return Text2SQLResult(
                    query=f"SELECT * FROM {tables[0]} ORDER BY {order_col} DESC LIMIT 10",
                    explanation=f"Most recent records from {tables[0]}",
                    success=True,
                    model_used="pattern_matching"
                )

        # Default query
        if tables:
            return Text2SQLResult(
                query=f"SELECT * FROM {tables[0]} LIMIT 10",
                explanation=f"Default query showing records from {tables[0]}",
                success=True,
                model_used="pattern_matching"
            )

        return Text2SQLResult(
            query="",
            explanation="",
            success=False,
            error="Could not determine appropriate query",
            model_used="pattern_matching"
        )

    def get_schema_for_tables(self, tables: Dict[str, Dict]) -> str:
        """
        Generate CREATE TABLE statements for the model.

        Args:
            tables: Dict of table configs from SQLQueryTool.ALLOWED_TABLES

        Returns:
            SQL schema string
        """
        schema_parts = []
        for table_name, config in tables.items():
            columns = config.get('columns', [])
            col_defs = [f"    {col} TEXT" for col in columns]  # Simplified types
            schema_parts.append(
                f"CREATE TABLE {table_name} (\n" +
                ",\n".join(col_defs) +
                "\n);"
            )
        return "\n\n".join(schema_parts)

    @property
    def is_loaded(self) -> bool:
        """Check if model is loaded"""
        return self._loaded

    def get_info(self) -> Dict[str, Any]:
        """Get service info"""
        return {
            "model_id": self.model_id,
            "loaded": self._loaded,
            "device": self.device,
            "transformers_available": TRANSFORMERS_AVAILABLE,
            "4bit_quantization": self.load_in_4bit
        }


# Singleton instance
_text2sql_service: Optional[Text2SQLService] = None


def get_text2sql_service() -> Text2SQLService:
    """Get or create singleton Text2SQL service"""
    global _text2sql_service
    if _text2sql_service is None:
        _text2sql_service = Text2SQLService()
    return _text2sql_service


async def generate_sql_from_question(
    question: str,
    schema: str,
    initialize: bool = True
) -> Text2SQLResult:
    """
    Convenience function to generate SQL from a question.

    Args:
        question: Natural language question
        schema: Database schema
        initialize: Whether to initialize the model if not loaded

    Returns:
        Text2SQLResult
    """
    service = get_text2sql_service()
    if initialize and not service.is_loaded:
        await service.initialize()
    return await service.generate_sql(question, schema)
