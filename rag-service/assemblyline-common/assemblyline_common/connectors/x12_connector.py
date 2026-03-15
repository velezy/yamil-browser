"""
X12 EDI Connector for Logic Weaver Flow Nodes

Provides X12 parsing and generation as flow node components.
Integrates with the microservices architecture for healthcare claims processing.

Node Types:
- x12-parser: Parse X12 EDI content (835, 837, 270, 271, 276, 277)
- x12-generator: Generate X12 EDI from structured data
- x12-validator: Validate X12 content
- x12-splitter: Split X12 file by claims/transactions

Usage in Flow:
    {
        "type": "x12-parser",
        "data": {
            "transaction_type": "835",
            "extract_claims": true,
            "determine_service_area": true
        }
    }

@author: Logic Weaver Development
@version: 1.0.0
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
import json

from assemblyline_common.parsers.x12_parser import (
    X12Parser,
    X12ParseResult,
    X12TransactionType,
    X12Claim,
)

logger = logging.getLogger(__name__)


class X12NodeType(Enum):
    """X12 node types for flow designer"""
    PARSER = "x12-parser"
    GENERATOR = "x12-generator"
    VALIDATOR = "x12-validator"
    SPLITTER = "x12-splitter"
    SERVICE_AREA = "x12-service-area"


@dataclass
class X12NodeConfig:
    """Configuration for X12 flow nodes"""
    node_type: X12NodeType = X12NodeType.PARSER

    # Parser options
    transaction_type: Optional[str] = None  # Auto-detect if None
    extract_claims: bool = True
    extract_segments: bool = False
    determine_service_area: bool = False

    # Service area lookup tables
    vendor_list: List[Tuple[str, str]] = field(default_factory=list)
    npi_list: List[Tuple[str, str]] = field(default_factory=list)

    # Validator options
    validate_structure: bool = True
    validate_segments: bool = True
    strict_mode: bool = False

    # Splitter options
    split_by: str = "claim"  # claim, transaction, functional_group
    output_format: str = "json"  # json, x12, both

    # Generator options
    version: str = "005010X221A1"
    sender_id: str = ""
    receiver_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_type": self.node_type.value,
            "transaction_type": self.transaction_type,
            "extract_claims": self.extract_claims,
            "extract_segments": self.extract_segments,
            "determine_service_area": self.determine_service_area,
            "validate_structure": self.validate_structure,
            "validate_segments": self.validate_segments,
            "strict_mode": self.strict_mode,
            "split_by": self.split_by,
            "output_format": self.output_format,
            "version": self.version,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
        }


@dataclass
class X12NodeResult:
    """Result from X12 node execution"""
    success: bool
    output: Dict[str, Any]
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    execution_time_ms: int = 0
    claims_processed: int = 0
    transaction_type: str = ""


class X12ParserNode:
    """
    X12 Parser Flow Node

    Parses X12 EDI content and extracts structured data.
    Can be used in Logic Weaver flows for healthcare claims processing.

    Example Flow Node:
        {
            "id": "parse-era",
            "type": "x12-parser",
            "position": {"x": 200, "y": 100},
            "data": {
                "label": "Parse ERA 835",
                "transaction_type": "835",
                "extract_claims": true,
                "determine_service_area": true
            }
        }
    """

    NODE_TYPE = "x12-parser"
    ICON = "📄"
    CATEGORY = "Healthcare"
    DESCRIPTION = "Parse X12 EDI files (835, 837, 270, 271, 276, 277)"

    def __init__(self, config: Optional[X12NodeConfig] = None):
        self.config = config or X12NodeConfig()
        self.parser = X12Parser()

        # Set service area lookup tables if configured
        if self.config.vendor_list:
            self.parser.set_vendor_list(self.config.vendor_list)
        if self.config.npi_list:
            self.parser.set_npi_list(self.config.npi_list)

    async def execute(
        self,
        payload: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> X12NodeResult:
        """
        Execute the X12 parser node.

        Args:
            payload: Input containing X12 content
                - content: Raw X12 string
                - file_path: Path to X12 file (alternative)
            context: Execution context (correlation_id, tenant_id, etc.)

        Returns:
            X12NodeResult with parsed data
        """
        start_time = datetime.now()
        errors = []
        warnings = []

        try:
            # Get X12 content
            content = payload.get("content") or payload.get("x12_content") or payload.get("raw")

            if not content and "file_path" in payload:
                # Read from file
                with open(payload["file_path"], "r") as f:
                    content = f.read()

            if not content:
                return X12NodeResult(
                    success=False,
                    output={},
                    errors=["No X12 content provided. Expected 'content' or 'file_path' in payload."],
                )

            # Parse X12
            result: X12ParseResult = self.parser.parse(content)

            # Build output
            output = result.to_dict()

            # Add context
            if context:
                output["_context"] = {
                    "correlation_id": context.get("correlation_id"),
                    "tenant_id": context.get("tenant_id"),
                    "execution_id": context.get("execution_id"),
                }

            # Collect warnings/errors
            errors.extend(result.errors)
            warnings.extend(result.warnings)

            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return X12NodeResult(
                success=len(errors) == 0,
                output=output,
                errors=errors,
                warnings=warnings,
                execution_time_ms=execution_time,
                claims_processed=result.total_claims,
                transaction_type=result.transaction_type.value,
            )

        except Exception as e:
            logger.exception(f"X12 parser error: {e}")
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
            return X12NodeResult(
                success=False,
                output={},
                errors=[str(e)],
                execution_time_ms=execution_time,
            )

    @classmethod
    def get_schema(cls) -> Dict[str, Any]:
        """Get JSON Schema for node configuration"""
        return {
            "type": "object",
            "properties": {
                "transaction_type": {
                    "type": "string",
                    "enum": ["835", "837", "270", "271", "276", "277", "auto"],
                    "default": "auto",
                    "description": "X12 transaction type to parse"
                },
                "extract_claims": {
                    "type": "boolean",
                    "default": True,
                    "description": "Extract individual claims"
                },
                "extract_segments": {
                    "type": "boolean",
                    "default": False,
                    "description": "Include raw segment data"
                },
                "determine_service_area": {
                    "type": "boolean",
                    "default": False,
                    "description": "Determine service area from NPI/invoice"
                },
            }
        }


class X12ValidatorNode:
    """
    X12 Validator Flow Node

    Validates X12 EDI content against HIPAA standards.

    Example Flow Node:
        {
            "id": "validate-claim",
            "type": "x12-validator",
            "data": {
                "label": "Validate 837 Claim",
                "strict_mode": true,
                "validate_segments": true
            }
        }
    """

    NODE_TYPE = "x12-validator"
    ICON = "✅"
    CATEGORY = "Healthcare"
    DESCRIPTION = "Validate X12 EDI content"

    def __init__(self, config: Optional[X12NodeConfig] = None):
        self.config = config or X12NodeConfig(node_type=X12NodeType.VALIDATOR)

    async def execute(
        self,
        payload: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> X12NodeResult:
        """Validate X12 content"""
        start_time = datetime.now()
        errors = []
        warnings = []

        try:
            content = payload.get("content") or payload.get("x12_content")

            if not content:
                return X12NodeResult(
                    success=False,
                    output={},
                    errors=["No X12 content provided"],
                )

            # Basic validation
            validation_result = self._validate(content)

            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return X12NodeResult(
                success=validation_result["valid"],
                output=validation_result,
                errors=validation_result.get("errors", []),
                warnings=validation_result.get("warnings", []),
                execution_time_ms=execution_time,
            )

        except Exception as e:
            logger.exception(f"X12 validation error: {e}")
            return X12NodeResult(
                success=False,
                output={},
                errors=[str(e)],
            )

    def _validate(self, content: str) -> Dict[str, Any]:
        """Perform X12 validation"""
        errors = []
        warnings = []

        # Check ISA segment
        if not content.strip().startswith("ISA"):
            errors.append("Missing ISA segment at start of file")

        # Check IEA segment
        if "IEA" not in content:
            errors.append("Missing IEA segment at end of file")

        # Check segment delimiter
        if len(content) >= 106:
            seg_delim = content[105]
            if seg_delim not in ["~", "\n"]:
                warnings.append(f"Unusual segment delimiter: {repr(seg_delim)}")

        # Check element delimiter
        if len(content) >= 4:
            elem_delim = content[3]
            if elem_delim != "*":
                warnings.append(f"Non-standard element delimiter: {repr(elem_delim)}")

        # Count segments
        seg_delim = content[105] if len(content) >= 106 else "~"
        segments = [s.strip() for s in content.split(seg_delim) if s.strip()]

        # Check ST/SE matching
        st_count = sum(1 for s in segments if s.startswith("ST*"))
        se_count = sum(1 for s in segments if s.startswith("SE*"))
        if st_count != se_count:
            errors.append(f"ST/SE mismatch: {st_count} ST segments, {se_count} SE segments")

        # Check GS/GE matching
        gs_count = sum(1 for s in segments if s.startswith("GS*"))
        ge_count = sum(1 for s in segments if s.startswith("GE*"))
        if gs_count != ge_count:
            errors.append(f"GS/GE mismatch: {gs_count} GS segments, {ge_count} GE segments")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "segment_count": len(segments),
            "transaction_sets": st_count,
            "functional_groups": gs_count,
        }


class X12SplitterNode:
    """
    X12 Splitter Flow Node

    Splits X12 files by claims, transactions, or functional groups.

    Example Flow Node:
        {
            "id": "split-claims",
            "type": "x12-splitter",
            "data": {
                "label": "Split by Claim",
                "split_by": "claim",
                "output_format": "json"
            }
        }
    """

    NODE_TYPE = "x12-splitter"
    ICON = "✂️"
    CATEGORY = "Healthcare"
    DESCRIPTION = "Split X12 file by claims or transactions"

    def __init__(self, config: Optional[X12NodeConfig] = None):
        self.config = config or X12NodeConfig(node_type=X12NodeType.SPLITTER)
        self.parser = X12Parser()

    async def execute(
        self,
        payload: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> X12NodeResult:
        """Split X12 content"""
        start_time = datetime.now()

        try:
            content = payload.get("content") or payload.get("x12_content")

            if not content:
                return X12NodeResult(
                    success=False,
                    output={},
                    errors=["No X12 content provided"],
                )

            # Parse first
            result = self.parser.parse(content)

            # Split based on config
            if self.config.split_by == "claim":
                splits = [claim for claim in result.claims]
                output = {
                    "splits": [self._claim_to_dict(c) for c in splits],
                    "count": len(splits),
                    "transaction_type": result.transaction_type.value,
                }
            elif self.config.split_by == "transaction":
                # Return full transaction
                output = {
                    "splits": [result.to_dict()],
                    "count": 1,
                    "transaction_type": result.transaction_type.value,
                }
            else:
                output = {
                    "splits": [result.to_dict()],
                    "count": 1,
                }

            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return X12NodeResult(
                success=True,
                output=output,
                execution_time_ms=execution_time,
                claims_processed=len(result.claims),
                transaction_type=result.transaction_type.value,
            )

        except Exception as e:
            logger.exception(f"X12 splitter error: {e}")
            return X12NodeResult(
                success=False,
                output={},
                errors=[str(e)],
            )

    def _claim_to_dict(self, claim: X12Claim) -> Dict[str, Any]:
        """Convert claim to dictionary"""
        return {
            "patient_control_number": claim.patient_control_number,
            "status": claim.claim_status,
            "status_description": claim.claim_status_description,
            "charge_amount": str(claim.charge_amount),
            "payment_amount": str(claim.payment_amount),
            "patient_responsibility": str(claim.patient_responsibility),
            "payer_claim_number": claim.payer_claim_number,
            "service_lines": len(claim.service_lines),
            "adjustments": len(claim.adjustments),
        }


class X12ServiceAreaNode:
    """
    X12 Service Area Determination Node

    Determines service area from ERA files based on NPI, invoice prefix, or check number.
    Adapted from HSS ERA processor for Logic Weaver integration.

    Example Flow Node:
        {
            "id": "determine-sa",
            "type": "x12-service-area",
            "data": {
                "label": "Determine Service Area",
                "vendor_list": [["400", "40"], ["500", "PB"]],
                "npi_list": [["500", "1234567890"]]
            }
        }
    """

    NODE_TYPE = "x12-service-area"
    ICON = "🏥"
    CATEGORY = "Healthcare"
    DESCRIPTION = "Determine service area from ERA files"

    def __init__(self, config: Optional[X12NodeConfig] = None):
        self.config = config or X12NodeConfig(node_type=X12NodeType.SERVICE_AREA)
        self.parser = X12Parser()

        # Set lookup tables
        if self.config.vendor_list:
            self.parser.set_vendor_list(self.config.vendor_list)
        if self.config.npi_list:
            self.parser.set_npi_list(self.config.npi_list)

    async def execute(
        self,
        payload: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> X12NodeResult:
        """Determine service area"""
        start_time = datetime.now()

        try:
            content = payload.get("content") or payload.get("x12_content")

            if not content:
                return X12NodeResult(
                    success=False,
                    output={},
                    errors=["No X12 content provided"],
                )

            # Parse ERA file
            result = self.parser.parse(content)

            # Build output with service area info
            output = {
                "service_area": result.service_area,
                "payee_npi": result.payee_npi,
                "invoice_prefixes": result.invoice_prefixes,
                "check_number": result.check_number,
                "payment_amount": str(result.payment_amount) if result.payment_amount else None,
                "total_claims": result.total_claims,
                "determination_method": self._get_determination_method(result),
            }

            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

            return X12NodeResult(
                success=True,
                output=output,
                execution_time_ms=execution_time,
                claims_processed=result.total_claims,
                transaction_type=result.transaction_type.value,
            )

        except Exception as e:
            logger.exception(f"Service area determination error: {e}")
            return X12NodeResult(
                success=False,
                output={},
                errors=[str(e)],
            )

    def _get_determination_method(self, result: X12ParseResult) -> str:
        """Determine how service area was identified"""
        if result.service_area == "9999":
            return "not_found"

        # Check if determined by NPI
        for sa, npi in self.config.npi_list:
            if result.payee_npi == npi and result.service_area == sa:
                return "npi"

        # Check if determined by invoice prefix
        for sa, prefix in self.config.vendor_list:
            for inv_prefix in result.invoice_prefixes:
                if inv_prefix == prefix and result.service_area == sa:
                    return "invoice_prefix"

        return "unknown"


# Node registry for flow engine
X12_NODES = {
    X12ParserNode.NODE_TYPE: X12ParserNode,
    X12ValidatorNode.NODE_TYPE: X12ValidatorNode,
    X12SplitterNode.NODE_TYPE: X12SplitterNode,
    X12ServiceAreaNode.NODE_TYPE: X12ServiceAreaNode,
}


def get_x12_node(node_type: str, config: Optional[Dict[str, Any]] = None):
    """Factory function to get X12 node by type"""
    node_class = X12_NODES.get(node_type)
    if not node_class:
        raise ValueError(f"Unknown X12 node type: {node_type}")

    node_config = X12NodeConfig(**config) if config else None
    return node_class(node_config)


def get_x12_node_definitions() -> List[Dict[str, Any]]:
    """Get node definitions for flow designer UI"""
    return [
        {
            "type": X12ParserNode.NODE_TYPE,
            "label": "X12 Parser",
            "icon": X12ParserNode.ICON,
            "category": X12ParserNode.CATEGORY,
            "description": X12ParserNode.DESCRIPTION,
            "schema": X12ParserNode.get_schema(),
            "inputs": ["x12_content"],
            "outputs": ["parsed_data", "claims"],
        },
        {
            "type": X12ValidatorNode.NODE_TYPE,
            "label": "X12 Validator",
            "icon": X12ValidatorNode.ICON,
            "category": X12ValidatorNode.CATEGORY,
            "description": X12ValidatorNode.DESCRIPTION,
            "inputs": ["x12_content"],
            "outputs": ["validation_result"],
        },
        {
            "type": X12SplitterNode.NODE_TYPE,
            "label": "X12 Splitter",
            "icon": X12SplitterNode.ICON,
            "category": X12SplitterNode.CATEGORY,
            "description": X12SplitterNode.DESCRIPTION,
            "inputs": ["x12_content"],
            "outputs": ["splits"],
        },
        {
            "type": X12ServiceAreaNode.NODE_TYPE,
            "label": "Service Area",
            "icon": X12ServiceAreaNode.ICON,
            "category": X12ServiceAreaNode.CATEGORY,
            "description": X12ServiceAreaNode.DESCRIPTION,
            "inputs": ["x12_content"],
            "outputs": ["service_area", "payee_info"],
        },
    ]
