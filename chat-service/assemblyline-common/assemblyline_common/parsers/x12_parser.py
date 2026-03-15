"""
X12 EDI Parser for Logic Weaver

Enterprise-grade X12 Electronic Data Interchange parser supporting healthcare transactions.
Adapted from HSS ERA processor with enhancements for universal payload processing.

Supported Transaction Sets:
- 835: Health Care Claim Payment/Advice (ERA)
- 837: Health Care Claim (Professional, Institutional, Dental)
- 270: Health Care Eligibility Benefit Inquiry
- 271: Health Care Eligibility Benefit Response
- 276: Health Care Claim Status Request
- 277: Health Care Claim Status Response
- 278: Health Care Services Review

Features:
- Automatic delimiter detection
- Segment-level parsing with element descriptions
- Claim, service line, and adjustment extraction
- Validation support
- JSON/dict output for flow integration
- Async-ready for high throughput

@author: Logic Weaver Development
@version: 1.0.0
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal

logger = logging.getLogger(__name__)


class X12TransactionType(Enum):
    """X12 Transaction Set Types"""
    ERA_835 = "835"  # Health Care Claim Payment/Advice
    CLAIM_837 = "837"  # Health Care Claim
    ELIGIBILITY_270 = "270"  # Eligibility Inquiry
    ELIGIBILITY_271 = "271"  # Eligibility Response
    CLAIM_STATUS_276 = "276"  # Claim Status Request
    CLAIM_STATUS_277 = "277"  # Claim Status Response
    SERVICES_REVIEW_278 = "278"  # Services Review
    FUNCTIONAL_ACK_999 = "999"  # Functional Acknowledgment
    UNKNOWN = "UNKNOWN"


class X12ClaimStatus(Enum):
    """Claim Status Codes (CLP02)"""
    PROCESSED_PRIMARY = "1"
    PROCESSED_SECONDARY = "2"
    PROCESSED_TERTIARY = "3"
    DENIED = "4"
    PROCESSED_PATIENT_RESP = "19"
    PENDING = "20"
    PROCESSED_FORWARD = "21"
    REVERSED = "22"
    NOT_ADJUDICATED = "23"


class X12AdjustmentGroup(Enum):
    """Claim Adjustment Group Codes (CAS01)"""
    CONTRACTUAL = "CO"  # Contractual Obligations
    CORRECTION = "CR"  # Corrections and Reversals
    OTHER = "OA"  # Other Adjustments
    PAYER = "PI"  # Payer Initiated Reductions
    PATIENT = "PR"  # Patient Responsibility


@dataclass
class X12Element:
    """Single X12 element"""
    position: int
    value: str
    description: str = ""
    component_values: List[str] = field(default_factory=list)


@dataclass
class X12Segment:
    """Parsed X12 segment"""
    segment_id: str
    elements: List[X12Element]
    raw: str
    description: str = ""
    line_number: int = 0

    def get_element(self, position: int, default: str = "") -> str:
        """Get element value by position (1-indexed as per X12 standard)"""
        if 0 < position <= len(self.elements):
            return self.elements[position - 1].value
        return default

    def to_dict(self) -> Dict[str, Any]:
        """Convert segment to dictionary"""
        return {
            "segment_id": self.segment_id,
            "description": self.description,
            "elements": [
                {
                    "position": e.position,
                    "value": e.value,
                    "description": e.description,
                    "components": e.component_values if e.component_values else None
                }
                for e in self.elements
            ],
            "raw": self.raw
        }


@dataclass
class X12Adjustment:
    """Claim or service line adjustment"""
    group_code: str
    group_description: str
    reason_code: str
    amount: Decimal
    quantity: Optional[int] = None


@dataclass
class X12ServiceLine:
    """Service line information (SVC segment)"""
    procedure_code: str
    procedure_modifier: List[str]
    charge_amount: Decimal
    payment_amount: Decimal
    units: Optional[int] = None
    adjustments: List[X12Adjustment] = field(default_factory=list)
    dates: Dict[str, str] = field(default_factory=dict)
    remarks: List[str] = field(default_factory=list)


@dataclass
class X12Claim:
    """Claim information (CLP segment)"""
    patient_control_number: str
    claim_status: str
    claim_status_description: str
    charge_amount: Decimal
    payment_amount: Decimal
    patient_responsibility: Decimal
    claim_filing_indicator: str
    payer_claim_number: str
    facility_code: str = ""
    frequency_code: str = ""
    drg_code: str = ""
    drg_weight: Optional[Decimal] = None
    service_lines: List[X12ServiceLine] = field(default_factory=list)
    adjustments: List[X12Adjustment] = field(default_factory=list)
    patient: Dict[str, str] = field(default_factory=dict)
    provider: Dict[str, str] = field(default_factory=dict)
    dates: Dict[str, str] = field(default_factory=dict)
    remarks: List[str] = field(default_factory=list)


@dataclass
class X12ParseResult:
    """Complete X12 parse result"""
    transaction_type: X12TransactionType
    transaction_type_code: str
    parsed_at: datetime

    # Envelope info
    interchange_control_number: str = ""
    interchange_date: str = ""
    interchange_time: str = ""
    sender_id: str = ""
    receiver_id: str = ""
    version: str = ""

    # Functional group info
    functional_id: str = ""
    group_control_number: str = ""

    # Transaction info
    transaction_control_number: str = ""

    # Payment info (835)
    payment_amount: Optional[Decimal] = None
    payment_method: str = ""
    check_number: str = ""
    check_date: str = ""
    credit_debit: str = ""

    # Payer/Payee info
    payer: Dict[str, str] = field(default_factory=dict)
    payee: Dict[str, str] = field(default_factory=dict)
    payee_npi: str = ""

    # Claims
    claims: List[X12Claim] = field(default_factory=list)

    # Eligibility (270/271)
    subscriber: Dict[str, str] = field(default_factory=dict)
    dependent: Dict[str, str] = field(default_factory=dict)
    benefits: List[Dict[str, Any]] = field(default_factory=list)

    # All segments
    segments: List[X12Segment] = field(default_factory=list)

    # Metadata
    total_segments: int = 0
    total_claims: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # Service area (for ERA processing)
    service_area: str = ""
    invoice_prefixes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "transaction_type": self.transaction_type.value,
            "transaction_type_code": self.transaction_type_code,
            "parsed_at": self.parsed_at.isoformat(),
            "envelope": {
                "interchange_control_number": self.interchange_control_number,
                "interchange_date": self.interchange_date,
                "interchange_time": self.interchange_time,
                "sender_id": self.sender_id,
                "receiver_id": self.receiver_id,
                "version": self.version,
            },
            "functional_group": {
                "functional_id": self.functional_id,
                "group_control_number": self.group_control_number,
            },
            "transaction": {
                "control_number": self.transaction_control_number,
            },
            "payment": {
                "amount": str(self.payment_amount) if self.payment_amount else None,
                "method": self.payment_method,
                "check_number": self.check_number,
                "check_date": self.check_date,
                "credit_debit": self.credit_debit,
            },
            "payer": self.payer,
            "payee": {**self.payee, "npi": self.payee_npi},
            "claims": [self._claim_to_dict(c) for c in self.claims],
            "subscriber": self.subscriber,
            "dependent": self.dependent,
            "benefits": self.benefits,
            "service_area": self.service_area,
            "invoice_prefixes": self.invoice_prefixes,
            "metadata": {
                "total_segments": self.total_segments,
                "total_claims": self.total_claims,
                "errors": self.errors,
                "warnings": self.warnings,
            }
        }

    def _claim_to_dict(self, claim: X12Claim) -> Dict[str, Any]:
        """Convert claim to dictionary"""
        return {
            "patient_control_number": claim.patient_control_number,
            "status": claim.claim_status,
            "status_description": claim.claim_status_description,
            "charge_amount": str(claim.charge_amount),
            "payment_amount": str(claim.payment_amount),
            "patient_responsibility": str(claim.patient_responsibility),
            "claim_filing_indicator": claim.claim_filing_indicator,
            "payer_claim_number": claim.payer_claim_number,
            "facility_code": claim.facility_code,
            "drg_code": claim.drg_code,
            "drg_weight": str(claim.drg_weight) if claim.drg_weight else None,
            "patient": claim.patient,
            "provider": claim.provider,
            "dates": claim.dates,
            "adjustments": [self._adjustment_to_dict(a) for a in claim.adjustments],
            "service_lines": [self._service_line_to_dict(s) for s in claim.service_lines],
            "remarks": claim.remarks,
        }

    def _adjustment_to_dict(self, adj: X12Adjustment) -> Dict[str, Any]:
        """Convert adjustment to dictionary"""
        return {
            "group_code": adj.group_code,
            "group_description": adj.group_description,
            "reason_code": adj.reason_code,
            "amount": str(adj.amount),
            "quantity": adj.quantity,
        }

    def _service_line_to_dict(self, svc: X12ServiceLine) -> Dict[str, Any]:
        """Convert service line to dictionary"""
        return {
            "procedure_code": svc.procedure_code,
            "procedure_modifier": svc.procedure_modifier,
            "charge_amount": str(svc.charge_amount),
            "payment_amount": str(svc.payment_amount),
            "units": svc.units,
            "dates": svc.dates,
            "adjustments": [self._adjustment_to_dict(a) for a in svc.adjustments],
            "remarks": svc.remarks,
        }


class X12Parser:
    """
    Enterprise X12 EDI Parser

    Supports multiple healthcare transaction types with automatic
    delimiter detection and comprehensive segment parsing.

    Example:
        parser = X12Parser()
        result = parser.parse(x12_content)
        print(f"Transaction: {result.transaction_type.value}")
        for claim in result.claims:
            print(f"Claim {claim.patient_control_number}: ${claim.payment_amount}")
    """

    # Segment descriptions
    SEGMENT_DESCRIPTIONS = {
        "ISA": "Interchange Control Header",
        "IEA": "Interchange Control Trailer",
        "GS": "Functional Group Header",
        "GE": "Functional Group Trailer",
        "ST": "Transaction Set Header",
        "SE": "Transaction Set Trailer",
        "BPR": "Financial Information",
        "TRN": "Trace Number",
        "CUR": "Currency",
        "REF": "Reference Information",
        "DTM": "Date/Time Reference",
        "N1": "Name",
        "N2": "Additional Name Information",
        "N3": "Address Information",
        "N4": "Geographic Location",
        "PER": "Administrative Communications Contact",
        "CLP": "Claim Payment Information",
        "CAS": "Claim Adjustment",
        "NM1": "Individual or Organizational Name",
        "MIA": "Inpatient Adjudication Information",
        "MOA": "Outpatient Adjudication Information",
        "SVC": "Service Payment Information",
        "LQ": "Industry Code Identification",
        "QTY": "Quantity Information",
        "AMT": "Monetary Amount Information",
        "PLB": "Provider Level Adjustment",
        "TS2": "Provider Summary Information",
        "TS3": "Provider Summary Information",
        "HL": "Hierarchical Level",
        "SBR": "Subscriber Information",
        "PAT": "Patient Information",
        "DTP": "Date or Time Period",
        "CLM": "Health Claim",
        "HI": "Health Care Information Codes",
        "HSD": "Health Care Services Delivery",
        "CRC": "Conditions Indicator",
        "OI": "Other Health Insurance Information",
        "NTE": "Note/Special Instruction",
        "K3": "File Information",
        "BHT": "Beginning of Hierarchical Transaction",
        "AAA": "Request Validation",
        "EB": "Eligibility or Benefit Information",
        "LS": "Loop Header",
        "LE": "Loop Trailer",
        "MSG": "Message Text",
        "STC": "Status Information",
        "TRN": "Trace",
    }

    # Claim status descriptions
    CLAIM_STATUS_DESC = {
        "1": "Processed as Primary",
        "2": "Processed as Secondary",
        "3": "Processed as Tertiary",
        "4": "Denied",
        "19": "Processed as Primary, Patient Responsibility",
        "20": "Pending",
        "21": "Processed as Primary, Forwarded",
        "22": "Reversed",
        "23": "Not Adjudicated",
    }

    # Adjustment group descriptions
    ADJUSTMENT_GROUP_DESC = {
        "CO": "Contractual Obligations",
        "CR": "Corrections and Reversals",
        "OA": "Other Adjustments",
        "PI": "Payer Initiated Reductions",
        "PR": "Patient Responsibility",
    }

    # Transaction type names
    TRANSACTION_TYPES = {
        "835": X12TransactionType.ERA_835,
        "837": X12TransactionType.CLAIM_837,
        "270": X12TransactionType.ELIGIBILITY_270,
        "271": X12TransactionType.ELIGIBILITY_271,
        "276": X12TransactionType.CLAIM_STATUS_276,
        "277": X12TransactionType.CLAIM_STATUS_277,
        "278": X12TransactionType.SERVICES_REVIEW_278,
        "999": X12TransactionType.FUNCTIONAL_ACK_999,
    }

    def __init__(self):
        """Initialize the X12 parser"""
        self.segment_delimiter = "~"
        self.element_delimiter = "*"
        self.component_delimiter = ":"
        self.repetition_delimiter = "^"

        # Service area lookup tables (for ERA processing)
        self.vendor_list: List[Tuple[str, str]] = []
        self.npi_list: List[Tuple[str, str]] = []

    def set_vendor_list(self, vendor_list: List[Tuple[str, str]]):
        """Set vendor list for service area determination (SA, prefix)"""
        self.vendor_list = vendor_list

    def set_npi_list(self, npi_list: List[Tuple[str, str]]):
        """Set NPI list for service area determination (SA, NPI)"""
        self.npi_list = npi_list

    def parse(self, content: str) -> X12ParseResult:
        """
        Parse X12 EDI content

        Args:
            content: Raw X12 EDI content

        Returns:
            X12ParseResult with parsed data
        """
        result = X12ParseResult(
            transaction_type=X12TransactionType.UNKNOWN,
            transaction_type_code="",
            parsed_at=datetime.now()
        )

        try:
            # Detect delimiters from ISA segment
            self._detect_delimiters(content)

            # Split into segments
            segments = self._split_segments(content)
            result.total_segments = len(segments)

            # Parse each segment
            parsed_segments = []
            for i, raw_segment in enumerate(segments):
                segment = self._parse_segment(raw_segment, i + 1)
                if segment:
                    parsed_segments.append(segment)

            result.segments = parsed_segments

            # Extract structured data based on transaction type
            self._extract_envelope(parsed_segments, result)
            self._extract_functional_group(parsed_segments, result)
            self._extract_transaction_header(parsed_segments, result)

            # Parse based on transaction type
            if result.transaction_type == X12TransactionType.ERA_835:
                self._parse_835(parsed_segments, result)
            elif result.transaction_type == X12TransactionType.CLAIM_837:
                self._parse_837(parsed_segments, result)
            elif result.transaction_type in (X12TransactionType.ELIGIBILITY_270,
                                              X12TransactionType.ELIGIBILITY_271):
                self._parse_270_271(parsed_segments, result)
            elif result.transaction_type in (X12TransactionType.CLAIM_STATUS_276,
                                              X12TransactionType.CLAIM_STATUS_277):
                self._parse_276_277(parsed_segments, result)

            result.total_claims = len(result.claims)

        except Exception as e:
            logger.exception(f"Error parsing X12 content: {e}")
            result.errors.append(f"Parse error: {str(e)}")

        return result

    def parse_file(self, file_path: str) -> X12ParseResult:
        """
        Parse X12 file

        Args:
            file_path: Path to X12 file

        Returns:
            X12ParseResult with parsed data
        """
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        return self.parse(content)

    def _detect_delimiters(self, content: str):
        """Detect X12 delimiters from ISA segment"""
        content = content.strip()

        if content.startswith("ISA"):
            # Element delimiter is position 3 (after ISA)
            if len(content) > 3:
                self.element_delimiter = content[3]

            # Segment delimiter is position 105
            if len(content) >= 106:
                self.segment_delimiter = content[105]

            # Component delimiter is ISA16
            if len(content) >= 105:
                isa_elements = content[:105].split(self.element_delimiter)
                if len(isa_elements) >= 16:
                    self.component_delimiter = isa_elements[16][0] if isa_elements[16] else ":"

            logger.debug(
                f"Delimiters detected - segment: '{self.segment_delimiter}', "
                f"element: '{self.element_delimiter}', "
                f"component: '{self.component_delimiter}'"
            )

    def _split_segments(self, content: str) -> List[str]:
        """Split content into segments"""
        # Handle both single-line and multi-line formats
        content = content.replace('\r\n', '').replace('\n', '').replace('\r', '')
        segments = content.split(self.segment_delimiter)
        return [s.strip() for s in segments if s.strip()]

    def _parse_segment(self, raw: str, line_number: int) -> Optional[X12Segment]:
        """Parse a single segment into structured form"""
        if not raw:
            return None

        elements_raw = raw.split(self.element_delimiter)
        if not elements_raw:
            return None

        segment_id = elements_raw[0]
        description = self.SEGMENT_DESCRIPTIONS.get(segment_id, f"Unknown ({segment_id})")

        elements = []
        for i, value in enumerate(elements_raw):
            # Split components if present
            component_values = []
            if self.component_delimiter in value and i > 0:
                component_values = value.split(self.component_delimiter)

            elements.append(X12Element(
                position=i,
                value=value,
                description=self._get_element_description(segment_id, i),
                component_values=component_values
            ))

        return X12Segment(
            segment_id=segment_id,
            elements=elements,
            raw=raw,
            description=description,
            line_number=line_number
        )

    def _get_element_description(self, segment_id: str, position: int) -> str:
        """Get description for element at position in segment"""
        # Element descriptions for common segments
        descriptions = {
            "ISA": {
                0: "Segment ID", 1: "Authorization Info Qualifier", 2: "Authorization Info",
                3: "Security Info Qualifier", 4: "Security Info", 5: "Interchange ID Qualifier",
                6: "Interchange Sender ID", 7: "Interchange ID Qualifier", 8: "Interchange Receiver ID",
                9: "Interchange Date", 10: "Interchange Time", 11: "Interchange Control Standards",
                12: "Interchange Control Version", 13: "Interchange Control Number",
                14: "Acknowledgment Requested", 15: "Usage Indicator", 16: "Component Separator"
            },
            "GS": {
                0: "Segment ID", 1: "Functional ID Code", 2: "Application Sender Code",
                3: "Application Receiver Code", 4: "Date", 5: "Time",
                6: "Group Control Number", 7: "Responsible Agency Code", 8: "Version"
            },
            "ST": {
                0: "Segment ID", 1: "Transaction Set ID", 2: "Transaction Set Control Number",
                3: "Implementation Convention Reference"
            },
            "BPR": {
                0: "Segment ID", 1: "Transaction Handling Code", 2: "Monetary Amount",
                3: "Credit/Debit Flag", 4: "Payment Method Code", 5: "Payment Format Code",
                6: "Sender DFI Qualifier", 7: "Sender DFI ID", 8: "Sender Account Qualifier",
                9: "Sender Account Number", 10: "Originating Company ID", 11: "Originating Company Suppl Code",
                12: "Receiver DFI Qualifier", 13: "Receiver DFI ID", 14: "Receiver Account Qualifier",
                15: "Receiver Account Number", 16: "Check Issue Date"
            },
            "TRN": {
                0: "Segment ID", 1: "Trace Type Code", 2: "Reference ID",
                3: "Originating Company ID", 4: "Reference ID"
            },
            "N1": {
                0: "Segment ID", 1: "Entity ID Code", 2: "Name",
                3: "ID Qualifier", 4: "Identification Code"
            },
            "CLP": {
                0: "Segment ID", 1: "Patient Control Number", 2: "Claim Status Code",
                3: "Charge Amount", 4: "Payment Amount", 5: "Patient Responsibility",
                6: "Claim Filing Indicator", 7: "Payer Claim Control Number",
                8: "Facility Code", 9: "Claim Frequency Code", 10: "Patient Status Code",
                11: "DRG Code", 12: "DRG Weight", 13: "Discharge Fraction"
            },
            "CAS": {
                0: "Segment ID", 1: "Adjustment Group Code", 2: "Adjustment Reason Code",
                3: "Adjustment Amount", 4: "Adjustment Quantity",
                5: "Reason Code 2", 6: "Amount 2", 7: "Quantity 2",
                8: "Reason Code 3", 9: "Amount 3", 10: "Quantity 3"
            },
            "SVC": {
                0: "Segment ID", 1: "Procedure Code", 2: "Charge Amount",
                3: "Payment Amount", 4: "Revenue Code", 5: "Units Paid",
                6: "Procedure Code 2"
            },
            "NM1": {
                0: "Segment ID", 1: "Entity ID Code", 2: "Entity Type Qualifier",
                3: "Last/Org Name", 4: "First Name", 5: "Middle Name",
                6: "Name Prefix", 7: "Name Suffix", 8: "ID Code Qualifier", 9: "ID Code"
            },
        }

        if segment_id in descriptions and position in descriptions[segment_id]:
            return descriptions[segment_id][position]
        return f"Element {position}"

    def _extract_envelope(self, segments: List[X12Segment], result: X12ParseResult):
        """Extract ISA envelope information"""
        for seg in segments:
            if seg.segment_id == "ISA":
                result.sender_id = seg.get_element(6, "").strip()
                result.receiver_id = seg.get_element(8, "").strip()
                result.interchange_date = seg.get_element(9, "")
                result.interchange_time = seg.get_element(10, "")
                result.version = seg.get_element(12, "")
                result.interchange_control_number = seg.get_element(13, "")
                break

    def _extract_functional_group(self, segments: List[X12Segment], result: X12ParseResult):
        """Extract GS functional group information"""
        for seg in segments:
            if seg.segment_id == "GS":
                result.functional_id = seg.get_element(1, "")
                result.group_control_number = seg.get_element(6, "")
                break

    def _extract_transaction_header(self, segments: List[X12Segment], result: X12ParseResult):
        """Extract ST transaction set header"""
        for seg in segments:
            if seg.segment_id == "ST":
                trans_code = seg.get_element(1, "")
                result.transaction_type_code = trans_code
                result.transaction_type = self.TRANSACTION_TYPES.get(
                    trans_code, X12TransactionType.UNKNOWN
                )
                result.transaction_control_number = seg.get_element(2, "")
                break

    def _parse_835(self, segments: List[X12Segment], result: X12ParseResult):
        """Parse 835 ERA-specific segments"""
        current_claim: Optional[X12Claim] = None
        current_service: Optional[X12ServiceLine] = None
        invoice_prefixes = set()

        for seg in segments:
            if seg.segment_id == "BPR":
                # Financial information
                result.payment_amount = self._to_decimal(seg.get_element(2, "0"))
                result.credit_debit = seg.get_element(3, "")
                result.payment_method = seg.get_element(4, "")
                result.check_date = seg.get_element(16, "")

            elif seg.segment_id == "TRN":
                result.check_number = seg.get_element(2, "")

            elif seg.segment_id == "N1":
                entity_code = seg.get_element(1, "")
                name = seg.get_element(2, "")
                id_qualifier = seg.get_element(3, "")
                id_code = seg.get_element(4, "")

                if entity_code == "PR":  # Payer
                    result.payer = {
                        "name": name,
                        "id_qualifier": id_qualifier,
                        "id": id_code
                    }
                elif entity_code == "PE":  # Payee
                    result.payee = {
                        "name": name,
                        "id_qualifier": id_qualifier,
                        "id": id_code
                    }
                    if id_qualifier == "XX":  # NPI
                        result.payee_npi = id_code

            elif seg.segment_id == "CLP":
                # New claim
                if current_claim:
                    result.claims.append(current_claim)

                pcn = seg.get_element(1, "")
                status = seg.get_element(2, "")

                current_claim = X12Claim(
                    patient_control_number=pcn,
                    claim_status=status,
                    claim_status_description=self.CLAIM_STATUS_DESC.get(status, "Unknown"),
                    charge_amount=self._to_decimal(seg.get_element(3, "0")),
                    payment_amount=self._to_decimal(seg.get_element(4, "0")),
                    patient_responsibility=self._to_decimal(seg.get_element(5, "0")),
                    claim_filing_indicator=seg.get_element(6, ""),
                    payer_claim_number=seg.get_element(7, ""),
                    facility_code=seg.get_element(8, ""),
                    frequency_code=seg.get_element(9, ""),
                    drg_code=seg.get_element(11, ""),
                    drg_weight=self._to_decimal(seg.get_element(12, "0")) if seg.get_element(12) else None
                )
                current_service = None

                # Extract invoice prefix
                if len(pcn) >= 2:
                    invoice_prefixes.add(pcn[:2])

            elif seg.segment_id == "CAS" and current_claim:
                # Claim adjustment
                adjustments = self._parse_cas_segment(seg)
                if current_service:
                    current_service.adjustments.extend(adjustments)
                else:
                    current_claim.adjustments.extend(adjustments)

            elif seg.segment_id == "NM1" and current_claim:
                entity_code = seg.get_element(1, "")
                if entity_code == "QC":  # Patient
                    current_claim.patient = {
                        "last_name": seg.get_element(3, ""),
                        "first_name": seg.get_element(4, ""),
                        "middle_name": seg.get_element(5, ""),
                        "id": seg.get_element(9, "")
                    }
                elif entity_code == "82":  # Rendering Provider
                    current_claim.provider = {
                        "last_name": seg.get_element(3, ""),
                        "first_name": seg.get_element(4, ""),
                        "npi": seg.get_element(9, "") if seg.get_element(8) == "XX" else ""
                    }

            elif seg.segment_id == "SVC" and current_claim:
                # Service line
                proc_code_composite = seg.get_element(1, "")
                proc_parts = proc_code_composite.split(self.component_delimiter)

                current_service = X12ServiceLine(
                    procedure_code=proc_parts[1] if len(proc_parts) > 1 else proc_parts[0],
                    procedure_modifier=proc_parts[2:6] if len(proc_parts) > 2 else [],
                    charge_amount=self._to_decimal(seg.get_element(2, "0")),
                    payment_amount=self._to_decimal(seg.get_element(3, "0")),
                    units=int(seg.get_element(5, "0")) if seg.get_element(5) else None
                )
                current_claim.service_lines.append(current_service)

            elif seg.segment_id == "DTM":
                date_qualifier = seg.get_element(1, "")
                date_value = seg.get_element(2, "")
                if current_service:
                    current_service.dates[date_qualifier] = date_value
                elif current_claim:
                    current_claim.dates[date_qualifier] = date_value

            elif seg.segment_id == "LQ" and current_claim:
                # Remark code
                code_qualifier = seg.get_element(1, "")
                code = seg.get_element(2, "")
                remark = f"{code_qualifier}:{code}"
                if current_service:
                    current_service.remarks.append(remark)
                else:
                    current_claim.remarks.append(remark)

        # Add last claim
        if current_claim:
            result.claims.append(current_claim)

        result.invoice_prefixes = list(invoice_prefixes)

        # Determine service area
        result.service_area = self._determine_service_area(result)

    def _parse_837(self, segments: List[X12Segment], result: X12ParseResult):
        """Parse 837 Claim segments"""
        current_claim: Optional[X12Claim] = None
        hl_level = ""

        for seg in segments:
            if seg.segment_id == "HL":
                hl_level = seg.get_element(3, "")  # 20=Information Source, 22=Subscriber, 23=Dependent

            elif seg.segment_id == "NM1":
                entity_code = seg.get_element(1, "")
                if entity_code == "IL":  # Subscriber
                    result.subscriber = {
                        "last_name": seg.get_element(3, ""),
                        "first_name": seg.get_element(4, ""),
                        "middle_name": seg.get_element(5, ""),
                        "id": seg.get_element(9, "")
                    }
                elif entity_code == "PR":  # Payer
                    result.payer = {
                        "name": seg.get_element(3, ""),
                        "id": seg.get_element(9, "")
                    }
                elif entity_code == "85":  # Billing Provider
                    result.payee = {
                        "name": seg.get_element(3, ""),
                        "npi": seg.get_element(9, "") if seg.get_element(8) == "XX" else ""
                    }
                    if seg.get_element(8) == "XX":
                        result.payee_npi = seg.get_element(9, "")

            elif seg.segment_id == "CLM":
                # New claim
                if current_claim:
                    result.claims.append(current_claim)

                current_claim = X12Claim(
                    patient_control_number=seg.get_element(1, ""),
                    claim_status="",
                    claim_status_description="Submitted",
                    charge_amount=self._to_decimal(seg.get_element(2, "0")),
                    payment_amount=Decimal("0"),
                    patient_responsibility=Decimal("0"),
                    claim_filing_indicator="",
                    payer_claim_number=""
                )

            elif seg.segment_id == "SV1" and current_claim:
                # Professional service line
                proc_code = seg.get_element(1, "")
                proc_parts = proc_code.split(self.component_delimiter) if self.component_delimiter in proc_code else [proc_code]

                service = X12ServiceLine(
                    procedure_code=proc_parts[1] if len(proc_parts) > 1 else proc_parts[0],
                    procedure_modifier=proc_parts[2:6] if len(proc_parts) > 2 else [],
                    charge_amount=self._to_decimal(seg.get_element(2, "0")),
                    payment_amount=Decimal("0"),
                    units=int(seg.get_element(4, "1")) if seg.get_element(4) else 1
                )
                current_claim.service_lines.append(service)

        # Add last claim
        if current_claim:
            result.claims.append(current_claim)

    def _parse_270_271(self, segments: List[X12Segment], result: X12ParseResult):
        """Parse 270/271 Eligibility segments"""
        current_benefit = None

        for seg in segments:
            if seg.segment_id == "NM1":
                entity_code = seg.get_element(1, "")
                if entity_code == "IL":  # Subscriber
                    result.subscriber = {
                        "last_name": seg.get_element(3, ""),
                        "first_name": seg.get_element(4, ""),
                        "middle_name": seg.get_element(5, ""),
                        "id": seg.get_element(9, ""),
                        "id_qualifier": seg.get_element(8, "")
                    }
                elif entity_code == "PR":  # Payer
                    result.payer = {
                        "name": seg.get_element(3, ""),
                        "id": seg.get_element(9, "")
                    }
                elif entity_code == "03":  # Dependent
                    result.dependent = {
                        "last_name": seg.get_element(3, ""),
                        "first_name": seg.get_element(4, ""),
                        "middle_name": seg.get_element(5, "")
                    }

            elif seg.segment_id == "EB":
                # Eligibility/Benefit information
                current_benefit = {
                    "eligibility_code": seg.get_element(1, ""),
                    "coverage_level": seg.get_element(2, ""),
                    "service_type": seg.get_element(3, ""),
                    "insurance_type": seg.get_element(4, ""),
                    "plan_coverage": seg.get_element(5, ""),
                    "time_period": seg.get_element(6, ""),
                    "amount": seg.get_element(7, ""),
                    "percent": seg.get_element(8, ""),
                    "quantity_qualifier": seg.get_element(9, ""),
                    "quantity": seg.get_element(10, ""),
                    "authorization_required": seg.get_element(11, ""),
                    "in_network": seg.get_element(12, ""),
                }
                result.benefits.append(current_benefit)

            elif seg.segment_id == "DTP" and current_benefit:
                date_qualifier = seg.get_element(1, "")
                date_format = seg.get_element(2, "")
                date_value = seg.get_element(3, "")
                current_benefit[f"date_{date_qualifier}"] = date_value

    def _parse_276_277(self, segments: List[X12Segment], result: X12ParseResult):
        """Parse 276/277 Claim Status segments"""
        current_claim = None

        for seg in segments:
            if seg.segment_id == "NM1":
                entity_code = seg.get_element(1, "")
                if entity_code == "IL":  # Subscriber
                    result.subscriber = {
                        "last_name": seg.get_element(3, ""),
                        "first_name": seg.get_element(4, ""),
                        "id": seg.get_element(9, "")
                    }
                elif entity_code == "PR":  # Payer
                    result.payer = {
                        "name": seg.get_element(3, ""),
                        "id": seg.get_element(9, "")
                    }

            elif seg.segment_id == "TRN":
                if not current_claim:
                    current_claim = X12Claim(
                        patient_control_number="",
                        claim_status="",
                        claim_status_description="",
                        charge_amount=Decimal("0"),
                        payment_amount=Decimal("0"),
                        patient_responsibility=Decimal("0"),
                        claim_filing_indicator="",
                        payer_claim_number=""
                    )
                # Store trace number
                current_claim.payer_claim_number = seg.get_element(2, "")

            elif seg.segment_id == "STC" and current_claim:
                # Status information
                status_composite = seg.get_element(1, "")
                status_parts = status_composite.split(self.component_delimiter)

                current_claim.claim_status = status_parts[0] if status_parts else ""
                current_claim.claim_status_description = status_parts[1] if len(status_parts) > 1 else ""

                current_claim.charge_amount = self._to_decimal(seg.get_element(4, "0"))
                current_claim.payment_amount = self._to_decimal(seg.get_element(5, "0"))

        if current_claim:
            result.claims.append(current_claim)

    def _parse_cas_segment(self, seg: X12Segment) -> List[X12Adjustment]:
        """Parse CAS adjustment segment (can have up to 6 adjustments)"""
        adjustments = []
        group_code = seg.get_element(1, "")
        group_desc = self.ADJUSTMENT_GROUP_DESC.get(group_code, "Unknown")

        # CAS can have up to 6 adjustment triplets (reason, amount, quantity)
        for i in range(6):
            reason_pos = 2 + (i * 3)
            amount_pos = 3 + (i * 3)
            qty_pos = 4 + (i * 3)

            reason = seg.get_element(reason_pos, "")
            if not reason:
                break

            amount = self._to_decimal(seg.get_element(amount_pos, "0"))
            qty_str = seg.get_element(qty_pos, "")
            qty = int(qty_str) if qty_str else None

            adjustments.append(X12Adjustment(
                group_code=group_code,
                group_description=group_desc,
                reason_code=reason,
                amount=amount,
                quantity=qty
            ))

        return adjustments

    def _determine_service_area(self, result: X12ParseResult) -> str:
        """Determine service area from NPI or invoice prefix"""
        # Try NPI first
        if result.payee_npi:
            for sa, npi in self.npi_list:
                if result.payee_npi == npi:
                    logger.debug(f"Service area {sa} found by NPI {npi}")
                    return sa

        # Try invoice prefix
        for prefix in result.invoice_prefixes:
            # Special case for Florida HB claims
            if prefix == "40":
                return "400"

            for sa, vendor_prefix in self.vendor_list:
                if prefix == vendor_prefix:
                    logger.debug(f"Service area {sa} found by prefix {prefix}")
                    return sa

        return "9999"  # Not found

    def _to_decimal(self, value: str) -> Decimal:
        """Convert string to Decimal safely"""
        try:
            cleaned = value.strip().replace(",", "")
            return Decimal(cleaned) if cleaned else Decimal("0")
        except Exception:
            return Decimal("0")


# Convenience functions
def parse_x12(content: str) -> X12ParseResult:
    """Parse X12 content and return result"""
    parser = X12Parser()
    return parser.parse(content)


def parse_x12_file(file_path: str) -> X12ParseResult:
    """Parse X12 file and return result"""
    parser = X12Parser()
    return parser.parse_file(file_path)


def x12_to_dict(content: str) -> Dict[str, Any]:
    """Parse X12 content and return as dictionary"""
    result = parse_x12(content)
    return result.to_dict()


def x12_to_json(content: str, indent: int = 2) -> str:
    """Parse X12 content and return as JSON string"""
    import json
    return json.dumps(x12_to_dict(content), indent=indent, default=str)
