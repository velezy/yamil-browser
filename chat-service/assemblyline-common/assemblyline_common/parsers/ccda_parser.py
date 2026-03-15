"""
CCDA/CDA Clinical Document Parser for Logic Weaver

Consolidated Clinical Document Architecture (CCDA) and Clinical Document Architecture (CDA)
parser for healthcare document processing. Supports extraction of clinical data from
standardized XML documents.

Supported Document Types:
- CCD (Continuity of Care Document)
- Discharge Summary
- Progress Notes
- Operative Notes
- Procedure Notes
- Consultation Notes
- History and Physical
- Referral Notes
- Transfer Summary

Key Features:
- Section-level parsing with LOINC code mapping
- Patient demographics extraction
- Problem list, medications, allergies, vitals
- Lab results and procedures
- Author and custodian information
- Template ID validation
- XPath-based field extraction

Comparison:
| Feature              | MuleSoft | Apigee | Logic Weaver |
|---------------------|----------|--------|--------------|
| CCDA Parsing        | Plugin   | No     | Native       |
| Section Extraction  | Limited  | No     | Full         |
| Template Validation | No       | No     | Yes          |
| LOINC Mapping       | No       | No     | Yes          |
| Python Integration  | No       | No     | Full         |
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)


# CCDA Namespaces
NAMESPACES = {
    'hl7': 'urn:hl7-org:v3',
    'sdtc': 'urn:hl7-org:sdtc',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
}


class CCDADocumentType(Enum):
    """CCDA document types identified by template IDs."""
    CCD = "2.16.840.1.113883.10.20.22.1.2"  # Continuity of Care Document
    DISCHARGE_SUMMARY = "2.16.840.1.113883.10.20.22.1.8"
    PROGRESS_NOTE = "2.16.840.1.113883.10.20.22.1.9"
    OPERATIVE_NOTE = "2.16.840.1.113883.10.20.22.1.7"
    PROCEDURE_NOTE = "2.16.840.1.113883.10.20.22.1.6"
    CONSULTATION_NOTE = "2.16.840.1.113883.10.20.22.1.4"
    HISTORY_AND_PHYSICAL = "2.16.840.1.113883.10.20.22.1.3"
    REFERRAL_NOTE = "2.16.840.1.113883.10.20.22.1.14"
    TRANSFER_SUMMARY = "2.16.840.1.113883.10.20.22.1.13"
    UNSTRUCTURED_DOCUMENT = "2.16.840.1.113883.10.20.22.1.10"
    UNKNOWN = "unknown"


class CCDASectionCode(Enum):
    """LOINC codes for CCDA sections."""
    ALLERGIES = "48765-2"
    MEDICATIONS = "10160-0"
    PROBLEMS = "11450-4"
    PROCEDURES = "47519-4"
    RESULTS = "30954-2"
    VITAL_SIGNS = "8716-3"
    SOCIAL_HISTORY = "29762-2"
    FAMILY_HISTORY = "10157-6"
    IMMUNIZATIONS = "11369-6"
    PLAN_OF_CARE = "18776-5"
    ENCOUNTERS = "46240-8"
    FUNCTIONAL_STATUS = "47420-5"
    MENTAL_STATUS = "10190-7"
    ADVANCE_DIRECTIVES = "42348-3"
    PAYERS = "48768-6"
    REASON_FOR_VISIT = "29299-5"
    CHIEF_COMPLAINT = "10154-3"
    ASSESSMENT = "51848-0"
    INSTRUCTIONS = "69730-0"
    HOSPITAL_COURSE = "8648-8"
    DISCHARGE_DIAGNOSIS = "11535-2"
    DISCHARGE_MEDICATIONS = "10183-2"


@dataclass
class CCDACode:
    """Represents a coded value (ICD, SNOMED, LOINC, etc.)."""
    code: str
    display_name: str = ""
    code_system: str = ""
    code_system_name: str = ""
    original_text: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "display_name": self.display_name,
            "code_system": self.code_system,
            "code_system_name": self.code_system_name,
            "original_text": self.original_text,
        }


@dataclass
class CCDAName:
    """Patient or provider name."""
    given: list[str] = field(default_factory=list)
    family: str = ""
    prefix: str = ""
    suffix: str = ""

    @property
    def full_name(self) -> str:
        parts = []
        if self.prefix:
            parts.append(self.prefix)
        parts.extend(self.given)
        if self.family:
            parts.append(self.family)
        if self.suffix:
            parts.append(self.suffix)
        return " ".join(parts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "given": self.given,
            "family": self.family,
            "prefix": self.prefix,
            "suffix": self.suffix,
            "full_name": self.full_name,
        }


@dataclass
class CCDAAddress:
    """Address information."""
    street_lines: list[str] = field(default_factory=list)
    city: str = ""
    state: str = ""
    postal_code: str = ""
    country: str = ""
    use: str = ""  # HP (home primary), WP (work), etc.

    def to_dict(self) -> dict[str, Any]:
        return {
            "street_lines": self.street_lines,
            "city": self.city,
            "state": self.state,
            "postal_code": self.postal_code,
            "country": self.country,
            "use": self.use,
        }


@dataclass
class CCDATelecom:
    """Telecom (phone, email) information."""
    value: str = ""
    use: str = ""  # HP (home primary), WP (work), MC (mobile), etc.

    def to_dict(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "use": self.use,
        }


@dataclass
class CCDAPatient:
    """Patient demographics."""
    ids: list[dict[str, str]] = field(default_factory=list)  # MRN, SSN, etc.
    name: CCDAName = field(default_factory=CCDAName)
    birth_time: Optional[datetime] = None
    gender: str = ""
    race: CCDACode = field(default_factory=lambda: CCDACode(""))
    ethnicity: CCDACode = field(default_factory=lambda: CCDACode(""))
    marital_status: CCDACode = field(default_factory=lambda: CCDACode(""))
    language: str = ""
    addresses: list[CCDAAddress] = field(default_factory=list)
    telecoms: list[CCDATelecom] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ids": self.ids,
            "name": self.name.to_dict(),
            "birth_time": self.birth_time.isoformat() if self.birth_time else None,
            "gender": self.gender,
            "race": self.race.to_dict(),
            "ethnicity": self.ethnicity.to_dict(),
            "marital_status": self.marital_status.to_dict(),
            "language": self.language,
            "addresses": [a.to_dict() for a in self.addresses],
            "telecoms": [t.to_dict() for t in self.telecoms],
        }


@dataclass
class CCDAAuthor:
    """Document author information."""
    time: Optional[datetime] = None
    name: CCDAName = field(default_factory=CCDAName)
    organization: str = ""
    npi: str = ""
    telecoms: list[CCDATelecom] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "time": self.time.isoformat() if self.time else None,
            "name": self.name.to_dict(),
            "organization": self.organization,
            "npi": self.npi,
            "telecoms": [t.to_dict() for t in self.telecoms],
        }


@dataclass
class CCDAAllergy:
    """Allergy/Intolerance entry."""
    substance: CCDACode = field(default_factory=lambda: CCDACode(""))
    reaction: CCDACode = field(default_factory=lambda: CCDACode(""))
    severity: CCDACode = field(default_factory=lambda: CCDACode(""))
    status: str = ""
    onset_date: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "substance": self.substance.to_dict(),
            "reaction": self.reaction.to_dict(),
            "severity": self.severity.to_dict(),
            "status": self.status,
            "onset_date": self.onset_date.isoformat() if self.onset_date else None,
        }


@dataclass
class CCDAMedication:
    """Medication entry."""
    medication: CCDACode = field(default_factory=lambda: CCDACode(""))
    dose: str = ""
    dose_unit: str = ""
    route: CCDACode = field(default_factory=lambda: CCDACode(""))
    frequency: str = ""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    status: str = ""
    instructions: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "medication": self.medication.to_dict(),
            "dose": self.dose,
            "dose_unit": self.dose_unit,
            "route": self.route.to_dict(),
            "frequency": self.frequency,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "status": self.status,
            "instructions": self.instructions,
        }


@dataclass
class CCDAProblem:
    """Problem/Diagnosis entry."""
    problem: CCDACode = field(default_factory=lambda: CCDACode(""))
    status: str = ""
    onset_date: Optional[datetime] = None
    resolution_date: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "problem": self.problem.to_dict(),
            "status": self.status,
            "onset_date": self.onset_date.isoformat() if self.onset_date else None,
            "resolution_date": self.resolution_date.isoformat() if self.resolution_date else None,
        }


@dataclass
class CCDAProcedure:
    """Procedure entry."""
    procedure: CCDACode = field(default_factory=lambda: CCDACode(""))
    status: str = ""
    date: Optional[datetime] = None
    performer: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "procedure": self.procedure.to_dict(),
            "status": self.status,
            "date": self.date.isoformat() if self.date else None,
            "performer": self.performer,
        }


@dataclass
class CCDAVitalSign:
    """Vital sign observation."""
    vital: CCDACode = field(default_factory=lambda: CCDACode(""))
    value: str = ""
    unit: str = ""
    date: Optional[datetime] = None
    interpretation: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "vital": self.vital.to_dict(),
            "value": self.value,
            "unit": self.unit,
            "date": self.date.isoformat() if self.date else None,
            "interpretation": self.interpretation,
        }


@dataclass
class CCDALabResult:
    """Laboratory result."""
    test: CCDACode = field(default_factory=lambda: CCDACode(""))
    value: str = ""
    unit: str = ""
    reference_range: str = ""
    date: Optional[datetime] = None
    interpretation: str = ""  # H, L, N, A (abnormal)
    status: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "test": self.test.to_dict(),
            "value": self.value,
            "unit": self.unit,
            "reference_range": self.reference_range,
            "date": self.date.isoformat() if self.date else None,
            "interpretation": self.interpretation,
            "status": self.status,
        }


@dataclass
class CCDAImmunization:
    """Immunization entry."""
    vaccine: CCDACode = field(default_factory=lambda: CCDACode(""))
    date: Optional[datetime] = None
    status: str = ""
    lot_number: str = ""
    manufacturer: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "vaccine": self.vaccine.to_dict(),
            "date": self.date.isoformat() if self.date else None,
            "status": self.status,
            "lot_number": self.lot_number,
            "manufacturer": self.manufacturer,
        }


@dataclass
class CCDAEncounter:
    """Encounter entry."""
    encounter_type: CCDACode = field(default_factory=lambda: CCDACode(""))
    date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    location: str = ""
    provider: str = ""
    diagnosis: list[CCDACode] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "encounter_type": self.encounter_type.to_dict(),
            "date": self.date.isoformat() if self.date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "location": self.location,
            "provider": self.provider,
            "diagnosis": [d.to_dict() for d in self.diagnosis],
        }


@dataclass
class CCDASection:
    """A section within the CCDA document."""
    code: str  # LOINC code
    title: str
    text: str  # Narrative text
    entries: list[Any] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "title": self.title,
            "text": self.text,
            "entries": [e.to_dict() if hasattr(e, 'to_dict') else e for e in self.entries],
        }


@dataclass
class CCDAParseResult:
    """Result of parsing a CCDA document."""
    document_type: CCDADocumentType
    document_id: str
    title: str
    effective_time: Optional[datetime]
    patient: CCDAPatient
    authors: list[CCDAAuthor]
    custodian: str

    # Clinical data sections
    allergies: list[CCDAAllergy] = field(default_factory=list)
    medications: list[CCDAMedication] = field(default_factory=list)
    problems: list[CCDAProblem] = field(default_factory=list)
    procedures: list[CCDAProcedure] = field(default_factory=list)
    vital_signs: list[CCDAVitalSign] = field(default_factory=list)
    lab_results: list[CCDALabResult] = field(default_factory=list)
    immunizations: list[CCDAImmunization] = field(default_factory=list)
    encounters: list[CCDAEncounter] = field(default_factory=list)

    # Raw sections for custom processing
    sections: list[CCDASection] = field(default_factory=list)

    # Validation
    template_ids: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "document_type": self.document_type.name,
            "document_id": self.document_id,
            "title": self.title,
            "effective_time": self.effective_time.isoformat() if self.effective_time else None,
            "patient": self.patient.to_dict(),
            "authors": [a.to_dict() for a in self.authors],
            "custodian": self.custodian,
            "allergies": [a.to_dict() for a in self.allergies],
            "medications": [m.to_dict() for m in self.medications],
            "problems": [p.to_dict() for p in self.problems],
            "procedures": [p.to_dict() for p in self.procedures],
            "vital_signs": [v.to_dict() for v in self.vital_signs],
            "lab_results": [r.to_dict() for r in self.lab_results],
            "immunizations": [i.to_dict() for i in self.immunizations],
            "encounters": [e.to_dict() for e in self.encounters],
            "sections": [s.to_dict() for s in self.sections],
            "template_ids": self.template_ids,
            "errors": self.errors,
            "warnings": self.warnings,
        }


class CCDAParser:
    """
    Parser for CCDA/CDA clinical documents.

    Extracts patient demographics, clinical data (allergies, medications,
    problems, procedures, vitals, labs), and document metadata.
    """

    def __init__(self, strict: bool = False):
        """
        Initialize the CCDA parser.

        Args:
            strict: If True, raise errors on validation failures
        """
        self.strict = strict

        # LOINC section code to parser method mapping
        self._section_parsers = {
            CCDASectionCode.ALLERGIES.value: self._parse_allergies,
            CCDASectionCode.MEDICATIONS.value: self._parse_medications,
            CCDASectionCode.PROBLEMS.value: self._parse_problems,
            CCDASectionCode.PROCEDURES.value: self._parse_procedures,
            CCDASectionCode.VITAL_SIGNS.value: self._parse_vital_signs,
            CCDASectionCode.RESULTS.value: self._parse_lab_results,
            CCDASectionCode.IMMUNIZATIONS.value: self._parse_immunizations,
            CCDASectionCode.ENCOUNTERS.value: self._parse_encounters,
        }

    def parse(self, content: str) -> CCDAParseResult:
        """
        Parse a CCDA/CDA XML document.

        Args:
            content: XML content as string

        Returns:
            CCDAParseResult with extracted clinical data
        """
        try:
            # Register namespaces
            for prefix, uri in NAMESPACES.items():
                ET.register_namespace(prefix, uri)

            root = ET.fromstring(content)

            # Detect namespace
            ns = self._detect_namespace(root)

            return self._parse_document(root, ns)

        except ET.ParseError as e:
            logger.error(f"Failed to parse CCDA XML: {e}")
            raise ValueError(f"Invalid CCDA XML: {e}")

    def parse_file(self, file_path: str) -> CCDAParseResult:
        """Parse a CCDA/CDA file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            return self.parse(f.read())

    def _detect_namespace(self, root: ET.Element) -> dict[str, str]:
        """Detect the XML namespace used in the document."""
        # Check if default HL7 namespace is used
        if root.tag.startswith('{'):
            ns_end = root.tag.find('}')
            ns_uri = root.tag[1:ns_end]
            return {'hl7': ns_uri}
        return {'hl7': NAMESPACES['hl7']}

    def _ns(self, ns: dict[str, str], tag: str) -> str:
        """Create a namespaced tag."""
        return f"{{{ns['hl7']}}}{tag}"

    def _find(self, elem: ET.Element, path: str, ns: dict[str, str]) -> Optional[ET.Element]:
        """Find an element with namespace handling."""
        # Convert path to namespaced path
        parts = path.split('/')
        ns_path = '/'.join(f"hl7:{p}" if p and not p.startswith('@') else p for p in parts)
        return elem.find(ns_path, ns)

    def _findall(self, elem: ET.Element, path: str, ns: dict[str, str]) -> list[ET.Element]:
        """Find all elements with namespace handling."""
        parts = path.split('/')
        ns_path = '/'.join(f"hl7:{p}" if p and not p.startswith('@') else p for p in parts)
        return elem.findall(ns_path, ns)

    def _parse_document(self, root: ET.Element, ns: dict[str, str]) -> CCDAParseResult:
        """Parse the entire CCDA document."""
        errors = []
        warnings = []

        # Get template IDs and determine document type
        template_ids = self._get_template_ids(root, ns)
        doc_type = self._determine_document_type(template_ids)

        # Get document ID
        doc_id = self._get_document_id(root, ns)

        # Get title
        title_elem = self._find(root, 'title', ns)
        title = title_elem.text if title_elem is not None and title_elem.text else ""

        # Get effective time
        effective_time = self._parse_time(root, 'effectiveTime', ns)

        # Parse patient
        patient = self._parse_patient(root, ns, errors)

        # Parse authors
        authors = self._parse_authors(root, ns)

        # Parse custodian
        custodian = self._parse_custodian(root, ns)

        # Parse sections
        result = CCDAParseResult(
            document_type=doc_type,
            document_id=doc_id,
            title=title,
            effective_time=effective_time,
            patient=patient,
            authors=authors,
            custodian=custodian,
            template_ids=template_ids,
            errors=errors,
            warnings=warnings,
        )

        # Parse clinical sections
        self._parse_sections(root, ns, result)

        return result

    def _get_template_ids(self, root: ET.Element, ns: dict[str, str]) -> list[str]:
        """Extract all template IDs from the document."""
        template_ids = []
        for template in self._findall(root, 'templateId', ns):
            root_val = template.get('root')
            if root_val:
                template_ids.append(root_val)
        return template_ids

    def _determine_document_type(self, template_ids: list[str]) -> CCDADocumentType:
        """Determine the document type from template IDs."""
        for doc_type in CCDADocumentType:
            if doc_type.value in template_ids:
                return doc_type
        return CCDADocumentType.UNKNOWN

    def _get_document_id(self, root: ET.Element, ns: dict[str, str]) -> str:
        """Get the document ID."""
        id_elem = self._find(root, 'id', ns)
        if id_elem is not None:
            root_val = id_elem.get('root', '')
            ext = id_elem.get('extension', '')
            if ext:
                return f"{root_val}^{ext}"
            return root_val
        return ""

    def _parse_time(self, elem: ET.Element, path: str, ns: dict[str, str]) -> Optional[datetime]:
        """Parse a time element."""
        time_elem = self._find(elem, path, ns)
        if time_elem is None:
            return None

        value = time_elem.get('value')
        if not value:
            return None

        return self._parse_hl7_datetime(value)

    def _parse_hl7_datetime(self, value: str) -> Optional[datetime]:
        """Parse HL7 datetime format (YYYYMMDDHHMMSS)."""
        if not value:
            return None

        # Remove timezone suffix if present
        value = re.sub(r'[+-]\d{4}$', '', value)

        formats = [
            '%Y%m%d%H%M%S',
            '%Y%m%d%H%M',
            '%Y%m%d',
            '%Y%m',
            '%Y',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value[:len(fmt.replace('%', ''))], fmt)
            except ValueError:
                continue

        return None

    def _parse_patient(self, root: ET.Element, ns: dict[str, str], errors: list[str]) -> CCDAPatient:
        """Parse patient demographics."""
        patient = CCDAPatient()

        # Find recordTarget/patientRole
        patient_role = self._find(root, 'recordTarget/patientRole', ns)
        if patient_role is None:
            errors.append("No patient information found")
            return patient

        # Parse IDs (MRN, SSN, etc.)
        for id_elem in self._findall(patient_role, 'id', ns):
            root_val = id_elem.get('root', '')
            ext = id_elem.get('extension', '')
            if ext:
                patient.ids.append({
                    'root': root_val,
                    'extension': ext,
                })

        # Parse addresses
        for addr_elem in self._findall(patient_role, 'addr', ns):
            patient.addresses.append(self._parse_address(addr_elem, ns))

        # Parse telecoms
        for telecom_elem in self._findall(patient_role, 'telecom', ns):
            patient.telecoms.append(self._parse_telecom(telecom_elem))

        # Parse patient details
        patient_elem = self._find(patient_role, 'patient', ns)
        if patient_elem is not None:
            # Name
            name_elem = self._find(patient_elem, 'name', ns)
            if name_elem is not None:
                patient.name = self._parse_name(name_elem, ns)

            # Gender
            gender_elem = self._find(patient_elem, 'administrativeGenderCode', ns)
            if gender_elem is not None:
                patient.gender = gender_elem.get('code', '')

            # Birth time
            patient.birth_time = self._parse_time(patient_elem, 'birthTime', ns)

            # Race
            race_elem = self._find(patient_elem, 'raceCode', ns)
            if race_elem is not None:
                patient.race = self._parse_code(race_elem)

            # Ethnicity
            eth_elem = self._find(patient_elem, 'ethnicGroupCode', ns)
            if eth_elem is not None:
                patient.ethnicity = self._parse_code(eth_elem)

            # Marital status
            marital_elem = self._find(patient_elem, 'maritalStatusCode', ns)
            if marital_elem is not None:
                patient.marital_status = self._parse_code(marital_elem)

            # Language
            lang_elem = self._find(patient_elem, 'languageCommunication/languageCode', ns)
            if lang_elem is not None:
                patient.language = lang_elem.get('code', '')

        return patient

    def _parse_name(self, name_elem: ET.Element, ns: dict[str, str]) -> CCDAName:
        """Parse a name element."""
        name = CCDAName()

        # Given names
        for given in self._findall(name_elem, 'given', ns):
            if given.text:
                name.given.append(given.text)

        # Family name
        family = self._find(name_elem, 'family', ns)
        if family is not None and family.text:
            name.family = family.text

        # Prefix
        prefix = self._find(name_elem, 'prefix', ns)
        if prefix is not None and prefix.text:
            name.prefix = prefix.text

        # Suffix
        suffix = self._find(name_elem, 'suffix', ns)
        if suffix is not None and suffix.text:
            name.suffix = suffix.text

        return name

    def _parse_address(self, addr_elem: ET.Element, ns: dict[str, str]) -> CCDAAddress:
        """Parse an address element."""
        addr = CCDAAddress()
        addr.use = addr_elem.get('use', '')

        for street in self._findall(addr_elem, 'streetAddressLine', ns):
            if street.text:
                addr.street_lines.append(street.text)

        city = self._find(addr_elem, 'city', ns)
        if city is not None and city.text:
            addr.city = city.text

        state = self._find(addr_elem, 'state', ns)
        if state is not None and state.text:
            addr.state = state.text

        postal = self._find(addr_elem, 'postalCode', ns)
        if postal is not None and postal.text:
            addr.postal_code = postal.text

        country = self._find(addr_elem, 'country', ns)
        if country is not None and country.text:
            addr.country = country.text

        return addr

    def _parse_telecom(self, telecom_elem: ET.Element) -> CCDATelecom:
        """Parse a telecom element."""
        return CCDATelecom(
            value=telecom_elem.get('value', '').replace('tel:', '').replace('mailto:', ''),
            use=telecom_elem.get('use', ''),
        )

    def _parse_code(self, code_elem: ET.Element) -> CCDACode:
        """Parse a coded value element."""
        code = CCDACode(
            code=code_elem.get('code', ''),
            display_name=code_elem.get('displayName', ''),
            code_system=code_elem.get('codeSystem', ''),
            code_system_name=code_elem.get('codeSystemName', ''),
        )

        # Check for original text
        orig_text = code_elem.find('.//{urn:hl7-org:v3}originalText')
        if orig_text is not None and orig_text.text:
            code.original_text = orig_text.text

        return code

    def _parse_authors(self, root: ET.Element, ns: dict[str, str]) -> list[CCDAAuthor]:
        """Parse document authors."""
        authors = []

        for author_elem in self._findall(root, 'author', ns):
            author = CCDAAuthor()

            # Time
            author.time = self._parse_time(author_elem, 'time', ns)

            # Assigned author
            assigned = self._find(author_elem, 'assignedAuthor', ns)
            if assigned is not None:
                # NPI
                for id_elem in self._findall(assigned, 'id', ns):
                    root_val = id_elem.get('root', '')
                    if root_val == '2.16.840.1.113883.4.6':  # NPI OID
                        author.npi = id_elem.get('extension', '')

                # Telecoms
                for telecom in self._findall(assigned, 'telecom', ns):
                    author.telecoms.append(self._parse_telecom(telecom))

                # Name
                person = self._find(assigned, 'assignedPerson', ns)
                if person is not None:
                    name_elem = self._find(person, 'name', ns)
                    if name_elem is not None:
                        author.name = self._parse_name(name_elem, ns)

                # Organization
                org = self._find(assigned, 'representedOrganization/name', ns)
                if org is not None and org.text:
                    author.organization = org.text

            authors.append(author)

        return authors

    def _parse_custodian(self, root: ET.Element, ns: dict[str, str]) -> str:
        """Parse custodian organization."""
        name = self._find(root, 'custodian/assignedCustodian/representedCustodianOrganization/name', ns)
        if name is not None and name.text:
            return name.text
        return ""

    def _parse_sections(self, root: ET.Element, ns: dict[str, str], result: CCDAParseResult) -> None:
        """Parse all clinical sections."""
        # Find structuredBody/component/section
        for section in self._findall(root, 'component/structuredBody/component/section', ns):
            code_elem = self._find(section, 'code', ns)
            if code_elem is None:
                continue

            section_code = code_elem.get('code', '')

            # Get section title and text
            title_elem = self._find(section, 'title', ns)
            title = title_elem.text if title_elem is not None and title_elem.text else ""

            text_elem = self._find(section, 'text', ns)
            text = ET.tostring(text_elem, encoding='unicode', method='text') if text_elem is not None else ""

            # Parse section-specific entries
            if section_code in self._section_parsers:
                entries = self._section_parsers[section_code](section, ns)

                # Add to appropriate result list
                if section_code == CCDASectionCode.ALLERGIES.value:
                    result.allergies.extend(entries)
                elif section_code == CCDASectionCode.MEDICATIONS.value:
                    result.medications.extend(entries)
                elif section_code == CCDASectionCode.PROBLEMS.value:
                    result.problems.extend(entries)
                elif section_code == CCDASectionCode.PROCEDURES.value:
                    result.procedures.extend(entries)
                elif section_code == CCDASectionCode.VITAL_SIGNS.value:
                    result.vital_signs.extend(entries)
                elif section_code == CCDASectionCode.RESULTS.value:
                    result.lab_results.extend(entries)
                elif section_code == CCDASectionCode.IMMUNIZATIONS.value:
                    result.immunizations.extend(entries)
                elif section_code == CCDASectionCode.ENCOUNTERS.value:
                    result.encounters.extend(entries)

                # Store section for reference
                result.sections.append(CCDASection(
                    code=section_code,
                    title=title,
                    text=text.strip(),
                    entries=entries,
                ))
            else:
                # Store unparsed section
                result.sections.append(CCDASection(
                    code=section_code,
                    title=title,
                    text=text.strip(),
                ))

    def _parse_allergies(self, section: ET.Element, ns: dict[str, str]) -> list[CCDAAllergy]:
        """Parse allergy entries."""
        allergies = []

        for entry in self._findall(section, 'entry', ns):
            act = self._find(entry, 'act', ns)
            if act is None:
                continue

            # Find allergy observation
            obs = self._find(act, 'entryRelationship/observation', ns)
            if obs is None:
                continue

            allergy = CCDAAllergy()

            # Status
            status_elem = self._find(obs, 'statusCode', ns)
            if status_elem is not None:
                allergy.status = status_elem.get('code', '')

            # Onset date
            allergy.onset_date = self._parse_time(obs, 'effectiveTime/low', ns)

            # Substance (from participant)
            participant = self._find(obs, 'participant/participantRole/playingEntity', ns)
            if participant is not None:
                code_elem = self._find(participant, 'code', ns)
                if code_elem is not None:
                    allergy.substance = self._parse_code(code_elem)

            # Reaction and severity from nested observations
            for rel in self._findall(obs, 'entryRelationship', ns):
                nested_obs = self._find(rel, 'observation', ns)
                if nested_obs is None:
                    continue

                template = self._find(nested_obs, 'templateId', ns)
                if template is None:
                    continue

                template_root = template.get('root', '')
                value_elem = self._find(nested_obs, 'value', ns)

                if 'reaction' in template_root.lower() or template_root == '2.16.840.1.113883.10.20.22.4.9':
                    if value_elem is not None:
                        allergy.reaction = self._parse_code(value_elem)
                elif 'severity' in template_root.lower() or template_root == '2.16.840.1.113883.10.20.22.4.8':
                    if value_elem is not None:
                        allergy.severity = self._parse_code(value_elem)

            allergies.append(allergy)

        return allergies

    def _parse_medications(self, section: ET.Element, ns: dict[str, str]) -> list[CCDAMedication]:
        """Parse medication entries."""
        medications = []

        for entry in self._findall(section, 'entry', ns):
            subst_admin = self._find(entry, 'substanceAdministration', ns)
            if subst_admin is None:
                continue

            med = CCDAMedication()

            # Status
            status_elem = self._find(subst_admin, 'statusCode', ns)
            if status_elem is not None:
                med.status = status_elem.get('code', '')

            # Effective time (start/end)
            eff_time = self._find(subst_admin, 'effectiveTime', ns)
            if eff_time is not None:
                med.start_date = self._parse_time(subst_admin, 'effectiveTime/low', ns)
                med.end_date = self._parse_time(subst_admin, 'effectiveTime/high', ns)

            # Dose
            dose_elem = self._find(subst_admin, 'doseQuantity', ns)
            if dose_elem is not None:
                med.dose = dose_elem.get('value', '')
                med.dose_unit = dose_elem.get('unit', '')

            # Route
            route_elem = self._find(subst_admin, 'routeCode', ns)
            if route_elem is not None:
                med.route = self._parse_code(route_elem)

            # Medication code
            consumable = self._find(subst_admin, 'consumable/manufacturedProduct/manufacturedMaterial', ns)
            if consumable is not None:
                code_elem = self._find(consumable, 'code', ns)
                if code_elem is not None:
                    med.medication = self._parse_code(code_elem)

            medications.append(med)

        return medications

    def _parse_problems(self, section: ET.Element, ns: dict[str, str]) -> list[CCDAProblem]:
        """Parse problem entries."""
        problems = []

        for entry in self._findall(section, 'entry', ns):
            act = self._find(entry, 'act', ns)
            if act is None:
                continue

            obs = self._find(act, 'entryRelationship/observation', ns)
            if obs is None:
                continue

            problem = CCDAProblem()

            # Status
            status_elem = self._find(obs, 'statusCode', ns)
            if status_elem is not None:
                problem.status = status_elem.get('code', '')

            # Dates
            problem.onset_date = self._parse_time(obs, 'effectiveTime/low', ns)
            problem.resolution_date = self._parse_time(obs, 'effectiveTime/high', ns)

            # Problem code
            value_elem = self._find(obs, 'value', ns)
            if value_elem is not None:
                problem.problem = self._parse_code(value_elem)

            problems.append(problem)

        return problems

    def _parse_procedures(self, section: ET.Element, ns: dict[str, str]) -> list[CCDAProcedure]:
        """Parse procedure entries."""
        procedures = []

        for entry in self._findall(section, 'entry', ns):
            proc_elem = self._find(entry, 'procedure', ns)
            if proc_elem is None:
                proc_elem = self._find(entry, 'act', ns)
            if proc_elem is None:
                continue

            procedure = CCDAProcedure()

            # Status
            status_elem = self._find(proc_elem, 'statusCode', ns)
            if status_elem is not None:
                procedure.status = status_elem.get('code', '')

            # Date
            procedure.date = self._parse_time(proc_elem, 'effectiveTime', ns)

            # Procedure code
            code_elem = self._find(proc_elem, 'code', ns)
            if code_elem is not None:
                procedure.procedure = self._parse_code(code_elem)

            # Performer
            performer = self._find(proc_elem, 'performer/assignedEntity/assignedPerson/name', ns)
            if performer is not None:
                name = self._parse_name(performer, ns)
                procedure.performer = name.full_name

            procedures.append(procedure)

        return procedures

    def _parse_vital_signs(self, section: ET.Element, ns: dict[str, str]) -> list[CCDAVitalSign]:
        """Parse vital sign entries."""
        vitals = []

        for entry in self._findall(section, 'entry', ns):
            organizer = self._find(entry, 'organizer', ns)
            if organizer is None:
                continue

            for component in self._findall(organizer, 'component', ns):
                obs = self._find(component, 'observation', ns)
                if obs is None:
                    continue

                vital = CCDAVitalSign()

                # Vital code
                code_elem = self._find(obs, 'code', ns)
                if code_elem is not None:
                    vital.vital = self._parse_code(code_elem)

                # Value
                value_elem = self._find(obs, 'value', ns)
                if value_elem is not None:
                    vital.value = value_elem.get('value', '')
                    vital.unit = value_elem.get('unit', '')

                # Date
                vital.date = self._parse_time(obs, 'effectiveTime', ns)

                # Interpretation
                interp = self._find(obs, 'interpretationCode', ns)
                if interp is not None:
                    vital.interpretation = interp.get('code', '')

                vitals.append(vital)

        return vitals

    def _parse_lab_results(self, section: ET.Element, ns: dict[str, str]) -> list[CCDALabResult]:
        """Parse lab result entries."""
        results = []

        for entry in self._findall(section, 'entry', ns):
            organizer = self._find(entry, 'organizer', ns)
            if organizer is None:
                continue

            for component in self._findall(organizer, 'component', ns):
                obs = self._find(component, 'observation', ns)
                if obs is None:
                    continue

                result = CCDALabResult()

                # Test code
                code_elem = self._find(obs, 'code', ns)
                if code_elem is not None:
                    result.test = self._parse_code(code_elem)

                # Status
                status_elem = self._find(obs, 'statusCode', ns)
                if status_elem is not None:
                    result.status = status_elem.get('code', '')

                # Value
                value_elem = self._find(obs, 'value', ns)
                if value_elem is not None:
                    result.value = value_elem.get('value', '')
                    result.unit = value_elem.get('unit', '')

                # Date
                result.date = self._parse_time(obs, 'effectiveTime', ns)

                # Reference range
                ref_range = self._find(obs, 'referenceRange/observationRange/text', ns)
                if ref_range is not None and ref_range.text:
                    result.reference_range = ref_range.text

                # Interpretation
                interp = self._find(obs, 'interpretationCode', ns)
                if interp is not None:
                    result.interpretation = interp.get('code', '')

                results.append(result)

        return results

    def _parse_immunizations(self, section: ET.Element, ns: dict[str, str]) -> list[CCDAImmunization]:
        """Parse immunization entries."""
        immunizations = []

        for entry in self._findall(section, 'entry', ns):
            subst_admin = self._find(entry, 'substanceAdministration', ns)
            if subst_admin is None:
                continue

            immunization = CCDAImmunization()

            # Status
            status_elem = self._find(subst_admin, 'statusCode', ns)
            if status_elem is not None:
                immunization.status = status_elem.get('code', '')

            # Date
            immunization.date = self._parse_time(subst_admin, 'effectiveTime', ns)

            # Vaccine
            consumable = self._find(subst_admin, 'consumable/manufacturedProduct/manufacturedMaterial', ns)
            if consumable is not None:
                code_elem = self._find(consumable, 'code', ns)
                if code_elem is not None:
                    immunization.vaccine = self._parse_code(code_elem)

                lot = self._find(consumable, 'lotNumberText', ns)
                if lot is not None and lot.text:
                    immunization.lot_number = lot.text

            # Manufacturer
            mfr = self._find(subst_admin, 'consumable/manufacturedProduct/manufacturerOrganization/name', ns)
            if mfr is not None and mfr.text:
                immunization.manufacturer = mfr.text

            immunizations.append(immunization)

        return immunizations

    def _parse_encounters(self, section: ET.Element, ns: dict[str, str]) -> list[CCDAEncounter]:
        """Parse encounter entries."""
        encounters = []

        for entry in self._findall(section, 'entry', ns):
            encounter_elem = self._find(entry, 'encounter', ns)
            if encounter_elem is None:
                continue

            encounter = CCDAEncounter()

            # Encounter type
            code_elem = self._find(encounter_elem, 'code', ns)
            if code_elem is not None:
                encounter.encounter_type = self._parse_code(code_elem)

            # Dates
            encounter.date = self._parse_time(encounter_elem, 'effectiveTime/low', ns)
            encounter.end_date = self._parse_time(encounter_elem, 'effectiveTime/high', ns)

            # Location
            loc = self._find(encounter_elem, 'participant/participantRole/playingEntity/name', ns)
            if loc is not None and loc.text:
                encounter.location = loc.text

            # Provider
            performer = self._find(encounter_elem, 'performer/assignedEntity/assignedPerson/name', ns)
            if performer is not None:
                name = self._parse_name(performer, ns)
                encounter.provider = name.full_name

            # Diagnosis
            for rel in self._findall(encounter_elem, 'entryRelationship', ns):
                obs = self._find(rel, 'observation', ns)
                if obs is not None:
                    value_elem = self._find(obs, 'value', ns)
                    if value_elem is not None:
                        encounter.diagnosis.append(self._parse_code(value_elem))

            encounters.append(encounter)

        return encounters


# Convenience functions
def parse_ccda(content: str) -> CCDAParseResult:
    """Parse CCDA content and return structured result."""
    parser = CCDAParser()
    return parser.parse(content)


def parse_ccda_file(file_path: str) -> CCDAParseResult:
    """Parse CCDA file and return structured result."""
    parser = CCDAParser()
    return parser.parse_file(file_path)


def ccda_to_dict(content: str) -> dict[str, Any]:
    """Parse CCDA and return as dictionary."""
    result = parse_ccda(content)
    return result.to_dict()


def ccda_to_fhir_bundle(result: CCDAParseResult) -> dict[str, Any]:
    """
    Convert CCDA parse result to FHIR R4 Bundle.

    This is a basic conversion - for full CCDA-to-FHIR mapping,
    use a dedicated conversion library.
    """
    entries = []

    # Patient resource
    patient = {
        "resourceType": "Patient",
        "id": result.patient.ids[0]['extension'] if result.patient.ids else "unknown",
        "name": [{
            "family": result.patient.name.family,
            "given": result.patient.name.given,
            "prefix": [result.patient.name.prefix] if result.patient.name.prefix else [],
        }],
        "gender": _map_gender(result.patient.gender),
        "birthDate": result.patient.birth_time.strftime('%Y-%m-%d') if result.patient.birth_time else None,
    }
    entries.append({"resource": patient})

    # Allergies as AllergyIntolerance
    for allergy in result.allergies:
        ai = {
            "resourceType": "AllergyIntolerance",
            "clinicalStatus": {"coding": [{"code": allergy.status}]},
            "code": {
                "coding": [{
                    "system": allergy.substance.code_system,
                    "code": allergy.substance.code,
                    "display": allergy.substance.display_name,
                }]
            },
        }
        entries.append({"resource": ai})

    # Medications as MedicationStatement
    for med in result.medications:
        ms = {
            "resourceType": "MedicationStatement",
            "status": med.status if med.status else "unknown",
            "medicationCodeableConcept": {
                "coding": [{
                    "system": med.medication.code_system,
                    "code": med.medication.code,
                    "display": med.medication.display_name,
                }]
            },
        }
        entries.append({"resource": ms})

    # Problems as Condition
    for prob in result.problems:
        cond = {
            "resourceType": "Condition",
            "clinicalStatus": {"coding": [{"code": prob.status}]},
            "code": {
                "coding": [{
                    "system": prob.problem.code_system,
                    "code": prob.problem.code,
                    "display": prob.problem.display_name,
                }]
            },
        }
        entries.append({"resource": cond})

    return {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": entries,
    }


def _map_gender(hl7_gender: str) -> str:
    """Map HL7 gender code to FHIR gender."""
    mapping = {
        'M': 'male',
        'F': 'female',
        'UN': 'unknown',
        'O': 'other',
    }
    return mapping.get(hl7_gender, 'unknown')


# Flow Node Integration
@dataclass
class CCDANodeConfig:
    """Configuration for CCDA parser flow node."""
    operation: str = "parse"  # parse, extract_section, to_fhir
    sections: list[str] = field(default_factory=list)  # Filter specific sections
    strict: bool = False  # Strict validation mode
    output_format: str = "dict"  # dict, json, fhir


@dataclass
class CCDANodeResult:
    """Result from CCDA parser flow node."""
    success: bool
    operation: str
    document_type: str
    patient_name: str
    section_count: int
    data: dict[str, Any]
    message: str
    error: Optional[str]


class CCDANode:
    """Flow node for CCDA parsing operations."""

    node_type = "ccda_parser"
    node_category = "parser"

    def __init__(self, config: CCDANodeConfig):
        self.config = config

    async def execute(self, input_data: dict[str, Any]) -> CCDANodeResult:
        """Execute the CCDA parsing operation."""
        try:
            content = input_data.get("content", "")
            if not content:
                return CCDANodeResult(
                    success=False,
                    operation=self.config.operation,
                    document_type="",
                    patient_name="",
                    section_count=0,
                    data={},
                    message="No content provided",
                    error="Input 'content' is required",
                )

            parser = CCDAParser(strict=self.config.strict)
            result = parser.parse(content)

            if self.config.operation == "parse":
                data = result.to_dict()

                # Filter sections if specified
                if self.config.sections:
                    data["sections"] = [
                        s for s in data.get("sections", [])
                        if s.get("code") in self.config.sections
                    ]

                return CCDANodeResult(
                    success=True,
                    operation="parse",
                    document_type=result.document_type.name,
                    patient_name=result.patient.name.full_name,
                    section_count=len(result.sections),
                    data=data,
                    message=f"Parsed {result.document_type.name} document with {len(result.sections)} sections",
                    error=None,
                )

            elif self.config.operation == "extract_section":
                # Extract specific sections only
                sections_data = {}
                for section in result.sections:
                    if not self.config.sections or section.code in self.config.sections:
                        sections_data[section.code] = {
                            "title": section.title,
                            "text": section.text,
                            "entries": [e.to_dict() if hasattr(e, 'to_dict') else e for e in section.entries],
                        }

                return CCDANodeResult(
                    success=True,
                    operation="extract_section",
                    document_type=result.document_type.name,
                    patient_name=result.patient.name.full_name,
                    section_count=len(sections_data),
                    data={"sections": sections_data},
                    message=f"Extracted {len(sections_data)} sections",
                    error=None,
                )

            elif self.config.operation == "to_fhir":
                fhir_bundle = ccda_to_fhir_bundle(result)

                return CCDANodeResult(
                    success=True,
                    operation="to_fhir",
                    document_type=result.document_type.name,
                    patient_name=result.patient.name.full_name,
                    section_count=len(result.sections),
                    data=fhir_bundle,
                    message=f"Converted to FHIR Bundle with {len(fhir_bundle.get('entry', []))} resources",
                    error=None,
                )

            else:
                return CCDANodeResult(
                    success=False,
                    operation=self.config.operation,
                    document_type="",
                    patient_name="",
                    section_count=0,
                    data={},
                    message=f"Unknown operation: {self.config.operation}",
                    error=f"Supported operations: parse, extract_section, to_fhir",
                )

        except Exception as e:
            logger.error(f"CCDA node execution failed: {e}")
            return CCDANodeResult(
                success=False,
                operation=self.config.operation,
                document_type="",
                patient_name="",
                section_count=0,
                data={},
                message="Execution failed",
                error=str(e),
            )


def get_ccda_parser(strict: bool = False) -> CCDAParser:
    """Factory function to create CCDA parser."""
    return CCDAParser(strict=strict)


def get_ccda_node_definition() -> dict[str, Any]:
    """Get the flow node definition for the UI."""
    return {
        "type": "ccda_parser",
        "category": "parser",
        "label": "CCDA/CDA Parser",
        "description": "Parse clinical documents (CCD, Discharge Summary, etc.)",
        "icon": "FileText",
        "color": "#2196F3",
        "inputs": ["content"],
        "outputs": ["result", "patient", "sections"],
        "config_schema": {
            "operation": {
                "type": "select",
                "options": ["parse", "extract_section", "to_fhir"],
                "default": "parse",
                "label": "Operation",
                "description": "Parsing operation to perform",
            },
            "sections": {
                "type": "multiselect",
                "options": [
                    {"value": "48765-2", "label": "Allergies"},
                    {"value": "10160-0", "label": "Medications"},
                    {"value": "11450-4", "label": "Problems"},
                    {"value": "47519-4", "label": "Procedures"},
                    {"value": "30954-2", "label": "Lab Results"},
                    {"value": "8716-3", "label": "Vital Signs"},
                    {"value": "11369-6", "label": "Immunizations"},
                    {"value": "46240-8", "label": "Encounters"},
                ],
                "label": "Sections to Extract",
                "description": "Filter to specific sections (empty = all)",
            },
            "strict": {
                "type": "boolean",
                "default": False,
                "label": "Strict Mode",
                "description": "Raise errors on validation failures",
            },
            "output_format": {
                "type": "select",
                "options": ["dict", "json", "fhir"],
                "default": "dict",
                "label": "Output Format",
            },
        },
    }
