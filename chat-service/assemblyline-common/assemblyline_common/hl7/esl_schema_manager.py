"""
ESL Schema Manager and HL7 Generator

This module provides:
1. ESL Schema parsing (basedefs.esl, hssdefs.esl)
2. Custom schema creation (new segments, message structures)
3. Dynamic HL7 message generation from any JSON payload
4. Schema validation and export
"""

import yaml
import re
import os
import json
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from enum import Enum


class FieldUsage(str, Enum):
    """HL7 field usage codes."""
    MANDATORY = 'M'
    OPTIONAL = 'O'
    CONDITIONAL = 'C'


class DataType(str, Enum):
    """Common HL7 data types."""
    ST = 'ST'   # String
    NM = 'NM'   # Numeric
    ID = 'ID'   # Coded value
    IS = 'IS'   # Coded value (user-defined)
    DT = 'DT'   # Date
    TM = 'TM'   # Time
    DTM = 'DTM' # Date/Time
    TS = 'TS'   # Timestamp
    SI = 'SI'   # Sequence ID
    TX = 'TX'   # Text
    FT = 'FT'   # Formatted Text
    CE = 'CE'   # Coded Element
    CWE = 'CWE' # Coded with Exceptions
    CNE = 'CNE' # Coded No Exceptions
    XPN = 'XPN' # Extended Person Name
    XAD = 'XAD' # Extended Address
    XTN = 'XTN' # Extended Telephone
    XCN = 'XCN' # Extended Composite Name
    XON = 'XON' # Extended Composite Org ID
    CX = 'CX'   # Extended Composite ID
    EI = 'EI'   # Entity Identifier
    HD = 'HD'   # Hierarchic Designator
    PL = 'PL'   # Person Location
    CP = 'CP'   # Composite Price
    varies = 'varies'


@dataclass
class FieldDefinition:
    """Definition of a field within a segment."""
    name: str
    data_type: str  # idRef in ESL
    usage: str = 'O'
    count: str = '1'  # '1' for single, '>1' for repeating
    
    def to_esl(self) -> Dict:
        """Convert to ESL format."""
        result = {
            'idRef': self.data_type,
            'name': self.name,
            'usage': self.usage
        }
        if self.count != '1':
            result['count'] = self.count
        return result
    
    @classmethod
    def from_esl(cls, data: Dict) -> 'FieldDefinition':
        """Create from ESL format."""
        return cls(
            name=data.get('name', ''),
            data_type=data.get('idRef', 'ST'),
            usage=data.get('usage', 'O'),
            count=data.get('count', '1')
        )


@dataclass
class SegmentDefinition:
    """Definition of an HL7 segment."""
    id: str
    name: str
    fields: List[FieldDefinition] = field(default_factory=list)
    is_custom: bool = False  # True for Z-segments and custom definitions
    
    def to_esl(self) -> Dict:
        """Convert to ESL format."""
        return {
            'id': self.id,
            'name': self.name,
            'values': [f.to_esl() for f in self.fields]
        }
    
    @classmethod
    def from_esl(cls, data: Dict) -> 'SegmentDefinition':
        """Create from ESL format."""
        values = data.get('values') or []
        fields = [FieldDefinition.from_esl(v) for v in values if v is not None]
        return cls(
            id=data.get('id', ''),
            name=data.get('name', ''),
            fields=fields,
            is_custom=data.get('id', '').startswith('Z')
        )
    
    def get_field(self, index: int) -> Optional[FieldDefinition]:
        """Get field definition by index (1-based)."""
        if 1 <= index <= len(self.fields):
            return self.fields[index - 1]
        return None


@dataclass
class MessageSegment:
    """A segment reference within a message structure."""
    segment_id: str
    position: str
    usage: str = 'O'
    group_id: Optional[str] = None
    min_occurs: int = 0
    max_occurs: int = 1
    
    def to_esl(self) -> Dict:
        """Convert to ESL format."""
        result = {
            'idRef': self.segment_id,
            'position': self.position,
            'usage': self.usage
        }
        if self.group_id:
            result['groupId'] = self.group_id
        return result


@dataclass
class MessageStructure:
    """Definition of an HL7 message structure."""
    message_type: str  # e.g., 'ADT_A08'
    name: str
    hl7_version: str = '2.4'
    segments: List[MessageSegment] = field(default_factory=list)
    groups: Dict[str, List[str]] = field(default_factory=dict)  # groupId -> list of segment positions
    
    def to_esl(self) -> str:
        """Convert to ESL file format."""
        lines = [
            f"form: HL7",
            f"version: '{self.hl7_version}'",
            f"imports: [ '/basedefs.esl', '/hssdefs.esl' ]",
            f"id: '{self.message_type}'",
            f"name: '{self.name}'",
            "data:",
        ]
        
        for seg in self.segments:
            line = f"- {{ idRef: '{seg.segment_id}', position: '{seg.position}', usage: {seg.usage}"
            if seg.group_id:
                line += f", groupId: '{seg.group_id}'"
            line += " }"
            lines.append(line)
        
        if self.groups:
            lines.append("groups:")
            for group_id, positions in self.groups.items():
                lines.append(f"- id: '{group_id}'")
                lines.append(f"  positions: {positions}")
        
        return '\n'.join(lines)


class ESLSchemaManager:
    """
    Manages ESL schemas for HL7 message generation.
    
    Capabilities:
    - Load existing schemas (basedefs.esl, hssdefs.esl)
    - Support multiple schema sources (Epic custom, HL7 standard library)
    - Create new custom segments
    - Create new message structures  
    - Save custom schemas
    - Generate HL7 messages from JSON
    """
    
    # Available HL7 versions in the hl7lax library
    HL7_VERSIONS = ['2.1', '2.2', '2.3', '2.3.1', '2.4', '2.5', '2.5.1', '2.6', '2.7', '2.7.1', '2.8', '2.8.1']
    
    def __init__(self, data_dir: str = None):
        if data_dir is None:
            data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        self.data_dir = Path(data_dir)
        
        # Epic custom schema directory (root data folder)
        self.epic_dir = self.data_dir
        
        # HL7 standard library directory
        self.hl7_library_dir = self.data_dir / 'hl7lax'
        
        # Segment definitions from basedefs and hssdefs (Epic)
        self.base_segments: Dict[str, SegmentDefinition] = {}
        self.custom_segments: Dict[str, SegmentDefinition] = {}
        
        # Message structures - Epic custom
        self.message_structures: Dict[str, MessageStructure] = {}
        
        # HL7 Library schemas by version
        self.hl7_library_segments: Dict[str, Dict[str, SegmentDefinition]] = {}  # version -> segments
        self.hl7_library_structures: Dict[str, Dict[str, MessageStructure]] = {}  # version -> structures
        
        # Custom schemas file path
        self.custom_schemas_file = self.data_dir / 'custom_schemas.esl'
        
        # Current active source ('epic' or version like '2.4')
        self.active_source = 'epic'
        
        # Load existing schemas
        self._load_all_schemas()
    
    def get_available_sources(self) -> List[Dict[str, Any]]:
        """Get list of available ESL sources."""
        sources = [
            {
                'id': 'epic',
                'name': 'Epic Custom',
                'description': 'Epic Healthcare custom ESL schemas',
                'type': 'epic',
                'segmentCount': len(self.base_segments) + len(self.custom_segments),
                'messageCount': len(self.message_structures)
            }
        ]
        
        # Add available HL7 versions
        for version in self.HL7_VERSIONS:
            version_key = f'v{version.replace(".", "_")}'
            version_dir = self.hl7_library_dir / version_key
            if version_dir.exists():
                seg_count = len(self.hl7_library_segments.get(version, {}))
                msg_count = len(self.hl7_library_structures.get(version, {}))
                sources.append({
                    'id': version,
                    'name': f'HL7 v{version}',
                    'description': f'Standard HL7 version {version} schemas',
                    'type': 'hl7',
                    'version': version,
                    'segmentCount': seg_count,
                    'messageCount': msg_count
                })
        
        return sources
    
    def set_active_source(self, source: str):
        """Set the active ESL source."""
        if source == 'epic':
            self.active_source = 'epic'
        elif source in self.HL7_VERSIONS or source.replace('v', '').replace('_', '.') in self.HL7_VERSIONS:
            # Normalize version format
            if source.startswith('v'):
                source = source[1:].replace('_', '.')
            self.active_source = source
            # Load if not already loaded
            if source not in self.hl7_library_segments:
                self._load_hl7_version(source)
        else:
            raise ValueError(f"Unknown source: {source}")
    
    def _load_all_schemas(self):
        """Load all ESL schema files."""
        # Load Epic custom schemas (from root data folder)
        self._load_epic_schemas()
        
        # Pre-load a default HL7 version (2.4 is common)
        if (self.hl7_library_dir / 'v2_4').exists():
            self._load_hl7_version('2.4')
    
    def _load_epic_schemas(self):
        """Load Epic custom ESL schemas from root data folder."""
        # Load basedefs
        basedefs = self.epic_dir / 'basedefs.esl'
        if basedefs.exists():
            self._load_segment_definitions(basedefs, is_custom=False)
        
        # Load hssdefs (HSS custom segments)
        hssdefs = self.epic_dir / 'hssdefs.esl'
        if hssdefs.exists():
            self._load_segment_definitions(hssdefs, is_custom=True)
        
        # Load custom schemas if exists
        if self.custom_schemas_file.exists():
            self._load_segment_definitions(self.custom_schemas_file, is_custom=True)
        
        # Load Epic message structures (from root data folder, not subfolders)
        for esl_file in self.epic_dir.glob('*.esl'):
            name = esl_file.stem
            if name not in ('basedefs', 'hssdefs', 'custom_schemas'):
                self._load_message_structure(esl_file)
    
    def _load_hl7_version(self, version: str):
        """Load HL7 schemas for a specific version from the library."""
        version_key = f'v{version.replace(".", "_")}'
        version_dir = self.hl7_library_dir / version_key
        
        if not version_dir.exists():
            raise ValueError(f"HL7 version {version} not found at {version_dir}")
        
        # Initialize storage for this version
        self.hl7_library_segments[version] = {}
        self.hl7_library_structures[version] = {}
        
        # Load basedefs for this version
        basedefs = version_dir / 'basedefs.esl'
        if basedefs.exists():
            self._load_segment_definitions_to_dict(
                basedefs, 
                self.hl7_library_segments[version], 
                is_custom=False
            )
        
        # Load message structures for this version
        for esl_file in version_dir.glob('*.esl'):
            name = esl_file.stem
            if name not in ('basedefs', 'hssdefs', 'custom_schemas', 'event-message'):
                structure = self._parse_message_structure(esl_file)
                if structure:
                    structure.hl7_version = version
                    self.hl7_library_structures[version][structure.message_type] = structure
    
    def _load_segment_definitions_to_dict(self, filepath: Path, target_dict: Dict, is_custom: bool = False):
        """Load segment definitions from an ESL file into a specific dictionary."""
        content = filepath.read_text()
        
        try:
            data = yaml.safe_load(content)
            segments = []
            
            if isinstance(data, dict) and 'segments' in data:
                segments = data['segments']
            elif isinstance(data, list):
                segments = data
            
            for seg_data in segments:
                if isinstance(seg_data, dict) and 'id' in seg_data:
                    seg_def = SegmentDefinition.from_esl(seg_data)
                    seg_def.is_custom = is_custom or seg_def.id.startswith('Z')
                    target_dict[seg_def.id] = seg_def
                        
        except yaml.YAMLError:
            # Parse manually for non-standard format
            self._parse_segments_manual_to_dict(content, target_dict, is_custom)
    
    def _parse_segments_manual_to_dict(self, content: str, target_dict: Dict, is_custom: bool):
        """Manually parse ESL content into a specific dictionary."""
        current_segment = None
        
        for line in content.split('\n'):
            line = line.strip()
            
            if line.startswith("- id:"):
                # Save previous segment
                if current_segment:
                    target_dict[current_segment.id] = current_segment
                
                # Start new segment
                match = re.match(r"- id: '([^']+)'", line)
                if match:
                    seg_id = match.group(1)
                    current_segment = SegmentDefinition(
                        id=seg_id,
                        name='',
                        fields=[],
                        is_custom=is_custom or seg_id.startswith('Z')
                    )
            
            elif current_segment and line.startswith("name:"):
                match = re.match(r"name: '([^']+)'", line)
                if match:
                    current_segment.name = match.group(1)
            
            elif current_segment and '{ idRef:' in line:
                match = re.search(
                    r"\{ idRef: '([^']+)', name: '([^']+)', usage: ([OMC])",
                    line
                )
                if match:
                    field_def = FieldDefinition(
                        data_type=match.group(1),
                        name=match.group(2),
                        usage=match.group(3)
                    )
                    count_match = re.search(r"count: '([^']+)'", line)
                    if count_match:
                        field_def.count = count_match.group(1)
                    current_segment.fields.append(field_def)
        
        # Save last segment
        if current_segment:
            target_dict[current_segment.id] = current_segment
    
    def _parse_message_structure(self, filepath: Path) -> Optional[MessageStructure]:
        """Parse a message structure file and return the structure object."""
        content = filepath.read_text()
        message_type = filepath.stem
        
        structure = MessageStructure(
            message_type=message_type,
            name=message_type.replace('_', ' ')
        )
        
        try:
            data = yaml.safe_load(content)
            if isinstance(data, dict):
                structure.hl7_version = str(data.get('version', '2.4')).strip("'")
                structure.name = data.get('name', message_type)
                
                if 'data' in data:
                    for i, seg_data in enumerate(data['data']):
                        if isinstance(seg_data, dict):
                            seg = MessageSegment(
                                segment_id=seg_data.get('idRef', ''),
                                position=str(seg_data.get('position', i + 1)),
                                usage=seg_data.get('usage', 'O'),
                                group_id=seg_data.get('groupId')
                            )
                            structure.segments.append(seg)
                return structure
        except yaml.YAMLError:
            pass
        
        return None

    def _load_segment_definitions(self, filepath: Path, is_custom: bool = False):
        """Load segment definitions from an ESL file."""
        content = filepath.read_text()
        
        try:
            data = yaml.safe_load(content)
            segments = []
            
            if isinstance(data, dict) and 'segments' in data:
                segments = data['segments']
            elif isinstance(data, list):
                segments = data
            
            for seg_data in segments:
                if isinstance(seg_data, dict) and 'id' in seg_data:
                    seg_def = SegmentDefinition.from_esl(seg_data)
                    seg_def.is_custom = is_custom or seg_def.id.startswith('Z')
                    
                    if seg_def.is_custom:
                        self.custom_segments[seg_def.id] = seg_def
                    else:
                        self.base_segments[seg_def.id] = seg_def
                        
        except yaml.YAMLError:
            # Parse manually for non-standard format
            self._parse_segments_manual(content, is_custom)
    
    def _parse_segments_manual(self, content: str, is_custom: bool):
        """Manually parse ESL content."""
        current_segment = None
        
        for line in content.split('\n'):
            line = line.strip()
            
            if line.startswith("- id:"):
                # Save previous segment
                if current_segment:
                    if current_segment.is_custom:
                        self.custom_segments[current_segment.id] = current_segment
                    else:
                        self.base_segments[current_segment.id] = current_segment
                
                # Start new segment
                match = re.match(r"- id: '([^']+)'", line)
                if match:
                    seg_id = match.group(1)
                    current_segment = SegmentDefinition(
                        id=seg_id,
                        name='',
                        fields=[],
                        is_custom=is_custom or seg_id.startswith('Z')
                    )
            
            elif current_segment and line.startswith("name:"):
                match = re.match(r"name: '([^']+)'", line)
                if match:
                    current_segment.name = match.group(1)
            
            elif current_segment and '{ idRef:' in line:
                match = re.search(
                    r"\{ idRef: '([^']+)', name: '([^']+)', usage: ([OMC])",
                    line
                )
                if match:
                    field_def = FieldDefinition(
                        data_type=match.group(1),
                        name=match.group(2),
                        usage=match.group(3)
                    )
                    count_match = re.search(r"count: '([^']+)'", line)
                    if count_match:
                        field_def.count = count_match.group(1)
                    current_segment.fields.append(field_def)
        
        # Save last segment
        if current_segment:
            if current_segment.is_custom:
                self.custom_segments[current_segment.id] = current_segment
            else:
                self.base_segments[current_segment.id] = current_segment
    
    def _load_message_structure(self, filepath: Path):
        """Load a message structure definition."""
        content = filepath.read_text()
        message_type = filepath.stem
        
        structure = MessageStructure(
            message_type=message_type,
            name=message_type.replace('_', ' ')
        )
        
        try:
            data = yaml.safe_load(content)
            if isinstance(data, dict):
                structure.hl7_version = str(data.get('version', '2.4')).strip("'")
                structure.name = data.get('name', message_type)
                
                if 'data' in data:
                    for i, seg_data in enumerate(data['data']):
                        if isinstance(seg_data, dict):
                            seg = MessageSegment(
                                segment_id=seg_data.get('idRef', ''),
                                position=str(seg_data.get('position', i + 1)),
                                usage=seg_data.get('usage', 'O'),
                                group_id=seg_data.get('groupId')
                            )
                            structure.segments.append(seg)
        except yaml.YAMLError:
            pass
        
        self.message_structures[message_type] = structure
    
    # === Schema Query Methods ===
    
    def get_segment(self, segment_id: str, source: str = None) -> Optional[SegmentDefinition]:
        """Get a segment definition by ID from specified or active source."""
        source = source or self.active_source
        
        if source == 'epic':
            return self.custom_segments.get(segment_id) or self.base_segments.get(segment_id)
        else:
            # HL7 library source
            if source not in self.hl7_library_segments:
                self._load_hl7_version(source)
            return self.hl7_library_segments.get(source, {}).get(segment_id)
    
    def get_all_segments(self, source: str = None) -> Dict[str, SegmentDefinition]:
        """Get all segments from specified or active source."""
        source = source or self.active_source
        
        if source == 'epic':
            # Combine base and custom segments
            all_segs = dict(self.base_segments)
            all_segs.update(self.custom_segments)
            return all_segs
        else:
            # HL7 library source
            if source not in self.hl7_library_segments:
                self._load_hl7_version(source)
            return self.hl7_library_segments.get(source, {})
    
    def list_segments(self, include_base: bool = True, include_custom: bool = True, source: str = None) -> List[str]:
        """List all available segment IDs from specified or active source."""
        source = source or self.active_source
        
        if source == 'epic':
            segments = []
            if include_base:
                segments.extend(sorted(self.base_segments.keys()))
            if include_custom:
                segments.extend(sorted(self.custom_segments.keys()))
            return segments
        else:
            # HL7 library source
            if source not in self.hl7_library_segments:
                self._load_hl7_version(source)
            return sorted(self.hl7_library_segments.get(source, {}).keys())
    
    def list_message_types(self, source: str = None) -> List[str]:
        """List all available message types from specified or active source."""
        source = source or self.active_source
        
        if source == 'epic':
            return sorted(self.message_structures.keys())
        else:
            # HL7 library source
            if source not in self.hl7_library_structures:
                self._load_hl7_version(source)
            return sorted(self.hl7_library_structures.get(source, {}).keys())
    
    def get_message_structure(self, message_type: str, source: str = None) -> Optional[MessageStructure]:
        """Get a message structure by type from specified or active source."""
        source = source or self.active_source
        
        if source == 'epic':
            return self.message_structures.get(message_type)
        else:
            # HL7 library source
            if source not in self.hl7_library_structures:
                self._load_hl7_version(source)
            return self.hl7_library_structures.get(source, {}).get(message_type)
    
    def get_all_message_structures(self, source: str = None) -> Dict[str, MessageStructure]:
        """Get all message structures from specified or active source."""
        source = source or self.active_source
        
        if source == 'epic':
            return self.message_structures
        else:
            # HL7 library source
            if source not in self.hl7_library_structures:
                self._load_hl7_version(source)
            return self.hl7_library_structures.get(source, {})
    
    # === Schema Creation Methods ===
    
    def create_segment(
        self,
        segment_id: str,
        name: str,
        fields: List[Dict[str, Any]]
    ) -> SegmentDefinition:
        """
        Create a new custom segment definition.
        
        Args:
            segment_id: Segment identifier (e.g., 'ZPX' for custom)
            name: Human-readable segment name
            fields: List of field definitions:
                    [{'name': 'Field Name', 'type': 'ST', 'usage': 'O', 'count': '1'}]
        
        Returns:
            The created SegmentDefinition
        """
        field_defs = []
        for f in fields:
            field_defs.append(FieldDefinition(
                name=f.get('name', ''),
                data_type=f.get('type', 'ST'),
                usage=f.get('usage', 'O'),
                count=f.get('count', '1')
            ))
        
        segment = SegmentDefinition(
            id=segment_id,
            name=name,
            fields=field_defs,
            is_custom=True
        )
        
        self.custom_segments[segment_id] = segment
        return segment
    
    def create_message_structure(
        self,
        message_type: str,
        name: str,
        segments: List[Dict[str, Any]],
        hl7_version: str = '2.4'
    ) -> MessageStructure:
        """
        Create a new message structure definition.
        
        Args:
            message_type: Message type (e.g., 'ZDT_Z01')
            name: Human-readable name
            segments: List of segment references:
                      [{'id': 'MSH', 'usage': 'M'}, {'id': 'PID', 'usage': 'M'}]
            hl7_version: HL7 version string
        
        Returns:
            The created MessageStructure
        """
        msg_segments = []
        for i, seg in enumerate(segments):
            msg_segments.append(MessageSegment(
                segment_id=seg.get('id', ''),
                position=str(i + 1),
                usage=seg.get('usage', 'O'),
                group_id=seg.get('group')
            ))
        
        structure = MessageStructure(
            message_type=message_type,
            name=name,
            hl7_version=hl7_version,
            segments=msg_segments
        )
        
        self.message_structures[message_type] = structure
        return structure
    
    def save_custom_schemas(self):
        """Save custom segment definitions to custom_schemas.esl."""
        if not self.custom_segments:
            return
        
        content = {
            'form': 'HL7',
            'version': '2.4',
            'segments': [seg.to_esl() for seg in self.custom_segments.values()]
        }
        
        with open(self.custom_schemas_file, 'w') as f:
            yaml.dump(content, f, default_flow_style=False, sort_keys=False)
    
    def save_message_structure(self, message_type: str):
        """Save a message structure to its own ESL file."""
        structure = self.message_structures.get(message_type)
        if not structure:
            raise ValueError(f"Message structure not found: {message_type}")
        
        filepath = self.data_dir / f'{message_type}.esl'
        with open(filepath, 'w') as f:
            f.write(structure.to_esl())
    
    def export_segment_to_hssdefs(self, segment_id: str):
        """Export a custom segment to hssdefs.esl format."""
        segment = self.custom_segments.get(segment_id)
        if not segment:
            raise ValueError(f"Custom segment not found: {segment_id}")
        
        return segment.to_esl()


class HL7MessageGenerator:
    """
    Generate HL7 messages from JSON payloads using ESL schemas.
    
    Supports:
    - Automatic field mapping from JSON keys
    - Explicit field mapping configuration
    - Custom segment support
    - Dynamic message structure
    """
    
    FIELD_SEP = '|'
    COMPONENT_SEP = '^'
    REPEAT_SEP = '~'
    ESCAPE_CHAR = '\\'
    SUBCOMPONENT_SEP = '&'
    SEGMENT_TERM = '\r'
    
    def __init__(self, schema_manager: ESLSchemaManager = None):
        self.schema_manager = schema_manager or ESLSchemaManager()
    
    def generate(
        self,
        json_data: Dict[str, Any],
        message_type: str = None,
        field_mapping: Dict[str, str] = None,
        segment_order: List[str] = None,
        sending_app: str = 'LOGICWEAVER',
        sending_facility: str = 'FACILITY',
        receiving_app: str = '',
        receiving_facility: str = '',
        hl7_version: str = '2.4'
    ) -> str:
        """
        Generate an HL7 message from JSON data.
        
        Args:
            json_data: Source JSON payload
            message_type: Message type (e.g., 'ADT_A08'). If None, infers from json_data
            field_mapping: Explicit mapping from JSON paths to HL7 fields
                          e.g., {"$.patientId": "PID.3.1", "$.name.last": "PID.5.1"}
            segment_order: Custom segment order. If None, uses message structure
            sending_app: MSH-3 Sending Application
            sending_facility: MSH-4 Sending Facility
            receiving_app: MSH-5 Receiving Application
            receiving_facility: MSH-6 Receiving Facility
            hl7_version: HL7 version (default 2.4)
        
        Returns:
            HL7 message string with \\r segment terminators
        """
        # Determine message type
        if message_type is None:
            message_type = json_data.get('_messageType', 'ADT_A08')
        
        # Get or create segment order
        if segment_order is None:
            structure = self.schema_manager.get_message_structure(message_type)
            if structure:
                segment_order = [seg.segment_id for seg in structure.segments]
            else:
                # Default minimal structure
                segment_order = ['MSH', 'EVN', 'PID']
        
        # Ensure MSH is first
        if 'MSH' not in segment_order:
            segment_order.insert(0, 'MSH')
        elif segment_order[0] != 'MSH':
            segment_order.remove('MSH')
            segment_order.insert(0, 'MSH')
        
        # Build segments
        segments = []
        
        for seg_id in segment_order:
            if seg_id == 'MSH':
                segment = self._build_msh(
                    message_type, hl7_version,
                    sending_app, sending_facility,
                    receiving_app, receiving_facility
                )
            else:
                segment = self._build_segment(
                    seg_id, json_data, field_mapping
                )
            
            if segment:
                segments.append(segment)
        
        return self.SEGMENT_TERM.join(segments)
    
    def generate_from_flat_json(
        self,
        json_data: Dict[str, Any],
        message_type: str = 'ADT_A08',
        **kwargs
    ) -> str:
        """
        Generate HL7 from flat JSON where keys match segment.field notation.
        
        Example JSON:
        {
            "PID.3.1": "12345",
            "PID.5.1": "Smith",
            "PID.5.2": "John",
            "PID.7": "19850101"
        }
        """
        # Convert flat keys to mapping format
        field_mapping = {}
        regular_data = {}
        
        for key, value in json_data.items():
            if '.' in key and key.split('.')[0].isupper():
                # This looks like a segment.field reference
                field_mapping[f'$.{key}'] = key
            else:
                regular_data[key] = value
        
        # Merge the mappings
        return self.generate(
            json_data, 
            message_type=message_type,
            field_mapping=field_mapping,
            **kwargs
        )
    
    def generate_dynamic(
        self,
        json_data: Dict[str, Any],
        segment_configs: List[Dict[str, Any]],
        **kwargs
    ) -> str:
        """
        Generate HL7 with fully dynamic segment configuration.
        
        Args:
            json_data: Source data
            segment_configs: List of segment configurations:
                [
                    {
                        "segment": "MSH",
                        "fields": {
                            "3": "MYAPP",
                            "4": "FACILITY"
                        }
                    },
                    {
                        "segment": "PID",
                        "mapping": {
                            "3.1": "$.patientId",
                            "5.1": "$.lastName",
                            "5.2": "$.firstName"
                        }
                    }
                ]
        """
        segments = []
        
        for config in segment_configs:
            seg_id = config.get('segment', '')
            static_fields = config.get('fields', {})
            mapping = config.get('mapping', {})
            
            if seg_id == 'MSH':
                segment = self._build_msh_dynamic(static_fields, json_data, mapping)
            else:
                segment = self._build_segment_dynamic(
                    seg_id, static_fields, json_data, mapping
                )
            
            if segment:
                segments.append(segment)
        
        return self.SEGMENT_TERM.join(segments)
    
    def _build_msh(
        self,
        message_type: str,
        version: str,
        sending_app: str,
        sending_facility: str,
        receiving_app: str,
        receiving_facility: str
    ) -> str:
        """Build MSH segment."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        msg_control_id = f'MSG{int(datetime.now().timestamp())}'
        msg_type = message_type.replace('_', '^')
        
        # MSH fields (note: MSH-1 is the field separator itself)
        fields = [
            'MSH',                          # 0: Segment ID
            '^~\\&',                        # 2: Encoding Characters
            sending_app,                    # 3: Sending Application
            sending_facility,               # 4: Sending Facility
            receiving_app,                  # 5: Receiving Application
            receiving_facility,             # 6: Receiving Facility
            timestamp,                      # 7: Date/Time of Message
            '',                             # 8: Security
            msg_type,                       # 9: Message Type
            msg_control_id,                 # 10: Message Control ID
            'P',                            # 11: Processing ID
            version,                        # 12: Version ID
        ]
        
        return self.FIELD_SEP.join(fields)
    
    def _build_msh_dynamic(
        self,
        static_fields: Dict[str, str],
        json_data: Dict[str, Any],
        mapping: Dict[str, str]
    ) -> str:
        """Build MSH with dynamic configuration."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        msg_control_id = f'MSG{int(datetime.now().timestamp())}'
        
        # Default MSH values
        fields = ['MSH', '^~\\&', '', '', '', '', timestamp, '', 'ADT^A08', msg_control_id, 'P', '2.4']
        
        # Apply static fields
        for idx_str, value in static_fields.items():
            idx = int(idx_str)
            while len(fields) <= idx:
                fields.append('')
            fields[idx] = value
        
        # Apply mapping
        for idx_str, json_path in mapping.items():
            idx = int(idx_str.split('.')[0])
            value = self._get_json_value(json_data, json_path)
            if value is not None:
                while len(fields) <= idx:
                    fields.append('')
                
                if '.' in idx_str:
                    # Component-level mapping
                    comp_idx = int(idx_str.split('.')[1])
                    self._set_component(fields, idx, comp_idx, str(value))
                else:
                    fields[idx] = str(value)
        
        return self.FIELD_SEP.join(fields)
    
    def _build_segment(
        self,
        segment_id: str,
        json_data: Dict[str, Any],
        field_mapping: Dict[str, str] = None
    ) -> Optional[str]:
        """Build a segment from JSON data using schema definitions."""
        segment_def = self.schema_manager.get_segment(segment_id)
        
        # Determine field count
        field_count = len(segment_def.fields) if segment_def else 20
        
        # Initialize fields
        fields = [segment_id] + [''] * field_count
        
        # Try to extract data from JSON
        # 1. Check for segment-keyed data: json_data[segment_id]
        segment_data = json_data.get(segment_id, {})
        if isinstance(segment_data, dict):
            for key, value in segment_data.items():
                if key.isdigit():
                    idx = int(key)
                    if idx < len(fields):
                        fields[idx] = str(value)
        
        # 2. Apply explicit field mapping
        if field_mapping:
            for json_path, hl7_path in field_mapping.items():
                if not hl7_path.startswith(f'{segment_id}.'):
                    continue
                
                value = self._get_json_value(json_data, json_path)
                if value is not None:
                    parts = hl7_path.split('.')
                    field_idx = int(parts[1])
                    comp_idx = int(parts[2]) if len(parts) > 2 else 0
                    
                    while len(fields) <= field_idx:
                        fields.append('')
                    
                    if comp_idx > 0:
                        self._set_component(fields, field_idx, comp_idx, str(value))
                    else:
                        fields[field_idx] = str(value)
        
        # 3. Auto-map by field names if segment definition exists
        if segment_def:
            self._auto_map_fields(fields, segment_def, json_data)
        
        # Check if segment has any data
        if all(f == '' for f in fields[1:]):
            return None
        
        return self.FIELD_SEP.join(fields)
    
    def _build_segment_dynamic(
        self,
        segment_id: str,
        static_fields: Dict[str, str],
        json_data: Dict[str, Any],
        mapping: Dict[str, str]
    ) -> Optional[str]:
        """Build segment with dynamic configuration."""
        fields = [segment_id] + [''] * 30
        
        # Apply static fields
        for idx_str, value in static_fields.items():
            parts = idx_str.split('.')
            idx = int(parts[0])
            while len(fields) <= idx:
                fields.append('')
            
            if len(parts) > 1:
                comp_idx = int(parts[1])
                self._set_component(fields, idx, comp_idx, value)
            else:
                fields[idx] = value
        
        # Apply mapping
        for idx_str, json_path in mapping.items():
            value = self._get_json_value(json_data, json_path)
            if value is not None:
                parts = idx_str.split('.')
                idx = int(parts[0])
                while len(fields) <= idx:
                    fields.append('')
                
                if len(parts) > 1:
                    comp_idx = int(parts[1])
                    self._set_component(fields, idx, comp_idx, str(value))
                else:
                    fields[idx] = str(value)
        
        # Trim trailing empty fields
        while fields and fields[-1] == '':
            fields.pop()
        
        if len(fields) <= 1:
            return None
        
        return self.FIELD_SEP.join(fields)
    
    def _auto_map_fields(
        self,
        fields: List[str],
        segment_def: SegmentDefinition,
        json_data: Dict[str, Any]
    ):
        """Auto-map JSON fields to segment fields based on field names."""
        # Create a mapping of normalized field names to indices
        name_map = {}
        for i, field_def in enumerate(segment_def.fields, 1):
            # Normalize the field name
            normalized = self._normalize_name(field_def.name)
            name_map[normalized] = i
        
        # Try to match JSON keys
        for key, value in json_data.items():
            normalized_key = self._normalize_name(key)
            if normalized_key in name_map:
                idx = name_map[normalized_key]
                if idx < len(fields) and fields[idx] == '':
                    if isinstance(value, dict):
                        # Handle component-level data
                        components = []
                        for comp_key in sorted(value.keys()):
                            components.append(str(value[comp_key]))
                        fields[idx] = self.COMPONENT_SEP.join(components)
                    else:
                        fields[idx] = str(value)
    
    def _normalize_name(self, name: str) -> str:
        """Normalize a field name for matching."""
        # Remove common prefixes/suffixes and normalize
        name = name.lower()
        name = re.sub(r'[^a-z0-9]', '', name)
        # Remove common HL7 prefixes
        for prefix in ('patient', 'set', 'id'):
            if name.startswith(prefix):
                name = name[len(prefix):]
        return name
    
    def _get_json_value(self, data: Dict, path: str) -> Any:
        """Get value from JSON using path notation."""
        if path.startswith('$.'):
            path = path[2:]
        elif path.startswith('$'):
            path = path[1:]
        
        parts = path.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            else:
                return None
            
            if current is None:
                return None
        
        return current
    
    def _set_component(
        self,
        fields: List[str],
        field_idx: int,
        comp_idx: int,
        value: str
    ):
        """Set a component within a field."""
        while len(fields) <= field_idx:
            fields.append('')
        
        current = fields[field_idx]
        components = current.split(self.COMPONENT_SEP) if current else []
        
        while len(components) < comp_idx:
            components.append('')
        
        if comp_idx <= len(components):
            components[comp_idx - 1] = value
        else:
            components.append(value)
        
        fields[field_idx] = self.COMPONENT_SEP.join(components)


# === API Functions ===

def generate_hl7_from_json(
    json_data: Dict[str, Any],
    message_type: str = 'ADT_A08',
    field_mapping: Dict[str, str] = None,
    **kwargs
) -> str:
    """
    Simple function to generate HL7 from JSON.
    
    Example:
        json_data = {
            "patientId": "12345",
            "lastName": "Smith",
            "firstName": "John"
        }
        
        mapping = {
            "$.patientId": "PID.3.1",
            "$.lastName": "PID.5.1", 
            "$.firstName": "PID.5.2"
        }
        
        hl7 = generate_hl7_from_json(json_data, "ADT_A08", mapping)
    """
    generator = HL7MessageGenerator()
    return generator.generate(json_data, message_type, field_mapping, **kwargs)


def create_custom_segment(
    segment_id: str,
    name: str,
    fields: List[Dict[str, str]],
    save: bool = True
) -> SegmentDefinition:
    """
    Create a new custom segment definition.
    
    Example:
        segment = create_custom_segment(
            "ZPX",
            "Custom Patient Extension",
            [
                {"name": "Custom Field 1", "type": "ST", "usage": "O"},
                {"name": "Custom Field 2", "type": "NM", "usage": "O"},
            ]
        )
    """
    manager = ESLSchemaManager()
    segment = manager.create_segment(segment_id, name, fields)
    if save:
        manager.save_custom_schemas()
    return segment


# === Test Code ===

if __name__ == "__main__":
    # Initialize schema manager
    data_dir = '/Users/velezy/Projects/parser_lite/logic-weaver/data'
    manager = ESLSchemaManager(data_dir)
    
    print(f"Loaded {len(manager.base_segments)} base segments")
    print(f"Loaded {len(manager.custom_segments)} custom segments")
    print(f"Loaded {len(manager.message_structures)} message structures")
    
    print("\n=== Available Message Types ===")
    print(manager.list_message_types())
    
    print("\n=== Custom Segments (Z-segments) ===")
    for seg_id in sorted(manager.custom_segments.keys()):
        seg = manager.custom_segments[seg_id]
        print(f"  {seg_id}: {seg.name} ({len(seg.fields)} fields)")
    
    # Test HL7 generation
    print("\n=== Testing HL7 Generation ===")
    generator = HL7MessageGenerator(manager)
    
    test_json = {
        "patientId": "12345",
        "lastName": "Smith",
        "firstName": "John",
        "dob": "19850315",
        "gender": "M",
        "ssn": "123-45-6789",
        "address": {
            "street": "123 Main St",
            "city": "Boston",
            "state": "MA",
            "zip": "02101"
        }
    }
    
    field_mapping = {
        "$.patientId": "PID.3.1",
        "$.lastName": "PID.5.1",
        "$.firstName": "PID.5.2",
        "$.dob": "PID.7",
        "$.gender": "PID.8",
        "$.ssn": "PID.19",
        "$.address.street": "PID.11.1",
        "$.address.city": "PID.11.3",
        "$.address.state": "PID.11.4",
        "$.address.zip": "PID.11.5"
    }
    
    hl7_msg = generator.generate(
        test_json,
        message_type="ADT_A08",
        field_mapping=field_mapping,
        sending_app="TESTAPP",
        sending_facility="TESTFAC"
    )
    
    print("\nGenerated HL7:")
    print(hl7_msg.replace('\r', '\n'))
    
    # Test flat JSON format
    print("\n=== Testing Flat JSON Format ===")
    flat_json = {
        "PID.3.1": "67890",
        "PID.5.1": "Jones",
        "PID.5.2": "Mary",
        "PID.7": "19900422",
        "PID.8": "F"
    }
    
    hl7_flat = generator.generate_from_flat_json(flat_json, "ADT_A01")
    print("\nGenerated HL7 from flat JSON:")
    print(hl7_flat.replace('\r', '\n'))
    
    # Test dynamic generation
    print("\n=== Testing Dynamic Generation ===")
    segment_configs = [
        {
            "segment": "MSH",
            "fields": {
                "3": "DYNAMICAPP",
                "4": "DYNAMICFAC",
                "9": "ADT^A31"
            }
        },
        {
            "segment": "EVN",
            "fields": {
                "1": "A31"
            },
            "mapping": {
                "2": "$.eventTime"
            }
        },
        {
            "segment": "PID",
            "mapping": {
                "3.1": "$.mrn",
                "5.1": "$.patient.lastName",
                "5.2": "$.patient.firstName"
            }
        }
    ]
    
    dynamic_json = {
        "eventTime": "20241209120000",
        "mrn": "MRN123456",
        "patient": {
            "lastName": "Dynamic",
            "firstName": "Test"
        }
    }
    
    hl7_dynamic = generator.generate_dynamic(dynamic_json, segment_configs)
    print("\nGenerated HL7 from dynamic config:")
    print(hl7_dynamic.replace('\r', '\n'))
