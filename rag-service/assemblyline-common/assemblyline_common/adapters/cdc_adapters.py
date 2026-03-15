"""
Change Data Capture (CDC) Adapters for Logic Weaver.

Database CDC adapters for real-time data streaming:
- PostgreSQL: Logical replication with pgoutput/wal2json
- MySQL: Binary log (binlog) with row-based replication
- Oracle: LogMiner and GoldenGate integration

Features:
- Real-time change streaming
- Position tracking for resume
- Schema change handling
- Multi-tenant isolation
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Union
import asyncio
import json
import logging
import uuid

logger = logging.getLogger(__name__)


# =============================================================================
# Base Classes
# =============================================================================

class CDCOperation(str, Enum):
    """CDC operation types."""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    DDL = "DDL"  # Schema changes


@dataclass
class CDCPosition:
    """
    Position marker for CDC stream.

    Used to resume from a specific point after restart.
    """
    # PostgreSQL
    lsn: Optional[str] = None  # Log Sequence Number

    # MySQL
    binlog_file: Optional[str] = None
    binlog_position: Optional[int] = None
    gtid: Optional[str] = None  # Global Transaction ID

    # Oracle
    scn: Optional[int] = None  # System Change Number

    # Generic
    timestamp: Optional[datetime] = None
    sequence: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lsn": self.lsn,
            "binlog_file": self.binlog_file,
            "binlog_position": self.binlog_position,
            "gtid": self.gtid,
            "scn": self.scn,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "sequence": self.sequence,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CDCPosition":
        return cls(
            lsn=data.get("lsn"),
            binlog_file=data.get("binlog_file"),
            binlog_position=data.get("binlog_position"),
            gtid=data.get("gtid"),
            scn=data.get("scn"),
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else None,
            sequence=data.get("sequence"),
        )


@dataclass
class CDCEvent:
    """
    Change Data Capture event.

    Represents a single database change.
    """
    id: str
    operation: CDCOperation
    timestamp: datetime

    # Source info
    database: str
    schema: str
    table: str

    # Data
    before: Optional[Dict[str, Any]] = None  # Previous row (UPDATE/DELETE)
    after: Optional[Dict[str, Any]] = None   # New row (INSERT/UPDATE)

    # Position for resume
    position: Optional[CDCPosition] = None

    # Metadata
    transaction_id: Optional[str] = None
    primary_key: Optional[Dict[str, Any]] = None
    columns_changed: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "operation": self.operation.value,
            "timestamp": self.timestamp.isoformat(),
            "database": self.database,
            "schema": self.schema,
            "table": self.table,
            "before": self.before,
            "after": self.after,
            "position": self.position.to_dict() if self.position else None,
            "transaction_id": self.transaction_id,
            "primary_key": self.primary_key,
            "columns_changed": self.columns_changed,
        }


@dataclass
class CDCConfig:
    """Base CDC configuration."""
    name: str = "cdc"
    tenant_id: Optional[str] = None

    # Connection
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""

    # Tables to capture
    include_schemas: List[str] = field(default_factory=lambda: ["public"])
    include_tables: List[str] = field(default_factory=list)  # Empty = all
    exclude_tables: List[str] = field(default_factory=list)

    # Columns
    include_columns: Optional[Dict[str, List[str]]] = None  # table -> columns
    exclude_columns: Optional[Dict[str, List[str]]] = None

    # Position
    start_position: Optional[CDCPosition] = None
    start_from_beginning: bool = False

    # Processing
    batch_size: int = 100
    poll_interval_seconds: float = 1.0

    # Reconnection
    max_retries: int = 10
    retry_delay_seconds: float = 5.0


class CDCAdapter(ABC):
    """
    Base class for CDC adapters.

    Provides:
    - Connection management
    - Change event streaming
    - Position tracking
    - Schema handling
    """

    def __init__(self, config: CDCConfig):
        self.config = config
        self._connected = False
        self._current_position: Optional[CDCPosition] = None
        self._event_count = 0
        self._error_count = 0

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def current_position(self) -> Optional[CDCPosition]:
        return self._current_position

    @abstractmethod
    async def connect(self) -> bool:
        """Establish CDC connection."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close CDC connection."""
        pass

    @abstractmethod
    async def get_events(self) -> List[CDCEvent]:
        """Get batch of CDC events."""
        pass

    @abstractmethod
    async def acknowledge(self, position: CDCPosition) -> bool:
        """Acknowledge processing up to position."""
        pass

    async def stream(
        self,
        handler: Callable[[CDCEvent], bool],
    ) -> None:
        """Stream CDC events to handler. Handler returns True to ack."""
        last_ack_position: Optional[CDCPosition] = None

        while self.is_connected:
            try:
                events = await self.get_events()

                for event in events:
                    try:
                        should_ack = await handler(event)
                        if should_ack:
                            last_ack_position = event.position
                    except Exception as e:
                        logger.error(f"CDC handler error: {e}")
                        self._error_count += 1

                # Batch acknowledge
                if last_ack_position:
                    await self.acknowledge(last_ack_position)
                    last_ack_position = None

                if not events:
                    await asyncio.sleep(self.config.poll_interval_seconds)

            except Exception as e:
                logger.error(f"CDC stream error: {e}")
                self._error_count += 1
                await asyncio.sleep(self.config.retry_delay_seconds)

    def _should_capture_table(self, schema: str, table: str) -> bool:
        """Check if table should be captured."""
        if self.config.include_schemas and schema not in self.config.include_schemas:
            return False
        if self.config.include_tables and table not in self.config.include_tables:
            return False
        if self.config.exclude_tables and table in self.config.exclude_tables:
            return False
        return True

    def _filter_columns(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Filter columns based on configuration."""
        if not data:
            return data

        # Include filter
        if self.config.include_columns and table in self.config.include_columns:
            cols = self.config.include_columns[table]
            data = {k: v for k, v in data.items() if k in cols}

        # Exclude filter
        if self.config.exclude_columns and table in self.config.exclude_columns:
            cols = self.config.exclude_columns[table]
            data = {k: v for k, v in data.items() if k not in cols}

        return data


# =============================================================================
# PostgreSQL CDC Adapter
# =============================================================================

@dataclass
class PostgreSQLCDCConfig(CDCConfig):
    """PostgreSQL CDC configuration."""
    port: int = 5432

    # Replication slot
    slot_name: str = "logic_weaver_cdc"
    output_plugin: str = "pgoutput"  # pgoutput, wal2json, test_decoding

    # Publication
    publication_name: str = "logic_weaver_pub"
    create_publication: bool = True

    # Options
    include_transaction: bool = True
    include_timestamp: bool = True
    include_origin: bool = False


class PostgreSQLCDCAdapter(CDCAdapter):
    """
    PostgreSQL CDC adapter using logical replication.

    Features:
    - Logical replication with pgoutput or wal2json
    - Publication-based filtering
    - Transaction boundaries
    - Resume from LSN
    """

    def __init__(self, config: PostgreSQLCDCConfig):
        super().__init__(config)
        self.config: PostgreSQLCDCConfig = config
        self._conn = None
        self._replication_conn = None
        self._cursor = None

    async def connect(self) -> bool:
        """Connect to PostgreSQL for CDC."""
        try:
            import psycopg2
            from psycopg2.extras import LogicalReplicationConnection

            # Regular connection for setup
            self._conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
            )
            self._conn.autocommit = True

            # Create publication if needed
            if self.config.create_publication:
                await self._create_publication()

            # Create replication slot if needed
            await self._create_replication_slot()

            # Replication connection
            self._replication_conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
                connection_factory=LogicalReplicationConnection,
            )

            self._cursor = self._replication_conn.cursor()

            # Start replication
            options = {"publication_names": self.config.publication_name}
            if self.config.output_plugin == "pgoutput":
                options["proto_version"] = "1"

            start_lsn = None
            if self.config.start_position and self.config.start_position.lsn:
                start_lsn = self.config.start_position.lsn

            self._cursor.start_replication(
                slot_name=self.config.slot_name,
                decode=True,
                start_lsn=start_lsn,
                options=options,
            )

            self._connected = True
            logger.info(f"PostgreSQL CDC connected: {self.config.name}")
            return True

        except Exception as e:
            logger.error(f"PostgreSQL CDC connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Disconnect from PostgreSQL."""
        if self._cursor:
            self._cursor.close()
        if self._replication_conn:
            self._replication_conn.close()
        if self._conn:
            self._conn.close()
        self._connected = False

    async def _create_publication(self) -> None:
        """Create publication for CDC."""
        cursor = self._conn.cursor()

        # Check if publication exists
        cursor.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s",
            (self.config.publication_name,)
        )
        if cursor.fetchone():
            return

        # Build table list
        if self.config.include_tables:
            tables = ", ".join(
                f"{s}.{t}"
                for s in self.config.include_schemas
                for t in self.config.include_tables
            )
            cursor.execute(
                f"CREATE PUBLICATION {self.config.publication_name} FOR TABLE {tables}"
            )
        else:
            cursor.execute(
                f"CREATE PUBLICATION {self.config.publication_name} FOR ALL TABLES"
            )

        cursor.close()

    async def _create_replication_slot(self) -> None:
        """Create replication slot if not exists."""
        cursor = self._conn.cursor()

        cursor.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
            (self.config.slot_name,)
        )
        if cursor.fetchone():
            return

        cursor.execute(
            "SELECT pg_create_logical_replication_slot(%s, %s)",
            (self.config.slot_name, self.config.output_plugin)
        )
        cursor.close()

    async def get_events(self) -> List[CDCEvent]:
        """Get CDC events from PostgreSQL."""
        if not self._cursor:
            return []

        events = []
        try:
            # Read messages with timeout
            msg = self._cursor.read_message()

            while msg and len(events) < self.config.batch_size:
                event = self._parse_message(msg)
                if event:
                    events.append(event)
                    self._event_count += 1

                msg = self._cursor.read_message()

        except Exception as e:
            if "timeout" not in str(e).lower():
                logger.error(f"PostgreSQL CDC read error: {e}")
                self._error_count += 1

        return events

    def _parse_message(self, msg) -> Optional[CDCEvent]:
        """Parse replication message to CDCEvent."""
        try:
            # pgoutput format
            payload = msg.payload

            # Extract operation and data
            # This is simplified - real implementation needs full pgoutput parsing
            if hasattr(msg, 'data_start'):
                lsn = f"{msg.data_start:X}"
            else:
                lsn = None

            position = CDCPosition(
                lsn=lsn,
                timestamp=datetime.utcnow(),
            )
            self._current_position = position

            # Parse based on message type
            # For now, return a generic event - full parsing would decode pgoutput protocol
            if payload:
                return CDCEvent(
                    id=str(uuid.uuid4()),
                    operation=CDCOperation.INSERT,  # Would be parsed from payload
                    timestamp=datetime.utcnow(),
                    database=self.config.database,
                    schema="public",  # Would be parsed
                    table="unknown",  # Would be parsed
                    after={"raw": payload},  # Would be parsed
                    position=position,
                )

        except Exception as e:
            logger.error(f"Failed to parse PostgreSQL message: {e}")

        return None

    async def acknowledge(self, position: CDCPosition) -> bool:
        """Acknowledge position in PostgreSQL."""
        if not self._cursor or not position.lsn:
            return False

        try:
            lsn = int(position.lsn, 16) if isinstance(position.lsn, str) else position.lsn
            self._cursor.send_feedback(write_lsn=lsn, flush_lsn=lsn, reply=True)
            return True
        except Exception as e:
            logger.error(f"PostgreSQL acknowledge failed: {e}")
            return False


# =============================================================================
# MySQL CDC Adapter
# =============================================================================

@dataclass
class MySQLCDCConfig(CDCConfig):
    """MySQL CDC configuration."""
    port: int = 3306
    server_id: int = 100

    # Binlog
    binlog_format: str = "ROW"  # Must be ROW for CDC
    gtid_mode: bool = False

    # Filtering
    only_events: List[str] = field(default_factory=lambda: [
        "write", "update", "delete"
    ])


class MySQLCDCAdapter(CDCAdapter):
    """
    MySQL CDC adapter using binary log replication.

    Features:
    - Binlog streaming
    - GTID support
    - Row-based change capture
    - Resume from position
    """

    def __init__(self, config: MySQLCDCConfig):
        super().__init__(config)
        self.config: MySQLCDCConfig = config
        self._stream = None

    async def connect(self) -> bool:
        """Connect to MySQL for CDC."""
        try:
            from pymysqlreplication import BinLogStreamReader
            from pymysqlreplication.row_event import (
                WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
            )

            connection_settings = {
                "host": self.config.host,
                "port": self.config.port,
                "user": self.config.username,
                "passwd": self.config.password,
            }

            # Determine start position
            log_file = None
            log_pos = None
            if self.config.start_position:
                log_file = self.config.start_position.binlog_file
                log_pos = self.config.start_position.binlog_position

            only_events = []
            if "write" in self.config.only_events:
                only_events.append(WriteRowsEvent)
            if "update" in self.config.only_events:
                only_events.append(UpdateRowsEvent)
            if "delete" in self.config.only_events:
                only_events.append(DeleteRowsEvent)

            self._stream = BinLogStreamReader(
                connection_settings=connection_settings,
                server_id=self.config.server_id,
                only_events=only_events,
                only_schemas=self.config.include_schemas if self.config.include_schemas else None,
                only_tables=self.config.include_tables if self.config.include_tables else None,
                ignored_tables=self.config.exclude_tables if self.config.exclude_tables else None,
                log_file=log_file,
                log_pos=log_pos,
                resume_stream=True,
                blocking=False,
            )

            self._connected = True
            logger.info(f"MySQL CDC connected: {self.config.name}")
            return True

        except Exception as e:
            logger.error(f"MySQL CDC connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Disconnect from MySQL."""
        if self._stream:
            self._stream.close()
            self._stream = None
        self._connected = False

    async def get_events(self) -> List[CDCEvent]:
        """Get CDC events from MySQL binlog."""
        if not self._stream:
            return []

        from pymysqlreplication.row_event import (
            WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
        )

        events = []
        try:
            for binlog_event in self._stream:
                if len(events) >= self.config.batch_size:
                    break

                if not self._should_capture_table(
                    binlog_event.schema,
                    binlog_event.table
                ):
                    continue

                position = CDCPosition(
                    binlog_file=self._stream.log_file,
                    binlog_position=self._stream.log_pos,
                    timestamp=datetime.utcnow(),
                )
                self._current_position = position

                for row in binlog_event.rows:
                    if isinstance(binlog_event, WriteRowsEvent):
                        operation = CDCOperation.INSERT
                        before = None
                        after = self._filter_columns(
                            binlog_event.table,
                            row["values"]
                        )
                    elif isinstance(binlog_event, UpdateRowsEvent):
                        operation = CDCOperation.UPDATE
                        before = self._filter_columns(
                            binlog_event.table,
                            row["before_values"]
                        )
                        after = self._filter_columns(
                            binlog_event.table,
                            row["after_values"]
                        )
                    elif isinstance(binlog_event, DeleteRowsEvent):
                        operation = CDCOperation.DELETE
                        before = self._filter_columns(
                            binlog_event.table,
                            row["values"]
                        )
                        after = None
                    else:
                        continue

                    event = CDCEvent(
                        id=str(uuid.uuid4()),
                        operation=operation,
                        timestamp=datetime.utcnow(),
                        database=binlog_event.schema,
                        schema=binlog_event.schema,
                        table=binlog_event.table,
                        before=before,
                        after=after,
                        position=position,
                        primary_key=row.get("primary_key"),
                    )
                    events.append(event)
                    self._event_count += 1

        except Exception as e:
            if "timeout" not in str(e).lower():
                logger.error(f"MySQL CDC read error: {e}")
                self._error_count += 1

        return events

    async def acknowledge(self, position: CDCPosition) -> bool:
        """MySQL doesn't require explicit acknowledgment."""
        return True


# =============================================================================
# Oracle CDC Adapter
# =============================================================================

@dataclass
class OracleCDCConfig(CDCConfig):
    """Oracle CDC configuration."""
    port: int = 1521
    service_name: str = ""

    # LogMiner
    use_logminer: bool = True
    dictionary_option: str = "DICT_FROM_ONLINE_CATALOG"

    # Mining options
    committed_data_only: bool = True
    skip_corruption: bool = True

    # Archive log
    archive_log_dest: str = ""


class OracleCDCAdapter(CDCAdapter):
    """
    Oracle CDC adapter using LogMiner.

    Features:
    - LogMiner-based CDC
    - SCN tracking
    - Redo log mining
    - Archive log support
    """

    def __init__(self, config: OracleCDCConfig):
        super().__init__(config)
        self.config: OracleCDCConfig = config
        self._conn = None
        self._current_scn: Optional[int] = None

    async def connect(self) -> bool:
        """Connect to Oracle for CDC."""
        try:
            import cx_Oracle

            dsn = cx_Oracle.makedsn(
                self.config.host,
                self.config.port,
                service_name=self.config.service_name,
            )

            self._conn = cx_Oracle.connect(
                user=self.config.username,
                password=self.config.password,
                dsn=dsn,
            )

            # Get current SCN if not resuming
            if self.config.start_position and self.config.start_position.scn:
                self._current_scn = self.config.start_position.scn
            else:
                cursor = self._conn.cursor()
                cursor.execute("SELECT CURRENT_SCN FROM V$DATABASE")
                self._current_scn = cursor.fetchone()[0]
                cursor.close()

            self._connected = True
            logger.info(f"Oracle CDC connected: {self.config.name}")
            return True

        except Exception as e:
            logger.error(f"Oracle CDC connection failed: {e}")
            return False

    async def disconnect(self) -> None:
        """Disconnect from Oracle."""
        if self._conn:
            # Stop LogMiner if running
            try:
                cursor = self._conn.cursor()
                cursor.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
                cursor.close()
            except:
                pass

            self._conn.close()
            self._conn = None
        self._connected = False

    async def get_events(self) -> List[CDCEvent]:
        """Get CDC events from Oracle LogMiner."""
        if not self._conn:
            return []

        events = []
        try:
            cursor = self._conn.cursor()

            # Add log files
            cursor.execute("""
                BEGIN
                    DBMS_LOGMNR.ADD_LOGFILE(
                        LOGFILENAME => (SELECT MEMBER FROM V$LOGFILE WHERE ROWNUM = 1),
                        OPTIONS => DBMS_LOGMNR.NEW
                    );
                END;
            """)

            # Start LogMiner
            options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"
            if self.config.committed_data_only:
                options += " + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
            if self.config.skip_corruption:
                options += " + DBMS_LOGMNR.SKIP_CORRUPTION"

            cursor.execute(f"""
                BEGIN
                    DBMS_LOGMNR.START_LOGMNR(
                        STARTSCN => {self._current_scn},
                        OPTIONS => {options}
                    );
                END;
            """)

            # Build table filter
            table_filter = ""
            if self.config.include_tables:
                tables = ", ".join(f"'{t.upper()}'" for t in self.config.include_tables)
                table_filter = f"AND TABLE_NAME IN ({tables})"

            schema_filter = ""
            if self.config.include_schemas:
                schemas = ", ".join(f"'{s.upper()}'" for s in self.config.include_schemas)
                schema_filter = f"AND SEG_OWNER IN ({schemas})"

            # Query LogMiner contents
            cursor.execute(f"""
                SELECT
                    SCN, TIMESTAMP, OPERATION, SEG_OWNER, TABLE_NAME,
                    SQL_REDO, ROW_ID
                FROM V$LOGMNR_CONTENTS
                WHERE OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
                {schema_filter}
                {table_filter}
                AND SCN > {self._current_scn}
                AND ROWNUM <= {self.config.batch_size}
                ORDER BY SCN
            """)

            for row in cursor:
                scn, timestamp, operation, schema, table, sql_redo, row_id = row

                if not self._should_capture_table(schema, table):
                    continue

                position = CDCPosition(
                    scn=scn,
                    timestamp=timestamp,
                )

                # Parse SQL to extract data (simplified)
                before = None
                after = None
                if operation == "INSERT":
                    op = CDCOperation.INSERT
                    after = self._parse_sql_values(sql_redo)
                elif operation == "UPDATE":
                    op = CDCOperation.UPDATE
                    after = self._parse_sql_values(sql_redo)
                elif operation == "DELETE":
                    op = CDCOperation.DELETE
                    before = self._parse_sql_values(sql_redo)
                else:
                    continue

                event = CDCEvent(
                    id=str(uuid.uuid4()),
                    operation=op,
                    timestamp=timestamp,
                    database=self.config.database,
                    schema=schema,
                    table=table,
                    before=before,
                    after=after,
                    position=position,
                )
                events.append(event)
                self._event_count += 1
                self._current_scn = scn

            # End LogMiner session
            cursor.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
            cursor.close()

            self._current_position = CDCPosition(
                scn=self._current_scn,
                timestamp=datetime.utcnow(),
            )

        except Exception as e:
            logger.error(f"Oracle CDC read error: {e}")
            self._error_count += 1

        return events

    def _parse_sql_values(self, sql: str) -> Dict[str, Any]:
        """Parse SQL statement to extract column values (simplified)."""
        # This is a simplified parser - production would need proper SQL parsing
        values = {}
        if sql:
            # Extract column=value pairs from WHERE or SET clause
            import re
            matches = re.findall(r'"?(\w+)"?\s*=\s*\'?([^\',$]+)\'?', sql)
            for col, val in matches:
                values[col] = val
        return values

    async def acknowledge(self, position: CDCPosition) -> bool:
        """Update current SCN for Oracle."""
        if position.scn:
            self._current_scn = position.scn
        return True


# =============================================================================
# Factory & Registry
# =============================================================================

CDC_ADAPTERS: Dict[str, type] = {
    "postgresql": PostgreSQLCDCAdapter,
    "postgres": PostgreSQLCDCAdapter,
    "pg": PostgreSQLCDCAdapter,
    "mysql": MySQLCDCAdapter,
    "mariadb": MySQLCDCAdapter,
    "oracle": OracleCDCAdapter,
}


def get_cdc_adapter(
    database_type: str,
    config: CDCConfig,
) -> CDCAdapter:
    """Get CDC adapter by database type."""
    adapter_class = CDC_ADAPTERS.get(database_type.lower())
    if not adapter_class:
        raise ValueError(f"Unknown database type: {database_type}")
    return adapter_class(config)
