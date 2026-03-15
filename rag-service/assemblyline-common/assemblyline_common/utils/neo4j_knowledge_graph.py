"""
T.A.L.O.S. Neo4j Knowledge Graph
Based on Memobytes patterns

Features:
- Temporal knowledge graph for entity relationships
- Entity deduplication with rapidfuzz
- Document connections
- User learning patterns
"""

import os
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# =============================================================================
# NEO4J AVAILABILITY CHECK
# =============================================================================

NEO4J_AVAILABLE = False

try:
    from neo4j import AsyncGraphDatabase, GraphDatabase
    NEO4J_AVAILABLE = True
    logger.info("Neo4j library loaded successfully")
except ImportError:
    logger.warning("Neo4j not installed. Run: pip install neo4j")

RAPIDFUZZ_AVAILABLE = False

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    logger.warning("rapidfuzz not installed. Run: pip install rapidfuzz")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Neo4jConfig:
    """Neo4j configuration"""
    uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user: str = os.getenv("NEO4J_USER", "neo4j")
    password: str = os.getenv("NEO4J_PASSWORD", "password")
    database: str = os.getenv("NEO4J_DATABASE", "neo4j")


# =============================================================================
# KNOWLEDGE GRAPH SERVICE
# =============================================================================

class KnowledgeGraphService:
    """
    Neo4j-based knowledge graph for entity relationships.

    Features:
    - Entity extraction and storage
    - Relationship mapping
    - Temporal tracking
    - Fuzzy entity matching
    """

    def __init__(self, config: Optional[Neo4jConfig] = None):
        self.config = config or Neo4jConfig()
        self._driver = None
        self._initialized = False

    async def initialize(self):
        """Initialize Neo4j connection"""
        if not NEO4J_AVAILABLE:
            logger.warning("Neo4j not available")
            return False

        try:
            self._driver = AsyncGraphDatabase.driver(
                self.config.uri,
                auth=(self.config.user, self.config.password)
            )
            # Verify connection
            async with self._driver.session(database=self.config.database) as session:
                await session.run("RETURN 1")

            # Create indexes
            await self._create_indexes()

            self._initialized = True
            logger.info("Neo4j Knowledge Graph initialized")
            return True

        except Exception as e:
            logger.error(f"Neo4j initialization failed: {e}")
            return False

    async def close(self):
        """Close Neo4j connection"""
        if self._driver:
            await self._driver.close()
            self._initialized = False

    async def _create_indexes(self):
        """Create necessary indexes"""
        async with self._driver.session(database=self.config.database) as session:
            # Entity indexes
            await session.run(
                "CREATE INDEX IF NOT EXISTS FOR (e:Entity) ON (e.name)"
            )
            await session.run(
                "CREATE INDEX IF NOT EXISTS FOR (e:Entity) ON (e.type)"
            )
            # Document indexes
            await session.run(
                "CREATE INDEX IF NOT EXISTS FOR (d:Document) ON (d.id)"
            )
            # User indexes
            await session.run(
                "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.id)"
            )

    # =========================================================================
    # ENTITY OPERATIONS
    # =========================================================================

    async def add_entity(
        self,
        name: str,
        entity_type: str,
        properties: Dict[str, Any] = None,
        source_document_id: int = None
    ) -> Optional[str]:
        """
        Add or update an entity in the knowledge graph.

        Args:
            name: Entity name
            entity_type: Type (Person, Concept, Organization, etc.)
            properties: Additional properties
            source_document_id: Source document ID

        Returns:
            Entity ID or None
        """
        if not self._initialized:
            await self.initialize()

        if not self._driver:
            return None

        properties = properties or {}

        try:
            async with self._driver.session(database=self.config.database) as session:
                # Check for similar existing entity
                existing = await self._find_similar_entity(session, name, entity_type)

                if existing:
                    # Update existing entity
                    result = await session.run("""
                        MATCH (e:Entity {id: $entity_id})
                        SET e.last_seen = datetime(),
                            e.mention_count = e.mention_count + 1
                        RETURN e.id as id
                    """, entity_id=existing["id"])
                    record = await result.single()
                    entity_id = record["id"]
                else:
                    # Create new entity
                    entity_id = f"{entity_type.lower()}_{name.lower().replace(' ', '_')}"
                    result = await session.run("""
                        CREATE (e:Entity {
                            id: $id,
                            name: $name,
                            type: $type,
                            properties: $properties,
                            created_at: datetime(),
                            last_seen: datetime(),
                            mention_count: 1
                        })
                        RETURN e.id as id
                    """,
                        id=entity_id,
                        name=name,
                        type=entity_type,
                        properties=properties
                    )
                    record = await result.single()
                    entity_id = record["id"]

                # Link to document if provided
                if source_document_id:
                    await session.run("""
                        MATCH (e:Entity {id: $entity_id})
                        MERGE (d:Document {id: $doc_id})
                        MERGE (e)-[:MENTIONED_IN {timestamp: datetime()}]->(d)
                    """, entity_id=entity_id, doc_id=source_document_id)

                return entity_id

        except Exception as e:
            logger.error(f"Failed to add entity: {e}")
            return None

    async def _find_similar_entity(
        self,
        session,
        name: str,
        entity_type: str
    ) -> Optional[Dict]:
        """Find similar existing entity using fuzzy matching"""
        result = await session.run("""
            MATCH (e:Entity {type: $type})
            RETURN e.id as id, e.name as name
        """, type=entity_type)

        records = await result.data()

        if not records:
            return None

        if RAPIDFUZZ_AVAILABLE:
            names = [r["name"] for r in records]
            match = process.extractOne(
                name,
                names,
                scorer=fuzz.ratio,
                score_cutoff=85
            )
            if match:
                idx = names.index(match[0])
                return records[idx]

        # Fallback: exact match
        for record in records:
            if record["name"].lower() == name.lower():
                return record

        return None

    async def add_relationship(
        self,
        from_entity_id: str,
        to_entity_id: str,
        relationship_type: str,
        properties: Dict[str, Any] = None
    ) -> bool:
        """
        Add a relationship between two entities.

        Args:
            from_entity_id: Source entity ID
            to_entity_id: Target entity ID
            relationship_type: Type of relationship
            properties: Additional properties

        Returns:
            True if successful
        """
        if not self._driver:
            return False

        properties = properties or {}

        try:
            async with self._driver.session(database=self.config.database) as session:
                await session.run(f"""
                    MATCH (a:Entity {{id: $from_id}})
                    MATCH (b:Entity {{id: $to_id}})
                    MERGE (a)-[r:{relationship_type}]->(b)
                    SET r.created_at = datetime(),
                        r.properties = $properties
                """,
                    from_id=from_entity_id,
                    to_id=to_entity_id,
                    properties=properties
                )
                return True
        except Exception as e:
            logger.error(f"Failed to add relationship: {e}")
            return False

    async def get_entity(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get entity by ID"""
        if not self._driver:
            return None

        try:
            async with self._driver.session(database=self.config.database) as session:
                result = await session.run("""
                    MATCH (e:Entity {id: $id})
                    RETURN e
                """, id=entity_id)
                record = await result.single()
                if record:
                    return dict(record["e"])
                return None
        except Exception as e:
            logger.error(f"Failed to get entity: {e}")
            return None

    async def get_entity_relationships(
        self,
        entity_id: str,
        relationship_type: str = None,
        direction: str = "both"
    ) -> List[Dict[str, Any]]:
        """
        Get relationships for an entity.

        Args:
            entity_id: Entity ID
            relationship_type: Optional filter by type
            direction: "in", "out", or "both"
        """
        if not self._driver:
            return []

        try:
            async with self._driver.session(database=self.config.database) as session:
                if direction == "out":
                    pattern = "(e)-[r]->(other)"
                elif direction == "in":
                    pattern = "(e)<-[r]-(other)"
                else:
                    pattern = "(e)-[r]-(other)"

                query = f"""
                    MATCH (e:Entity {{id: $id}})
                    MATCH {pattern}
                    WHERE other:Entity
                """

                if relationship_type:
                    query += f" AND type(r) = '{relationship_type}'"

                query += """
                    RETURN type(r) as relationship,
                           other.id as entity_id,
                           other.name as entity_name,
                           other.type as entity_type,
                           r.properties as properties
                """

                result = await session.run(query, id=entity_id)
                return await result.data()

        except Exception as e:
            logger.error(f"Failed to get relationships: {e}")
            return []

    # =========================================================================
    # DOCUMENT OPERATIONS
    # =========================================================================

    async def add_document(
        self,
        document_id: int,
        title: str,
        properties: Dict[str, Any] = None
    ) -> bool:
        """Add a document node"""
        if not self._driver:
            return False

        properties = properties or {}

        try:
            async with self._driver.session(database=self.config.database) as session:
                await session.run("""
                    MERGE (d:Document {id: $id})
                    SET d.title = $title,
                        d.properties = $properties,
                        d.updated_at = datetime()
                    ON CREATE SET d.created_at = datetime()
                """,
                    id=document_id,
                    title=title,
                    properties=properties
                )
                return True
        except Exception as e:
            logger.error(f"Failed to add document: {e}")
            return False

    async def get_document_entities(
        self,
        document_id: int
    ) -> List[Dict[str, Any]]:
        """Get all entities mentioned in a document"""
        if not self._driver:
            return []

        try:
            async with self._driver.session(database=self.config.database) as session:
                result = await session.run("""
                    MATCH (e:Entity)-[:MENTIONED_IN]->(d:Document {id: $id})
                    RETURN e.id as id, e.name as name, e.type as type
                """, id=document_id)
                return await result.data()
        except Exception as e:
            logger.error(f"Failed to get document entities: {e}")
            return []

    # =========================================================================
    # SEARCH OPERATIONS
    # =========================================================================

    async def search_entities(
        self,
        query: str,
        entity_type: str = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Search for entities by name"""
        if not self._driver:
            return []

        try:
            async with self._driver.session(database=self.config.database) as session:
                cypher = """
                    MATCH (e:Entity)
                    WHERE toLower(e.name) CONTAINS toLower($query)
                """
                if entity_type:
                    cypher += " AND e.type = $type"
                cypher += """
                    RETURN e.id as id, e.name as name, e.type as type,
                           e.mention_count as mentions
                    ORDER BY e.mention_count DESC
                    LIMIT $limit
                """

                result = await session.run(
                    cypher,
                    query=query,
                    type=entity_type,
                    limit=limit
                )
                return await result.data()
        except Exception as e:
            logger.error(f"Entity search failed: {e}")
            return []

    async def find_path(
        self,
        from_entity_id: str,
        to_entity_id: str,
        max_depth: int = 5
    ) -> List[Dict[str, Any]]:
        """Find shortest path between two entities"""
        if not self._driver:
            return []

        try:
            async with self._driver.session(database=self.config.database) as session:
                result = await session.run("""
                    MATCH path = shortestPath(
                        (a:Entity {id: $from_id})-[*..%d]-(b:Entity {id: $to_id})
                    )
                    RETURN [n IN nodes(path) | {id: n.id, name: n.name, type: n.type}] as nodes,
                           [r IN relationships(path) | type(r)] as relationships
                """ % max_depth,
                    from_id=from_entity_id,
                    to_id=to_entity_id
                )
                record = await result.single()
                if record:
                    return {
                        "nodes": record["nodes"],
                        "relationships": record["relationships"]
                    }
                return []
        except Exception as e:
            logger.error(f"Path finding failed: {e}")
            return []

    async def get_stats(self) -> Dict[str, Any]:
        """Get knowledge graph statistics"""
        if not self._driver:
            return {"status": "not_connected"}

        try:
            async with self._driver.session(database=self.config.database) as session:
                result = await session.run("""
                    MATCH (e:Entity)
                    WITH count(e) as entity_count
                    MATCH ()-[r]->()
                    RETURN entity_count, count(r) as relationship_count
                """)
                record = await result.single()

                types_result = await session.run("""
                    MATCH (e:Entity)
                    RETURN e.type as type, count(*) as count
                    ORDER BY count DESC
                """)
                types = await types_result.data()

                return {
                    "status": "connected",
                    "entity_count": record["entity_count"],
                    "relationship_count": record["relationship_count"],
                    "entity_types": types
                }
        except Exception as e:
            return {"status": "error", "error": str(e)}


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_knowledge_graph: Optional[KnowledgeGraphService] = None


async def get_knowledge_graph() -> KnowledgeGraphService:
    """Get or create knowledge graph singleton"""
    global _knowledge_graph
    if _knowledge_graph is None:
        _knowledge_graph = KnowledgeGraphService()
        await _knowledge_graph.initialize()
    return _knowledge_graph


def is_neo4j_available() -> bool:
    """Check if Neo4j is available"""
    return NEO4J_AVAILABLE
