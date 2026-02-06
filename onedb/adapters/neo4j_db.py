"""
Neo4j Graph Database Adapter.
"""

from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class Neo4j(BaseAdapter):
    """
    Neo4j graph database adapter.
    
    Usage:
        db = Neo4j(
            host="localhost",
            port=7687,
            user="neo4j",
            password="password",
            database="neo4j"
        )
        db.connect()
        
        # Create nodes
        db.execute('''
            CREATE (p:Person {name: $name, age: $age})
            RETURN p
        ''', {"name": "John", "age": 30})
        
        # Create relationships
        db.execute('''
            MATCH (a:Person {name: $name1})
            MATCH (b:Person {name: $name2})
            CREATE (a)-[:KNOWS]->(b)
        ''', {"name1": "John", "name2": "Jane"})
        
        # Query
        result = db.execute('''
            MATCH (p:Person)-[:KNOWS]->(friend)
            WHERE p.name = $name
            RETURN friend.name as friend_name
        ''', {"name": "John"})
    
    Install:
        pip install onedb[neo4j]
    """
    
    db_type = DatabaseType.NEO4J
    driver_name = "neo4j"
    install_command = "pip install onedb[neo4j]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        if self.config.port is None:
            self.config.port = 7687
        if self.config.database is None:
            self.config.database = "neo4j"
        self._driver = None
        self._session = None
    
    def _import_driver(self):
        try:
            from neo4j import GraphDatabase
            return GraphDatabase
        except ImportError:
            raise DriverNotInstalledError("neo4j", self.install_command)
    
    def connect(self) -> None:
        GraphDatabase = self._import_driver()
        
        try:
            uri = f"bolt://{self.config.host}:{self.config.port}"
            
            auth = None
            if self.config.user and self.config.password:
                auth = (self.config.user, self.config.password)
            
            self._driver = GraphDatabase.driver(
                uri, 
                auth=auth,
                connection_timeout=self.config.timeout
            )
            
            # Test connection
            with self._driver.session(database=self.config.database) as session:
                session.run("RETURN 1")
            
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Neo4j: {e}",
                host=self.config.host,
                port=self.config.port,
                database=self.config.database
            )
    
    def disconnect(self) -> None:
        if self._session:
            self._session.close()
            self._session = None
        if self._driver:
            self._driver.close()
            self._driver = None
        self._is_connected = False
    
    def is_connected(self) -> bool:
        try:
            if self._driver:
                with self._driver.session(database=self.config.database) as session:
                    session.run("RETURN 1")
                return True
        except Exception:
            pass
        return False
    
    def execute(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        """
        Execute Cypher query.
        
        Args:
            query: Cypher query string
            params: Query parameters (dict recommended)
            
        Returns:
            QueryResult with data
        """
        start_time = time.time()
        
        try:
            with self._driver.session(database=self.config.database) as session:
                # Convert tuple params to dict if needed
                if params and isinstance(params, tuple):
                    params = {f"p{i}": v for i, v in enumerate(params)}
                
                result = session.run(query, params or {})
                
                # Collect all records
                records = []
                for record in result:
                    # Convert Neo4j record to dict
                    record_dict = {}
                    for key in record.keys():
                        value = record[key]
                        # Handle Neo4j Node objects
                        if hasattr(value, 'items'):
                            record_dict[key] = dict(value.items())
                        elif hasattr(value, '_properties'):
                            record_dict[key] = dict(value._properties)
                        else:
                            record_dict[key] = value
                    records.append(record_dict)
                
                # Get summary
                summary = result.consume()
                
                query_result = QueryResult()
                query_result.data = records
                
                if records:
                    query_result.columns = list(records[0].keys())
                
                # Calculate affected rows from counters
                counters = summary.counters
                query_result.affected_rows = (
                    counters.nodes_created +
                    counters.nodes_deleted +
                    counters.relationships_created +
                    counters.relationships_deleted +
                    counters.properties_set
                ) or len(records)
                
                query_result.execution_time = (time.time() - start_time) * 1000
                return query_result
                
        except Exception as e:
            raise QueryError(str(e), query=query, params=params)
    
    def execute_many(
        self,
        query: str,
        params_list: List[Union[tuple, dict]]
    ) -> QueryResult:
        """Execute query with multiple parameter sets."""
        start_time = time.time()
        total_affected = 0
        
        with self._driver.session(database=self.config.database) as session:
            for params in params_list:
                if isinstance(params, tuple):
                    params = {f"p{i}": v for i, v in enumerate(params)}
                
                result = session.run(query, params)
                summary = result.consume()
                
                counters = summary.counters
                total_affected += (
                    counters.nodes_created +
                    counters.nodes_deleted +
                    counters.relationships_created +
                    counters.relationships_deleted +
                    counters.properties_set
                ) or 1
        
        query_result = QueryResult(affected_rows=total_affected)
        query_result.execution_time = (time.time() - start_time) * 1000
        return query_result
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        """
        Create a node with given label and properties.
        
        Args:
            table: Node label (e.g., "Person", "Product")
            data: Node properties
            
        Returns:
            QueryResult with created node
        """
        # Build property string
        props = ", ".join([f"{k}: ${k}" for k in data.keys()])
        
        query = f"CREATE (n:{table} {{{props}}}) RETURN n, id(n) as node_id"
        
        result = self.execute(query, data)
        
        if result.data:
            result.last_id = result.data[0].get("node_id")
        
        return result
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        """Create multiple nodes."""
        start_time = time.time()
        total_created = 0
        
        with self._driver.session(database=self.config.database) as session:
            for item in data:
                props = ", ".join([f"{k}: ${k}" for k in item.keys()])
                query = f"CREATE (n:{table} {{{props}}})"
                result = session.run(query, item)
                summary = result.consume()
                total_created += summary.counters.nodes_created
        
        query_result = QueryResult(affected_rows=total_created)
        query_result.execution_time = (time.time() - start_time) * 1000
        return query_result
    
    def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """
        Update node properties.
        
        Args:
            table: Node label
            data: Properties to update
            where: Match conditions
        """
        # Build MATCH clause
        if where:
            match_props = ", ".join([f"{k}: $where_{k}" for k in where.keys()])
            match_clause = f"MATCH (n:{table} {{{match_props}}})"
        else:
            match_clause = f"MATCH (n:{table})"
        
        # Build SET clause
        set_parts = [f"n.{k} = $set_{k}" for k in data.keys()]
        set_clause = "SET " + ", ".join(set_parts)
        
        query = f"{match_clause} {set_clause} RETURN n"
        
        # Prepare params with prefixes
        params = {}
        if where:
            for k, v in where.items():
                params[f"where_{k}"] = v
        for k, v in data.items():
            params[f"set_{k}"] = v
        
        return self.execute(query, params)
    
    def delete(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """
        Delete nodes.
        
        Args:
            table: Node label
            where: Match conditions (if None, deletes ALL nodes with label!)
        """
        if where:
            match_props = ", ".join([f"{k}: ${k}" for k in where.keys()])
            query = f"MATCH (n:{table} {{{match_props}}}) DETACH DELETE n"
            params = where
        else:
            query = f"MATCH (n:{table}) DETACH DELETE n"
            params = None
        
        return self.execute(query, params)
    
    def find(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> QueryResult:
        """
        Find nodes by label and properties.
        
        Args:
            table: Node label
            where: Property filters
            columns: Properties to return (None = all)
            order_by: Property to order by (e.g., "name DESC")
            limit: Maximum results
            offset: Skip results
        """
        # Build MATCH clause
        if where:
            match_props = ", ".join([f"{k}: ${k}" for k in where.keys()])
            query = f"MATCH (n:{table} {{{match_props}}})"
            params = where
        else:
            query = f"MATCH (n:{table})"
            params = {}
        
        # Build RETURN clause
        if columns:
            return_parts = [f"n.{col} as {col}" for col in columns]
            query += f" RETURN {', '.join(return_parts)}"
        else:
            query += " RETURN n"
        
        # ORDER BY
        if order_by:
            parts = order_by.split()
            prop = parts[0]
            direction = parts[1].upper() if len(parts) > 1 else "ASC"
            query += f" ORDER BY n.{prop} {direction}"
        
        # SKIP and LIMIT
        if offset:
            query += f" SKIP {offset}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute(query, params if params else None)
    
    def find_one(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """Find single node."""
        result = self.find(table, where, columns, limit=1)
        return result.first
    
    # ==================== Graph-Specific Methods ====================
    
    def create_relationship(
        self,
        from_label: str,
        from_where: Dict[str, Any],
        to_label: str,
        to_where: Dict[str, Any],
        rel_type: str,
        rel_properties: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """
        Create relationship between nodes.
        
        Args:
            from_label: Source node label
            from_where: Source node match properties
            to_label: Target node label
            to_where: Target node match properties
            rel_type: Relationship type (e.g., "KNOWS", "PURCHASED")
            rel_properties: Relationship properties
            
        Example:
            db.create_relationship(
                "Person", {"name": "John"},
                "Person", {"name": "Jane"},
                "KNOWS",
                {"since": 2020}
            )
        """
        # Build MATCH clauses
        from_props = ", ".join([f"{k}: $from_{k}" for k in from_where.keys()])
        to_props = ", ".join([f"{k}: $to_{k}" for k in to_where.keys()])
        
        # Build relationship
        if rel_properties:
            rel_props = ", ".join([f"{k}: $rel_{k}" for k in rel_properties.keys()])
            rel_clause = f"[r:{rel_type} {{{rel_props}}}]"
        else:
            rel_clause = f"[r:{rel_type}]"
        
        query = f"""
            MATCH (a:{from_label} {{{from_props}}})
            MATCH (b:{to_label} {{{to_props}}})
            CREATE (a)-{rel_clause}->(b)
            RETURN a, r, b
        """
        
        # Prepare params
        params = {}
        for k, v in from_where.items():
            params[f"from_{k}"] = v
        for k, v in to_where.items():
            params[f"to_{k}"] = v
        if rel_properties:
            for k, v in rel_properties.items():
                params[f"rel_{k}"] = v
        
        return self.execute(query, params)
    
    def find_relationships(
        self,
        from_label: Optional[str] = None,
        to_label: Optional[str] = None,
        rel_type: Optional[str] = None,
        where: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> QueryResult:
        """
        Find relationships.
        
        Args:
            from_label: Source node label (optional)
            to_label: Target node label (optional)
            rel_type: Relationship type (optional)
            where: Source node properties to match
            limit: Maximum results
        """
        # Build pattern
        from_pattern = f"(a:{from_label})" if from_label else "(a)"
        to_pattern = f"(b:{to_label})" if to_label else "(b)"
        rel_pattern = f"[r:{rel_type}]" if rel_type else "[r]"
        
        query = f"MATCH {from_pattern}-{rel_pattern}->{to_pattern}"
        
        params = {}
        if where:
            where_parts = [f"a.{k} = ${k}" for k in where.keys()]
            query += f" WHERE {' AND '.join(where_parts)}"
            params = where
        
        query += " RETURN a, r, b, type(r) as rel_type"
        
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute(query, params if params else None)
    
    def shortest_path(
        self,
        from_label: str,
        from_where: Dict[str, Any],
        to_label: str,
        to_where: Dict[str, Any],
        max_depth: int = 10
    ) -> QueryResult:
        """
        Find shortest path between two nodes.
        
        Args:
            from_label: Source node label
            from_where: Source node properties
            to_label: Target node label
            to_where: Target node properties
            max_depth: Maximum path length
        """
        from_props = ", ".join([f"{k}: $from_{k}" for k in from_where.keys()])
        to_props = ", ".join([f"{k}: $to_{k}" for k in to_where.keys()])
        
        query = f"""
            MATCH (a:{from_label} {{{from_props}}}), (b:{to_label} {{{to_props}}})
            MATCH path = shortestPath((a)-[*..{max_depth}]-(b))
            RETURN path, length(path) as path_length
        """
        
        params = {}
        for k, v in from_where.items():
            params[f"from_{k}"] = v
        for k, v in to_where.items():
            params[f"to_{k}"] = v
        
        return self.execute(query, params)
    
    def get_neighbors(
        self,
        label: str,
        where: Dict[str, Any],
        rel_type: Optional[str] = None,
        direction: str = "both",
        depth: int = 1
    ) -> QueryResult:
        """
        Get neighboring nodes.
        
        Args:
            label: Node label
            where: Node properties
            rel_type: Filter by relationship type
            direction: "in", "out", or "both"
            depth: How many hops
        """
        props = ", ".join([f"{k}: ${k}" for k in where.keys()])
        
        rel_pattern = f":{rel_type}" if rel_type else ""
        
        if direction == "out":
            pattern = f"-[r{rel_pattern}*1..{depth}]->"
        elif direction == "in":
            pattern = f"<-[r{rel_pattern}*1..{depth}]-"
        else:
            pattern = f"-[r{rel_pattern}*1..{depth}]-"
        
        query = f"""
            MATCH (n:{label} {{{props}}}){pattern}(neighbor)
            RETURN DISTINCT neighbor, labels(neighbor) as labels
        """
        
        return self.execute(query, where)
    
    # ==================== Transaction Methods ====================
    
    def begin_transaction(self) -> None:
        """Start a transaction session."""
        self._session = self._driver.session(database=self.config.database)
        self._transaction = self._session.begin_transaction()
        self._in_transaction = True
    
    def commit(self) -> None:
        """Commit transaction."""
        if self._transaction:
            self._transaction.commit()
            self._transaction = None
        if self._session:
            self._session.close()
            self._session = None
        self._in_transaction = False
    
    def rollback(self) -> None:
        """Rollback transaction."""
        if self._transaction:
            self._transaction.rollback()
            self._transaction = None
        if self._session:
            self._session.close()
            self._session = None
        self._in_transaction = False
    
    # ==================== Schema Methods ====================
    
    def get_tables(self) -> List[str]:
        """Get all node labels."""
        result = self.execute("CALL db.labels()")
        return [row.get("label", row.get("labels", "")) for row in result.data]
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get properties used by nodes with given label."""
        result = self.execute(f"""
            MATCH (n:{table})
            UNWIND keys(n) as key
            RETURN DISTINCT key as column_name, 'unknown' as data_type
            LIMIT 100
        """)
        return result.data
    
    def table_exists(self, table: str) -> bool:
        """Check if label exists (has any nodes)."""
        result = self.execute(f"MATCH (n:{table}) RETURN count(n) as count LIMIT 1")
        return result.first.get("count", 0) > 0 if result.first else False
    
    def get_relationship_types(self) -> List[str]:
        """Get all relationship types."""
        result = self.execute("CALL db.relationshipTypes()")
        return [row.get("relationshipType", "") for row in result.data]
    
    def get_schema(self) -> Dict[str, Any]:
        """Get database schema overview."""
        labels = self.get_tables()
        rel_types = self.get_relationship_types()
        
        # Get node counts
        node_counts = {}
        for label in labels:
            result = self.execute(f"MATCH (n:{label}) RETURN count(n) as count")
            node_counts[label] = result.first.get("count", 0) if result.first else 0
        
        # Get relationship counts
        rel_counts = {}
        for rel_type in rel_types:
            result = self.execute(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
            rel_counts[rel_type] = result.first.get("count", 0) if result.first else 0
        
        return {
            "labels": labels,
            "relationship_types": rel_types,
            "node_counts": node_counts,
            "relationship_counts": rel_counts
        }
    
    # ==================== Utility Methods ====================
    
    def clear_database(self) -> QueryResult:
        """Delete all nodes and relationships. USE WITH CAUTION!"""
        return self.execute("MATCH (n) DETACH DELETE n")
    
    def create_index(self, label: str, property_name: str) -> QueryResult:
        """Create index on property."""
        return self.execute(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.{property_name})")
    
    def create_constraint_unique(self, label: str, property_name: str) -> QueryResult:
        """Create uniqueness constraint."""
        return self.execute(
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{property_name} IS UNIQUE"
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        # Node count
        node_result = self.execute("MATCH (n) RETURN count(n) as count")
        node_count = node_result.first.get("count", 0) if node_result.first else 0
        
        # Relationship count
        rel_result = self.execute("MATCH ()-[r]->() RETURN count(r) as count")
        rel_count = rel_result.first.get("count", 0) if rel_result.first else 0
        
        return {
            "total_nodes": node_count,
            "total_relationships": rel_count,
            "labels": self.get_tables(),
            "relationship_types": self.get_relationship_types()
        }
