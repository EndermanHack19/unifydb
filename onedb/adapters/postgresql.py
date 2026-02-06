"""
PostgreSQL Adapter.
Supports both sync (psycopg2) and async (asyncpg) connections.
"""

from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import (
    BaseAdapter, AsyncBaseAdapter, ConnectionConfig, 
    QueryResult, DatabaseType
)
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class PostgreSQL(BaseAdapter):
    """
    PostgreSQL database adapter.
    
    Usage:
        # Basic connection
        db = PostgreSQL(
            host="localhost",
            port=5432,
            database="mydb",
            user="postgres",
            password="secret"
        )
        db.connect()
        
        # Using context manager
        with PostgreSQL(host="localhost", database="mydb") as db:
            users = db.find("users")
        
        # With connection config
        config = ConnectionConfig(
            host="localhost",
            database="mydb",
            ssl=True
        )
        db = PostgreSQL(config=config)
    
    Install:
        pip install onedb[postgresql]
    """
    
    db_type = DatabaseType.POSTGRESQL
    driver_name = "psycopg2"
    install_command = "pip install onedb[postgresql]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        self._cursor = None
        
        # Set default port
        if self.config.port is None:
            self.config.port = 5432
    
    def _import_driver(self):
        """Import psycopg2 driver."""
        try:
            import psycopg2
            import psycopg2.extras
            return psycopg2
        except ImportError:
            raise DriverNotInstalledError(
                "psycopg2",
                self.install_command
            )
    
    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
        psycopg2 = self._import_driver()
        
        try:
            conn_params = {
                "host": self.config.host,
                "port": self.config.port,
                "database": self.config.database,
                "user": self.config.user,
                "password": self.config.password,
                "connect_timeout": self.config.timeout,
            }
            
            if self.config.ssl:
                conn_params["sslmode"] = "require"
            
            # Add extra parameters
            conn_params.update(self.config.extra)
            
            # Remove None values
            conn_params = {k: v for k, v in conn_params.items() if v is not None}
            
            self._connection = psycopg2.connect(**conn_params)
            self._connection.autocommit = True
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to PostgreSQL: {e}",
                host=self.config.host,
                port=self.config.port,
                database=self.config.database
            )
    
    def disconnect(self) -> None:
        """Close connection."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None
        self._is_connected = False
    
    def is_connected(self) -> bool:
        """Check connection status."""
        if not self._connection:
            return False
        try:
            # Check if connection is alive
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            self._is_connected = False
            return False
    
    def execute(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        """Execute query and return results."""
        import psycopg2.extras
        
        if not self._is_connected:
            raise ConnectionError("Not connected to database")
        
        start_time = time.time()
        
        try:
            # Use RealDictCursor for dict results
            cursor = self._connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            
            cursor.execute(query, params)
            
            # Determine result type
            result = QueryResult()
            result.affected_rows = cursor.rowcount
            
            if cursor.description:
                # SELECT query
                result.columns = [desc[0] for desc in cursor.description]
                result.data = [dict(row) for row in cursor.fetchall()]
            
            # Get last inserted ID for INSERT
            if query.strip().upper().startswith("INSERT") and "RETURNING" in query.upper():
                if result.data:
                    result.last_id = result.data[0].get("id")
            
            cursor.close()
            
            result.execution_time = (time.time() - start_time) * 1000
            return result
            
        except Exception as e:
            raise QueryError(str(e), query=query, params=params)
    
    def execute_many(
        self,
        query: str,
        params_list: List[Union[tuple, dict]]
    ) -> QueryResult:
        """Execute query with multiple parameter sets."""
        import psycopg2.extras
        
        start_time = time.time()
        
        try:
            cursor = self._connection.cursor()
            psycopg2.extras.execute_batch(cursor, query, params_list)
            
            result = QueryResult()
            result.affected_rows = cursor.rowcount
            result.execution_time = (time.time() - start_time) * 1000
            
            cursor.close()
            return result
            
        except Exception as e:
            raise QueryError(str(e), query=query)
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        """Insert single record."""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ", ".join(["%s"] * len(values))
        columns_str = ", ".join(columns)
        
        query = f"""
            INSERT INTO {table} ({columns_str})
            VALUES ({placeholders})
            RETURNING id
        """
        
        return self.execute(query, tuple(values))
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        """Insert multiple records."""
        if not data:
            return QueryResult()
        
        columns = list(data[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        params_list = [tuple(row.get(col) for col in columns) for row in data]
        
        return self.execute_many(query, params_list)
    
    def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Update records."""
        set_parts = []
        values = []
        
        for key, value in data.items():
            set_parts.append(f"{key} = %s")
            values.append(value)
        
        query = f"UPDATE {table} SET {', '.join(set_parts)}"
        
        if where:
            where_parts = []
            for key, value in where.items():
                where_parts.append(f"{key} = %s")
                values.append(value)
            query += f" WHERE {' AND '.join(where_parts)}"
        
        return self.execute(query, tuple(values))
    
    def delete(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Delete records."""
        query = f"DELETE FROM {table}"
        values = []
        
        if where:
            where_parts = []
            for key, value in where.items():
                where_parts.append(f"{key} = %s")
                values.append(value)
            query += f" WHERE {' AND '.join(where_parts)}"
        
        return self.execute(query, tuple(values) if values else None)
    
    def find(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> QueryResult:
        """Find records."""
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"
        values = []
        
        if where:
            where_parts = []
            for key, value in where.items():
                if value is None:
                    where_parts.append(f"{key} IS NULL")
                else:
                    where_parts.append(f"{key} = %s")
                    values.append(value)
            query += f" WHERE {' AND '.join(where_parts)}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        if offset:
            query += f" OFFSET {offset}"
        
        return self.execute(query, tuple(values) if values else None)
    
    def begin_transaction(self) -> None:
        """Start transaction."""
        self._connection.autocommit = False
        self._in_transaction = True
    
    def commit(self) -> None:
        """Commit transaction."""
        self._connection.commit()
        self._connection.autocommit = True
        self._in_transaction = False
    
    def rollback(self) -> None:
        """Rollback transaction."""
        self._connection.rollback()
        self._connection.autocommit = True
        self._in_transaction = False
    
    def get_tables(self) -> List[str]:
        """Get list of tables."""
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """
        result = self.execute(query)
        return [row["table_name"] for row in result.data]
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get column information."""
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        result = self.execute(query, (table,))
        return result.data
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists."""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            )
        """
        result = self.execute(query, (table,))
        return result.data[0]["exists"] if result.data else False
