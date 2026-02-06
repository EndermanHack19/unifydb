"""
SQLite Adapter.
Lightweight file-based database (built-in, no extra install needed).
"""

import sqlite3
from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError


class SQLite(BaseAdapter):
    """
    SQLite database adapter.
    
    No additional installation required - SQLite is built into Python.
    
    Usage:
        # File database
        db = SQLite(database="mydb.sqlite")
        
        # In-memory database
        db = SQLite(database=":memory:")
        
        # Or using URI
        from unifydb import Database
        db = Database.connect("sqlite:///mydb.sqlite")
        db = Database.connect("sqlite:///:memory:")
    """
    
    db_type = DatabaseType.SQLITE
    driver_name = "sqlite3"
    install_command = "Built-in, no installation needed"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        
        if not self.config.database:
            self.config.database = ":memory:"
    
    def connect(self) -> None:
        """Connect to SQLite database."""
        try:
            self._connection = sqlite3.connect(
                self.config.database,
                timeout=self.config.timeout,
                check_same_thread=False
            )
            
            # Enable dict-like row access
            self._connection.row_factory = sqlite3.Row
            
            # Enable foreign keys
            self._connection.execute("PRAGMA foreign_keys = ON")
            
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to SQLite: {e}",
                database=self.config.database
            )
    
    def disconnect(self) -> None:
        """Close connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
        self._is_connected = False
    
    def is_connected(self) -> bool:
        """Check if connected."""
        try:
            if self._connection:
                self._connection.execute("SELECT 1")
                return True
        except Exception:
            pass
        return False
    
    def execute(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        """Execute query."""
        start_time = time.time()
        
        try:
            cursor = self._connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            result = QueryResult()
            result.affected_rows = cursor.rowcount
            result.last_id = cursor.lastrowid
            
            if cursor.description:
                result.columns = [desc[0] for desc in cursor.description]
                result.data = [dict(row) for row in cursor.fetchall()]
            
            if not self._in_transaction:
                self._connection.commit()
            
            result.execution_time = (time.time() - start_time) * 1000
            return result
            
        except Exception as e:
            raise QueryError(str(e), query=query, params=params)
    
    def execute_many(
        self,
        query: str,
        params_list: List[Union[tuple, dict]]
    ) -> QueryResult:
        """Execute query with multiple parameters."""
        start_time = time.time()
        
        try:
            cursor = self._connection.cursor()
            cursor.executemany(query, params_list)
            
            if not self._in_transaction:
                self._connection.commit()
            
            return QueryResult(
                affected_rows=cursor.rowcount,
                execution_time=(time.time() - start_time) * 1000
            )
        except Exception as e:
            raise QueryError(str(e), query=query)
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        """Insert record."""
        columns = list(data.keys())
        placeholders = ", ".join(["?"] * len(columns))
        columns_str = ", ".join(columns)
        values = tuple(data.values())
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        return self.execute(query, values)
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        """Insert multiple records."""
        if not data:
            return QueryResult()
        
        columns = list(data[0].keys())
        placeholders = ", ".join(["?"] * len(columns))
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
        set_parts = [f"{k} = ?" for k in data.keys()]
        values = list(data.values())
        
        query = f"UPDATE {table} SET {', '.join(set_parts)}"
        
        if where:
            where_parts = [f"{k} = ?" for k in where.keys()]
            values.extend(where.values())
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
            where_parts = [f"{k} = ?" for k in where.keys()]
            values = list(where.values())
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
            where_parts = [f"{k} = ?" for k in where.keys()]
            values = list(where.values())
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
        self._connection.execute("BEGIN TRANSACTION")
        self._in_transaction = True
    
    def commit(self) -> None:
        """Commit transaction."""
        self._connection.commit()
        self._in_transaction = False
    
    def rollback(self) -> None:
        """Rollback transaction."""
        self._connection.rollback()
        self._in_transaction = False
    
    def get_tables(self) -> List[str]:
        """Get table names."""
        query = """
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """
        result = self.execute(query)
        return [row["name"] for row in result.data]
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get column info."""
        result = self.execute(f"PRAGMA table_info({table})")
        return [
            {
                "column_name": row["name"],
                "data_type": row["type"],
                "is_nullable": not row["notnull"],
                "column_default": row["dflt_value"],
                "is_primary_key": bool(row["pk"])
            }
            for row in result.data
        ]
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists."""
        query = """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """
        result = self.execute(query, (table,))
        return len(result.data) > 0
