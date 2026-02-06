"""
MySQL/MariaDB Adapter.
Supports both mysql-connector-python and PyMySQL drivers.
"""

from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class MySQL(BaseAdapter):
    """
    MySQL database adapter.
    
    Usage:
        db = MySQL(
            host="localhost",
            port=3306,
            database="mydb",
            user="root",
            password="secret"
        )
        db.connect()
        
        # CRUD operations
        db.insert("users", {"name": "John", "email": "john@example.com"})
        users = db.find("users", where={"active": True})
        
    Install:
        pip install unifydb[mysql]
    """
    
    db_type = DatabaseType.MYSQL
    driver_name = "pymysql"
    install_command = "pip install unifydb[mysql]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        self._cursor = None
        
        # Default MySQL port
        if self.config.port is None:
            self.config.port = 3306
    
    def _import_driver(self):
        """Import MySQL driver (prefer PyMySQL)."""
        # Try PyMySQL first (pure Python, more compatible)
        try:
            import pymysql
            pymysql.install_as_MySQLdb()
            return pymysql, "pymysql"
        except ImportError:
            pass
        
        # Try mysql-connector-python
        try:
            import mysql.connector
            return mysql.connector, "mysql-connector"
        except ImportError:
            pass
        
        raise DriverNotInstalledError(
            "pymysql or mysql-connector-python",
            self.install_command
        )
    
    def connect(self) -> None:
        """Establish connection to MySQL."""
        driver, driver_name = self._import_driver()
        self._driver = driver
        self._driver_name = driver_name
        
        try:
            conn_params = {
                "host": self.config.host,
                "port": self.config.port,
                "database": self.config.database,
                "user": self.config.user,
                "password": self.config.password,
                "connect_timeout": self.config.timeout,
            }
            
            # SSL configuration
            if self.config.ssl:
                conn_params["ssl"] = {"ssl": True}
            
            # Add extra parameters
            conn_params.update(self.config.extra)
            
            # Remove None values
            conn_params = {k: v for k, v in conn_params.items() if v is not None}
            
            # Connect based on driver
            if driver_name == "pymysql":
                conn_params["cursorclass"] = driver.cursors.DictCursor
                conn_params["autocommit"] = True
                self._connection = driver.connect(**conn_params)
            else:
                # mysql-connector
                self._connection = driver.connect(**conn_params)
                self._connection.autocommit = True
            
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to MySQL: {e}",
                host=self.config.host,
                port=self.config.port,
                database=self.config.database
            )
    
    def disconnect(self) -> None:
        """Close connection."""
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
            self._cursor = None
        
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None
        
        self._is_connected = False
    
    def is_connected(self) -> bool:
        """Check if connected."""
        if not self._connection:
            return False
        try:
            self._connection.ping(reconnect=False)
            return True
        except Exception:
            self._is_connected = False
            return False
    
    def _get_cursor(self):
        """Get cursor with dict results."""
        if self._driver_name == "pymysql":
            return self._connection.cursor()
        else:
            # mysql-connector
            return self._connection.cursor(dictionary=True)
    
    def execute(
        self,
        query: str,
        params: Optional[Union[tuple, dict, list]] = None
    ) -> QueryResult:
        """Execute query and return results."""
        if not self._is_connected:
            raise ConnectionError("Not connected to database")
        
        start_time = time.time()
        
        try:
            cursor = self._get_cursor()
            
            # Execute query
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            result = QueryResult()
            result.affected_rows = cursor.rowcount
            result.last_id = cursor.lastrowid
            
            # Fetch results if SELECT query
            if cursor.description:
                result.columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                # Convert to list of dicts if needed
                if rows and not isinstance(rows[0], dict):
                    result.data = [
                        dict(zip(result.columns, row)) 
                        for row in rows
                    ]
                else:
                    result.data = list(rows) if rows else []
            
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
        start_time = time.time()
        
        try:
            cursor = self._get_cursor()
            cursor.executemany(query, params_list)
            
            result = QueryResult()
            result.affected_rows = cursor.rowcount
            
            if not self._in_transaction:
                self._connection.commit()
            
            cursor.close()
            
            result.execution_time = (time.time() - start_time) * 1000
            return result
            
        except Exception as e:
            raise QueryError(str(e), query=query)
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        """Insert single record."""
        columns = list(data.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(f"`{col}`" for col in columns)
        values = list(data.values())
        
        query = f"INSERT INTO `{table}` ({columns_str}) VALUES ({placeholders})"
        return self.execute(query, values)
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        """Insert multiple records."""
        if not data:
            return QueryResult()
        
        columns = list(data[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(f"`{col}`" for col in columns)
        
        query = f"INSERT INTO `{table}` ({columns_str}) VALUES ({placeholders})"
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
            set_parts.append(f"`{key}` = %s")
            values.append(value)
        
        query = f"UPDATE `{table}` SET {', '.join(set_parts)}"
        
        if where:
            where_parts = []
            for key, value in where.items():
                if value is None:
                    where_parts.append(f"`{key}` IS NULL")
                else:
                    where_parts.append(f"`{key}` = %s")
                    values.append(value)
            query += f" WHERE {' AND '.join(where_parts)}"
        
        return self.execute(query, values)
    
    def delete(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Delete records."""
        query = f"DELETE FROM `{table}`"
        values = []
        
        if where:
            where_parts = []
            for key, value in where.items():
                if value is None:
                    where_parts.append(f"`{key}` IS NULL")
                else:
                    where_parts.append(f"`{key}` = %s")
                    values.append(value)
            query += f" WHERE {' AND '.join(where_parts)}"
        
        return self.execute(query, values if values else None)
    
    def find(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> QueryResult:
        """Find records with conditions."""
        # Build column list
        if columns:
            cols = ", ".join(f"`{col}`" for col in columns)
        else:
            cols = "*"
        
        query = f"SELECT {cols} FROM `{table}`"
        values = []
        
        # WHERE clause
        if where:
            where_parts = []
            for key, value in where.items():
                if value is None:
                    where_parts.append(f"`{key}` IS NULL")
                else:
                    where_parts.append(f"`{key}` = %s")
                    values.append(value)
            query += f" WHERE {' AND '.join(where_parts)}"
        
        # ORDER BY
        if order_by:
            query += f" ORDER BY {order_by}"
        
        # LIMIT
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        
        # OFFSET
        if offset is not None:
            query += f" OFFSET {int(offset)}"
        
        return self.execute(query, values if values else None)
    
    def find_one(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """Find single record."""
        result = self.find(table, where, columns, limit=1)
        return result.first
    
    def begin_transaction(self) -> None:
        """Start transaction."""
        self._connection.autocommit = False
        self._connection.begin()
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
        result = self.execute("SHOW TABLES")
        if result.data:
            # Get first column value from each row
            first_col = result.columns[0] if result.columns else None
            if first_col:
                return [row[first_col] for row in result.data]
            return [list(row.values())[0] for row in result.data]
        return []
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get column information for table."""
        result = self.execute(f"DESCRIBE `{table}`")
        columns = []
        for row in result.data:
            columns.append({
                "column_name": row.get("Field"),
                "data_type": row.get("Type"),
                "is_nullable": row.get("Null") == "YES",
                "column_default": row.get("Default"),
                "is_primary_key": row.get("Key") == "PRI",
                "extra": row.get("Extra")
            })
        return columns
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists."""
        try:
            result = self.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (self.config.database, table)
            )
            return len(result.data) > 0
        except Exception:
            return False
    
    def get_version(self) -> str:
        """Get MySQL server version."""
        result = self.execute("SELECT VERSION() as version")
        return result.first["version"] if result.first else "Unknown"


# Alias for MariaDB compatibility
MariaDB = MySQL
