"""
Snowflake Adapter.
"""

from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class Snowflake(BaseAdapter):
    """
    Snowflake data warehouse adapter.
    
    Install:
        pip install onedb[snowflake]
    """
    
    db_type = DatabaseType.SNOWFLAKE
    driver_name = "snowflake-connector-python"
    install_command = "pip install onedb[snowflake]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        self._account = self.config.extra.get("account", self.config.host)
        self._warehouse = self.config.extra.get("warehouse")
        self._schema = self.config.extra.get("schema", "PUBLIC")
    
    def _import_driver(self):
        try:
            import snowflake.connector
            return snowflake.connector
        except ImportError:
            raise DriverNotInstalledError("snowflake-connector-python", self.install_command)
    
    def connect(self) -> None:
        snowflake = self._import_driver()
        
        try:
            self._connection = snowflake.connect(
                account=self._account,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                schema=self._schema,
                warehouse=self._warehouse,
                login_timeout=self.config.timeout
            )
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Snowflake: {e}",
                host=self._account,
                database=self.config.database
            )
    
    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
        self._is_connected = False
    
    def is_connected(self) -> bool:
        try:
            if self._connection:
                cursor = self._connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return True
        except Exception:
            pass
        return False
    
    def execute(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        start_time = time.time()
        
        try:
            cursor = self._connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            result = QueryResult()
            
            if cursor.description:
                result.columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                result.data = [dict(zip(result.columns, row)) for row in rows]
            
            result.affected_rows = cursor.rowcount
            cursor.close()
            
            result.execution_time = (time.time() - start_time) * 1000
            return result
            
        except Exception as e:
            raise QueryError(str(e), query=query, params=params)
    
    def execute_many(self, query: str, params_list: List) -> QueryResult:
        start_time = time.time()
        cursor = self._connection.cursor()
        cursor.executemany(query, params_list)
        
        result = QueryResult(affected_rows=cursor.rowcount)
        result.execution_time = (time.time() - start_time) * 1000
        cursor.close()
        return result
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        columns = list(data.keys())
        placeholders = ", ".join(["%s" for _ in columns])
        columns_str = ", ".join(columns)
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        return self.execute(query, tuple(data.values()))
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        if not data:
            return QueryResult()
        
        columns = list(data[0].keys())
        placeholders = ", ".join(["%s" for _ in columns])
        columns_str = ", ".join(columns)
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        params_list = [tuple(row.values()) for row in data]
        return self.execute_many(query, params_list)
    
    def update(self, table: str, data: Dict[str, Any], where: Optional[Dict[str, Any]] = None) -> QueryResult:
        set_parts = [f"{k} = %s" for k in data.keys()]
        values = list(data.values())
        
        query = f"UPDATE {table} SET {', '.join(set_parts)}"
        
        if where:
            where_parts = [f"{k} = %s" for k in where.keys()]
            values.extend(where.values())
            query += f" WHERE {' AND '.join(where_parts)}"
        
        return self.execute(query, tuple(values))
    
    def delete(self, table: str, where: Optional[Dict[str, Any]] = None) -> QueryResult:
        query = f"DELETE FROM {table}"
        values = []
        
        if where:
            where_parts = [f"{k} = %s" for k in where.keys()]
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
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"
        values = []
        
        if where:
            where_parts = [f"{k} = %s" for k in where.keys()]
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
        self._connection.cursor().execute("BEGIN")
        self._in_transaction = True
    
    def commit(self) -> None:
        self._connection.cursor().execute("COMMIT")
        self._in_transaction = False
    
    def rollback(self) -> None:
        self._connection.cursor().execute("ROLLBACK")
        self._in_transaction = False
    
    def get_tables(self) -> List[str]:
        result = self.execute("SHOW TABLES")
        return [row.get("name", row.get("TABLE_NAME", "")) for row in result.data]
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        result = self.execute(f"DESCRIBE TABLE {table}")
        return [
            {
                "column_name": row.get("name", row.get("COLUMN_NAME")),
                "data_type": row.get("type", row.get("DATA_TYPE"))
            }
            for row in result.data
        ]
    
    def table_exists(self, table: str) -> bool:
        try:
            self.execute(f"DESCRIBE TABLE {table}")
            return True
        except Exception:
            return False
