"""
Oracle Database Adapter.
"""

from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class Oracle(BaseAdapter):
    """
    Oracle database adapter.
    
    Install:
        pip install onedb[oracle]
    """
    
    db_type = DatabaseType.ORACLE
    driver_name = "oracledb"
    install_command = "pip install onedb[oracle]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        if self.config.port is None:
            self.config.port = 1521
    
    def _import_driver(self):
        try:
            import oracledb
            return oracledb
        except ImportError:
            try:
                import cx_Oracle
                return cx_Oracle
            except ImportError:
                raise DriverNotInstalledError("oracledb", self.install_command)
    
    def connect(self) -> None:
        driver = self._import_driver()
        
        try:
            dsn = driver.makedsn(
                self.config.host,
                self.config.port,
                service_name=self.config.database
            )
            
            self._connection = driver.connect(
                user=self.config.user,
                password=self.config.password,
                dsn=dsn
            )
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Oracle: {e}",
                host=self.config.host,
                port=self.config.port,
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
                self._connection.ping()
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
            
            if not self._in_transaction:
                self._connection.commit()
            
            cursor.close()
            result.execution_time = (time.time() - start_time) * 1000
            return result
            
        except Exception as e:
            raise QueryError(str(e), query=query, params=params)
    
    def execute_many(self, query: str, params_list: List) -> QueryResult:
        start_time = time.time()
        cursor = self._connection.cursor()
        cursor.executemany(query, params_list)
        
        if not self._in_transaction:
            self._connection.commit()
        
        result = QueryResult(affected_rows=cursor.rowcount)
        result.execution_time = (time.time() - start_time) * 1000
        cursor.close()
        return result
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        columns = list(data.keys())
        placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
        columns_str = ", ".join(columns)
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        return self.execute(query, tuple(data.values()))
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        if not data:
            return QueryResult()
        
        columns = list(data[0].keys())
        placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
        columns_str = ", ".join(columns)
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        params_list = [tuple(row.values()) for row in data]
        return self.execute_many(query, params_list)
    
    def update(self, table: str, data: Dict[str, Any], where: Optional[Dict[str, Any]] = None) -> QueryResult:
        set_parts = [f"{k} = :{i+1}" for i, k in enumerate(data.keys())]
        values = list(data.values())
        
        query = f"UPDATE {table} SET {', '.join(set_parts)}"
        
        if where:
            where_parts = [f"{k} = :{len(values)+i+1}" for i, k in enumerate(where.keys())]
            values.extend(where.values())
            query += f" WHERE {' AND '.join(where_parts)}"
        
        return self.execute(query, tuple(values))
    
    def delete(self, table: str, where: Optional[Dict[str, Any]] = None) -> QueryResult:
        query = f"DELETE FROM {table}"
        values = []
        
        if where:
            where_parts = [f"{k} = :{i+1}" for i, k in enumerate(where.keys())]
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
            where_parts = [f"{k} = :{i+1}" for i, k in enumerate(where.keys())]
            values = list(where.values())
            query += f" WHERE {' AND '.join(where_parts)}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" FETCH FIRST {limit} ROWS ONLY"
        
        if offset:
            query += f" OFFSET {offset} ROWS"
        
        return self.execute(query, tuple(values) if values else None)
    
    def begin_transaction(self) -> None:
        self._in_transaction = True
    
    def commit(self) -> None:
        self._connection.commit()
        self._in_transaction = False
    
    def rollback(self) -> None:
        self._connection.rollback()
        self._in_transaction = False
    
    def get_tables(self) -> List[str]:
        result = self.execute("SELECT table_name FROM user_tables ORDER BY table_name")
        return [row["TABLE_NAME"] for row in result.data]
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        result = self.execute("""
            SELECT column_name, data_type, nullable, data_default
            FROM user_tab_columns
            WHERE table_name = :1
            ORDER BY column_id
        """, (table.upper(),))
        
        return [
            {
                "column_name": row["COLUMN_NAME"],
                "data_type": row["DATA_TYPE"],
                "is_nullable": row["NULLABLE"] == "Y",
                "column_default": row["DATA_DEFAULT"]
            }
            for row in result.data
        ]
    
    def table_exists(self, table: str) -> bool:
        result = self.execute(
            "SELECT COUNT(*) as cnt FROM user_tables WHERE table_name = :1",
            (table.upper(),)
        )
        return result.first["CNT"] > 0
