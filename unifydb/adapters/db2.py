"""
IBM Db2 Adapter.
"""

from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class DB2(BaseAdapter):
    """
    IBM Db2 database adapter.
    
    Install:
        pip install onedb[db2]
    """
    
    db_type = DatabaseType.DB2
    driver_name = "ibm_db"
    install_command = "pip install onedb[db2]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        if self.config.port is None:
            self.config.port = 50000
    
    def _import_driver(self):
        try:
            import ibm_db
            import ibm_db_dbi
            return ibm_db, ibm_db_dbi
        except ImportError:
            raise DriverNotInstalledError("ibm_db", self.install_command)
    
    def connect(self) -> None:
        ibm_db, ibm_db_dbi = self._import_driver()
        self._ibm_db = ibm_db
        
        try:
            conn_str = (
                f"DATABASE={self.config.database};"
                f"HOSTNAME={self.config.host};"
                f"PORT={self.config.port};"
                f"PROTOCOL=TCPIP;"
                f"UID={self.config.user};"
                f"PWD={self.config.password};"
            )
            
            self._connection = ibm_db_dbi.connect(conn_str, "", "")
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to DB2: {e}",
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
                cursor = self._connection.cursor()
                cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
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
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        return self.execute(query, tuple(data.values()))
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        if not data:
            return QueryResult()
        
        columns = list(data[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        params_list = [tuple(row.values()) for row in data]
        return self.execute_many(query, params_list)
    
    def update(self, table: str, data: Dict[str, Any], where: Optional[Dict[str, Any]] = None) -> QueryResult:
        set_parts = [f"{k} = ?" for k in data.keys()]
        values = list(data.values())
        
        query = f"UPDATE {table} SET {', '.join(set_parts)}"
        
        if where:
            where_parts = [f"{k} = ?" for k in where.keys()]
            values.extend(where.values())
            query += f" WHERE {' AND '.join(where_parts)}"
        
        return self.execute(query, tuple(values))
    
    def delete(self, table: str, where: Optional[Dict[str, Any]] = None) -> QueryResult:
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
        result = self.execute("""
            SELECT TABNAME FROM SYSCAT.TABLES 
            WHERE TABSCHEMA = CURRENT SCHEMA
            ORDER BY TABNAME
        """)
        return [row["TABNAME"] for row in result.data]
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        result = self.execute("""
            SELECT COLNAME, TYPENAME, NULLS, DEFAULT
            FROM SYSCAT.COLUMNS
            WHERE TABNAME = ?
            ORDER BY COLNO
        """, (table.upper(),))
        
        return [
            {
                "column_name": row["COLNAME"],
                "data_type": row["TYPENAME"],
                "is_nullable": row["NULLS"] == "Y",
                "column_default": row["DEFAULT"]
            }
            for row in result.data
        ]
    
    def table_exists(self, table: str) -> bool:
        result = self.execute(
            "SELECT COUNT(*) as cnt FROM SYSCAT.TABLES WHERE TABNAME = ?",
            (table.upper(),)
        )
        return result.first["cnt"] > 0
