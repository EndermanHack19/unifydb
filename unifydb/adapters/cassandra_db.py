"""
Apache Cassandra Adapter.
"""

from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class Cassandra(BaseAdapter):
    """
    Apache Cassandra adapter.
    
    Install:
        pip install unifydb[cassandra]
    """
    
    db_type = DatabaseType.CASSANDRA
    driver_name = "cassandra-driver"
    install_command = "pip install unifydb[cassandra]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        if self.config.port is None:
            self.config.port = 9042
        self._session = None
    
    def _import_driver(self):
        try:
            from cassandra.cluster import Cluster
            from cassandra.auth import PlainTextAuthProvider
            return Cluster, PlainTextAuthProvider
        except ImportError:
            raise DriverNotInstalledError("cassandra-driver", self.install_command)
    
    def connect(self) -> None:
        Cluster, PlainTextAuthProvider = self._import_driver()
        
        try:
            auth_provider = None
            if self.config.user and self.config.password:
                auth_provider = PlainTextAuthProvider(
                    username=self.config.user,
                    password=self.config.password
                )
            
            self._cluster = Cluster(
                contact_points=[self.config.host],
                port=self.config.port,
                auth_provider=auth_provider
            )
            
            self._session = self._cluster.connect(self.config.database)
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Cassandra: {e}",
                host=self.config.host,
                port=self.config.port,
                database=self.config.database
            )
    
    def disconnect(self) -> None:
        if self._session:
            self._session.shutdown()
            self._session = None
        if hasattr(self, '_cluster') and self._cluster:
            self._cluster.shutdown()
            self._cluster = None
        self._is_connected = False
    
    def is_connected(self) -> bool:
        try:
            if self._session:
                self._session.execute("SELECT now() FROM system.local")
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
            if params:
                rows = self._session.execute(query, params)
            else:
                rows = self._session.execute(query)
            
            result = QueryResult()
            result.data = [dict(row._asdict()) for row in rows]
            result.affected_rows = len(result.data)
            
            if result.data:
                result.columns = list(result.data[0].keys())
            
            result.execution_time = (time.time() - start_time) * 1000
            return result
            
        except Exception as e:
            raise QueryError(str(e), query=query, params=params)
    
    def execute_many(self, query: str, params_list: List) -> QueryResult:
        start_time = time.time()
        
        from cassandra.query import BatchStatement
        batch = BatchStatement()
        
        prepared = self._session.prepare(query)
        for params in params_list:
            batch.add(prepared, params)
        
        self._session.execute(batch)
        
        result = QueryResult(affected_rows=len(params_list))
        result.execution_time = (time.time() - start_time) * 1000
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
        
        return self.execute(query, tuple(values) if values else None)
    
    def begin_transaction(self) -> None:
        pass  # Cassandra uses lightweight transactions
    
    def commit(self) -> None:
        pass
    
    def rollback(self) -> None:
        pass
    
    def get_tables(self) -> List[str]:
        result = self.execute(f"""
            SELECT table_name FROM system_schema.tables 
            WHERE keyspace_name = '{self.config.database}'
        """)
        return [row["table_name"] for row in result.data]
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        result = self.execute(f"""
            SELECT column_name, type FROM system_schema.columns 
            WHERE keyspace_name = '{self.config.database}' AND table_name = '{table}'
        """)
        return [
            {"column_name": row["column_name"], "data_type": row["type"]}
            for row in result.data
        ]
    
    def table_exists(self, table: str) -> bool:
        result = self.execute(f"""
            SELECT table_name FROM system_schema.tables 
            WHERE keyspace_name = '{self.config.database}' AND table_name = '{table}'
        """)
        return len(result.data) > 0
