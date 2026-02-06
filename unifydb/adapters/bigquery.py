"""
Google BigQuery Adapter.
"""

from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class BigQuery(BaseAdapter):
    """
    Google BigQuery adapter.
    
    Install:
        pip install unifydb[bigquery]
    """
    
    db_type = DatabaseType.BIGQUERY
    driver_name = "google-cloud-bigquery"
    install_command = "pip install unifydb[bigquery]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        self._client = None
        self._project = self.config.extra.get("project", self.config.database)
        self._dataset = self.config.extra.get("dataset")
        self._credentials_path = self.config.extra.get("credentials")
    
    def _import_driver(self):
        try:
            from google.cloud import bigquery
            return bigquery
        except ImportError:
            raise DriverNotInstalledError("google-cloud-bigquery", self.install_command)
    
    def connect(self) -> None:
        bigquery = self._import_driver()
        
        try:
            if self._credentials_path:
                self._client = bigquery.Client.from_service_account_json(
                    self._credentials_path,
                    project=self._project
                )
            else:
                self._client = bigquery.Client(project=self._project)
            
            # Test connection
            list(self._client.list_datasets(max_results=1))
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to BigQuery: {e}",
                database=self._project
            )
    
    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
        self._is_connected = False
    
    def is_connected(self) -> bool:
        try:
            if self._client:
                list(self._client.list_datasets(max_results=1))
                return True
        except Exception:
            pass
        return False
    
    def _get_full_table_name(self, table: str) -> str:
        """Get fully qualified table name."""
        if "." not in table and self._dataset:
            return f"{self._project}.{self._dataset}.{table}"
        return table
    
    def execute(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        start_time = time.time()
        
        try:
            job_config = None
            
            if params:
                from google.cloud.bigquery import ScalarQueryParameter, QueryJobConfig
                
                query_params = []
                if isinstance(params, dict):
                    for name, value in params.items():
                        param_type = "STRING"
                        if isinstance(value, int):
                            param_type = "INT64"
                        elif isinstance(value, float):
                            param_type = "FLOAT64"
                        elif isinstance(value, bool):
                            param_type = "BOOL"
                        query_params.append(ScalarQueryParameter(name, param_type, value))
                
                job_config = QueryJobConfig(query_parameters=query_params)
            
            query_job = self._client.query(query, job_config=job_config)
            rows = query_job.result()
            
            result = QueryResult()
            result.data = [dict(row) for row in rows]
            result.affected_rows = query_job.num_dml_affected_rows or len(result.data)
            
            if result.data:
                result.columns = list(result.data[0].keys())
            
            result.execution_time = (time.time() - start_time) * 1000
            return result
            
        except Exception as e:
            raise QueryError(str(e), query=query, params=params)
    
    def execute_many(self, query: str, params_list: List) -> QueryResult:
        # BigQuery doesn't have native executemany, use streaming insert
        results = QueryResult()
        for params in params_list:
            self.execute(query, params)
            results.affected_rows += 1
        return results
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        """Insert using streaming."""
        start_time = time.time()
        
        full_table = self._get_full_table_name(table)
        table_ref = self._client.get_table(full_table)
        
        errors = self._client.insert_rows_json(table_ref, [data])
        
        if errors:
            raise QueryError(f"Insert errors: {errors}")
        
        result = QueryResult(affected_rows=1)
        result.execution_time = (time.time() - start_time) * 1000
        return result
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        """Batch insert using streaming."""
        start_time = time.time()
        
        full_table = self._get_full_table_name(table)
        table_ref = self._client.get_table(full_table)
        
        errors = self._client.insert_rows_json(table_ref, data)
        
        if errors:
            raise QueryError(f"Insert errors: {errors}")
        
        result = QueryResult(affected_rows=len(data))
        result.execution_time = (time.time() - start_time) * 1000
        return result
    
    def update(self, table: str, data: Dict[str, Any], where: Optional[Dict[str, Any]] = None) -> QueryResult:
        full_table = self._get_full_table_name(table)
        
        set_parts = [f"{k} = @{k}" for k in data.keys()]
        query = f"UPDATE `{full_table}` SET {', '.join(set_parts)}"
        
        params = dict(data)
        
        if where:
            where_parts = [f"{k} = @where_{k}" for k in where.keys()]
            query += f" WHERE {' AND '.join(where_parts)}"
            for k, v in where.items():
                params[f"where_{k}"] = v
        
        return self.execute(query, params)
    
    def delete(self, table: str, where: Optional[Dict[str, Any]] = None) -> QueryResult:
        full_table = self._get_full_table_name(table)
        query = f"DELETE FROM `{full_table}`"
        
        params = {}
        if where:
            where_parts = [f"{k} = @{k}" for k in where.keys()]
            query += f" WHERE {' AND '.join(where_parts)}"
            params = where
        
        return self.execute(query, params if params else None)
    
    def find(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> QueryResult:
        full_table = self._get_full_table_name(table)
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM `{full_table}`"
        
        params = {}
        if where:
            where_parts = [f"{k} = @{k}" for k in where.keys()]
            query += f" WHERE {' AND '.join(where_parts)}"
            params = where
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        if offset:
            query += f" OFFSET {offset}"
        
        return self.execute(query, params if params else None)
    
    def begin_transaction(self) -> None:
        pass  # BigQuery transactions are per-query
    
    def commit(self) -> None:
        pass
    
    def rollback(self) -> None:
        pass
    
    def get_tables(self) -> List[str]:
        if not self._dataset:
            return []
        
        tables = self._client.list_tables(f"{self._project}.{self._dataset}")
        return [table.table_id for table in tables]
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        full_table = self._get_full_table_name(table)
        table_ref = self._client.get_table(full_table)
        
        return [
            {
                "column_name": field.name,
                "data_type": field.field_type,
                "is_nullable": field.mode != "REQUIRED"
            }
            for field in table_ref.schema
        ]
    
    def table_exists(self, table: str) -> bool:
        try:
            full_table = self._get_full_table_name(table)
            self._client.get_table(full_table)
            return True
        except Exception:
            return False
