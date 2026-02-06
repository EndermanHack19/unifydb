"""
Amazon DynamoDB Adapter.
"""

from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class DynamoDB(BaseAdapter):
    """
    Amazon DynamoDB adapter.
    
    Install:
        pip install onedb[dynamodb]
    """
    
    db_type = DatabaseType.DYNAMODB
    driver_name = "boto3"
    install_command = "pip install onedb[dynamodb]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        self._dynamodb = None
        self._client = None
        
        # AWS specific config
        self._region = self.config.extra.get("region", "us-east-1")
        self._aws_access_key = self.config.user
        self._aws_secret_key = self.config.password
    
    def _import_driver(self):
        try:
            import boto3
            return boto3
        except ImportError:
            raise DriverNotInstalledError("boto3", self.install_command)
    
    def connect(self) -> None:
        boto3 = self._import_driver()
        
        try:
            session_params = {"region_name": self._region}
            
            if self._aws_access_key and self._aws_secret_key:
                session_params["aws_access_key_id"] = self._aws_access_key
                session_params["aws_secret_access_key"] = self._aws_secret_key
            
            session = boto3.Session(**session_params)
            self._dynamodb = session.resource("dynamodb")
            self._client = session.client("dynamodb")
            
            # Test connection
            self._client.list_tables(Limit=1)
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to DynamoDB: {e}",
                host=self._region
            )
    
    def disconnect(self) -> None:
        self._dynamodb = None
        self._client = None
        self._is_connected = False
    
    def is_connected(self) -> bool:
        try:
            if self._client:
                self._client.list_tables(Limit=1)
                return True
        except Exception:
            pass
        return False
    
    def execute(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        """Execute PartiQL query."""
        start_time = time.time()
        
        try:
            response = self._client.execute_statement(
                Statement=query,
                Parameters=list(params) if params else None
            )
            
            result = QueryResult()
            result.data = response.get("Items", [])
            result.affected_rows = len(result.data)
            result.execution_time = (time.time() - start_time) * 1000
            
            return result
            
        except Exception as e:
            raise QueryError(str(e), query=query, params=params)
    
    def execute_many(self, query: str, params_list: List) -> QueryResult:
        start_time = time.time()
        
        for params in params_list:
            self._client.execute_statement(Statement=query, Parameters=list(params))
        
        result = QueryResult(affected_rows=len(params_list))
        result.execution_time = (time.time() - start_time) * 1000
        return result
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        """Put item into table."""
        start_time = time.time()
        
        table_resource = self._dynamodb.Table(table)
        table_resource.put_item(Item=data)
        
        result = QueryResult(affected_rows=1)
        result.execution_time = (time.time() - start_time) * 1000
        return result
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        """Batch write items."""
        start_time = time.time()
        
        table_resource = self._dynamodb.Table(table)
        
        with table_resource.batch_writer() as batch:
            for item in data:
                batch.put_item(Item=item)
        
        result = QueryResult(affected_rows=len(data))
        result.execution_time = (time.time() - start_time) * 1000
        return result
    
    def update(self, table: str, data: Dict[str, Any], where: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Update item."""
        start_time = time.time()
        
        if not where:
            raise QueryError("DynamoDB update requires primary key in 'where'")
        
        table_resource = self._dynamodb.Table(table)
        
        update_expr_parts = []
        expr_values = {}
        expr_names = {}
        
        for i, (key, value) in enumerate(data.items()):
            update_expr_parts.append(f"#{key} = :val{i}")
            expr_values[f":val{i}"] = value
            expr_names[f"#{key}"] = key
        
        table_resource.update_item(
            Key=where,
            UpdateExpression="SET " + ", ".join(update_expr_parts),
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names
        )
        
        result = QueryResult(affected_rows=1)
        result.execution_time = (time.time() - start_time) * 1000
        return result
    
    def delete(self, table: str, where: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Delete item."""
        start_time = time.time()
        
        if not where:
            raise QueryError("DynamoDB delete requires primary key in 'where'")
        
        table_resource = self._dynamodb.Table(table)
        table_resource.delete_item(Key=where)
        
        result = QueryResult(affected_rows=1)
        result.execution_time = (time.time() - start_time) * 1000
        return result
    
    def find(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> QueryResult:
        """Scan or query table."""
        start_time = time.time()
        
        table_resource = self._dynamodb.Table(table)
        
        scan_params = {}
        
        if columns:
            scan_params["ProjectionExpression"] = ", ".join(columns)
        
        if where:
            filter_parts = []
            expr_values = {}
            for i, (key, value) in enumerate(where.items()):
                filter_parts.append(f"{key} = :val{i}")
                expr_values[f":val{i}"] = value
            
            scan_params["FilterExpression"] = " AND ".join(filter_parts)
            scan_params["ExpressionAttributeValues"] = expr_values
        
        if limit:
            scan_params["Limit"] = limit
        
        response = table_resource.scan(**scan_params)
        
        result = QueryResult()
        result.data = response.get("Items", [])
        result.affected_rows = len(result.data)
        result.execution_time = (time.time() - start_time) * 1000
        
        return result
    
    def begin_transaction(self) -> None:
        pass
    
    def commit(self) -> None:
        pass
    
    def rollback(self) -> None:
        pass
    
    def get_tables(self) -> List[str]:
        response = self._client.list_tables()
        return response.get("TableNames", [])
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        response = self._client.describe_table(TableName=table)
        
        columns = []
        for attr in response["Table"]["AttributeDefinitions"]:
            columns.append({
                "column_name": attr["AttributeName"],
                "data_type": attr["AttributeType"]
            })
        return columns
    
    def table_exists(self, table: str) -> bool:
        try:
            self._client.describe_table(TableName=table)
            return True
        except Exception:
            return False
