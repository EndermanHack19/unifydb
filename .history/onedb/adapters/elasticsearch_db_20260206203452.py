"""
Elasticsearch Adapter.
"""

from typing import Any, Dict, List, Optional, Union
import time
import json

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError as OneDBConnectionError
from ..exceptions import QueryError, DriverNotInstalledError


class Elasticsearch(BaseAdapter):
    """
    Elasticsearch adapter.
    
    Install:
        pip install onedb[elasticsearch]
    """
    
    db_type = DatabaseType.ELASTICSEARCH
    driver_name = "elasticsearch"
    install_command = "pip install onedb[elasticsearch]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        if self.config.port is None:
            self.config.port = 9200
        self._client = None
    
    def _import_driver(self):
        try:
            from elasticsearch import Elasticsearch as ES
            return ES
        except ImportError:
            raise DriverNotInstalledError("elasticsearch", self.install_command)
    
    def connect(self) -> None:
        ES = self._import_driver()
        
        try:
            hosts = [f"http://{self.config.host}:{self.config.port}"]
            
            auth = None
            if self.config.user and self.config.password:
                auth = (self.config.user, self.config.password)
            
            self._client = ES(
                hosts=hosts,
                basic_auth=auth,
                request_timeout=self.config.timeout
            )
            
            # Test connection
            self._client.info()
            self._is_connected = True
            
        except Exception as e:
            raise OneDBConnectionError(
                f"Failed to connect to Elasticsearch: {e}",
                host=self.config.host,
                port=self.config.port
            )
    
    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
        self._is_connected = False
    
    def is_connected(self) -> bool:
        try:
            if self._client:
                self._client.ping()
                return True
        except Exception:
            pass
        return False
    
    def execute(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        """Execute Elasticsearch query (expects JSON query)."""
        start_time = time.time()
        
        try:
            if isinstance(query, str):
                query_dict = json.loads(query)
            else:
                query_dict = query
            
            index = query_dict.pop("_index", self.config.database or "*")
            
            response = self._client.search(index=index, body=query_dict)
            
            result = QueryResult()
            result.data = [hit["_source"] for hit in response["hits"]["hits"]]
            result.affected_rows = len(result.data)
            result.execution_time = (time.time() - start_time) * 1000
            
            return result
            
        except Exception as e:
            raise QueryError(str(e), query=str(query))
    
    def execute_many(self, query: str, params_list: List) -> QueryResult:
        """Bulk operations."""
        from elasticsearch.helpers import bulk
        
        start_time = time.time()
        
        actions = []
        for params in params_list:
            action = {
                "_index": self.config.database,
                "_source": params
            }
            actions.append(action)
        
        success, _ = bulk(self._client, actions)
        
        result = QueryResult(affected_rows=success)
        result.execution_time = (time.time() - start_time) * 1000
        return result
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        """Index a document."""
        start_time = time.time()
        
        doc_id = data.pop("_id", None)
        
        response = self._client.index(
            index=table,
            id=doc_id,
            document=data
        )
        
        result = QueryResult()
        result.last_id = response["_id"]
        result.affected_rows = 1
        result.execution_time = (time.time() - start_time) * 1000
        
        return result
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        """Bulk index documents."""
        from elasticsearch.helpers import bulk
        
        start_time = time.time()
        
        actions = [{"_index": table, "_source": doc} for doc in data]
        success, _ = bulk(self._client, actions)
        
        result = QueryResult(affected_rows=success)
        result.execution_time = (time.time() - start_time) * 1000
        return result
    
    def update(
        self, 
        table: str, 
        data: Dict[str, Any], 
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Update by query."""
        start_time = time.time()
        
        query = {"match_all": {}}
        if where:
            query = {"bool": {"must": [{"term": {k: v}} for k, v in where.items()]}}
        
        script_parts = [f"ctx._source.{k} = params.{k}" for k in data.keys()]
        
        response = self._client.update_by_query(
            index=table,
            body={
                "query": query,
                "script": {
                    "source": "; ".join(script_parts),
                    "params": data
                }
            }
        )
        
        result = QueryResult(affected_rows=response.get("updated", 0))
        result.execution_time = (time.time() - start_time) * 1000
        return result
    
    def delete(
        self, 
        table: str, 
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Delete by query."""
        start_time = time.time()
        
        query = {"match_all": {}}
        if where:
            query = {"bool": {"must": [{"term": {k: v}} for k, v in where.items()]}}
        
        response = self._client.delete_by_query(
            index=table, 
            body={"query": query}
        )
        
        result = QueryResult(affected_rows=response.get("deleted", 0))
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
        """Search documents."""
        start_time = time.time()
        
        body: Dict[str, Any] = {}
        
        if where:
            body["query"] = {
                "bool": {
                    "must": [{"term": {k: v}} for k, v in where.items()]
                }
            }
        else:
            body["query"] = {"match_all": {}}
        
        if columns:
            body["_source"] = columns
        
        if order_by:
            parts = order_by.split()
            field = parts[0]
            direction = parts[1].lower() if len(parts) > 1 else "asc"
            body["sort"] = [{field: {"order": direction}}]
        
        if limit:
            body["size"] = limit
        
        if offset:
            body["from"] = offset
        
        response = self._client.search(index=table, body=body)
        
        result = QueryResult()
        result.data = [hit["_source"] for hit in response["hits"]["hits"]]
        result.affected_rows = len(result.data)
        result.execution_time = (time.time() - start_time) * 1000
        
        return result
    
    def search(self, index: str, query: Dict[str, Any]) -> QueryResult:
        """Native Elasticsearch search."""
        start_time = time.time()
        
        response = self._client.search(index=index, body=query)
        
        result = QueryResult()
        result.data = [hit["_source"] for hit in response["hits"]["hits"]]
        
        total = response["hits"]["total"]
        if isinstance(total, dict):
            result.affected_rows = total["value"]
        else:
            result.affected_rows = total
        
        result.execution_time = (time.time() - start_time) * 1000
        
        # Include aggregations if present
        if "aggregations" in response:
            result.data.append({"_aggregations": response["aggregations"]})
        
        return result
    
    def begin_transaction(self) -> None:
        """Elasticsearch doesn't support transactions."""
        pass
    
    def commit(self) -> None:
        """Refresh indices."""
        if self._client:
            self._client.indices.refresh()
    
    def rollback(self) -> None:
        """Not supported."""
        pass
    
    def get_tables(self) -> List[str]:
        """Get all indices."""
        indices = self._client.indices.get_alias(index="*")
        return list(indices.keys())
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get index mapping (fields)."""
        mapping = self._client.indices.get_mapping(index=table)
        
        if table not in mapping:
            return []
        
        properties = mapping[table].get("mappings", {}).get("properties", {})
        
        return [
            {"column_name": name, "data_type": props.get("type", "unknown")}
            for name, props in properties.items()
        ]
    
    def table_exists(self, table: str) -> bool:
        """Check if index exists."""
        return self._client.indices.exists(index=table)