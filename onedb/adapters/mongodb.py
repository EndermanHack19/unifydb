"""
MongoDB Adapter.
NoSQL document database support.
"""

from typing import Any, Dict, List, Optional, Union
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class MongoDB(BaseAdapter):
    """
    MongoDB database adapter.
    
    Usage:
        db = MongoDB(
            host="localhost",
            port=27017,
            database="mydb"
        )
        db.connect()
        
        # Insert
        db.insert("users", {"name": "John", "age": 30})
        
        # Find
        users = db.find("users", {"age": {"$gt": 25}})
        
        # Update
        db.update("users", {"age": 31}, {"name": "John"})
        
        # Aggregation
        result = db.aggregate("users", [
            {"$match": {"age": {"$gt": 20}}},
            {"$group": {"_id": "$city", "count": {"$sum": 1}}}
        ])
    
    Install:
        pip install onedb[mongodb]
    """
    
    db_type = DatabaseType.MONGODB
    driver_name = "pymongo"
    install_command = "pip install onedb[mongodb]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        self._client = None
        self._db = None
        
        if self.config.port is None:
            self.config.port = 27017
    
    def _import_driver(self):
        """Import pymongo driver."""
        try:
            import pymongo
            from bson import ObjectId
            return pymongo, ObjectId
        except ImportError:
            raise DriverNotInstalledError("pymongo", self.install_command)
    
    def connect(self) -> None:
        """Connect to MongoDB."""
        pymongo, _ = self._import_driver()
        
        try:
            # Build connection URI
            uri = self.config.to_uri("mongodb")
            
            self._client = pymongo.MongoClient(
                uri,
                serverSelectionTimeoutMS=self.config.timeout * 1000,
                **self.config.extra
            )
            
            # Test connection
            self._client.admin.command("ping")
            
            # Select database
            if self.config.database:
                self._db = self._client[self.config.database]
            
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to MongoDB: {e}",
                host=self.config.host,
                port=self.config.port,
                database=self.config.database
            )
    
    def disconnect(self) -> None:
        """Close connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
        self._is_connected = False
    
    def is_connected(self) -> bool:
        """Check connection."""
        if not self._client:
            return False
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            self._is_connected = False
            return False
    
    def use_database(self, database: str) -> None:
        """Switch to different database."""
        self._db = self._client[database]
        self.config.database = database
    
    def execute(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        """
        Execute raw command.
        For MongoDB, use specific methods like insert, find, etc.
        """
        start_time = time.time()
        
        try:
            # Parse as MongoDB command
            result = self._db.command(query)
            
            return QueryResult(
                data=[result] if isinstance(result, dict) else result,
                execution_time=(time.time() - start_time) * 1000
            )
        except Exception as e:
            raise QueryError(str(e), query=query)
    
    def execute_many(
        self,
        query: str,
        params_list: List[Union[tuple, dict]]
    ) -> QueryResult:
        """Execute is not typical for MongoDB - use insert_many."""
        raise NotImplementedError("Use insert_many for batch operations")
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        """Insert document into collection."""
        start_time = time.time()
        
        try:
            collection = self._db[table]
            result = collection.insert_one(data)
            
            return QueryResult(
                data=[{"_id": result.inserted_id}],
                last_id=str(result.inserted_id),
                affected_rows=1,
                execution_time=(time.time() - start_time) * 1000
            )
        except Exception as e:
            raise QueryError(f"Insert failed: {e}")
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        """Insert multiple documents."""
        start_time = time.time()
        
        try:
            collection = self._db[table]
            result = collection.insert_many(data)
            
            return QueryResult(
                data=[{"_id": id} for id in result.inserted_ids],
                affected_rows=len(result.inserted_ids),
                execution_time=(time.time() - start_time) * 1000
            )
        except Exception as e:
            raise QueryError(f"Insert many failed: {e}")
    
    def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Update documents."""
        start_time = time.time()
        
        try:
            collection = self._db[table]
            filter_query = where or {}
            update_query = {"$set": data}
            
            result = collection.update_many(filter_query, update_query)
            
            return QueryResult(
                affected_rows=result.modified_count,
                execution_time=(time.time() - start_time) * 1000
            )
        except Exception as e:
            raise QueryError(f"Update failed: {e}")
    
    def delete(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Delete documents."""
        start_time = time.time()
        
        try:
            collection = self._db[table]
            filter_query = where or {}
            
            result = collection.delete_many(filter_query)
            
            return QueryResult(
                affected_rows=result.deleted_count,
                execution_time=(time.time() - start_time) * 1000
            )
        except Exception as e:
            raise QueryError(f"Delete failed: {e}")
    
    def find(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> QueryResult:
        """Find documents."""
        start_time = time.time()
        
        try:
            collection = self._db[table]
            filter_query = where or {}
            
            # Projection
            projection = None
            if columns:
                projection = {col: 1 for col in columns}
            
            cursor = collection.find(filter_query, projection)
            
            # Sort
            if order_by:
                # Parse "field DESC" or "field ASC"
                parts = order_by.split()
                field = parts[0]
                direction = -1 if len(parts) > 1 and parts[1].upper() == "DESC" else 1
                cursor = cursor.sort(field, direction)
            
            # Pagination
            if offset:
                cursor = cursor.skip(offset)
            if limit:
                cursor = cursor.limit(limit)
            
            # Convert to list
            data = []
            for doc in cursor:
                # Convert ObjectId to string
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                data.append(doc)
            
            return QueryResult(
                data=data,
                execution_time=(time.time() - start_time) * 1000
            )
        except Exception as e:
            raise QueryError(f"Find failed: {e}")
    
    def aggregate(
        self, 
        table: str, 
        pipeline: List[Dict[str, Any]]
    ) -> QueryResult:
        """
        Run aggregation pipeline.
        
        Example:
            result = db.aggregate("orders", [
                {"$match": {"status": "completed"}},
                {"$group": {
                    "_id": "$customer_id",
                    "total": {"$sum": "$amount"}
                }},
                {"$sort": {"total": -1}},
                {"$limit": 10}
            ])
        """
        start_time = time.time()
        
        try:
            collection = self._db[table]
            cursor = collection.aggregate(pipeline)
            
            data = []
            for doc in cursor:
                if "_id" in doc and hasattr(doc["_id"], "__str__"):
                    doc["_id"] = str(doc["_id"])
                data.append(doc)
            
            return QueryResult(
                data=data,
                execution_time=(time.time() - start_time) * 1000
            )
        except Exception as e:
            raise QueryError(f"Aggregation failed: {e}")
    
    def count(
        self, 
        table: str, 
        where: Optional[Dict[str, Any]] = None
    ) -> int:
        """Count documents."""
        collection = self._db[table]
        return collection.count_documents(where or {})
    
    def create_index(
        self, 
        table: str, 
        keys: Union[str, List[tuple]],
        unique: bool = False
    ) -> str:
        """Create index on collection."""
        collection = self._db[table]
        return collection.create_index(keys, unique=unique)
    
    def begin_transaction(self) -> None:
        """Start session/transaction."""
        self._session = self._client.start_session()
        self._session.start_transaction()
        self._in_transaction = True
    
    def commit(self) -> None:
        """Commit transaction."""
        if self._session:
            self._session.commit_transaction()
            self._session.end_session()
            self._session = None
        self._in_transaction = False
    
    def rollback(self) -> None:
        """Abort transaction."""
        if self._session:
            self._session.abort_transaction()
            self._session.end_session()
            self._session = None
        self._in_transaction = False
    
    def get_tables(self) -> List[str]:
        """Get collection names."""
        return self._db.list_collection_names()
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get sample document fields (schema-less)."""
        collection = self._db[table]
        sample = collection.find_one()
        
        if not sample:
            return []
        
        return [
            {"column_name": key, "data_type": type(value).__name__}
            for key, value in sample.items()
        ]
    
    def table_exists(self, table: str) -> bool:
        """Check if collection exists."""
        return table in self._db.list_collection_names()
