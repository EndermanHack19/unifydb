"""
Redis Adapter.
Key-value store with support for various data structures.
"""

from typing import Any, Dict, List, Optional, Union
import json
import time

from ..core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from ..exceptions import ConnectionError, QueryError, DriverNotInstalledError


class Redis(BaseAdapter):
    """
    Redis database adapter.
    
    Usage:
        db = Redis(host="localhost", port=6379, database=0)
        db.connect()
        
        # String operations
        db.set("key", "value")
        value = db.get("key")
        
        # Hash operations
        db.hset("user:1", {"name": "John", "age": 30})
        user = db.hgetall("user:1")
        
        # List operations
        db.lpush("queue", "item1", "item2")
        items = db.lrange("queue", 0, -1)
        
        # Set operations
        db.sadd("tags", "python", "redis")
        tags = db.smembers("tags")
    
    Install:
        pip install unifydb[redis]
    """
    
    db_type = DatabaseType.REDIS
    driver_name = "redis"
    install_command = "pip install unifydb[redis]"
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        
        if self.config.port is None:
            self.config.port = 6379
        if self.config.database is None:
            self.config.database = "0"
    
    def _import_driver(self):
        """Import redis driver."""
        try:
            import redis
            return redis
        except ImportError:
            raise DriverNotInstalledError("redis", self.install_command)
    
    def connect(self) -> None:
        """Connect to Redis."""
        redis_lib = self._import_driver()
        
        try:
            self._connection = redis_lib.Redis(
                host=self.config.host,
                port=self.config.port,
                db=int(self.config.database or 0),
                password=self.config.password,
                socket_timeout=self.config.timeout,
                decode_responses=True,
                **self.config.extra
            )
            
            # Test connection
            self._connection.ping()
            self._is_connected = True
            
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Redis: {e}",
                host=self.config.host,
                port=self.config.port
            )
    
    def disconnect(self) -> None:
        """Close connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
        self._is_connected = False
    
    def is_connected(self) -> bool:
        """Check connection."""
        try:
            return self._connection and self._connection.ping()
        except Exception:
            return False
    
    # ==================== String Operations ====================
    
    def set(
        self, 
        key: str, 
        value: Any, 
        expire: Optional[int] = None
    ) -> bool:
        """Set string value."""
        if not isinstance(value, str):
            value = json.dumps(value)
        return self._connection.set(key, value, ex=expire)
    
    def get(self, key: str) -> Optional[str]:
        """Get string value."""
        return self._connection.get(key)
    
    def get_json(self, key: str) -> Optional[Any]:
        """Get and parse JSON value."""
        value = self.get(key)
        return json.loads(value) if value else None
    
    def delete(self, *keys: str) -> int:
        """Delete keys."""
        return self._connection.delete(*keys)
    
    def exists(self, *keys: str) -> int:
        """Check if keys exist."""
        return self._connection.exists(*keys)
    
    def expire(self, key: str, seconds: int) -> bool:
        """Set key expiration."""
        return self._connection.expire(key, seconds)
    
    def ttl(self, key: str) -> int:
        """Get time to live."""
        return self._connection.ttl(key)
    
    def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern."""
        return self._connection.keys(pattern)
    
    # ==================== Hash Operations ====================
    
    def hset(self, name: str, mapping: Dict[str, Any]) -> int:
        """Set hash fields."""
        # Convert non-string values
        clean_mapping = {}
        for k, v in mapping.items():
            if isinstance(v, (dict, list)):
                v = json.dumps(v)
            clean_mapping[k] = v
        return self._connection.hset(name, mapping=clean_mapping)
    
    def hget(self, name: str, key: str) -> Optional[str]:
        """Get hash field."""
        return self._connection.hget(name, key)
    
    def hgetall(self, name: str) -> Dict[str, str]:
        """Get all hash fields."""
        return self._connection.hgetall(name)
    
    def hdel(self, name: str, *keys: str) -> int:
        """Delete hash fields."""
        return self._connection.hdel(name, *keys)
    
    # ==================== List Operations ====================
    
    def lpush(self, name: str, *values: Any) -> int:
        """Push to list (left)."""
        return self._connection.lpush(name, *values)
    
    def rpush(self, name: str, *values: Any) -> int:
        """Push to list (right)."""
        return self._connection.rpush(name, *values)
    
    def lpop(self, name: str) -> Optional[str]:
        """Pop from list (left)."""
        return self._connection.lpop(name)
    
    def rpop(self, name: str) -> Optional[str]:
        """Pop from list (right)."""
        return self._connection.rpop(name)
    
    def lrange(self, name: str, start: int, end: int) -> List[str]:
        """Get list range."""
        return self._connection.lrange(name, start, end)
    
    def llen(self, name: str) -> int:
        """Get list length."""
        return self._connection.llen(name)
    
    # ==================== Set Operations ====================
    
    def sadd(self, name: str, *values: Any) -> int:
        """Add to set."""
        return self._connection.sadd(name, *values)
    
    def srem(self, name: str, *values: Any) -> int:
        """Remove from set."""
        return self._connection.srem(name, *values)
    
    def smembers(self, name: str) -> set:
        """Get all set members."""
        return self._connection.smembers(name)
    
    def sismember(self, name: str, value: Any) -> bool:
        """Check set membership."""
        return self._connection.sismember(name, value)
    
    # ==================== Sorted Set Operations ====================
    
    def zadd(self, name: str, mapping: Dict[str, float]) -> int:
        """Add to sorted set."""
        return self._connection.zadd(name, mapping)
    
    def zrange(
        self, 
        name: str, 
        start: int, 
        end: int, 
        withscores: bool = False
    ) -> List:
        """Get sorted set range."""
        return self._connection.zrange(name, start, end, withscores=withscores)
    
    def zrank(self, name: str, value: str) -> Optional[int]:
        """Get rank in sorted set."""
        return self._connection.zrank(name, value)
    
    # ==================== Base Adapter Methods ====================
    
    def execute(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        """Execute Redis command."""
        start_time = time.time()
        
        try:
            parts = query.split()
            command = parts[0].upper()
            args = parts[1:] if len(parts) > 1 else []
            
            result = self._connection.execute_command(command, *args)
            
            data = []
            if result is not None:
                if isinstance(result, (list, set)):
                    data = [{"value": item} for item in result]
                elif isinstance(result, dict):
                    data = [result]
                else:
                    data = [{"value": result}]
            
            return QueryResult(
                data=data,
                execution_time=(time.time() - start_time) * 1000
            )
        except Exception as e:
            raise QueryError(str(e), query=query)
    
    def execute_many(self, query: str, params_list: List) -> QueryResult:
        """Execute multiple commands using pipeline."""
        start_time = time.time()
        
        pipe = self._connection.pipeline()
        for params in params_list:
            pipe.execute_command(query, *params)
        
        results = pipe.execute()
        
        return QueryResult(
            data=[{"result": r} for r in results],
            execution_time=(time.time() - start_time) * 1000
        )
    
    def insert(self, table: str, data: Dict[str, Any]) -> QueryResult:
        """Insert as hash (table:id pattern)."""
        key_id = data.get("id", str(time.time_ns()))
        key = f"{table}:{key_id}"
        
        self.hset(key, data)
        self.sadd(f"{table}:_keys", key)
        
        return QueryResult(data=[{"key": key}], last_id=key_id)
    
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> QueryResult:
        """Insert multiple as hashes."""
        keys = []
        for item in data:
            result = self.insert(table, item)
            keys.append(result.data[0]["key"])
        
        return QueryResult(data=[{"keys": keys}], affected_rows=len(keys))
    
    def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Update hash fields."""
        if where and "id" in where:
            key = f"{table}:{where['id']}"
            self.hset(key, data)
            return QueryResult(affected_rows=1)
        raise QueryError("Update requires 'id' in where clause for Redis")
    
    def delete(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Delete hash."""
        if where and "id" in where:
            key = f"{table}:{where['id']}"
            count = self._connection.delete(key)
            self.srem(f"{table}:_keys", key)
            return QueryResult(affected_rows=count)
        raise QueryError("Delete requires 'id' in where clause for Redis")
    
    def find(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> QueryResult:
        """Find hashes by pattern."""
        if where and "id" in where:
            key = f"{table}:{where['id']}"
            data = self.hgetall(key)
            return QueryResult(data=[data] if data else [])
        
        # Get all keys for table
        keys = self.smembers(f"{table}:_keys")
        data = []
        
        for key in keys:
            item = self.hgetall(key)
            if item:
                data.append(item)
        
        # Apply limit/offset
        if offset:
            data = data[offset:]
        if limit:
            data = data[:limit]
        
        return QueryResult(data=data)
    
    def begin_transaction(self) -> None:
        """Start pipeline (pseudo-transaction)."""
        self._pipeline = self._connection.pipeline()
        self._in_transaction = True
    
    def commit(self) -> None:
        """Execute pipeline."""
        if hasattr(self, "_pipeline"):
            self._pipeline.execute()
            self._pipeline = None
        self._in_transaction = False
    
    def rollback(self) -> None:
        """Discard pipeline."""
        if hasattr(self, "_pipeline"):
            self._pipeline.reset()
            self._pipeline = None
        self._in_transaction = False
    
    def get_tables(self) -> List[str]:
        """Get 'table' patterns (keys ending with :_keys)."""
        keys = self._connection.keys("*:_keys")
        return [k.replace(":_keys", "") for k in keys]
    
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get fields from sample hash."""
        keys = self.smembers(f"{table}:_keys")
        if keys:
            sample = self.hgetall(list(keys)[0])
            return [
                {"column_name": k, "data_type": "string"}
                for k in sample.keys()
            ]
        return []
    
    def table_exists(self, table: str) -> bool:
        """Check if table pattern exists."""
        return self._connection.exists(f"{table}:_keys") > 0
    
    # ==================== Redis-Specific Methods ====================
    
    def flushdb(self) -> bool:
        """Clear current database."""
        return self._connection.flushdb()
    
    def info(self) -> Dict[str, Any]:
        """Get Redis server info."""
        return self._connection.info()
    
    def dbsize(self) -> int:
        """Get number of keys."""
        return self._connection.dbsize()
