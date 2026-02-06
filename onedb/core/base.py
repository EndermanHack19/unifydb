"""
Base adapter class and connection configuration.
All database adapters inherit from BaseAdapter.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    Any, Dict, List, Optional, Union, 
    AsyncIterator, Iterator, TypeVar, Generic
)
from contextlib import contextmanager, asynccontextmanager
from enum import Enum
import logging

logger = logging.getLogger("onedb")

T = TypeVar("T")


class DatabaseType(Enum):
    """Supported database types."""
    ORACLE = "oracle"
    MYSQL = "mysql"
    MSSQL = "mssql"
    POSTGRESQL = "postgresql"
    MONGODB = "mongodb"
    SQLITE = "sqlite"
    REDIS = "redis"
    DB2 = "db2"
    ELASTICSEARCH = "elasticsearch"
    CASSANDRA = "cassandra"
    MARIADB = "mariadb"
    DYNAMODB = "dynamodb"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    NEO4J = "neo4j"


@dataclass
class ConnectionConfig:
    """
    Universal connection configuration.
    
    Attributes:
        host: Database host
        port: Database port
        database: Database name
        user: Username
        password: Password
        ssl: Enable SSL
        pool_size: Connection pool size
        timeout: Connection timeout in seconds
        extra: Additional driver-specific options
    """
    host: str = "localhost"
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    ssl: bool = False
    pool_size: int = 5
    timeout: int = 30
    extra: Dict[str, Any] = field(default_factory=dict)
    
    def to_uri(self, scheme: str) -> str:
        """Convert config to connection URI."""
        auth = ""
        if self.user:
            auth = f"{self.user}"
            if self.password:
                auth += f":{self.password}"
            auth += "@"
        
        host_port = self.host
        if self.port:
            host_port += f":{self.port}"
        
        db = f"/{self.database}" if self.database else ""
        
        return f"{scheme}://{auth}{host_port}{db}"


@dataclass
class QueryResult:
    """
    Universal query result container.
    
    Attributes:
        data: Query result data
        affected_rows: Number of affected rows
        last_id: Last inserted ID
        columns: Column names
        execution_time: Query execution time in ms
    """
    data: List[Dict[str, Any]] = field(default_factory=list)
    affected_rows: int = 0
    last_id: Optional[Any] = None
    columns: List[str] = field(default_factory=list)
    execution_time: float = 0.0
    
    def __len__(self) -> int:
        return len(self.data)
    
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self.data)
    
    def __getitem__(self, index: int) -> Dict[str, Any]:
        return self.data[index]
    
    @property
    def first(self) -> Optional[Dict[str, Any]]:
        """Get first row."""
        return self.data[0] if self.data else None
    
    @property
    def scalar(self) -> Optional[Any]:
        """Get first value of first row."""
        if self.data and self.columns:
            return self.data[0].get(self.columns[0])
        return None


class BaseAdapter(ABC):
    """
    Abstract base class for all database adapters.
    
    All adapters must implement these methods to ensure
    consistent API across different database systems.
    
    Example:
        class PostgreSQL(BaseAdapter):
            def connect(self):
                # Implementation
                pass
    """
    
    db_type: DatabaseType
    driver_name: str
    install_command: str
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        """
        Initialize adapter.
        
        Args:
            config: ConnectionConfig instance
            **kwargs: Direct connection parameters
        """
        if config:
            self.config = config
        else:
            self.config = ConnectionConfig(**kwargs)
        
        self._connection = None
        self._is_connected = False
        self._in_transaction = False
        
        logger.debug(f"Initialized {self.__class__.__name__} adapter")
    
    # ==================== Connection Methods ====================
    
    @abstractmethod
    def connect(self) -> None:
        """Establish database connection."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if connected to database."""
        pass
    
    def ping(self) -> bool:
        """Test database connection."""
        try:
            self.execute("SELECT 1")
            return True
        except Exception:
            return False
    
    def reconnect(self) -> None:
        """Reconnect to database."""
        self.disconnect()
        self.connect()
    
    # ==================== Query Methods ====================
    
    @abstractmethod
    def execute(
        self, 
        query: str, 
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        """
        Execute a query.
        
        Args:
            query: SQL query or command
            params: Query parameters
            
        Returns:
            QueryResult with data and metadata
        """
        pass
    
    @abstractmethod
    def execute_many(
        self, 
        query: str, 
        params_list: List[Union[tuple, dict]]
    ) -> QueryResult:
        """Execute query with multiple parameter sets."""
        pass
    
    def fetch_one(
        self, 
        query: str, 
        params: Optional[Union[tuple, dict]] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch single row."""
        result = self.execute(query, params)
        return result.first
    
    def fetch_all(
        self, 
        query: str, 
        params: Optional[Union[tuple, dict]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all rows."""
        result = self.execute(query, params)
        return result.data
    
    def fetch_scalar(
        self, 
        query: str, 
        params: Optional[Union[tuple, dict]] = None
    ) -> Optional[Any]:
        """Fetch single value."""
        result = self.execute(query, params)
        return result.scalar
    
    # ==================== CRUD Methods ====================
    
    @abstractmethod
    def insert(
        self, 
        table: str, 
        data: Dict[str, Any]
    ) -> QueryResult:
        """Insert single record."""
        pass
    
    @abstractmethod
    def insert_many(
        self, 
        table: str, 
        data: List[Dict[str, Any]]
    ) -> QueryResult:
        """Insert multiple records."""
        pass
    
    @abstractmethod
    def update(
        self, 
        table: str, 
        data: Dict[str, Any], 
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Update records."""
        pass
    
    @abstractmethod
    def delete(
        self, 
        table: str, 
        where: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Delete records."""
        pass
    
    @abstractmethod
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
        pass
    
    def find_one(
        self, 
        table: str, 
        where: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """Find single record."""
        result = self.find(table, where, columns, limit=1)
        return result.first
    
    # ==================== Transaction Methods ====================
    
    @abstractmethod
    def begin_transaction(self) -> None:
        """Start a transaction."""
        pass
    
    @abstractmethod
    def commit(self) -> None:
        """Commit current transaction."""
        pass
    
    @abstractmethod
    def rollback(self) -> None:
        """Rollback current transaction."""
        pass
    
    @contextmanager
    def transaction(self):
        """
        Transaction context manager.
        
        Example:
            with db.transaction():
                db.insert("users", {"name": "John"})
                db.insert("logs", {"action": "user_created"})
        """
        self.begin_transaction()
        try:
            yield self
            self.commit()
        except Exception as e:
            self.rollback()
            raise e
    
    # ==================== Schema Methods ====================
    
    @abstractmethod
    def get_tables(self) -> List[str]:
        """Get list of tables."""
        pass
    
    @abstractmethod
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get column info for table."""
        pass
    
    @abstractmethod
    def table_exists(self, table: str) -> bool:
        """Check if table exists."""
        pass
    
    # ==================== Utility Methods ====================
    
    def get_info(self) -> Dict[str, Any]:
        """Get database connection info."""
        return {
            "type": self.db_type.value,
            "driver": self.driver_name,
            "host": self.config.host,
            "port": self.config.port,
            "database": self.config.database,
            "connected": self.is_connected(),
        }
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"host={self.config.host!r}, "
            f"database={self.config.database!r})"
        )


class AsyncBaseAdapter(BaseAdapter):
    """
    Async version of base adapter.
    For databases that support async operations.
    """
    
    @abstractmethod
    async def connect_async(self) -> None:
        """Async connect."""
        pass
    
    @abstractmethod
    async def disconnect_async(self) -> None:
        """Async disconnect."""
        pass
    
    @abstractmethod
    async def execute_async(
        self, 
        query: str, 
        params: Optional[Union[tuple, dict]] = None
    ) -> QueryResult:
        """Async execute."""
        pass
    
    @asynccontextmanager
    async def transaction_async(self):
        """Async transaction context manager."""
        await self.begin_transaction_async()
        try:
            yield self
            await self.commit_async()
        except Exception as e:
            await self.rollback_async()
            raise e
    
    async def __aenter__(self):
        await self.connect_async()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect_async()
