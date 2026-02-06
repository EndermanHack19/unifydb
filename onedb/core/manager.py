"""
Database Manager - Main entry point for OneDB.
Auto-detects database type and creates appropriate adapter.
"""

from typing import Any, Dict, Optional, Type, Union
from urllib.parse import urlparse, parse_qs
import re

from .base import BaseAdapter, ConnectionConfig, DatabaseType
from ..exceptions import AdapterNotFoundError, DriverNotInstalledError


# Adapter registry
_ADAPTERS: Dict[str, Type[BaseAdapter]] = {}


def register_adapter(name: str):
    """Decorator to register adapter class."""
    def decorator(cls: Type[BaseAdapter]):
        _ADAPTERS[name.lower()] = cls
        return cls
    return decorator


class DatabaseManager:
    """
    Manages multiple database connections.
    
    Example:
        manager = DatabaseManager()
        manager.add("primary", "postgresql://localhost/mydb")
        manager.add("cache", "redis://localhost:6379/0")
        
        users = manager["primary"].find("users")
        manager["cache"].set("users_cache", users)
    """
    
    def __init__(self):
        self._connections: Dict[str, BaseAdapter] = {}
        self._default: Optional[str] = None
    
    def add(
        self, 
        name: str, 
        connection: Union[str, BaseAdapter, dict],
        default: bool = False
    ) -> BaseAdapter:
        """
        Add database connection.
        
        Args:
            name: Connection name
            connection: URI string, adapter instance, or config dict
            default: Set as default connection
            
        Returns:
            Database adapter
        """
        if isinstance(connection, str):
            adapter = Database.connect(connection)
        elif isinstance(connection, dict):
            adapter = Database.connect(**connection)
        else:
            adapter = connection
        
        self._connections[name] = adapter
        
        if default or self._default is None:
            self._default = name
        
        return adapter
    
    def get(self, name: Optional[str] = None) -> BaseAdapter:
        """Get connection by name."""
        name = name or self._default
        if name not in self._connections:
            raise KeyError(f"Connection '{name}' not found")
        return self._connections[name]
    
    def remove(self, name: str) -> None:
        """Remove and disconnect."""
        if name in self._connections:
            self._connections[name].disconnect()
            del self._connections[name]
    
    def close_all(self) -> None:
        """Close all connections."""
        for adapter in self._connections.values():
            adapter.disconnect()
        self._connections.clear()
    
    def __getitem__(self, name: str) -> BaseAdapter:
        return self.get(name)
    
    def __contains__(self, name: str) -> bool:
        return name in self._connections
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close_all()


class Database:
    """
    Main database factory class.
    
    Usage:
        # From URI
        db = Database.connect("postgresql://user:pass@localhost/mydb")
        
        # From parameters
        db = Database.connect(
            type="postgresql",
            host="localhost",
            database="mydb",
            user="user",
            password="pass"
        )
        
        # Using specific adapter
        from onedb import PostgreSQL
        db = PostgreSQL(host="localhost", database="mydb")
    """
    
    # URI scheme to adapter mapping
    SCHEME_MAP = {
        # PostgreSQL
        "postgresql": "postgresql",
        "postgres": "postgresql",
        "pg": "postgresql",
        
        # MySQL
        "mysql": "mysql",
        "mysql+pymysql": "mysql",
        
        # MariaDB  
        "mariadb": "mariadb",
        
        # SQLite
        "sqlite": "sqlite",
        "sqlite3": "sqlite",
        
        # MongoDB
        "mongodb": "mongodb",
        "mongodb+srv": "mongodb",
        
        # Redis
        "redis": "redis",
        "rediss": "redis",
        
        # Microsoft SQL Server
        "mssql": "mssql",
        "mssql+pyodbc": "mssql",
        "mssql+pymssql": "mssql",
        
        # Oracle
        "oracle": "oracle",
        "oracle+cx_oracle": "oracle",
        
        # Others
        "elasticsearch": "elasticsearch",
        "cassandra": "cassandra",
        "dynamodb": "dynamodb",
        "snowflake": "snowflake",
        "bigquery": "bigquery",
        "neo4j": "neo4j",
        "bolt": "neo4j",  # Neo4j protocol
        "db2": "db2",
        "ibm_db": "db2",
    }
    
    @classmethod
    def connect(
        cls,
        uri: Optional[str] = None,
        **kwargs
    ) -> BaseAdapter:
        """
        Connect to database.
        
        Args:
            uri: Database connection URI
            **kwargs: Connection parameters
            
        Returns:
            Database adapter instance
            
        Example:
            # PostgreSQL
            db = Database.connect("postgresql://user:pass@localhost:5432/mydb")
            
            # SQLite
            db = Database.connect("sqlite:///path/to/db.sqlite")
            
            # MongoDB
            db = Database.connect("mongodb://localhost:27017/mydb")
            
            # With parameters
            db = Database.connect(
                type="mysql",
                host="localhost",
                user="root",
                password="secret",
                database="mydb"
            )
        """
        if uri:
            return cls._connect_from_uri(uri)
        
        db_type = kwargs.pop("type", None)
        if not db_type:
            raise ValueError("Either 'uri' or 'type' parameter is required")
        
        return cls._get_adapter(db_type, **kwargs)
    
    @classmethod
    def _connect_from_uri(cls, uri: str) -> BaseAdapter:
        """Parse URI and connect."""
        # Handle SQLite special case
        if uri.startswith("sqlite"):
            return cls._connect_sqlite(uri)
        
        parsed = urlparse(uri)
        scheme = parsed.scheme.lower()
        
        # Get adapter name
        adapter_name = cls.SCHEME_MAP.get(scheme)
        if not adapter_name:
            raise AdapterNotFoundError(scheme)
        
        # Parse connection parameters
        config = ConnectionConfig(
            host=parsed.hostname or "localhost",
            port=parsed.port,
            database=parsed.path.lstrip("/") if parsed.path else None,
            user=parsed.username,
            password=parsed.password,
        )
        
        # Parse query parameters
        if parsed.query:
            query_params = parse_qs(parsed.query)
            for key, values in query_params.items():
                value = values[0] if len(values) == 1 else values
                if key == "ssl":
                    config.ssl = value.lower() in ("true", "1", "yes")
                else:
                    config.extra[key] = value
        
        return cls._get_adapter(adapter_name, config=config)
    
    @classmethod
    def _connect_sqlite(cls, uri: str) -> BaseAdapter:
        """Handle SQLite connection."""
        # sqlite:///path/to/db.sqlite or sqlite:///:memory:
        match = re.match(r"sqlite(?:3)?:///(.+)", uri)
        if match:
            path = match.group(1)
        else:
            path = ":memory:"
        
        return cls._get_adapter("sqlite", database=path)
    
    @classmethod
    def _get_adapter(cls, name: str, **kwargs) -> BaseAdapter:
        """Get and instantiate adapter."""
        name = name.lower()
        
        # Lazy import adapter
        adapter_class = cls._load_adapter(name)
        
        # Create and connect
        adapter = adapter_class(**kwargs)
        adapter.connect()
        
        return adapter
    
    @classmethod
    def _load_adapter(cls, name: str) -> Type[BaseAdapter]:
        """Load adapter class."""
        # Import mapping
        import_map = {
            "postgresql": ("onedb.adapters.postgresql", "PostgreSQL"),
            "mysql": ("onedb.adapters.mysql", "MySQL"),
            "mariadb": ("onedb.adapters.mariadb", "MariaDB"),
            "sqlite": ("onedb.adapters.sqlite", "SQLite"),
            "mongodb": ("onedb.adapters.mongodb", "MongoDB"),
            "redis": ("onedb.adapters.redis_db", "Redis"),
            "mssql": ("onedb.adapters.mssql", "MSSQL"),
            "oracle": ("onedb.adapters.oracle", "Oracle"),
            "elasticsearch": ("onedb.adapters.elasticsearch_db", "Elasticsearch"),
            "cassandra": ("onedb.adapters.cassandra_db", "Cassandra"),
            "dynamodb": ("onedb.adapters.dynamodb", "DynamoDB"),
            "snowflake": ("onedb.adapters.snowflake_db", "Snowflake"),
            "bigquery": ("onedb.adapters.bigquery", "BigQuery"),
            "neo4j": ("onedb.adapters.neo4j_db", "Neo4j"),
            "db2": ("onedb.adapters.db2", "DB2"),
        }
        
        if name not in import_map:
            raise AdapterNotFoundError(name)
        
        module_path, class_name = import_map[name]
        
        try:
            import importlib
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except ImportError as e:
            # Determine install command
            install_cmds = {
                "postgresql": "pip install onedb[postgresql]",
                "mysql": "pip install onedb[mysql]",
                "mongodb": "pip install onedb[mongodb]",
                "redis": "pip install onedb[redis]",
                "oracle": "pip install onedb[oracle]",
                "mssql": "pip install onedb[mssql]",
                "elasticsearch": "pip install onedb[elasticsearch]",
                "cassandra": "pip install onedb[cassandra]",
                "dynamodb": "pip install onedb[dynamodb]",
                "snowflake": "pip install onedb[snowflake]",
                "bigquery": "pip install onedb[bigquery]",
                "neo4j": "pip install onedb[neo4j]",
                "db2": "pip install onedb[db2]",
            }
            raise DriverNotInstalledError(
                name, 
                install_cmds.get(name, f"pip install onedb[{name}]")
            ) from e
    
    @classmethod
    def supported_databases(cls) -> list:
        """Get list of supported databases."""
        return list(set(cls.SCHEME_MAP.values()))
