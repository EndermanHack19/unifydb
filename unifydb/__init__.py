"""
UnifyDB - One library to rule them all databases.
Unified interface for 15+ database systems.

Usage:
    from unifydb import Database
    
    # Connect to any database
    db = Database.connect("postgresql://user:pass@localhost/mydb")
    
    # Or use specific adapter
    from unifydb import PostgreSQL
    db = PostgreSQL(host="localhost", database="mydb")

Install options:
    pip install unifydb                    # Core only
    pip install unifydb[postgresql]        # With PostgreSQL support
    pip install unifydb[mysql]             # With MySQL support
    pip install unifydb[mongodb]           # With MongoDB support
    pip install unifydb[all]               # All database drivers
"""

__version__ = "1.0.1"
__author__ = "EndermanHack19"
__license__ = "MIT"

# Core imports
from .core.base import BaseAdapter, ConnectionConfig, QueryResult, DatabaseType
from .core.query_builder import Query, QueryBuilder
from .core.manager import Database, DatabaseManager

# Exceptions
from .exceptions import (
    UnifyDBError,
    ConnectionError,
    QueryError,
    AdapterNotFoundError,
    DriverNotInstalledError,
    ValidationError,
    TransactionError,
)

# Optional: Connection pool
try:
    from .core.connection import ConnectionPool
except ImportError:
    ConnectionPool = None


def __getattr__(name: str):
    """Lazy import adapters."""
    adapters = {
        'Oracle': '.adapters.oracle',
        'MySQL': '.adapters.mysql',
        'MSSQL': '.adapters.mssql',
        'PostgreSQL': '.adapters.postgresql',
        'MongoDB': '.adapters.mongodb',
        'SQLite': '.adapters.sqlite',
        'Redis': '.adapters.redis_db',
        'DB2': '.adapters.db2',
        'Elasticsearch': '.adapters.elasticsearch_db',
        'Cassandra': '.adapters.cassandra_db',
        'MariaDB': '.adapters.mariadb',
        'DynamoDB': '.adapters.dynamodb',
        'Snowflake': '.adapters.snowflake_db',
        'BigQuery': '.adapters.bigquery',
        'Neo4j': '.adapters.neo4j_db',
    }
    
    if name in adapters:
        import importlib
        try:
            module = importlib.import_module(adapters[name], package='unifydb')
            return getattr(module, name)
        except ImportError as e:
            raise ImportError(
                f"Adapter '{name}' requires additional dependencies. "
                f"Install with: pip install unifydb[{name.lower()}]"
            ) from e
    
    raise AttributeError(f"module 'unifydb' has no attribute '{name}'")


__all__ = [
    # Version
    "__version__",
    # Core
    "Database",
    "DatabaseManager", 
    "BaseAdapter",
    "ConnectionConfig",
    "QueryResult",
    "DatabaseType",
    "Query",
    "QueryBuilder",
    "ConnectionPool",
    # Exceptions
    "UnifyDBError",
    "ConnectionError",
    "QueryError",
    "AdapterNotFoundError",
    "DriverNotInstalledError",
    "ValidationError",
    "TransactionError",
    # Adapters (lazy loaded)
    "Oracle",
    "MySQL",
    "MSSQL",
    "PostgreSQL",
    "MongoDB",
    "SQLite",
    "Redis",
    "DB2",
    "Elasticsearch",
    "Cassandra",
    "MariaDB",
    "DynamoDB",
    "Snowflake",
    "BigQuery",
    "Neo4j",
]
