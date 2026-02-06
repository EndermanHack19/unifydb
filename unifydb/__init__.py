"""
UnifyDB - One library to rule them all databases.
Unified interface for 15+ database systems with optional web dashboard.

Usage:
    from unifydb import Database
    
    # Connect to any database
    db = Database.connect("postgresql://user:pass@localhost/mydb")
    
    # Or use specific adapter
    from unifydb.adapters import PostgreSQL
    db = PostgreSQL(host="localhost", database="mydb")

Install options:
    pip install unifydb                    # Core only
    pip install unifydb[postgresql]        # With PostgreSQL support
    pip install unifydb[all]               # All database drivers
    pip install unifydb[web]               # Web dashboard panel
    pip install unifydb[full]              # Everything
"""

__version__ = "1.0.0"
__author__ = "Your Name"
__license__ = "MIT"

from .core.manager import Database, DatabaseManager
from .core.base import BaseAdapter, ConnectionConfig
from .core.query_builder import Query, QueryBuilder
from .core.connection import ConnectionPool
from .exceptions import (
    UnifyDBError,
    ConnectionError,
    QueryError,
    AdapterNotFoundError,
    DriverNotInstalledError,
)

# Lazy loading of adapters
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
        module = importlib.import_module(adapters[name], package='unifydb')
        return getattr(module, name)
    
    raise AttributeError(f"module 'unifydb' has no attribute '{name}'")


__all__ = [
    # Core
    "Database",
    "DatabaseManager",
    "BaseAdapter",
    "ConnectionConfig",
    "Query",
    "QueryBuilder",
    "ConnectionPool",
    # Exceptions
    "UnifyDBError",
    "ConnectionError", 
    "QueryError",
    "AdapterNotFoundError",
    "DriverNotInstalledError",
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
