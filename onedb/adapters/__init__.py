"""
Database adapters for OneDB.
Each adapter provides consistent interface for specific database.
"""

__all__ = [
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


def __getattr__(name: str):
    """Lazy load adapters on demand."""
    adapters = {
        "Oracle": ".oracle",
        "MySQL": ".mysql",
        "MSSQL": ".mssql",
        "PostgreSQL": ".postgresql",
        "MongoDB": ".mongodb",
        "SQLite": ".sqlite",
        "Redis": ".redis_db",
        "DB2": ".db2",
        "Elasticsearch": ".elasticsearch_db",
        "Cassandra": ".cassandra_db",
        "MariaDB": ".mariadb",
        "DynamoDB": ".dynamodb",
        "Snowflake": ".snowflake_db",
        "BigQuery": ".bigquery",
        "Neo4j": ".neo4j_db",
    }
    
    if name in adapters:
        import importlib
        module = importlib.import_module(adapters[name], package="onedb.adapters")
        return getattr(module, name)
    
    raise AttributeError(f"module 'onedb.adapters' has no attribute '{name}'")
