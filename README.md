<div align="center">

# 🗄️ UnifyDB

### One Library to Rule Them All Databases

**Unified Python interface for 15+ database systems with optional web dashboard**

[![PyPI version](https://img.shields.io/pypi/v/unifydb?color=blue&label=PyPI)](https://pypi.org/project/unifydb/)
[![Python](https://img.shields.io/pypi/pyversions/unifydb?color=green)](https://python.org)
[![Downloads](https://img.shields.io/pypi/dm/unifydb?color=orange)](https://pypi.org/project/unifydb/)
[![License](https://img.shields.io/badge/license-MIT-purple)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen)](tests/)
[![Documentation](https://img.shields.io/badge/docs-available-brightgreen)](https://docs.unifydb.org)

[Installation](#-installation) •
[Quick Start](#-quick-start) •
[Examples](#-examples) •
[Web Panel](#-web-panel) •
[API Reference](#-api-reference) •
[FAQ](#-faq)

</div>

---

## ✨ Why UnifyDB?

```python
# ❌ Without UnifyDB - different syntax for each database
import psycopg2      # PostgreSQL
import pymongo       # MongoDB  
import redis         # Redis
import cx_Oracle     # Oracle
# ... and so on, each with its own API

# ✅ With UnifyDB - unified interface
from unifydb import Database

db = Database.connect("postgresql://...")  # or mysql, mongodb, redis...
db.insert("users", {"name": "John"})       # Same for all databases!
```

### 🎯 Key Features

| Feature | Description |
|---------|-------------|
| 🔌 **15+ Databases** | SQL, NoSQL, Graph, TimeSeries - all in one |
| 🎯 **Unified API** | Learn once - use everywhere |
| 📦 **Modular** | Install only needed database drivers |
| 🌐 **Web Dashboard** | Visual management for all databases |
| ⚡ **Async Support** | For high-performance applications |
| 🛡️ **Type Hints** | Full type hints support |
| 🔄 **Query Builder** | Programmatic query construction |
| 🔒 **Security** | Built-in protection against SQL injection |
| 📊 **Connection Pool** | Efficient connection management |

---

## 📦 Installation

### Basic Installation

```bash
# Core library (without database drivers)
pip install unifydb
```

### Installation with Drivers

```bash
# Single database
pip install unifydb[postgresql]
pip install unifydb[mysql]
pip install unifydb[mongodb]
pip install unifydb[redis]
pip install unifydb[sqlite]      # Already built-in with Python!

# Multiple databases
pip install unifydb[postgresql,mongodb,redis]

# All databases (without web panel)
pip install unifydb[all]

# 🌐 Web Panel (installed SEPARATELY)
pip install unifydb[web]

# Everything
pip install unifydb[full]
```

### Installation Table

| Database | Command | Driver |
|----------|---------|--------|
| PostgreSQL | `pip install unifydb[postgresql]` | psycopg2, asyncpg |
| MySQL | `pip install unifydb[mysql]` | mysql-connector, PyMySQL |
| SQLite | Built-in | sqlite3 |
| MongoDB | `pip install unifydb[mongodb]` | pymongo, motor |
| Redis | `pip install unifydb[redis]` | redis-py |
| MariaDB | `pip install unifydb[mariadb]` | mariadb |
| MS SQL Server | `pip install unifydb[mssql]` | pyodbc, pymssql |
| Oracle | `pip install unifydb[oracle]` | cx_Oracle, oracledb |
| Elasticsearch | `pip install unifydb[elasticsearch]` | elasticsearch |
| Cassandra | `pip install unifydb[cassandra]` | cassandra-driver |
| DynamoDB | `pip install unifydb[dynamodb]` | boto3 |
| Snowflake | `pip install unifydb[snowflake]` | snowflake-connector |
| BigQuery | `pip install unifydb[bigquery]` | google-cloud-bigquery |
| Neo4j | `pip install unifydb[neo4j]` | neo4j |
| IBM Db2 | `pip install unifydb[db2]` | ibm_db |

---

## 🚀 Quick Start

### Database Connection

```python
from unifydb import Database

# 🐘 PostgreSQL
db = Database.connect("postgresql://user:password@localhost:5432/mydb")

# 🐬 MySQL
db = Database.connect("mysql://user:password@localhost:3306/mydb")

# 🍃 MongoDB
db = Database.connect("mongodb://localhost:27017/mydb")

# 🔴 Redis
db = Database.connect("redis://localhost:6379/0")

# 📁 SQLite
db = Database.connect("sqlite:///path/to/database.db")
db = Database.connect("sqlite:///:memory:")  # In-memory database

# 🔷 Microsoft SQL Server
db = Database.connect("mssql://user:password@localhost:1433/mydb")

# 🔶 Oracle
db = Database.connect("oracle://user:password@localhost:1521/mydb")
```

### Alternative Connection Methods

```python
from unifydb import Database, PostgreSQL, MongoDB

# Via parameters
db = Database.connect(
    type="postgresql",
    host="localhost",
    port=5432,
    database="mydb",
    user="admin",
    password="secret",
    ssl=True
)

# Directly through adapter
db = PostgreSQL(
    host="localhost",
    database="mydb",
    user="admin",
    password="secret"
)
db.connect()
```

### Context Manager (Recommended)

```python
from unifydb import Database

# Automatic connection management
with Database.connect("postgresql://localhost/mydb") as db:
    users = db.find("users")
    print(f"Found {len(users)} users")
# Connection automatically closed
```

---

## 📖 Examples

### 🔹 CRUD Operations

#### Create

```python
from unifydb import Database

db = Database.connect("postgresql://localhost/mydb")

# Insert single record
result = db.insert("users", {
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30,
    "active": True
})
print(f"Inserted ID: {result.last_id}")

# Insert multiple records
users = [
    {"name": "Alice", "email": "alice@example.com", "age": 25},
    {"name": "Bob", "email": "bob@example.com", "age": 35},
    {"name": "Charlie", "email": "charlie@example.com", "age": 28},
]
result = db.insert_many("users", users)
print(f"Inserted {result.affected_rows} users")
```

#### Read

```python
# Find all records
all_users = db.find("users")
for user in all_users:
    print(f"{user['name']} - {user['email']}")

# Find with conditions
active_users = db.find("users", where={"active": True})

# Find with selected columns
emails = db.find("users", columns=["name", "email"])

# Find with sorting and limit
recent_users = db.find(
    "users",
    order_by="created_at DESC",
    limit=10
)

# Find with pagination
page_2 = db.find(
    "users",
    limit=20,
    offset=20  # Skip first 20
)

# Find single record
user = db.find_one("users", where={"id": 1})
if user:
    print(f"Found: {user['name']}")

# Get scalar value
count = db.fetch_scalar("SELECT COUNT(*) FROM users")
print(f"Total users: {count}")
```

#### Update

```python
# Update with conditions
result = db.update(
    "users",
    data={"active": False, "updated_at": "NOW()"},
    where={"id": 1}
)
print(f"Updated {result.affected_rows} rows")

# Update multiple records
db.update(
    "users",
    data={"verified": True},
    where={"email_confirmed": True}
)

# Update all (use with caution!)
db.update("products", data={"in_stock": True})
```

#### Delete

```python
# Delete with conditions
result = db.delete("users", where={"id": 1})
print(f"Deleted {result.affected_rows} rows")

# Delete multiple
db.delete("sessions", where={"expired": True})

# Delete all (use with caution!)
db.delete("temp_data")
```

---

### 🔹 Raw SQL Queries

```python
from unifydb import Database

db = Database.connect("postgresql://localhost/mydb")

# SELECT query
result = db.execute("""
    SELECT u.name, COUNT(o.id) as order_count
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.active = %s
    GROUP BY u.id
    HAVING COUNT(o.id) > %s
    ORDER BY order_count DESC
""", (True, 5))

for row in result:
    print(f"{row['name']}: {row['order_count']} orders")

# INSERT with RETURNING
result = db.execute("""
    INSERT INTO users (name, email) 
    VALUES (%s, %s) 
    RETURNING id, created_at
""", ("New User", "new@example.com"))

print(f"New user ID: {result.first['id']}")

# Result metadata
print(f"Columns: {result.columns}")
print(f"Rows affected: {result.affected_rows}")
print(f"Execution time: {result.execution_time}ms")
```

---

### 🔹 Query Builder

```python
from unifydb import Query, Operator

# Simple query
query = Query("users").select("id", "name", "email")
sql, params = query.to_sql()
# SELECT id, name, email FROM users

# With conditions
query = (Query("users")
    .select("*")
    .where("age", ">", 18)
    .where("status", "active")
    .where("country", Operator.IN, ["US", "UK", "CA"])
)
# SELECT * FROM users WHERE age > %s AND status = %s AND country IN (%s, %s, %s)

# OR conditions
query = (Query("products")
    .select("name", "price")
    .where("category", "electronics")
    .or_where("category", "computers")
    .where("price", "<", 1000)
)

# LIKE search
query = (Query("articles")
    .select("title", "content")
    .like("title", "%python%")
    .where_not_null("published_at")
)

# BETWEEN
query = (Query("orders")
    .select("*")
    .where_between("created_at", "2024-01-01", "2024-12-31")
    .where("status", "completed")
)

# JOIN
query = (Query("orders")
    .select("orders.id", "users.name", "orders.total")
    .join("users", "orders.user_id = users.id")
    .left_join("discounts", "orders.discount_id = discounts.id")
    .where("orders.total", ">", 100)
)

# GROUP BY and HAVING
query = (Query("orders")
    .select("user_id", "SUM(total) as total_spent")
    .group_by("user_id")
    .having("SUM(total)", ">", 1000)
    .order_by("total_spent", "DESC")
)

# Pagination
query = (Query("products")
    .select("*")
    .order_by("created_at", "DESC")
    .paginate(page=3, per_page=20)  # Page 3, 20 per page
)

# Distinct
query = Query("logs").select("user_id").distinct()

# Execute query
sql, params = query.to_sql()
result = db.execute(sql, params)

# For MongoDB
mongo_filter, mongo_options = query.to_mongo()
```

---

### 🔹 Transactions

```python
from unifydb import Database

db = Database.connect("postgresql://localhost/mydb")

# ✅ Method 1: Context Manager (recommended)
try:
    with db.transaction():
        # Debit from account
        db.execute(
            "UPDATE accounts SET balance = balance - %s WHERE id = %s",
            (100, 1)
        )
        
        # Credit to another account
        db.execute(
            "UPDATE accounts SET balance = balance + %s WHERE id = %s",
            (100, 2)
        )
        
        # Record in history
        db.insert("transactions", {
            "from_account": 1,
            "to_account": 2,
            "amount": 100,
            "type": "transfer"
        })
        
        # If successful - automatic COMMIT
except Exception as e:
    # Automatic ROLLBACK on error
    print(f"Transaction failed: {e}")

# ✅ Method 2: Manual control
db.begin_transaction()
try:
    db.insert("orders", {"user_id": 1, "total": 99.99})
    db.update("inventory", {"stock": 10}, where={"product_id": 5})
    
    # Condition check
    stock = db.fetch_scalar(
        "SELECT stock FROM inventory WHERE product_id = %s", (5,)
    )
    if stock < 0:
        raise ValueError("Insufficient stock!")
    
    db.commit()
    print("Order placed successfully!")
    
except Exception as e:
    db.rollback()
    print(f"Order failed: {e}")
```

---

### 🔹 Working with Multiple Databases

```python
from unifydb import DatabaseManager, Database

# Create manager
manager = DatabaseManager()

# Add connections
manager.add("primary", "postgresql://localhost/main", default=True)
manager.add("replica", "postgresql://replica.host/main")
manager.add("cache", "redis://localhost:6379/0")
manager.add("analytics", "mongodb://localhost/analytics")
manager.add("search", "elasticsearch://localhost:9200")

# Use
users = manager["primary"].find("users", where={"active": True})

# Cache in Redis
for user in users:
    manager["cache"].set(f"user:{user['id']}", user, expire=3600)

# Log to MongoDB
manager["analytics"].insert("user_queries", {
    "query": "active_users",
    "count": len(users),
    "timestamp": "2024-01-15T10:30:00Z"
})

# Index in Elasticsearch
for user in users:
    manager["search"].insert("users", {
        "id": user["id"],
        "name": user["name"],
        "email": user["email"]
    })

# Close all connections
manager.close_all()

# Or through context manager
with DatabaseManager() as manager:
    manager.add("db", "postgresql://localhost/mydb")
    # work...
# Everything automatically closes
```

---

### 🔹 Database-Specific Examples

#### 🐘 PostgreSQL

```python
from unifydb import PostgreSQL

db = PostgreSQL(
    host="localhost",
    port=5432,
    database="myapp",
    user="postgres",
    password="secret",
    ssl=True
)
db.connect()

# JSON fields
db.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        metadata JSONB
    )
""")

db.insert("products", {
    "name": "Laptop",
    "metadata": '{"brand": "Apple", "specs": {"ram": 16, "storage": 512}}'
})

# JSONB queries
result = db.execute("""
    SELECT * FROM products 
    WHERE metadata->>'brand' = %s
""", ("Apple",))

# Full-text search
result = db.execute("""
    SELECT * FROM articles 
    WHERE to_tsvector('english', content) @@ plainto_tsquery('english', %s)
""", ("python programming",))

# Get tables and columns
tables = db.get_tables()
columns = db.get_columns("users")
```

#### 🍃 MongoDB

```python
from unifydb import MongoDB

db = MongoDB(
    host="localhost",
    port=27017,
    database="myapp"
)
db.connect()

# Insert document
db.insert("users", {
    "name": "John",
    "email": "john@example.com",
    "profile": {
        "age": 30,
        "interests": ["coding", "gaming"]
    },
    "tags": ["premium", "verified"]
})

# Nested queries
users = db.find("users", where={
    "profile.age": {"$gte": 25},
    "tags": {"$in": ["premium"]}
})

# Aggregation
result = db.aggregate("orders", [
    {"$match": {"status": "completed"}},
    {"$group": {
        "_id": "$customer_id",
        "total_spent": {"$sum": "$amount"},
        "order_count": {"$sum": 1}
    }},
    {"$sort": {"total_spent": -1}},
    {"$limit": 10}
])

for doc in result:
    print(f"Customer {doc['_id']}: ${doc['total_spent']}")

# Create index
db.create_index("users", [("email", 1)], unique=True)
db.create_index("products", [("name", "text")])  # Text index

# Count documents
count = db.count("users", where={"active": True})
```

#### 🔴 Redis

```python
from unifydb import Redis

db = Redis(host="localhost", port=6379, database=0)
db.connect()

# Strings
db.set("user:session:123", "active", expire=3600)
session = db.get("user:session:123")

# JSON data
db.set("config", {"theme": "dark", "lang": "en"})
config = db.get_json("config")

# Hashes (great for objects)
db.hset("user:1", {
    "name": "John",
    "email": "john@example.com",
    "visits": "42"
})
user = db.hgetall("user:1")
name = db.hget("user:1", "name")

# Lists (queues, stacks)
db.rpush("queue:emails", "email1", "email2", "email3")
email = db.lpop("queue:emails")  # Get and remove first
all_emails = db.lrange("queue:emails", 0, -1)  # All elements

# Sets (unique values)
db.sadd("user:1:tags", "premium", "verified", "active")
db.sadd("user:2:tags", "basic", "active")
tags = db.smembers("user:1:tags")

# Intersection
common_tags = db.sinter("user:1:tags", "user:2:tags")

# Sorted Sets (rankings, leaderboards)
db.zadd("leaderboard", {
    "player1": 1500,
    "player2": 2300,
    "player3": 1800
})
top_3 = db.zrange("leaderboard", 0, 2, withscores=True)

# TTL and existence
if db.exists("user:session:123"):
    ttl = db.ttl("user:session:123")
    print(f"Session expires in {ttl} seconds")

# Delete
db.delete("old:key", "another:old:key")

# Pattern search
user_keys = db.keys("user:*")

# Server info
info = db.info()
print(f"Redis version: {info['redis_version']}")
print(f"Used memory: {info['used_memory_human']}")
```

#### 📁 SQLite

```python
from unifydb import SQLite

# File database
db = SQLite(database="myapp.db")
db.connect()

# Or in-memory (for testing)
db = SQLite(database=":memory:")
db.connect()

# Create tables
db.execute("""
    CREATE TABLE IF NOT EXISTS notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        content TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# CRUD operations
db.insert("notes", {"title": "My Note", "content": "Hello World"})

notes = db.find("notes", order_by="created_at DESC", limit=10)

# Check existence
if db.table_exists("notes"):
    columns = db.get_columns("notes")
    for col in columns:
        print(f"{col['column_name']}: {col['data_type']}")
```

#### 🔷 Elasticsearch

```python
from unifydb import Elasticsearch

db = Elasticsearch(host="localhost", port=9200)
db.connect()

# Index document
db.insert("products", {
    "id": "1",
    "name": "iPhone 15 Pro",
    "description": "Latest Apple smartphone with A17 chip",
    "price": 999,
    "category": "electronics",
    "tags": ["apple", "smartphone", "premium"]
})

# Full-text search
result = db.search("products", {
    "query": {
        "multi_match": {
            "query": "apple smartphone",
            "fields": ["name", "description", "tags"]
        }
    }
})

# Filter and sort
result = db.search("products", {
    "query": {
        "bool": {
            "must": [
                {"match": {"category": "electronics"}}
            ],
            "filter": [
                {"range": {"price": {"lte": 1000}}}
            ]
        }
    },
    "sort": [{"price": "asc"}]
})

# Aggregations
result = db.search("products", {
    "size": 0,
    "aggs": {
        "by_category": {
            "terms": {"field": "category.keyword"},
            "aggs": {
                "avg_price": {"avg": {"field": "price"}}
            }
        }
    }
})
```

#### 🔷 Neo4j (Graph Database)

```python
from unifydb import Neo4j

db = Neo4j(
    host="localhost",
    port=7687,
    user="neo4j",
    password="password"
)
db.connect()

# Create nodes
db.execute("""
    CREATE (john:Person {name: 'John', age: 30})
    CREATE (jane:Person {name: 'Jane', age: 28})
    CREATE (company:Company {name: 'TechCorp'})
""")

# Create relationships
db.execute("""
    MATCH (john:Person {name: 'John'})
    MATCH (jane:Person {name: 'Jane'})
    CREATE (john)-[:KNOWS {since: 2020}]->(jane)
""")

db.execute("""
    MATCH (john:Person {name: 'John'})
    MATCH (company:Company {name: 'TechCorp'})
    CREATE (john)-[:WORKS_AT {role: 'Developer'}]->(company)
""")

# Queries
result = db.execute("""
    MATCH (p:Person)-[:WORKS_AT]->(c:Company)
    WHERE c.name = $company
    RETURN p.name as name, p.age as age
""", {"company": "TechCorp"})

# Path finding
result = db.execute("""
    MATCH path = shortestPath(
        (a:Person {name: 'John'})-[*]-(b:Person {name: 'Jane'})
    )
    RETURN path
""")

# Recommendations (friends of friends)
result = db.execute("""
    MATCH (me:Person {name: 'John'})-[:KNOWS]->(friend)-[:KNOWS]->(foaf)
    WHERE NOT (me)-[:KNOWS]->(foaf) AND me <> foaf
    RETURN DISTINCT foaf.name as recommended
    LIMIT 5
""")
```

---

### 🔹 Async Operations

```python
import asyncio
from unifydb import AsyncPostgreSQL, AsyncMongoDB

async def main():
    # PostgreSQL async
    db = AsyncPostgreSQL(
        host="localhost",
        database="mydb",
        user="postgres"
    )
    await db.connect_async()
    
    # Async queries
    result = await db.execute_async(
        "SELECT * FROM users WHERE active = $1",
        (True,)
    )
    
    # Async transaction
    async with db.transaction_async():
        await db.execute_async(
            "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
            (100, 1)
        )
        await db.execute_async(
            "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
            (100, 2)
        )
    
    await db.disconnect_async()

# Run
asyncio.run(main())
```

---

## 🌐 Web Panel

### Installation

```bash
# Web panel is installed SEPARATELY from [all]
pip install unifydb[web]
```

### Quick Start

```python
from unifydb import Database
from unifydb.web import run_server

# Start with auto-configuration
run_server(port=5000)
```

### Advanced Configuration

```python
from unifydb import Database, DatabaseManager
from unifydb.web import create_app

# Create Flask application
app = create_app()

# Configure connections
manager = DatabaseManager()
manager.add("production", "postgresql://prod.server/maindb")
manager.add("staging", "postgresql://staging.server/maindb")
manager.add("cache", "redis://cache.server:6379/0")
manager.add("analytics", "mongodb://analytics.server/data")

# Pass to application
app.config["UNIFYDB_CONNECTIONS"] = manager._connections

# Additional settings
app.config["SECRET_KEY"] = "your-secret-key"
app.config["UNIFYDB_READONLY"] = True  # Read-only mode

# Run
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
```

### Integration with Existing Flask App

```python
from flask import Flask
from unifydb import Database
from unifydb.web import create_app as create_unifydb_app

# Your main application
app = Flask(__name__)

@app.route("/")
def index():
    return "Main Application"

# Mount UnifyDB panel
unifydb_app = create_unifydb_app()
unifydb_app.config["UNIFYDB_CONNECTIONS"] = {
    "main": Database.connect("postgresql://localhost/mydb")
}

app.register_blueprint(unifydb_app, url_prefix="/admin/db")

# Now panel available at /admin/db/
```

### Web Panel Features

- 📊 **Dashboard** - Overview of all connections
- 📋 **Tables** - Browse tables/collections
- 🔍 **Query** - SQL/NoSQL editor with syntax highlighting
- 📈 **Stats** - Statistics and metrics
- 🔒 **Read-only mode** - Safe view-only access
- 📤 **Export** - Export data to CSV, JSON
- 🔄 **Real-time** - Live updates for some databases

---

## 📖 API Reference

### Database Class

```python
class Database:
    @classmethod
    def connect(uri: str = None, **kwargs) -> BaseAdapter:
        """Connect to database."""
    
    @classmethod
    def supported_databases() -> List[str]:
        """Get list of supported databases."""
```

### BaseAdapter Class (all adapters)

```python
class BaseAdapter:
    # Connection
    def connect() -> None
    def disconnect() -> None
    def is_connected() -> bool
    def ping() -> bool
    def reconnect() -> None
    
    # Queries
    def execute(query: str, params: tuple = None) -> QueryResult
    def execute_many(query: str, params_list: List) -> QueryResult
    def fetch_one(query: str, params: tuple = None) -> Optional[Dict]
    def fetch_all(query: str, params: tuple = None) -> List[Dict]
    def fetch_scalar(query: str, params: tuple = None) -> Any
    
    # CRUD
    def insert(table: str, data: Dict) -> QueryResult
    def insert_many(table: str, data: List[Dict]) -> QueryResult
    def update(table: str, data: Dict, where: Dict = None) -> QueryResult
    def delete(table: str, where: Dict = None) -> QueryResult
    def find(table: str, where: Dict = None, **options) -> QueryResult
    def find_one(table: str, where: Dict = None) -> Optional[Dict]
    
    # Transactions
    def begin_transaction() -> None
    def commit() -> None
    def rollback() -> None
    def transaction() -> ContextManager  # context manager
    
    # Schema
    def get_tables() -> List[str]
    def get_columns(table: str) -> List[Dict]
    def table_exists(table: str) -> bool
    
    # Information
    def get_info() -> Dict
```

### QueryResult Class

```python
class QueryResult:
    data: List[Dict]          # Results
    affected_rows: int        # Affected rows
    last_id: Any              # Last inserted ID
    columns: List[str]        # Column names
    execution_time: float     # Execution time (ms)
    
    @property
    def first(self) -> Optional[Dict]   # First row
    
    @property  
    def scalar(self) -> Any             # First value
    
    def __len__(self) -> int            # Row count
    def __iter__(self)                  # Iterate over rows
```

### Query Class (Query Builder)

```python
class Query:
    def __init__(table: str)
    
    def select(*columns: str) -> Query
    def distinct() -> Query
    def where(column: str, operator: str, value: Any) -> Query
    def or_where(column: str, operator: str, value: Any) -> Query
    def where_in(column: str, values: List) -> Query
    def where_null(column: str) -> Query
    def where_not_null(column: str) -> Query
    def where_between(column: str, start: Any, end: Any) -> Query
    def like(column: str, pattern: str) -> Query
    def join(table: str, on: str, type: JoinType = INNER) -> Query
    def left_join(table: str, on: str) -> Query
    def right_join(table: str, on: str) -> Query
    def group_by(*columns: str) -> Query
    def having(column: str, operator: str, value: Any) -> Query
    def order_by(column: str, direction: str = "ASC") -> Query
    def limit(count: int) -> Query
    def offset(count: int) -> Query
    def paginate(page: int, per_page: int = 10) -> Query
    
    def to_sql(placeholder: str = "%s") -> Tuple[str, List]
    def to_mongo() -> Tuple[Dict, Dict]
    def copy() -> Query
```

---

## ❓ FAQ

### How to choose driver when multiple available?

```python
from unifydb import PostgreSQL

# psycopg2 is used by default
db = PostgreSQL(host="localhost", database="mydb")

# For async use asyncpg
from unifydb import AsyncPostgreSQL
db = AsyncPostgreSQL(host="localhost", database="mydb")
```

### How to handle errors?

```python
from unifydb import Database
from unifydb.exceptions import (
    UnifyDBError,
    ConnectionError,
    QueryError,
    DriverNotInstalledError
)

try:
    db = Database.connect("postgresql://localhost/mydb")
    result = db.execute("SELECT * FROM users")
except ConnectionError as e:
    print(f"Cannot connect: {e}")
    print(f"Host: {e.details['host']}")
except QueryError as e:
    print(f"Query failed: {e}")
    print(f"Query: {e.details['query']}")
except DriverNotInstalledError as e:
    print(f"Install driver: {e.details['install_command']}")
except UnifyDBError as e:
    print(f"Database error: {e}")
```

### How to use connection pooling?

```python
from unifydb import PostgreSQL

db = PostgreSQL(
    host="localhost",
    database="mydb",
    pool_size=10,  # Pool size
    extra={
        "pool_min": 2,
        "pool_max": 20
    }
)
```

### Is ORM supported?

UnifyDB is **not an ORM**, but a universal database interface. For ORM functionality, use SQLAlchemy or Django ORM together with UnifyDB for specific tasks.

### Security: SQL injections?

Always use parameterized queries:

```python
# ✅ Safe
db.execute("SELECT * FROM users WHERE id = %s", (user_id,))
db.find("users", where={"id": user_id})

# ❌ UNSAFE - never do this!
db.execute(f"SELECT * FROM users WHERE id = {user_id}")
```

---

## 🐛 Troubleshooting

### Error: Driver not installed

```
DriverNotInstalledError: Driver 'psycopg2' is not installed.
Install with: pip install unifydb[postgresql]
```

**Solution:** Install required driver:
```bash
pip install unifydb[postgresql]
```

### Error: Connection refused

```
ConnectionError: Failed to connect to PostgreSQL
```

**Solution:** 
1. Check if database server is running
2. Check host, port, credentials
3. Check firewall/network settings

### Error: Timeout

```python
# Increase timeout
db = PostgreSQL(
    host="remote-host",
    timeout=60  # seconds
)
```

---

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing`)
5. Open Pull Request

### Development Setup

```bash
git clone https://github.com/yourusername/unifydb
cd unifydb
pip install -e ".[dev]"
pytest tests/
```

---

## 📜 Changelog

### v1.0.0 (2024-01-15)
- 🎉 First stable release
- ✅ 15 supported databases
- ✅ Web Dashboard
- ✅ Query Builder
- ✅ Async support
- ✅ Comprehensive documentation

---

## 📄 License

MIT License — free to use for both personal and commercial projects.

---

<div align="center">

**Made with ❤️ for the Python community**

[⬆ Back to Top](#-unifydb)

</div>
```
