# LongJRM Database Operations Guide

## Overview

LongJRM is a **database-oriented** JSON Relational Mapping (JRM) library that provides a SQL-aligned interface for database operations. Unlike traditional object-oriented ORMs, LongJRM maintains a close relationship with database SQL concepts while providing JSON-based data handling and cross-database compatibility.

**Core Philosophy**: LongJRM operations are designed to mirror SQL database operations directly, making it intuitive for developers familiar with SQL while providing the flexibility of JSON data structures.

**GraphQL-Like Querying**: LongJRM's comprehensive `select` function provides GraphQL-style querying capabilities, allowing complex table queries with a single function call through flexible JSON-based conditions.

## Architecture

### Database-Oriented Design

LongJRM follows a database-centric approach where:

- **SQL Alignment**: All operations correspond directly to SQL operations (SELECT, INSERT, UPDATE, DELETE)
- **JSON Data Format**: Input/output uses JSON structures for column-value pairs
- **Cross-Database Support**: Unified interface across SQL and NoSQL databases
- **Raw Query Support**: Direct SQL execution alongside abstracted operations

### Core Component: Db Class

The `Db` class is the primary interface for all database operations:

```python
from longjrm.database.db import Db

# Initialize with a connection client
db = Db(client)
```

## Current Implementation Status

### ✅ Implemented Features

1. **Query Operations** - Raw SQL execution with parameter binding
2. **Select Operations** - JSON-based SELECT queries with comprehensive WHERE conditions
3. **Insert Operations** - Bulk and single record insertion with RETURNING support
4. **Update Operations** - Conditional record updates with flexible WHERE conditions
5. **Delete Operations** - Conditional record deletion with comprehensive filtering
6. **Merge Operations** - UPSERT operations with key-based conflict resolution
7. **GraphQL-Style Querying** - Single function comprehensive table queries
8. **Bulk Load Operations** - Native database bulk loading (COPY, ADMIN_CMD)
9. **Streaming Operations** - Memory-efficient data processing and export
10. **Partition Management** - DB2-specific partition maintenance methods

## Advanced Operations

### Bulk Load Operations

LongJRM supports native bulk loading for supported databases (PostgreSQL, DB2) and optimized batch inserts for others.

#### PostgreSQL Bulk Load

Uses PostgreSQL's `COPY` command for high-speed data loading.

```python
# 1. Load from file
load_info = {
    "source": "/path/to/data.csv",
    "format": "csv",
    "delimiter": ",",
    "header": True
}
db.bulk_load("target_table", load_info)

# 2. Load from memory (via file-like object)
import io
csv_data = io.StringIO("col1,col2\n1,test\n2,test2")
db.bulk_load("target_table", {"source": csv_data})
```

#### DB2 Bulk Load

Uses DB2's `ADMIN_CMD` with `LOAD` for efficient loading.

```python
load_info = {
    "source": "data.del",
    "filetype": "DEL",
    "operation": "REPLACE",  # or INSERT
    "warningcount": 100
}
db.bulk_load("target_table", load_info)
```

### Partition Management (DB2 Specific)

LongJRM provides methods to manage DB2 table partitions.

```python
# Add a new partition
db.add_partition(
    table="sales", 
    partition_name="p2024jan", 
    boundary="STARTING '2024-01-01' ENDING '2024-01-31'"
)

# Detach a partition into a separate table
db.detach_partition("sales", "p2023dec", "sales_archive_2023dec")

# Attach a table as a new partition
db.attach_partition("sales", "p2023dec", "STARTING '2023-12-01' ENDING '2023-12-31'", "sales_archive_2023dec")
```

### 3. Data Export

LongJRM provides robust data export capabilities, allowing you to stream query results directly to files efficiently.

#### Universal CSV Export (`stream_to_csv`)

The `stream_to_csv` method is the core function for exporting data. It uses streaming to handle large datasets with minimal memory footprint.

```python
# Export query results with header (default)
result = db.stream_to_csv(
    sql="SELECT * FROM users WHERE status = %s",
    csv_file="output/users.csv",
    values=["active"],
    options={
        "header": "Y",           # 'Y' to include header (default), 'N' for no header
        "null_value": "",        # String to use for NULLs (default: empty string)
        "quotechar": '"',        # Optional char to force quoting
        "abort_on_error": "Y"    # Abort export if row streaming fails
    }
)

if result['status'] == 0:
    print(f"Exported {result['row_count']} rows")
```

#### DB2-Specific Export (`export_admin_cmd`)

For DB2 databases, you can also use the native `EXPORT` utility via `ADMIN_CMD` for high-performance server-side exports.

```python
# DB2 Native Export
result = db.export_admin_cmd({
    "source": "SELECT * FROM huge_table",
    "target": "/server/path/export.ixf",
    "filetype": "IXF"
})
```

### 4. Streaming Operations

Streaming operations allow you to process large result sets row-by-row without loading the entire dataset into memory.

```python
# Stream Insert: Process large dataset with periodic commits
def data_generator():
    for i in range(1000000):
        yield {"id": i, "data": "value"}

# Insert generator data, committing every 10k rows
db.stream_insert(data_generator(), "large_table", commit_count=10000)

#### Stream Query Results

```python
# Stream rows one by one
for row_num, row_data, status in db.stream_query("SELECT * FROM big_table"):
    if status == 0:
        process(row_data)
    else:
        log_error(f"Row {row_num} failed")
```

## Database Client Integration

### Setting Up Database Operations

```python
from longjrm.config.runtime import get_config
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database import get_db

# 1. Initialize Pool from configuration
cfg = get_config()
pool = Pool.from_config(cfg.require("primary"), PoolBackend.SQLALCHEMY)

# 2. Use client context
with pool.client() as client:
    # 3. Get Db instance (automatically selects correct subclass)
    db = get_db(client)
    
    # 4. Perform operations
    result = db.select(table="users", columns=["id", "name"])

pool.dispose()
```

### Client Structure

The `Db` class expects a client dictionary with the following structure:

```python
client = {
    "conn": <database_connection_object>,
    "database_type": "postgres",  # postgres, mysql, sqlite, etc.
    "database_name": "mydb",
    "db_lib": "psycopg"  # Database driver library
}
```

## Query Operations

### Raw SQL Execution

Execute raw SQL queries with parameter binding for maximum flexibility:

```python
# Basic query execution
result = db.query("SELECT VERSION()")
print(result)
# Output: {"data": [{"version": "PostgreSQL 14.2"}], "columns": ["version"], "count": 1}

# Parameterized queries (recommended for security)
result = db.query(
    "SELECT * FROM users WHERE age > %s AND city = %s",
    [25, "New York"]
)

# Query with placeholder injection for CURRENT keywords
result = db.query(
    "INSERT INTO logs (timestamp, message) VALUES (%s, %s)",
    ["`CURRENT TIMESTAMP`", "System started"]
)
```

### Query Response Format

All query operations return a standardized response format:

```python
{
    "data": [
        {"id": 1, "name": "John", "email": "john@example.com"},
        {"id": 2, "name": "Jane", "email": "jane@example.com"}
    ],
    "columns": ["id", "name", "email"],
    "count": 2
}
```

### Database-Specific Query Handling

#### SQL Databases (PostgreSQL, MySQL)

```python
# PostgreSQL example
result = db.query(
    "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = %s",
    [True]
)

# MySQL example with different cursor types
result = db.query("SELECT * FROM products WHERE price BETWEEN %s AND %s", [10.0, 100.0])
```



### Stream Query Batching (Generic)

Process large result sets in memory-efficient batches.

```python
sql = "SELECT * FROM large_table WHERE status = %s"
# Yields batches of 1000 rows
for total_rows, batch, status in db.stream_query_batch(sql, ["active"], batch_size=1000):
    if status == 0:
        process_batch(batch) # process list of 1000 dicts
        print(f"Processed {len(batch)} rows. Total so far: {total_rows}")
    else:
        print("Error encountered in stream")
```

### Script Execution

LongJRM supports executing raw SQL scripts containing multiple statements (e.g., from a `.sql` file).

#### Execute Script String

```python
sql_script = """
    UPDATE users SET status = 'inactive' WHERE last_login < '2023-01-01';
    DELETE FROM sessions WHERE expired = 1;
"""
# Execute script (statements split by ';')
# transaction=True ensures atomicity (rollback on failure)
db.execute_script(sql_script, transaction=True)
```

#### Execute Script File

```python
# Execute directly from a file
db.run_script_from_file("path/to/schema_update.sql", transaction=True)
```

## Select Operations

### Basic Select Queries

The `select()` method provides a SQL-like interface for data retrieval across all database types:

```python
# Select all columns from a table
result = db.select(table="users")

# Select specific columns
result = db.select(
    table="users", 
    columns=["id", "name", "email"]
)
```

### WHERE Conditions

LongJRM supports multiple condition formats for flexible querying:

#### 1. Simple Conditions

```python
# Simple equality condition
result = db.select(
    table="users",
    where={"status": "active"}
)

# Multiple conditions (AND logic)
result = db.select(
    table="products",
    where={
        "category": "electronics",
        "price": 299.99,
        "in_stock": True
    }
)
```

#### 2. Regular Conditions (Multiple Operators)

```python
# Range queries
result = db.select(
    table="products",
    where={
        "price": {
            ">=": 10.0,
            "<=": 100.0
        }
    }
)

# Multiple operators on same column
result = db.select(
    table="users",
    where={
        "age": {
            ">": 18,
            "<": 65
        },
        "status": {
            "!=": "banned"
        }
    }
)
```

#### 3. Comprehensive Conditions (Explicit Control)

```python
# Explicit operator and placeholder control
result = db.select(
    table="orders",
    where={
        "total": {
            "operator": ">",
            "value": 1000.0,
            "placeholder": "Y"  # Use parameter binding (default)
        },
        "created_date": {
            "operator": ">=",
            "value": "`CURRENT DATE`",
            "placeholder": "N"  # Insert value directly (for SQL keywords)
        }
    }
)
```

### Query Options

Control query behavior with the `options` parameter:

```python
result = db.select(
    table="users",
    columns=["id", "name", "created_at"],
    where={"status": "active"},
    options={
        "limit": 50,              # Limit results (default: configured limit)
        "order_by": ["name ASC", "created_at DESC"]  # Sort order
    }
)
```

### Database-Specific Select Examples

#### PostgreSQL Select

```python
# PostgreSQL with JSON column queries
result = db.select(
    table="user_profiles",
    columns=["id", "name", "preferences"],
    where={
        "preferences->>'theme'": "dark",
        "active": True
    },
    options={
        "limit": 25,
        "order_by": ["created_at DESC"]
    }
)
```

#### MySQL Select

```python
# MySQL with full-text search
result = db.select(
    table="articles",
    columns=["id", "title", "content"],
    where={
        "status": "published",
        "MATCH(title, content) AGAINST": {
            "operator": "MATCH",
            "value": "'database tutorial'",
            "placeholder": "N"
        }
    }
)
```

#### MySQL Select

```python
# MySQL with full-text search
result = db.select(
    table="articles",
    columns=["id", "title", "content"],
    where={
        "status": "published",
        "MATCH(title, content) AGAINST": {
            "operator": "MATCH",
            "value": "'database tutorial'",
            "placeholder": "N"
        }
    }
)
```

### DB2 Specific Operations

LongJRM provides specialized support for IBM DB2, including MERGE statements, Admin Commands, and Partition Management.

#### DB2 Merge (Upsert)

DB2 does not support `INSERT ... ON CONFLICT`. Instead, LongJRM overrides `merge` to use DB2's `MERGE INTO` syntax.

```python
data = {"id": 1, "name": "Updated Name", "role": "admin"}
# Merges data into 'users' table matching on 'id'
# Updates existing records or inserts new ones
db.merge(table="users", data=data, key_columns=["id"])

# Bulk Merge
bulk_data = [{"id": 1, "val": "A"}, {"id": 2, "val": "B"}]
db.merge(table="users", data=bulk_data, key_columns=["id"])
```

#### DB2 Admin Commands

Execute DB2 administrative stored procedures via `SYSPROC.ADMIN_CMD`.

```python
# Load Data
load_info = {
    "source": "data.del",
    "filetype": "DEL",
    "target": "my_table",
    "operation": "REPLACE",
    "warningcount": 10
}
db.load_admin_cmd(load_info)

# Export Data
export_info = {
    "source": "select * from my_table",
    "target": "data.ixf",
    "filetype": "IXF"
}
db.export_admin_cmd(export_info)

# Generic Admin Command
db.admin_cmd(("reorg table my_table",))
```

#### DB2 Partition Management

Manage table partitions directly.

```python
# Add Partition
db.add_partition("sales", "p2024", "starting '2024-01-01' ending '2024-12-31'")

# Attach Partition
db.attach_partition("sales", "p2023", "starting '2023-01-01' ending '2023-12-31'", "sales_2023_staging")

# Detach Partition
db.detach_partition("sales", "p_old", "sales_archive_table")

# Check Partition Status
status = db.check_partition("myschema", "sales", "p2024")

# Drop Detached Partition (Safe Drop)
# Checks if partition is fully detached before dropping the table
db.drop_detached_partition("myschema", "sales_archive_table")
```

#### DB2 Runstats

Execute `RUNSTATS` to update catalog statistics for query optimization.

```python
# Default runstats (distribution and indexes all, 10% sampling)
db.runstats("my_table")

# Custom runstats command
db.runstats("my_table", "runstats on table my_table and indexes all")
```

### Bulk Update (High Performance)
For updating many records at once with different values, use `bulk_update`. This uses `executemany` for performance.

```python
updates = [
    {"id": 1, "status": "active", "score": 10},
    {"id": 2, "status": "inactive", "score": 20},
    {"id": 3, "status": "pending", "score": 0}
]

# Update 'users' table, using 'id' as the key to match records
db.bulk_update(
    table="users",
    data_list=updates,
    key_columns=["id"]
)
```

## Data Type Handling

### JSON Data Structure Support

LongJRM automatically handles various data types in JSON format:

```python
# Complex data structures
user_data = {
    "profile": {"age": 30, "preferences": {"theme": "dark"}},  # Nested objects
    "tags": ["admin", "developer"],                           # Arrays
    "metadata": [{"key": "role", "value": "manager"}],       # Array of objects
    "created_at": datetime.now(),                            # Datetime objects
    "birth_date": date(1990, 1, 1)                         # Date objects
}

# LongJRM handles conversion automatically
result = db.select(table="users", where={"id": 123})
```

### SQL Keywords and Special Values

Handle SQL keywords and special values with backtick escaping:

```python
# CURRENT timestamp and date values
data = {
    "created_at": "`CURRENT TIMESTAMP`",
    "modified_date": "`CURRENT DATE`",
    "version": "`CURRENT_TIMESTAMP`"
}

# These are handled specially and not parameterized
result = db.select(
    table="audit_log",
    where={
        "created_at": {
            "operator": ">=",
            "value": "`CURRENT DATE`",
            "placeholder": "N"
        }
    }
)
```

### Parameter Binding and Security

LongJRM uses secure parameter binding by default:

```python
# Secure parameter binding (default behavior)
result = db.select(
    table="users",
    where={
        "email": "user@example.com",  # Automatically parameterized
        "password_hash": "hashed_value"
    }
)

# Manual control over parameter binding
result = db.select(
    table="stats",
    where={
        "calculation": {
            "operator": "=",
            "value": "price * 1.1",     # Direct SQL expression
            "placeholder": "N"           # Don't parameterize
        }
    }
)
```

## GraphQL-Style Querying

### Comprehensive Single-Function Queries

LongJRM's `select` function provides GraphQL-like querying capabilities, allowing you to build complex, comprehensive table queries with a single function call. This approach combines the flexibility of GraphQL with the performance and directness of SQL.

#### GraphQL-Like Features

1. **Single Query Interface** - One function handles all query complexity
2. **Flexible Field Selection** - Choose exactly which columns to retrieve  
3. **Complex Filtering** - Rich condition syntax similar to GraphQL where clauses
4. **Nested Conditions** - Support for multiple operators and logical combinations
5. **Cross-Database Compatibility** - Same query syntax works across all database types

### Basic GraphQL-Style Queries

```python
# Simple field selection (like GraphQL field selection)
result = db.select(
    table="users",
    columns=["id", "name", "email", "profile"]  # Select specific fields
)

# Complex filtering with multiple conditions
result = db.select(
    table="products",
    columns=["id", "name", "price", "category", "ratings"],
    where={
        "category": "electronics",                    # Simple equality
        "price": {">=": 100, "<=": 1000},            # Range conditions
        "ratings": {">": 4.0},                       # Comparison
        "in_stock": True,                            # Boolean condition
        "created_at": {">=": "2024-01-01"}           # Date filtering
    },
    options={
        "limit": 50,
        "order_by": ["ratings DESC", "price ASC"]    # Multi-field sorting
    }
)
```

### Advanced GraphQL-Style Patterns

#### Comprehensive E-commerce Query

```python
# Complex e-commerce product search (equivalent to sophisticated GraphQL query)
def search_products(db, filters):
    """
    GraphQL-style product search with comprehensive filtering
    Similar to: query { products(where: {...}, orderBy: [...]) { ... } }
    """
    where_conditions = {}
    
    # Category filtering
    if filters.get("categories"):
        where_conditions["category"] = {"IN": filters["categories"]}
    
    # Price range
    if filters.get("min_price") or filters.get("max_price"):
        price_condition = {}
        if filters.get("min_price"):
            price_condition[">="] = filters["min_price"]
        if filters.get("max_price"):
            price_condition["<="] = filters["max_price"]
        where_conditions["price"] = price_condition
    
    # Rating filter
    if filters.get("min_rating"):
        where_conditions["average_rating"] = {">=": filters["min_rating"]}
    
    # Availability
    if filters.get("in_stock_only"):
        where_conditions["stock_quantity"] = {">": 0}
    
    # Brand filtering
    if filters.get("brands"):
        where_conditions["brand"] = {"IN": filters["brands"]}
    
    # Tag filtering (array contains)
    if filters.get("tags"):
        where_conditions["tags"] = {"LIKE": f"%{filters['tags']}%"}
    
    return db.select(
        table="products",
        columns=[
            "id", "name", "description", "price", 
            "category", "brand", "average_rating",
            "stock_quantity", "image_url", "tags"
        ],
        where=where_conditions,
        options={
            "limit": filters.get("limit", 50),
            "order_by": filters.get("sort", ["average_rating DESC", "price ASC"])
        }
    )

# Usage like GraphQL variables
search_filters = {
    "categories": ["electronics", "gadgets"],
    "min_price": 50,
    "max_price": 500,
    "min_rating": 4.0,
    "in_stock_only": True,
    "brands": ["Apple", "Samsung", "Sony"],
    "tags": "wireless",
    "limit": 25,
    "sort": ["price ASC"]
}

products = search_products(db, search_filters)
```

#### User Profile with Relationships

```python
def get_user_profile(db, user_id):
    """
    GraphQL-style user profile query with related data
    Similar to: query { user(id: $id) { profile, orders, preferences } }
    """
    
    # Get user basic info
    user = db.select(
        table="users",
        columns=["id", "name", "email", "created_at", "status"],
        where={"id": user_id}
    )
    
    if user["count"] == 0:
        return None
    
    user_data = user["data"][0]
    
    # Get user orders (related data)
    orders = db.select(
        table="orders",
        columns=["id", "total", "status", "created_at"],
        where={
            "user_id": user_id,
            "status": {"!=": "cancelled"}
        },
        options={
            "limit": 10,
            "order_by": ["created_at DESC"]
        }
    )
    
    # Get user preferences (nested data)
    preferences = db.select(
        table="user_preferences",
        columns=["category", "value", "updated_at"],
        where={"user_id": user_id}
    )
    
    # Combine like GraphQL resolver
    return {
        "user": user_data,
        "orders": orders["data"],
        "preferences": {pref["category"]: pref["value"] for pref in preferences["data"]},
        "order_count": orders["count"],
        "total_spent": sum(order["total"] for order in orders["data"])
    }
```

#### Analytics Dashboard Query

```python
def get_dashboard_analytics(db, date_range):
    """
    GraphQL-style analytics query for dashboard
    Similar to: query { analytics(dateRange: $range) { sales, users, products } }
    """
    
    base_where = {
        "created_at": {
            ">=": date_range["start"],
            "<=": date_range["end"]
        }
    }
    
    # Sales analytics
    sales = db.select(
        table="orders",
        columns=["id", "total", "status", "created_at"],
        where={
            **base_where,
            "status": "completed"
        }
    )
    
    # User growth
    new_users = db.select(
        table="users",
        columns=["id", "created_at"],
        where=base_where
    )
    
    # Product performance
    top_products = db.select(
        table="order_items",
        columns=["product_id", "quantity", "price"],
        where={
            "order_created_at": {
                ">=": date_range["start"],
                "<=": date_range["end"]
            }
        }
    )
    
    # Aggregate results (like GraphQL computed fields)
    return {
        "sales": {
            "total_revenue": sum(order["total"] for order in sales["data"]),
            "order_count": sales["count"],
            "average_order_value": sum(order["total"] for order in sales["data"]) / max(sales["count"], 1)
        },
        "users": {
            "new_users": new_users["count"],
            "growth_rate": calculate_growth_rate(new_users["data"])
        },
        "products": {
            "top_selling": aggregate_product_sales(top_products["data"]),
            "categories": get_category_performance(db, date_range)
        }
    }
```

### GraphQL-Style Filtering Operators

LongJRM supports GraphQL-like filtering operators through its comprehensive condition system:

```python
# Comparison operators (like GraphQL comparison operators)
where = {
    "price": {">=": 100},           # gte in GraphQL
    "rating": {"<=": 5.0},          # lte in GraphQL  
    "views": {">": 1000},           # gt in GraphQL
    "stock": {"<": 10},             # lt in GraphQL
    "status": {"!=": "deleted"}     # ne in GraphQL
}

# Array/List operations (like GraphQL list operators)
where = {
    "category": {"IN": ["electronics", "gadgets"]},        # in operator
    "tags": {"LIKE": "%featured%"},                        # contains-like
    "id": {"NOT IN": [1, 2, 3]}                           # not in
}

# Complex nested conditions
where = {
    "price": {">=": 50, "<=": 500},                       # Range conditions
    "created_at": {
        ">=": "2024-01-01",
        "<=": "`CURRENT DATE`"                            # SQL functions
    }
}

# GraphQL-style comprehensive conditions
where = {
    "search_score": {
        "operator": ">=",
        "value": 0.8,
        "placeholder": "Y"                                 # Parameter binding control
    }
}

### GraphQL Resolver Pattern

Implement GraphQL-like resolvers using LongJRM's single-function querying:

```python
class ProductResolver:
    def __init__(self, db):
        self.db = db
    
    def resolve_products(self, args):
        """GraphQL-style product resolver"""
        return self.db.select(
            table="products",
            columns=args.get("fields", ["*"]),
            where=self.build_where_from_args(args),
            options={
                "limit": args.get("first", 50),
                "order_by": args.get("orderBy", ["created_at DESC"])
            }
        )
    
    def resolve_product(self, product_id, args):
        """Single product resolver with related data"""
        product = self.db.select(
            table="products",
            columns=args.get("fields", ["*"]),
            where={"id": product_id}
        )
        
        if product["count"] == 0:
            return None
            
        # Get related data if requested
        result = product["data"][0]
        
        if "reviews" in args.get("include", []):
            reviews = self.db.select(
                table="reviews",
                where={"product_id": product_id},
                options={"limit": 10, "order_by": ["created_at DESC"]}
            )
            result["reviews"] = reviews["data"]
        
        if "variants" in args.get("include", []):
            variants = self.db.select(
                table="product_variants",
                where={"product_id": product_id}
            )
            result["variants"] = variants["data"]
            
        return result
    
    def build_where_from_args(self, args):
        """Convert GraphQL-style arguments to LongJRM where conditions"""
        where = {}
        
        if args.get("category"):
            where["category"] = args["category"]
        
        if args.get("priceRange"):
            price_range = args["priceRange"]
            where["price"] = {}
            if price_range.get("min"):
                where["price"][">="] = price_range["min"]
            if price_range.get("max"):
                where["price"]["<="] = price_range["max"]
        
        if args.get("inStock"):
            where["stock_quantity"] = {">": 0}
            
        return where

# Usage like GraphQL
resolver = ProductResolver(db)

# Query like: products(category: "electronics", priceRange: {min: 100, max: 500}, first: 20)
products = resolver.resolve_products({
    "category": "electronics",
    "priceRange": {"min": 100, "max": 500},
    "first": 20,
    "fields": ["id", "name", "price", "image_url"],
    "orderBy": ["price ASC"]
})

# Query like: product(id: 123) { name, price, reviews, variants }
product = resolver.resolve_product(123, {
    "fields": ["id", "name", "price", "description"],
    "include": ["reviews", "variants"]
})
```

### Performance Optimization for GraphQL-Style Queries

```python
def optimized_graphql_query(db, query_spec):
    """
    Optimized GraphQL-style query with performance considerations
    """
    
    # Field selection optimization
    selected_fields = query_spec.get("fields", ["*"])
    if len(selected_fields) > 10:
        # Limit field selection for performance
        selected_fields = selected_fields[:10]
    
    # Condition optimization
    where_conditions = query_spec.get("where", {})
    
    # Use indexed fields first for better performance
    indexed_conditions = {}
    other_conditions = {}
    
    indexed_fields = ["id", "status", "created_at", "category", "user_id"]
    
    for field, condition in where_conditions.items():
        if field in indexed_fields:
            indexed_conditions[field] = condition
        else:
            other_conditions[field] = condition
    
    # Combine conditions with indexed fields first
    optimized_where = {**indexed_conditions, **other_conditions}
    
    # Limit optimization
    limit = min(query_spec.get("limit", 100), 1000)  # Cap at 1000 for performance
    
    return db.select(
        table=query_spec["table"],
        columns=selected_fields,
        where=optimized_where,
        options={
            "limit": limit,
            "order_by": query_spec.get("orderBy", ["created_at DESC"])
        }
    )
```

## Cross-Database Compatibility

### Unified Interface

LongJRM provides a consistent interface across different database types:

```python
def get_active_users(db, limit=10):
    """Get active users - works with any database type"""
    return db.select(
        table="users",
        columns=["id", "name", "email"],
        where={"status": "active"},
        options={"limit": limit}
    )

# Works with PostgreSQL
pg_result = get_active_users(pg_db)

# Works with MySQL  
mysql_result = get_active_users(mysql_db)
```

### Database-Specific Optimizations

While maintaining compatibility, LongJRM optimizes for each database type:

```python
# Automatic cursor type selection
if db.database_type in ['postgres', 'postgresql']:
    # Uses psycopg.extras.RealDictCursor for dict results
    pass
elif db.database_type == 'mysql':
    # Uses pymysql.cursors.DictCursor for dict results
    pass
```

## Error Handling

### Query Error Management

```python
import logging
from longjrm.database.db import Db

logger = logging.getLogger(__name__)

try:
    result = db.select(
        table="users",
        where={"invalid_column": "value"}
    )
except Exception as e:
    logger.error(f"Query failed: {e}")
    # Handle database errors appropriately
    # - Log the error
    # - Return default/cached data
    # - Raise user-friendly exception
```

### Connection Error Handling

```python
from longjrm.connection.dbconn import JrmConnectionError

try:
    with pool.client() as client:
        db = Db(client)
        result = db.select(table="users")
except JrmConnectionError as e:
    logger.error(f"Database connection failed: {e}")
    # Handle connection issues
```

## Performance Considerations

### Query Optimization

```python
# Use column selection to reduce data transfer
result = db.select(
    table="large_table",
    columns=["id", "name"],  # Select only needed columns
    where={"status": "active"},
    options={"limit": 100}   # Limit result set size
)

# Use appropriate data fetch limits
config = get_config()
print(f"Default fetch limit: {config.data_fetch_limit}")

# Override default limit for specific queries
result = db.select(
    table="small_table",
    options={"limit": 10}  # Custom limit
)
```

### Connection Pool Management

```python

# Efficient usage pattern using pool's built-in context manager
with pool.client() as client:
    db = Db(client)
    users = db.select(table="users", where={"active": True})
    orders = db.select(table="orders", where={"user_id": users["data"][0]["id"]})
    # Connection automatically returned to pool
```

## Usage Patterns

### Application Integration

#### Web Application Pattern

```python
from flask import Flask, jsonify
from longjrm.config.runtime import get_config, require_db
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database.db import Db

app = Flask(__name__)

# Initialize database pool
db_config = require_db("primary")
pool = Pool.from_config(db_config, PoolBackend.SQLALCHEMY)

@app.route('/api/users')
def get_users():
    try:
        with pool.client() as client:
            db = Db(client)
            result = db.select(
                table="users",
                columns=["id", "name", "email"],
                where={"status": "active"},
                options={"limit": 50}
            )
            return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/search')
def search_users():
    search_term = request.args.get('q', '')
    try:
        with pool.client() as client:
            db = Db(client)
            result = db.select(
                table="users",
                where={
                    "name": {"LIKE": f"%{search_term}%"}
                },
                options={"limit": 20}
            )
            return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
```

#### Data Processing Pattern

```python
def process_user_data(pool):
    """Process user data across multiple queries"""
    with pool.client() as client:
        db = Db(client)
        # Get all active users
        users = db.select(
            table="users",
            where={"status": "active"}
        )
        
        # Process each user's orders
        for user in users["data"]:
            orders = db.select(
                table="orders",
                where={"user_id": user["id"]},
                options={"order_by": ["created_at DESC"]}
            )
            
            # Calculate user statistics
            if orders["count"] > 0:
                total_amount = sum(order["total"] for order in orders["data"])
                print(f"User {user['name']}: {orders['count']} orders, ${total_amount}")
```

#### Multi-Database Pattern

```python
class DataService:
    def __init__(self, primary_pool, analytics_pool):
        self.primary_pool = primary_pool
        self.analytics_pool = analytics_pool
    
    def get_user_with_analytics(self, user_id):
        """Get user data from primary DB and analytics from analytics DB"""
        
        # Get user from primary database
        with self.primary_pool.client() as primary_client:
            primary_db = Db(primary_client)
            user_result = primary_db.select(
                table="users",
                where={"id": user_id}
            )
        
        if user_result["count"] == 0:
            return None
            
        user = user_result["data"][0]
        
        # Get analytics from analytics database
        with self.analytics_pool.client() as analytics_client:
            analytics_db = Db(analytics_client)
            analytics_result = analytics_db.select(
                table="user_analytics",
                where={"user_id": user_id}
            )
        
        # Combine results
        if analytics_result["count"] > 0:
            user["analytics"] = analytics_result["data"][0]
        
        return user
```

## CRUD Operations

### Insert Operations

Insert single records or multiple records with support for PostgreSQL RETURNING clause:

```python
# Single record insert
result = db.insert(
    table="users",
    data={
        "name": "John Doe",
        "email": "john@example.com",
        "created_at": "`CURRENT TIMESTAMP`"
    }
)

# Bulk insert support
records = [
    {"name": "User 1", "email": "user1@example.com"},
    {"name": "User 2", "email": "user2@example.com"}
]
result = db.insert(table="users", data=records)

# PostgreSQL RETURNING clause
result = db.insert(
    table="users",
    data={"name": "Alice", "email": "alice@example.com"},
    return_columns=["id", "created_at"]
)
```

### Update Operations

Update records with flexible WHERE conditions:

```python
# Basic update
result = db.update(
    table="users",
    data={"status": "inactive", "modified_at": "`CURRENT TIMESTAMP`"},
    where={"last_login": {"<": "2023-01-01"}}
)

# Complex conditions
result = db.update(
    table="products",
    data={"price": 99.99, "updated_at": "`CURRENT TIMESTAMP`"},
    where={
        "category": "electronics",
        "stock": {">": 0},
        "created_at": {">=": "2024-01-01"}
    }
)
```

### Delete Operations

Delete records with comprehensive filtering:

```python
# Basic delete
result = db.delete(
    table="users",
    where={"status": "banned", "created_at": {"<": "2022-01-01"}}
)

# Complex delete conditions  
result = db.delete(
    table="logs",
    where={
        "level": "DEBUG",
        "timestamp": {"<": "`CURRENT_DATE - INTERVAL 30 DAY`"},
        "processed": True
    }
)
```

### Merge Operations

UPSERT operations with key-based conflict resolution:

```python
# Merge operation (INSERT or UPDATE based on key match)
result = db.merge(
    table="user_profiles",
    data={
        "user_id": 123,
        "profile_data": {"theme": "dark", "language": "en"},
        "updated_at": "`CURRENT TIMESTAMP`"
    },
    key_columns=["user_id"]
)
```

## Testing

### Unit Testing Database Operations

```python
import unittest
from longjrm.config.config import DatabaseConfig
from longjrm.connection.pool import Pool, PoolBackend
from longjrm.database.db import Db

class TestDatabaseOperations(unittest.TestCase):
    def setUp(self):
        # Use in-memory SQLite for testing
        self.db_config = DatabaseConfig(
            type="sqlite",
            database=":memory:"
        )
        self.pool = Pool.from_config(self.db_config, PoolBackend.SQLALCHEMY)
        
        # Create test table
        with self.pool.client() as client:
            db = Db(client)
            db.query("""
                CREATE TABLE test_users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    status TEXT DEFAULT 'active'
                )
            """)
            # Insert test data
            db.query("INSERT INTO test_users (name, email) VALUES (?, ?)", ["John", "john@test.com"])
            db.query("INSERT INTO test_users (name, email) VALUES (?, ?)", ["Jane", "jane@test.com"])
        
    def tearDown(self):
        self.pool.dispose()
    
    def test_select_all(self):
        """Test selecting all records"""
        with self.pool.client() as client:
            db = Db(client)
            
            result = db.select(table="test_users")
            self.assertEqual(result["count"], 2)
            self.assertIn("id", result["columns"])
            self.assertIn("name", result["columns"])
    
    def test_select_with_where(self):
        """Test selecting with WHERE conditions"""
        with self.pool.client() as client:
            db = Db(client)
            
            result = db.select(
                table="test_users",
                where={"name": "John"}
            )
            self.assertEqual(result["count"], 1)
            self.assertEqual(result["data"][0]["name"], "John")
    
    def test_select_with_columns(self):
        """Test selecting specific columns"""
        with self.pool.client() as client:
            db = Db(client)
            
            result = db.select(
                table="test_users",
                columns=["name", "email"]
            )
            self.assertEqual(len(result["columns"]), 2)
            self.assertNotIn("id", result["columns"])
```

### Integration Testing

```python
def test_cross_database_compatibility():
    """Test that operations work across different database types"""
    databases = [
        ("sqlite", {"type": "sqlite", "database": ":memory:"}),
        ("postgres", {"type": "postgres", "dsn": "postgres://test:test@localhost/test"}),
        ("mysql", {"type": "mysql", "host": "localhost", "user": "test", "password": "test", "database": "test"})
    ]
    
    for db_name, config in databases:
        try:
            db_config = DatabaseConfig.from_dict(config)
            pool = Pool.from_config(db_config, PoolBackend.SQLALCHEMY)
            
            with pool.client() as client:
                db = Db(client)
                
                # Test basic query
                if db.database_type == "sqlite":
                    result = db.query("SELECT 1 as test")
                else:
                    result = db.query("SELECT 1 as test")
                
                assert result["count"] == 1
                assert result["data"][0]["test"] == 1
            pool.dispose()
            
        except Exception as e:
            print(f"Database {db_name} test failed: {e}")
```

## Best Practices

### Security

1. **Always use parameter binding** for user input
2. **Validate input data** before database operations
3. **Use least privilege** database accounts
4. **Log database operations** for audit trails

```python
# Secure query pattern
def get_user_safely(db, user_id):
    """Safely get user by ID with input validation"""
    # Validate input
    if not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("Invalid user ID")
    
    # Use parameter binding
    result = db.select(
        table="users",
        where={"id": user_id}  # Automatically parameterized
    )
    
    return result
```

### Performance

1. **Select only needed columns** to reduce data transfer
2. **Use appropriate limits** to prevent large result sets
3. **Leverage database indexes** in WHERE conditions
4. **Monitor query performance** and optimize as needed

```python
# Performance-optimized query
def get_recent_orders(db, limit=50):
    """Get recent orders with performance optimization"""
    return db.select(
        table="orders",
        columns=["id", "user_id", "total", "created_at"],  # Only needed columns
        where={"status": "completed"},                      # Use indexed column
        options={
            "limit": limit,                                 # Limit result set
            "order_by": ["created_at DESC"]                # Use indexed ordering
        }
    )
```

### Maintainability

1. **Use configuration** for database settings
2. **Implement proper error handling**
3. **Write comprehensive tests**
4. **Document complex queries**

```python
def complex_user_query(db, filters):
    """
    Complex user query with multiple conditions
    
    Args:
        db: Database instance
        filters: Dict with optional keys: status, min_age, max_age, city
    
    Returns:
        Query result with user data
    """
    where_conditions = {}
    
    # Build conditions based on provided filters
    if filters.get("status"):
        where_conditions["status"] = filters["status"]
    
    if filters.get("min_age"):
        where_conditions["age"] = where_conditions.get("age", {})
        where_conditions["age"][">="] = filters["min_age"]
    
    if filters.get("max_age"):
        where_conditions["age"] = where_conditions.get("age", {})
        where_conditions["age"]["<="] = filters["max_age"]
    
    if filters.get("city"):
        where_conditions["city"] = filters["city"]
    
    return db.select(
        table="users",
        columns=["id", "name", "email", "age", "city"],
        where=where_conditions,
        options={"limit": 100, "order_by": ["name ASC"]}
    )
```

## API Reference

### Db Class

```python
class Db:
    def __init__(self, client: dict)
    
    def query(self, sql: str, arr_values: list = None, collection_name: str = None) -> dict
    """Execute raw SQL query with parameter binding"""
    
    def select(self, table: str, columns: list = None, where: dict = None, options: dict = None) -> dict
    """Execute SELECT query with JSON-based conditions"""
    
    def insert(self, table: str, data: Union[dict, list], return_columns: list = None) -> dict
    """Insert single record or multiple records"""
    
    def update(self, table: str, data: dict, where: dict = None) -> dict
    """Update records with WHERE conditions"""
    
    def delete(self, table: str, where: dict = None) -> dict
    """Delete records with WHERE conditions"""
    
    def merge(self, table: str, data: dict, key_columns: list) -> dict
    """UPSERT operation with key-based conflict resolution"""
```

### Response Format

All database operations return a standardized response:

```python
{
    "data": [],        # List of result records (dicts)
    "columns": [],     # List of column names
    "count": 0         # Number of records returned
}
```

### Condition Formats

1. **Simple**: `{"column": value}`
2. **Regular**: `{"column": {"operator": value}}`  
3. **Comprehensive**: `{"column": {"operator": "=", "value": value, "placeholder": "Y"}}`

### Special Values

- **SQL Keywords**: Use backtick escaping like `"CURRENT TIMESTAMP"`
- **Parameter Binding**: Controlled via `placeholder` option ("Y" = bind, "N" = direct)
- **Data Types**: Automatic JSON, datetime, and array handling