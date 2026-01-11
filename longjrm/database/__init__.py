"""
Database module for longjrm.

This package provides database connectivity and operations for:
- PostgreSQL (via psycopg)
- MySQL (via pymysql)
- MariaDB (via pymysql)
- SQLite (via sqlite3)
- DB2 (via ibm_db_dbi)
- Oracle (via oracledb)
- SQL Server (via pyodbc)
- Spark SQL (via pyspark)

Submodules:
- db: Base Db class with SQL database operations
- postgres: PostgresDb class for PostgreSQL
- mysql: MySQLDb class for MySQL/MariaDB
- sqlite: SqliteDb class for SQLite
- spark: SparkDb class for Spark SQL
- placeholder_handler: SQL placeholder handling utilities

Usage:
    from longjrm.database import get_db
    
    # client is a dict with 'conn', 'database_type', 'database_name', 'db_lib'
    db = get_db(client)
    result = db.select('users', ['id', 'name'], {'active': True})
"""

from longjrm.database.db import Db
from longjrm.database.placeholder_handler import PlaceholderHandler


def get_db(client):
    """
    Factory function to create database-specific Db instance.
    
    Args:
        client: Dictionary containing:
            - conn: Database connection object
            - database_type: Type of database ('postgres', 'postgresql', 'mysql', 'mariadb', 'sqlite')
            - database_name: Name of the database
            - db_lib: Database library being used ('psycopg', 'pymysql', 'sqlite3')
                        
    Returns:
        Db subclass instance (PostgresDb, MySQLDb, or SqliteDb) for the specified database type
        
    Raises:
        ValueError: If database_type is not supported
    """
    database_type = client.get('database_type', '')
    
    if database_type in ['postgres', 'postgresql']:
        from longjrm.database.postgres import PostgresDb
        return PostgresDb(client)
    elif database_type in ['mysql', 'mariadb']:
        from longjrm.database.mysql import MySQLDb
        return MySQLDb(client)
    elif database_type == 'sqlite':
        from longjrm.database.sqlite import SqliteDb
        return SqliteDb(client)
    elif database_type == 'db2':
        from longjrm.database.db2 import Db2Db
        return Db2Db(client)
    elif database_type == 'oracle':
        from longjrm.database.oracle import OracleDb
        return OracleDb(client)
    elif database_type in ['sqlserver', 'mssql']:
        from longjrm.database.sqlserver import SqlServerDb
        return SqlServerDb(client)
    elif database_type == 'spark':
        from longjrm.database.spark import SparkDb
        return SparkDb(client)
    else:
        # Fallback to GenericDb
        from longjrm.database.generic import GenericDb
        # Ensure we don't crash if db_lib isn't set, though connectors usually set it
        return GenericDb(client)


__all__ = [
    'Db',
    'PlaceholderHandler',
    'get_db',
]

