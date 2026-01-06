"""
Database module for longjrm.

This package provides database connectivity and operations for:
- PostgreSQL (via psycopg2)
- MySQL (via pymysql)
- MariaDB (via pymysql)
- SQLite (via sqlite3)

Submodules:
- db: Base Db class with SQL database operations
- postgres: PostgresDb class for PostgreSQL
- mysql: MySQLDb class for MySQL/MariaDB
- sqlite: SqliteDb class for SQLite
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
            - db_lib: Database library being used ('psycopg2', 'pymysql', 'sqlite3')
                        
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
    else:
        raise ValueError(f"Unsupported database type: {database_type}")


__all__ = [
    'Db',
    'PlaceholderHandler',
    'get_db',
]

