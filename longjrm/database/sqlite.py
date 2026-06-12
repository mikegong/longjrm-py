"""
SQLite-specific database implementation.

This module contains the SqliteDb class that extends the base Db class
with SQLite-specific behavior for cursors and UPSERT operations.
"""
import sqlite3
import json
from longjrm.database.db import Db


class SqliteDb(Db):
    """
    SQLite-specific database implementation.
    
    Overrides:
    - get_cursor(): Row factory cursor for dictionary-style row access
    - get_stream_cursor(): Same as get_cursor (SQLite doesn't have server-side cursors)
    - _build_upsert_clause(): Uses SQLite's ON CONFLICT syntax (similar to PostgreSQL)
    """

    # SQLite needs a WHERE clause to disambiguate INSERT ... SELECT ... ON CONFLICT
    # (otherwise 'ON' is parsed as a join clause of the SELECT).
    _upsert_select_needs_where = True

    def __init__(self, client):
        """Initialize SQLite database connection."""
        super().__init__(client)
        # SQLite uses ? as placeholder
        self.placeholder = '?'
        # Enable row factory for dictionary-like access
        self.conn.row_factory = sqlite3.Row

    def _process_value(self, value):
        """
        Process individual values for SQLite.
        sqlite3 doesn't automatically convert lists to JSON/String,
        raising errors like "type 'list' is not supported".
        """
        if isinstance(value, list):
            return json.dumps(value, ensure_ascii=False)
        return super()._process_value(value)

    def get_cursor(self):
        """Get a SQLite cursor with Row factory for dictionary-style access."""
        cursor = self.conn.cursor()
        return cursor
    
    def get_stream_cursor(self, cursor_name='stream_cursor'):
        """
        Get a SQLite cursor for streaming.
        
        Note: SQLite doesn't support server-side cursors like PostgreSQL,
        so this returns a regular cursor. For large result sets, consider
        using LIMIT/OFFSET pagination.
        """
        return self.get_cursor()
    
    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        """
        Build SQLite ON CONFLICT clause for UPSERT operations.
        
        SQLite 3.24+ supports ON CONFLICT similar to PostgreSQL.
        
        Args:
            key_columns: list of column names for conflict detection
            update_columns: list of columns to update on conflict
            for_values: True for VALUES-based, False for SELECT-based
            
        Returns:
            str: SQLite ON CONFLICT clause
        """
        conflict_cols = ', '.join(key_columns)
        
        if update_columns:
            # SQLite uses excluded.column_name (same as PostgreSQL)
            update_parts = [f"{col} = excluded.{col}" for col in update_columns]
            update_clause = ', '.join(update_parts)
            return f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
        else:
            return f"ON CONFLICT ({conflict_cols}) DO NOTHING"
