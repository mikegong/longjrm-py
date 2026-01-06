"""
MySQL-specific database implementation.

This module contains the MySQLDb class that extends the base Db class
with MySQL-specific behavior for cursors.
"""
from longjrm.database.db import Db
import pymysql
import json


class MySQLDb(Db):
    """
    MySQL-specific database implementation.
    
    Overrides:
    - get_cursor(): DictCursor for dictionary-style row access
    - get_stream_cursor(): SSDictCursor for memory-efficient streaming
    - supports_returning(): MySQL doesn't support RETURNING clause
    """
    
    def get_cursor(self):
        """Get a MySQL DictCursor for query execution."""
        return self.conn.cursor(pymysql.cursors.DictCursor)
    
    def get_stream_cursor(self, cursor_name='stream_cursor'):
        """Get a MySQL SSDictCursor for memory-efficient streaming."""
        return self.conn.cursor(pymysql.cursors.SSDictCursor)
    
    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        """
        Build MySQL ON DUPLICATE KEY UPDATE clause for UPSERT operations.
        
        Args:
            key_columns: list of column names that define uniqueness
            update_columns: list of columns to update on duplicate
            for_values: True for VALUES-based, False for SELECT-based (both use same syntax in MySQL)
            
        Returns:
            str: MySQL ON DUPLICATE KEY UPDATE clause
        """
        if update_columns:
            update_parts = [f"{col} = VALUES({col})" for col in update_columns]
            update_clause = ', '.join(update_parts)
            return f"ON DUPLICATE KEY UPDATE {update_clause}"
        else:
            # If no update columns, just use first key column to essentially do nothing
            return f"ON DUPLICATE KEY UPDATE {key_columns[0]} = {key_columns[0]}"

    def _process_value(self, value):
        """
        Process individual values for MySQL operations.
        Overrides base method to ensure lists are serialized to JSON.
        """
        if isinstance(value, list):
            return json.dumps(value, ensure_ascii=False)
        return super()._process_value(value)
