"""
PostgreSQL-specific database implementation.

This module contains the PostgresDb class that extends the base Db class
with PostgreSQL-specific behavior for cursors and RETURNING clause.
"""
import logging
from longjrm.database.db import Db
import psycopg2.extras
from longjrm.utils import sql as sql_utils

logger = logging.getLogger(__name__)


class PostgresDb(Db):
    """
    PostgreSQL-specific database implementation.
    
    Overrides:
    - get_cursor(): RealDictCursor for dictionary-style row access
    - get_stream_cursor(): Server-side cursor for memory-efficient streaming
    - supports_returning(): PostgreSQL supports RETURNING clause
    - get_returning_clause(): Generate RETURNING clause
    """
    
    def get_cursor(self):
        """Get a PostgreSQL RealDictCursor for query execution."""
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    def get_stream_cursor(self, cursor_name='stream_cursor'):
        """Get a PostgreSQL server-side cursor for memory-efficient streaming."""
        # withhold=True (WITH HOLD) allows the cursor to survive commits and, 
        # in some cases, work more flexibly with transaction states.
        return self.conn.cursor(name=cursor_name, cursor_factory=psycopg2.extras.RealDictCursor, withhold=True)
    
    def supports_returning(self):
        """PostgreSQL supports RETURNING clause."""
        return True
    
    def get_returning_clause(self, columns):
        """Get PostgreSQL RETURNING clause."""
        if columns:
            return f" RETURNING {','.join(columns)}"
        return ""
    
    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        """
        Build PostgreSQL ON CONFLICT clause for UPSERT operations.
        
        Args:
            key_columns: list of column names for conflict detection
            update_columns: list of columns to update on conflict
            for_values: True for VALUES-based, False for SELECT-based (both use same syntax in PostgreSQL)
            
        Returns:
            str: PostgreSQL ON CONFLICT clause
        """
        conflict_cols = ', '.join(key_columns)
        
        if update_columns:
            update_parts = [f"{col} = EXCLUDED.{col}" for col in update_columns]
            update_clause = ', '.join(update_parts)
            return f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
        else:
            return f"ON CONFLICT ({conflict_cols}) DO NOTHING"
