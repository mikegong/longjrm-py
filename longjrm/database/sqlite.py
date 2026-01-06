"""
SQLite-specific database implementation.

This module contains the SqliteDb class that extends the base Db class
with SQLite-specific behavior for cursors and UPSERT operations.
"""
import sqlite3
from longjrm.database.db import Db


class SqliteDb(Db):
    """
    SQLite-specific database implementation.
    
    Overrides:
    - get_cursor(): Row factory cursor for dictionary-style row access
    - get_stream_cursor(): Same as get_cursor (SQLite doesn't have server-side cursors)
    - _build_upsert_clause(): Uses SQLite's ON CONFLICT syntax (similar to PostgreSQL)
    """
    
    def __init__(self, client):
        """Initialize SQLite database connection."""
        super().__init__(client)
        # SQLite uses ? as placeholder
        self.placeholder = '?'
        # Enable row factory for dictionary-like access
        self.conn.row_factory = sqlite3.Row
    
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
    
    def query(self, sql, arr_values=None):
        """
        Execute query with small result set, return entire result set.
        
        Overridden to handle SQLite Row objects conversion to dictionaries.
        """
        try:
            prepared_sql, processed_values = self._prepare_sql(sql, arr_values)
            
            cur = self.get_cursor()
            if processed_values is None:
                cur.execute(prepared_sql)
            else:
                cur.execute(prepared_sql, processed_values)
            rows = cur.fetchall()
            
            # Convert rows to regular dictionaries using cursor description
            columns = [col[0] for col in cur.description] if cur.description else []
            data = [dict(zip(columns, row)) for row in rows]
            row_count = len(data)
            
            cur.close()
            
            return {
                'status': 0,
                'message': f'Query executed successfully. {row_count} rows returned.',
                'data': data,
                'count': row_count
            }
        except Exception as e:
            import traceback
            import logging
            logger = logging.getLogger(__name__)
            message = f'Failed to execute query: {e}'
            logger.error(message)
            logger.error(traceback.format_exc())
            return {
                'status': -1,
                'message': message,
                'data': [],
                'count': 0
            }

    def execute(self, sql, arr_values=None):
        """
        Execute query with no return result set.
        
        Overridden to handle SQLite parameter binding issues with None.
        """
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"Execute SQL: {sql}")
        logger.debug(f"Execute values: {arr_values}")

        cur = None
        try:
            cur = self.get_cursor()
            
            prepared_sql, processed_values = self._prepare_sql(sql, arr_values)
            
            logger.debug(f"Executing query: {sql}")
            if processed_values is None:
                cur.execute(prepared_sql)
            else:
                cur.execute(prepared_sql, processed_values)
            
            affected_rows = cur.rowcount
            
            success_msg = f"SQL statement succeeded. {affected_rows} rows is affected."
            logger.info(success_msg)
            
            return {
                "status": 0,
                "message": success_msg,
                "data": [],
                "count": affected_rows
            }
                
        finally:
            if cur:
                cur.close()
