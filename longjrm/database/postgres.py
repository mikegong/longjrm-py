"""
PostgreSQL-specific database implementation.

This module contains the PostgresDb class that extends the base Db class
with PostgreSQL-specific behavior for cursors and RETURNING clause.
"""
import logging
from longjrm.database.db import Db
from psycopg import rows as psycopg_rows
from psycopg import sql as psycopg_sql
from longjrm.utils import sql as sql_utils

logger = logging.getLogger(__name__)


class PostgresDb(Db):
    """
    PostgreSQL-specific database implementation.
    
    Overrides:
    - get_cursor(): dict_row factory for dictionary-style row access
    - get_stream_cursor(): Server-side cursor for memory-efficient streaming
    - supports_returning(): PostgreSQL supports RETURNING clause
    - get_returning_clause(): Generate RETURNING clause
    """
    
    def get_cursor(self):
        """Get a PostgreSQL cursor with dict_row factory for query execution."""
        return self.conn.cursor(row_factory=psycopg_rows.dict_row)
    
    def get_stream_cursor(self, cursor_name='stream_cursor'):
        """Get a PostgreSQL server-side cursor for memory-efficient streaming."""
        # psycopg3: Server-side cursors are created by providing a name
        # withhold=True allows the cursor to survive commits
        return self.conn.cursor(name=cursor_name, row_factory=psycopg_rows.dict_row, withhold=True)
    
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

    def bulk_load(self, table, load_info=None, *, command=None):
        """
        Bulk load data into table using PostgreSQL COPY command.
        
        Args:
            table: Target table name, e.g., "my_table".
                   Also supports "my_table(col1, col2)" for consistency.
            load_info: Data source (file path, file-like object, or query) or config dict.
                If dict, supported keys:
                - source: 'file path', file object, or 'SELECT query'
                - source_type: 'file' or 'cursor' (optional, auto-detected)
                - format: 'text', 'csv', or 'binary' (default 'csv')
                - delimiter: Field delimiter (default ',')
                - header: Boolean, whether file has header (default True)
                - columns: List of columns to load into
                - null: Null string representation
                - quote: Quote character (default '"')
                - escape: Escape character
                - encoding: File encoding (default 'UTF8')
            command: Optional raw COPY command (bypasses processing)
        """
        import io
        
        cur = None
        file_handle = None
        should_close_file = False
        
        try:
            # 0. Enforce dict-only load_info
            if not isinstance(load_info, dict):
                raise TypeError(f"Postgres bulk_load requires a configuration dictionary for 'load_info', got {type(load_info).__name__}")

            # Default options
            opts = {
                'delimiter': ',', 'header': False, 'quote': '"', 
                'encoding': 'UTF8', 'format': 'csv', 'null': None,
                'escape': None
            }
            columns = None
            source_type = None

            # Robust table and column handling
            if table and '(' in str(table):
                # If table is "table(c1, c2)", split it
                t_parts = table.split('(', 1)
                table = t_parts[0].strip()
                t_cols = t_parts[1].rstrip(')').split(',')
                columns = [c.strip() for c in t_cols]

            source = load_info.get('source')
            for k in opts.keys():
                if k in load_info:
                    opts[k] = load_info[k]
            
            if 'columns' in load_info:
                columns = load_info['columns']
            if 'source_type' in load_info:
                source_type = load_info['source_type']
            
            # 1. Handle raw command mode
            if command:
                logger.info(f"Executing raw COPY command: {command}")
                return self.execute(command)

            # 2. Source type must be specified or auto-detect from source value in dict
            if source_type is None:
                if hasattr(source, 'read'):
                    source_type = 'file'  # file-like object
                elif isinstance(source, str):
                    upper_src = source.strip().upper()
                    if upper_src.startswith('SELECT') or upper_src.startswith('(SELECT'):
                        source_type = 'cursor'
                    else:
                        source_type = 'file'  # assume file path
                else:
                     # Attempt to check if dict had source_type
                     if isinstance(load_info, dict) and 'source_type' in load_info:
                         source_type = load_info['source_type']
                     else:
                        raise ValueError(f"Unable to detect source_type for source: {type(source)}")
            
            # Build column list if provided
            col_clause = f"({', '.join(columns)})" if columns else ""
            
            if source_type == 'cursor':
                if columns:
                    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) {source}"
                else:
                    insert_sql = f"INSERT INTO {table} {source}"
                return self.execute(insert_sql)
            


            elif source_type == 'file':
                # COPY FROM file/stdin
                logger.info(f"Bulk load from file into {table}")
                
                # Build COPY command from opts
                copy_options = [f"FORMAT {opts['format']}"]
                copy_options.append(f"DELIMITER '{opts['delimiter']}'")
                if opts['null']:
                    copy_options.append(f"NULL '{opts['null']}'")
                if opts['header']:
                    copy_options.append("HEADER")
                if opts['format'].lower() == 'csv':
                    copy_options.append(f"QUOTE '{opts['quote']}'")
                    if opts['escape']:
                        copy_options.append(f"ESCAPE '{opts['escape']}'")
                copy_options.append(f"ENCODING '{opts['encoding']}'")
                
                options_str = ', '.join(copy_options)
                copy_sql = f"COPY {table} {col_clause} FROM STDIN WITH ({options_str})"
                
                logger.debug(f"COPY SQL: {copy_sql}")
                
                # Get file handle
                if hasattr(source, 'read'):
                    # Already a file-like object
                    file_handle = source
                else:
                    # File path - open it
                    file_handle = open(source, 'r', encoding=opts['encoding'])
                    should_close_file = True
                
                # Execute COPY using psycopg3's copy() API
                cur = self.conn.cursor()
                with cur.copy(copy_sql) as copy:
                    while data := file_handle.read(8192):
                        copy.write(data)
                
                # Get row count - psycopg3 provides rowcount after copy completes
                row_count = cur.rowcount if cur.rowcount >= 0 else 0
                
                message = f"COPY to {table} completed. {row_count} rows loaded."
                logger.info(message)
                
                return {
                    "status": 0,
                    "message": message,
                    "data": [],
                    "count": row_count
                }
                
        except Exception as e:
            message = f"Failed to bulk load into {table}: {e}"
            logger.error(message, exc_info=True)
            return {"status": -1, "message": message, "data": [], "count": 0}
            
        finally:
            if cur:
                cur.close()
            if should_close_file and file_handle:
                file_handle.close()

