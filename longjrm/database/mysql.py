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

    def bulk_load(self, table, load_info=None, *, command=None):
        """
        Bulk load data into table using MySQL LOAD DATA LOCAL INFILE.
        
        Args:
            table: Target table name, e.g., "my_table". 
                   Also supports "my_table(col1, col2)" for consistency.
            load_info: Data source string (file path, query) or config dict.
                If dict, supported keys:
                - source: 'file path' or 'SELECT query'
                - source_type: 'file' or 'cursor' (optional, auto-detected)
                - columns: List of columns to load into
                - delimiter: Field delimiter (default ',')
                - enclosed_by: Field enclosure (default '"')
                - escaped_by: Escape character (default '\\')
                - lines_terminated_by: Line terminator (default '\n')
                - ignore_lines: Number of header lines to skip (default 0)
                - character_set: Encoding (default 'utf8mb4')
            command: Optional raw LOAD DATA command (bypasses processing)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        cur = None
        
        try:
            # 0. Enforce dict-only load_info
            if not isinstance(load_info, dict):
                raise TypeError(f"MySQL bulk_load requires a configuration dictionary for 'load_info', got {type(load_info).__name__}")

            # Default options
            opts = {
                'delimiter': ',', 'enclosed_by': '"', 
                'escaped_by': '\\\\', 'lines_terminated_by': '\\n', 
                'ignore_lines': 0, 'character_set': 'utf8mb4'
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
                logger.info(f"Executing raw LOAD DATA command: {command}")
                return self.execute(command)

            # 2. Source type must be specified or auto-detect from source value in dict
            if source_type is None:
                if isinstance(source, str):
                    upper_src = source.strip().upper()
                    if upper_src.startswith('SELECT') or upper_src.startswith('(SELECT'):
                        source_type = 'cursor'
                    else:
                        source_type = 'file'
                else:
                    raise ValueError(f"MySQL bulk_load requires string source (file path or SQL query) inside load_info dict")
            
            if source_type == 'cursor':
                # Use INSERT INTO ... SELECT for cursor mode
                logger.info(f"Bulk load from cursor/query into {table}")
                
                if columns:
                    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) {source}"
                else:
                    insert_sql = f"INSERT INTO {table} {source}"
                
                return self.execute(insert_sql)
            
            elif source_type == 'command':
                 # Legacy support for source_type='command' with load_info=cmd
                 logger.warning("source_type='command' is deprecated. Use 'command' parameter instead.")
                 return self.execute(source)
                
            elif source_type == 'file':
                # Use LOAD DATA LOCAL INFILE
                logger.info(f"Bulk load from file into {table}")
                
                # Build column list
                col_clause = f"({', '.join(columns)})" if columns else ""
                
                # Normalize path for MySQL (forward slashes required even on Windows)
                normalized_source = source.replace('\\', '/')
                
                # Build LOAD DATA command
                # Note: File path must be accessible by the client, not server (LOCAL keyword)
                load_sql = f"""
                    LOAD DATA LOCAL INFILE '{normalized_source}'
                    INTO TABLE {table}
                    CHARACTER SET {opts['character_set']}
                    FIELDS TERMINATED BY '{opts['delimiter']}'
                    OPTIONALLY ENCLOSED BY '{opts['enclosed_by']}'
                    ESCAPED BY '{opts['escaped_by']}'
                    LINES TERMINATED BY '{opts['lines_terminated_by']}'
                """
                
                if opts['ignore_lines'] > 0:
                    load_sql += f" IGNORE {opts['ignore_lines']} LINES"
                
                if col_clause:
                    load_sql += f" {col_clause}"
                
                logger.debug(f"LOAD DATA SQL: {load_sql}")
                
                cur = self.get_cursor()
                cur.execute(load_sql)
                
                row_count = cur.rowcount if cur.rowcount >= 0 else 0
                
                message = f"LOAD DATA to {table} completed. {row_count} rows loaded."
                logger.info(message)
                
                return {
                    "status": 0,
                    "message": message,
                    "data": [],
                    "count": row_count
                }
            else:
                raise ValueError(f"Unknown source_type: {source_type}")
                
        except Exception as e:
            message = f"Failed to bulk load into {table}: {e}"
            logger.error(message, exc_info=True)
            return {"status": -1, "message": message, "data": [], "count": 0}
            
        finally:
            if cur:
                cur.close()

