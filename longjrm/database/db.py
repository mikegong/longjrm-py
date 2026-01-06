"""
Description:
JRM - This is a generic JSON Relational Mapping library that wrap crud sql statements via database api.
conn - database connection
table - the table that is populated
data - json data including column and value pairs
where condition - json data that defines where column and value pairs
*/
"""

import json
import re
import datetime
import logging
import traceback
from longjrm.config.runtime import get_config
from longjrm.database.placeholder_handler import PlaceholderHandler
from longjrm.connection import dbconn
from longjrm.utils import sql as sql_utils, data as data_utils


logger = logging.getLogger(__name__)

class Db:
    """
    Base database class for JSON Relational Mapping operations.
    
    Note: 
    self.conn is a raw DB-API connection (e.g., psycopg2.connection,
    pymysql.Connection), NOT a DatabaseConnection wrapper.
    
    For driver-specific operations like autocommit, we use standalone functions
    from dbconn module which handle different driver APIs transparently.
    """

    def __init__(self, client):
        # client['conn'] is the raw DB-API connection from DatabaseConnection.connect()
        self.conn = client['conn']
        self.database_type = client['database_type']
        self.database_name = client['database_name']
        self.placeholder = '%s'  # default placeholder, subclasses may override
        jrm_cfg = get_config()
        self.data_fetch_limit = jrm_cfg.data_fetch_limit
        self.placeholder_handler = PlaceholderHandler()

    # -------------------------------------------------------------------------
    # Abstract methods - must be implemented by subclasses
    # -------------------------------------------------------------------------
    
    def get_cursor(self):
        """Get a cursor for executing queries. Subclasses must override."""
        raise NotImplementedError("Subclass must implement get_cursor()")
    
    def get_stream_cursor(self):
        """Get a cursor optimized for streaming large result sets. Subclasses must override."""
        raise NotImplementedError("Subclass must implement get_stream_cursor()")

    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        """
        Build the database-specific UPSERT clause (ON CONFLICT, ON DUPLICATE KEY, etc.)
        
        Subclasses must override this method with database-specific syntax.
        
        Args:
            key_columns: list of column names that define uniqueness
            update_columns: list of column names to update on conflict
            for_values: True for VALUES-based upsert, False for SELECT-based upsert
            
        Returns:
            str: The database-specific upsert clause (e.g., "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name")
        """
        raise NotImplementedError("Subclasses must implement _build_upsert_clause with database-specific syntax")

    # -------------------------------------------------------------------------
    # Transaction control - uses dbconn functions for driver-specific handling
    # -------------------------------------------------------------------------
    
    def set_autocommit(self, value):
        """Set autocommit mode. Uses dbconn for driver-specific handling."""
        dbconn.set_autocommit(self.conn, value)
    
    def get_autocommit(self):
        """Get current autocommit state. Uses dbconn for driver-specific handling."""
        return dbconn.get_autocommit(self.conn)

    def commit(self):
        """Commit the current transaction."""
        self.conn.commit()

    def rollback(self):
        """Rollback the current transaction."""
        self.conn.rollback()
    
    def supports_returning(self):
        """
        Return True if database supports RETURNING clause. 
        Default is False. Subclasses (like Postgres) can override.
        """
        return False
    
    def get_returning_clause(self, columns):
        """
        Get the RETURNING clause for INSERT statements.
        Default implementation returns empty string. Subclasses supporting RETURNING (like Postgres) can override.
        """
        return ""

    def select(self, table, columns=None, where=None, options=None):
        """
        Execute SELECT query.
        """
        select_query, arr_values = self._select_constructor(table, columns, where, options)
        return self.query(select_query, arr_values)

    def _select_constructor(self, table, columns=None, where=None, options=None):
        # handle default
        if columns is None:
            columns = ["*"]
        if options is None:
            options = {
                "limit": self.data_fetch_limit,
                "order_by": []
            }

        str_column = ', '.join(columns) if isinstance(columns, list) else None
        if not str_column:
            raise ValueError('Invalid columns')

        str_where, arr_values = sql_utils.where_parser(where, self.placeholder)
        str_order = ' order by ' + ', '.join(options.get('order_by', [])) if options.get('order_by') else ''
        str_limit = '' if not options.get('limit') or options.get('limit') == 0 else ' limit ' + str(options['limit'])

        select_query = "select " + str_column + " from " + table + str_where + str_order + str_limit
        return select_query, arr_values

    def _prepare_sql(self, sql, arr_values):
        """
        Prepare SQL statement for execution.
        Handles placeholder conversion, CURRENT keyword injection, and database-specific escaping.
        
        Args:
            sql: SQL statement to prepare
            arr_values: values to bind to query
            
        Returns:
            tuple of (prepared_sql, processed_values)
        """
        if arr_values:
            sql, converted_values = self.placeholder_handler.convert_to_positional(
                sql, arr_values, self.placeholder
            )
            sql, processed_values = sql_utils.inject_current(sql, converted_values, self.placeholder)
        else:
            processed_values = arr_values
            
        # Escape SQL for database-specific requirements
        sql = self._escape_sql(sql)
        return sql, processed_values

    def _escape_sql(self, sql):
        """
        Handles escaping of special characters (like %) that might conflict
        with parameterized query placeholders.
        Default implementation handles standard SQL escaping (Postgres/MySQL).
        
        Args:
            sql: SQL string to escape
        
        Returns:
            Escaped SQL string
        """
        # Replace standalone % with %% (escape them) but leave %s placeholders alone
        return re.sub(r'%(?!s)', '%%', sql)

    def query(self, sql, arr_values=None):
        """
        Execute query with small result set, return entire result set.
        
        Args:
            sql: SQL statement to execute
            arr_values: values to bind to query (supports positional and named placeholders)
        """
        logger.debug(f"Execute SQL: {sql}")
        logger.debug(f"Execute values: {arr_values}")

        cur = None
        try:
            # Get cursor from subclass (PostgresDb/MySQLDb provide their specific cursor types)
            cur = self.get_cursor()
            
            # Prepare SQL with placeholder conversion and escaping
            sql, processed_values = self._prepare_sql(sql, arr_values)
                
            logger.debug(f"Executing query: {sql}")
            cur.execute(sql, processed_values)
            
            rows = cur.fetchall()
            columns = list(rows[0].keys()) if len(rows) > 0 else []
            logger.info(f"Query completed successfully with {len(rows)} rows returned")
            return {"status": 0, "message": "Query completed successfully", "data": rows, "columns": columns, "count": len(rows)}

        finally:
            if cur:
                cur.close()

    def stream_query(self, sql, arr_values=None, *, max_error_count=0):
        """
        Execute query and stream results row by row using generator.
        This is memory-efficient for large result sets as it doesn't load all rows at once.
        
        Args:
            sql: SQL statement to execute
            arr_values: values to bind to query (supports positional and named placeholders)
            max_error_count: Maximum number of errors to tolerate before failing (default 0)
            
        Yields:
            tuple of (row_number, row_dict, status):
                - row_number: 1-indexed sequence number of the row (0 if error before any rows)
                - row_dict: dictionary with column-value pairs ({} if error)
                - status: 0 for success, -1 for error
        """
        row_number = 0
        current_error_count = 0
        cur = None
        logger.debug(f"Query to stream: {sql}")

        try:
            # Get stream cursor from subclass (PostgresDb/MySQLDb provide their specific cursor types)
            cur = self.get_stream_cursor()
            
            # Prepare SQL with placeholder conversion and escaping
            sql, processed_values = self._prepare_sql(sql, arr_values)
            
            cur.execute(sql, processed_values)
            logger.debug(f"Stream query executed: {sql}")
            
            # Fetch and yield rows one by one
            while True:
                try:
                    row = cur.fetchone()
                    if not row:
                        break
                    
                    row_number += 1
                    # Convert RealDictRow to regular dict for consistency
                    yield row_number, dict(row), 0
                    
                except Exception as e:
                    current_error_count += 1
                    logger.warning(f"Error fetching row {row_number + 1} (Error {current_error_count}/{max_error_count}): {e}")
                    
                    if current_error_count > max_error_count:
                        logger.error(f"Max error count ({max_error_count}) exceeded. Aborting.")
                        raise e
                    
                    # Yield error status for downstream handling if tolerating error
                    row_number += 1
                    yield row_number, {}, -1
            
            logger.info(f"{row_number} rows sent to downstream successfully via stream")

        except Exception as e:
            logger.error(f'stream_query failed at row {row_number}: {e}', exc_info=True)
            yield row_number, {}, -1
        
        finally:
            if cur:
                cur.close()

    def stream_insert(self, stream, table, *, commit_count=10000, max_error_count=0):
        """
        Insert stream data into table with optional periodic commits.
        
        This method consumes a stream (generator/iterator) of rows and inserts them
        into the specified table. It supports transactional control with periodic commits
        for large data sets.
        
        Args:
            stream: Iterator/generator yielding rows in one of these formats:
                - (row_number, row_dict): Simple format with row number and data
                - (row_number, row_dict, status): Extended format including status (0=success, -1=error)
            table: Target table/collection name to insert into
            commit_count: Number of rows between commits.
                         - When > 0: Disables autocommit, performs manual commits every N rows,
                                   commits remaining rows at end, restores original autocommit state.
                         - When = 0: Respects current autocommit setting, no manual commits.
                                   Caller is responsible for committing if autocommit is disabled.
                         Default is 10000 for batch transaction control.
            max_error_count: Maximum number of errors to tolerate before failing (default 0)
                          
        Returns:
            Dictionary with:
                - status: 0 for success, -1 for failure
                - record_count: Number of rows processed
                - message: Descriptive message about the operation result
        """
        row_number = 0
        result = {}
        autocommit_was_enabled = True  # Track original autocommit state
        current_error_count = 0
        
        try:
            # Determine autocommit handling based on commit_count
            if commit_count != 0:
                # Transaction mode - manual commits every commit_count rows
                autocommit_was_enabled = self.get_autocommit()
                self.set_autocommit(False)
            
            for stream_row in stream:
                row_number, row, row_status = stream_row
                # Check for upstream errors
                if row_status and row_status != 0:
                    current_error_count += 1
                    message = f"Upstream error at row {row_number} to table {table} (Error {current_error_count}/{max_error_count})"
                    logger.warning(message)
                    
                    if current_error_count > max_error_count:
                        # Rollback any uncommitted changes
                        if commit_count != 0:
                            self.rollback()
                        return {"status": -1, "record_count": row_number, "message": message}
                    
                    # Continue to next row if tolerating error
                    continue
                
                # Insert the row
                result = self.insert(table, row)
                
                if result.get('status') != 0:
                    current_error_count += 1
                    message = f"Failed to insert into table {table} at row {row_number} (Error {current_error_count}/{max_error_count}): {result.get('message', 'Unknown error')}"
                    logger.warning(message)
                        
                    if current_error_count > max_error_count:
                        if commit_count != 0:
                            self.rollback()
                        return {"status": -1, "record_count": row_number, "message": message}
                    
                    # Continue if tolerating error. 
                    continue
                
                # Periodic commit based on commit_count
                if commit_count != 0 and row_number % commit_count == 0:
                    self.commit()
                    logger.info(f"Committed {row_number} rows into {table}")

            # Final commit for remaining rows
            if result:  # Stream was not empty
                if result.get('status') == 0:
                    if commit_count != 0:
                        self.commit()
                    message = f"{row_number} rows inserted into {table} successfully"
                    logger.info(message)
                    return {"status": 0, "record_count": row_number, "message": message}
            else:
                # Empty stream
                message = "Incoming stream is empty"
                logger.info(message)
                return {"status": 0, "record_count": 0, "message": message}

        except Exception as e:
            # Database exceptions are always fatal
            error_message = f"Fatal database error during insert at row {row_number}: {e}"
            logger.error(error_message, exc_info=True)
            if commit_count != 0:
                self.rollback()
            return {"status": -1, "record_count": row_number, "message": error_message}

        finally:
            if commit_count != 0:
                self.set_autocommit(autocommit_was_enabled)

    def stream_update(self, stream, table, *, commit_count=10000, max_error_count=0):
        """
        Update table from stream data with optional periodic commits.
        
        This method consumes a stream (generator/iterator) of update operations and applies
        them to the specified table. Each stream row must contain both the data to update
        and the condition (where clause) for identifying rows to update.
        
        Args:
            stream: Iterator/generator yielding rows in one of these formats:
                - (row_number, row_dict): Simple format where row_dict contains:
                    - 'data': dict of column-value pairs to update
                    - 'condition': dict defining the where clause
                - (row_number, row_dict, status): Extended format including status (0=success, -1=error)
            table: Target table/collection name to update
            commit_count: Number of rows between commits.
                         - When > 0: Disables autocommit, performs manual commits every N rows,
                                   commits remaining rows at end, restores original autocommit state.
                         - When = 0: Respects current autocommit setting, no manual commits.
                                   Caller is responsible for committing if autocommit is disabled.
                         Default is 10000 for batch transaction control.
            max_error_count: Maximum number of errors to tolerate before failing (default 0)
                          
        Returns:
            Dictionary with:
                - status: 0 for success, -1 for failure
                - record_count: Number of rows processed
                - message: Descriptive message about the operation result
        """
        row_number = 0
        result = {}
        autocommit_was_enabled = True  # Track original autocommit state
        current_error_count = 0
        
        try:
            # Determine autocommit handling based on commit_count
            if commit_count != 0:
                # Transaction mode - manual commits every commit_count rows
                autocommit_was_enabled = self.get_autocommit()
                self.set_autocommit(False)
            
            for stream_row in stream:
                row_status = 0
                row_number, row, row_status = stream_row
                
                # Check for upstream errors
                if row_status and row_status != 0:
                    current_error_count += 1
                    message = f"Upstream error at row {row_number} to table {table} (Error {current_error_count}/{max_error_count})"
                    logger.warning(message)
                    
                    if current_error_count > max_error_count:
                        if commit_count != 0:
                            self.rollback()
                        return {"status": -1, "record_count": row_number, "message": message}
                    continue
                
                # Validate row structure
                if not isinstance(row, dict) or 'data' not in row or 'condition' not in row:
                    current_error_count += 1
                    message = f"Invalid row format at row {row_number}: expected dict with 'data' and 'condition' keys (Error {current_error_count}/{max_error_count})"
                    logger.warning(message)
                    
                    if current_error_count > max_error_count:
                        if commit_count != 0:
                            self.rollback()
                        return {"status": -1, "record_count": row_number, "message": message}
                    continue
                
                # Update the row
                result = self.update(table, row['data'], row['condition'])
                
                if result.get('status') != 0:
                    current_error_count += 1
                    message = f"Failed to update table {table} at row {row_number} (Error {current_error_count}/{max_error_count}): {result.get('message', 'Unknown error')}"
                    logger.warning(message)
                    
                    if current_error_count > max_error_count:
                        if commit_count != 0:
                            self.rollback()
                        return {"status": -1, "record_count": row_number, "message": message}
                    continue
                
                # Periodic commit based on commit_count
                if commit_count != 0 and row_number % commit_count == 0:
                    self.commit()
                    logger.info(f"Committed {row_number} updates into {table}")
            
            # Final commit for remaining updates
            if result:  # Stream was not empty
                if result.get('status') == 0:
                    if commit_count != 0:
                        self.commit()
                    message = f"{row_number} rows updated in {table} successfully"
                    logger.info(message)
                    return {"status": 0, "record_count": row_number, "message": message}
            else:
                # Empty stream
                message = "Incoming stream is empty"
                logger.info(message)
                return {"status": 0, "record_count": 0, "message": message}


        except Exception as e:
            # Database exceptions are always fatal
            error_message = f"Fatal database error during update at row {row_number}: {e}"
            logger.error(error_message, exc_info=True)
            if commit_count != 0:
                self.rollback()
            return {"status": -1, "record_count": row_number, "message": error_message}

        finally:
            if commit_count != 0:
                self.set_autocommit(autocommit_was_enabled)

    def stream_merge(self, stream, table, key_columns, *, commit_count=10000, max_error_count=0):
        """
        Merge (upsert) stream data into table with optional periodic commits.
        
        This method consumes a stream (generator/iterator) of rows and merges them
        into the specified table. For each row, it performs an INSERT if the record
        doesn't exist (based on key_columns), or UPDATE if it does exist.
        
        Args:
            stream: Iterator/generator yielding rows in one of these formats:
                - (row_number, row_dict): Simple format with row number and data
                - (row_number, row_dict, status): Extended format including status (0=success, -1=error)
            table: Target table/collection name to merge into
            key_columns: List of column names that define uniqueness for matching records
            commit_count: Number of rows between commits.
                         - When > 0: Disables autocommit, performs manual commits every N rows,
                                   commits remaining rows at end, restores original autocommit state.
                         - When = 0: Respects current autocommit setting, no manual commits.
                                   Caller is responsible for committing if autocommit is disabled.
                         Default is 10000 for batch transaction control.
            max_error_count: Maximum number of errors to tolerate before failing (default 0)
                          
        Returns:
            Dictionary with:
                - status: 0 for success, -1 for failure
                - record_count: Number of rows processed
                - message: Descriptive message about the operation result
        """
        row_number = 0
        result = {}
        autocommit_was_enabled = True  # Track original autocommit state
        current_error_count = 0
        
        try:
            # Determine autocommit handling based on commit_count
            if commit_count != 0:
                # Transaction mode - manual commits every commit_count rows
                autocommit_was_enabled = self.get_autocommit()
                self.set_autocommit(False)
            
            for stream_row in stream:
                row_status = 0
                row_number, row, row_status = stream_row
                
                if row_status and row_status != 0:
                    current_error_count += 1
                    message = f"Upstream error at row {row_number} to table {table} (Error {current_error_count}/{max_error_count})"
                    logger.warning(message)
                    
                    if current_error_count > max_error_count:
                        if commit_count != 0:
                            self.rollback()
                        return {"status": -1, "record_count": row_number, "message": message}
                    continue
                
                result = self.merge(table, row, key_columns)
                
                if result.get('status') != 0:
                    current_error_count += 1
                    message = f"Failed to merge into table {table} at row {row_number} (Error {current_error_count}/{max_error_count}): {result.get('message', 'Unknown error')}"
                    logger.warning(message)
                    
                    if current_error_count > max_error_count:
                        if commit_count != 0:
                            self.rollback()
                        return {"status": -1, "record_count": row_number, "message": message}
                    continue
                
                if commit_count != 0 and row_number % commit_count == 0:
                    self.commit()
                    logger.info(f"Committed {row_number} merges into {table}")
            
            if row_number == 0:
                message = f"Incoming stream for {table} is empty"
                logger.info(message)
                return {"status": 0, "record_count": 0, "message": message}
            elif result.get('status') == 0:
                if commit_count != 0:
                    self.commit()
                message = f"{row_number} rows merged into {table} successfully"
                logger.info(message)
                return {"status": 0, "record_count": row_number, "message": message}

        except Exception as e:
            # Database exceptions are always fatal
            error_message = f"Fatal database error during merge at row {row_number}: {e}"
            logger.error(error_message, exc_info=True)
            if commit_count != 0:
                self.rollback()
            return {"status": -1, "record_count": row_number, "message": error_message}

        finally:
            if commit_count != 0:
                self.set_autocommit(autocommit_was_enabled)

    def insert(self, table, data, return_columns=None):
        """
        Insert data in JSON format into table
        Args:
            table: target table name
            data: JSON data - either:
                  - Single record: {"col1": "val1", "col2": "val2"}
                  - Multiple records: [{"col1": "val1"}, {"col1": "val2"}]
            return_columns: optional list of columns to return from inserted records
        Returns:
            dictionary with status, message, data (empty), and count (affected rows)
        """
        if return_columns is None:
            return_columns = []
            
        # construct SQL and values, then use execute()
        insert_query, arr_values = self._insert_constructor(table, data, return_columns)
        if return_columns:
            return self.query(insert_query, arr_values)
        else:
            return self.execute(insert_query, arr_values)

    def _insert_constructor(self, table, data, return_columns=None):
        """
        Construct SQL INSERT statement from JSON data
        Args:
            table: target table name
            data: JSON data - either single record or list of records
            return_columns: optional list of columns to return from inserted records
        Returns:
            tuple of (sql_query, values_array)
        """
        if return_columns is None:
            return_columns = []
            
        # Handle both single record and bulk insert
        if isinstance(data, list):
            return self._bulk_insert_constructor(table, data, return_columns)
        else:
            return self._single_insert_constructor(table, data, return_columns)

    def _single_insert_constructor(self, table, data, return_columns=None):
        """
        Construct SQL INSERT statement for a single record
        """
        if return_columns is None:
            return_columns = []
            
        columns = list(data.keys())
        str_col = ', '.join(columns)
        
        # Use placeholder for all values; CURRENT keywords will be injected by inject_current later
        placeholders = [self.placeholder for _ in columns]
        str_qm = ', '.join(placeholders)
        
        list_val = [self._process_value(data[k]) for k in columns]

        if return_columns and self.supports_returning():
            returning_clause = self.get_returning_clause(return_columns)
            sql = f"INSERT INTO {table} ({str_col}) VALUES ({str_qm}){returning_clause}"
        else:
            sql = f"INSERT INTO {table} ({str_col}) VALUES ({str_qm})"

        return sql, list_val

    def _bulk_insert_constructor(self, table, data_list, return_columns=None):
        """
        Construct SQL INSERT statement for bulk records using single SQL with multiple VALUES
        """
        if return_columns is None:
            return_columns = []
            
        if not data_list:
            return "SELECT 1 WHERE 1=0", []  # No-op query for empty list
            
        # Validate all records have consistent columns
        first_record_keys = set(data_list[0].keys())
        for i, record in enumerate(data_list):
            if set(record.keys()) != first_record_keys:
                raise ValueError(f"Record {i} has inconsistent columns. Expected: {first_record_keys}, Got: {set(record.keys())}")
        
        # Get columns from first record
        columns = list(data_list[0].keys())
        str_col = ', '.join(columns)
        
        # Build multiple VALUES clauses and collect all values
        values_clauses = []
        all_values = []
        
        for record in data_list:
            record_placeholders = []
            for col in columns:
                # Regular value: process it and add placeholder
                data_value = self._process_value(record[col])
                record_placeholders.append(self.placeholder)
                all_values.append(data_value)
            
            values_clauses.append(f"({', '.join(record_placeholders)})")
        
        # Construct INSERT statement with multiple VALUES
        values_sql = ', '.join(values_clauses)
        
        if return_columns and self.supports_returning():
            returning_clause = self.get_returning_clause(return_columns)
            sql = f"INSERT INTO {table} ({str_col}) VALUES {values_sql}{returning_clause}"
        else:
            sql = f"INSERT INTO {table} ({str_col}) VALUES {values_sql}"
        
        return sql, all_values

    def _process_value(self, value):
        """
        Process individual values for database operations
        Handles different data types: dict, list, primitives, datetime objects, None
        """
        if value is None:
            return None
        elif isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)
        elif isinstance(value, list):
            if len(value) > 0 and isinstance(value[0], dict):
                # Only convert list of dicts to JSON
                return json.dumps(value, ensure_ascii=False)
            else:
                # For simple lists, pass as-is to database driver
                # The driver will handle conversion (e.g., psycopg2 -> PostgreSQL arrays)
                return value
        elif isinstance(value, datetime.date):
            return str(value)
        elif isinstance(value, datetime.datetime):
            return datetime.datetime.strftime(value, '%Y-%m-%d %H:%M:%S.%f')
        elif isinstance(value, str):
            return value
        else:
            return value

    def execute(self, sql, arr_values=None):
        """
        Execute query with no return result set such as insert, update, delete, etc.
        Args:
            sql: SQL statement to execute
            arr_values: values that need to be bound to query (supports both positional and named placeholders)
        Returns:
            dictionary with status, message, data (empty), and count (affected rows)
        """
            
        logger.debug(f"Execute SQL: {sql}")
        logger.debug(f"Execute values: {arr_values}")

        cur = None
        try:
            # Get appropriate cursor for the database type
            cur = self.get_cursor()

            # Prepare SQL with placeholder conversion, current injection and escaping
            sql, processed_values = self._prepare_sql(sql, arr_values)

            # Execute the statement
            logger.debug(f"Executing query: {sql}")
            cur.execute(sql, processed_values)
            
            # Get affected row count
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

    def update(self, table, data, where=None):
        """
        Update data in JSON format to table
        Args:
            table: target table name
            data: JSON data with column-value pairs to update {"col1": "val1", "col2": "val2"}
            where: JSON where condition (optional)
        Returns:
            dictionary with status, message, data (empty), and count (affected rows)
        """
        # Handle empty update data
        if not data:
            logger.warning("Update called with empty data - no operation performed")
            return {
                'status': 0,
                'message': 'No data to update',
                'data': [],
                'count': 0
            }
        
        # construct SQL and values, then use execute()
        update_query, arr_values = self._update_constructor(table, data, where)
        return self.execute(update_query, arr_values)

    def _update_constructor(self, table, data, where=None):
        """
        Construct SQL UPDATE statement from JSON data
        Args:
            table: target table name
            data: JSON data with column-value pairs to update
            where: JSON where condition (optional)
        Returns:
            tuple of (sql_query, values_array)
        """
        update_str = ''
        list_val = []

        for k, v in data.items():
            data_value = self._process_value(v)
            
            if data_value is None:
                update_str += ", " + k + " = NULL"
            else:
                update_str += ", " + k + " = " + self.placeholder
                list_val.append(data_value)

        # Remove leading comma and space from update string
        update_str = update_str[2:] if update_str else ''

        # Parse where condition
        where_str, where_values = sql_utils.where_parser(where, self.placeholder)
        
        # Combine update values and where values
        all_values = list_val + where_values

        sql = f"UPDATE {table} SET {update_str}{where_str}"
        return sql, all_values

    def delete(self, table, where=None):
        """
        Delete data from table based on where conditions
        Args:
            table: target table name
            where: JSON where condition (optional, but recommended to avoid deleting all records)
        Returns:
            dictionary with status, message, data (empty), and count (affected rows)
        """
        # construct SQL and values, then use execute()
        delete_query, arr_values = self._delete_constructor(table, where)
        return self.execute(delete_query, arr_values)

    def _delete_constructor(self, table, where=None):
        """
        Construct SQL DELETE statement from JSON where condition
        Args:
            table: target table name
            where: JSON where condition (optional)
        Returns:
            tuple of (sql_query, values_array)
        """
        # Parse where condition
        where_str, where_values = sql_utils.where_parser(where, self.placeholder)
        
        # Construct DELETE statement
        sql = f"DELETE FROM {table}{where_str}"
        return sql, where_values

    def merge(self, table, data, key_columns):
        """
        Merge (upsert) data in JSON format into table
        This function performs an INSERT if the record doesn't exist based on key_columns,
        or UPDATE if it does exist.
        
        Args:
            table: target table name
            data: JSON data with column-value pairs {"col1": "val1", "col2": "val2"}
                  Can be a single record (dict) or list of records (list of dicts)
            key_columns: list of column names that define uniqueness for matching records
        Returns:
            dictionary with status, message, data (empty), and count (affected rows)
        """
        # Validate input parameters
        if not data:
            logger.warning("Merge called with empty data - no operation performed")
            return {
                'status': 0,
                'message': 'No data to merge',
                'data': [],
                'count': 0
            }
        
        if not key_columns:
            raise ValueError("key_columns cannot be empty for merge operation")
        
        # Handle both single record and bulk merge
        if isinstance(data, list):
            return self._bulk_merge(table, data, key_columns)
        else:
            return self._single_merge(table, data, key_columns)

    def _single_merge(self, table, data, key_columns):
        """
        Merge a single record into the table
        """
        # construct merge/upsert SQL
        merge_query, arr_values = self._merge_constructor(table, data, key_columns)
        return self.execute(merge_query, arr_values)

    def _bulk_merge(self, table, data_list, key_columns):
        """
        Merge multiple records into the table using batch execution.
        
        Uses executemany() for efficient batch processing, allowing the database
        driver to optimize the execution of repeated statements with different values.
        
        Args:
            table: target table name
            data_list: list of records (dicts) to merge
            key_columns: list of column names that define uniqueness
            
        Returns:
            dictionary with status, message, data (empty), and count (affected rows)
        """
        cur = None
        try:
            if not data_list:
                return {
                    "status": 0,
                    "message": "No data to merge",
                    "data": [],
                    "count": 0
                }

            sql_template = None
            all_values = []
            
            # Generate SQL and collect values for all records
            for i, record in enumerate(data_list):
                sql, values = self._merge_constructor(table, record, key_columns)
                
                if sql_template is None:
                    sql_template = sql
                elif sql != sql_template:
                    raise ValueError(f"Record {i} generated different SQL than previous records. Bulk merge requires consistent schema/keys.")
                
                all_values.append(values)
            
            # Prepare SQL (handles placeholder conversion and escaping)
            sql_template, _ = self._prepare_sql(sql_template, all_values[0] if all_values else None)
            
            # Execute batch using executemany
            cur = self.get_cursor()
            cur.executemany(sql_template, all_values)
            total_affected = cur.rowcount if cur.rowcount >= 0 else len(data_list)
            
            success_msg = f"Bulk merge completed successfully. {total_affected} rows affected."
            logger.info(success_msg)
            
            return {
                "status": 0,
                "message": success_msg,
                "data": [],
                "count": total_affected
            }
            
        except Exception as e:
            logger.error(f"_bulk_merge failed: {e}")
            raise
            
        finally:
            if cur:
                cur.close()

    def _merge_constructor(self, table, data, key_columns):
        """
        Construct SQL MERGE/UPSERT statement from JSON data.
        
        Uses common logic for building INSERT part, delegates to _build_upsert_clause
        for database-specific conflict handling.
        
        Args:
            table: target table name
            data: JSON data with column-value pairs
            key_columns: list of column names for matching
        Returns:
            tuple of (sql_query, values_array)
        """
        # Validate key columns exist in data
        for key_col in key_columns:
            if key_col not in data:
                raise ValueError(f"Key column '{key_col}' not found in data")
        
        # Build INSERT part
        columns = list(data.keys())
        str_col = ', '.join(columns)
        placeholders = []
        values = []
        
        for col in columns:
            placeholders.append(self.placeholder)
            values.append(self._process_value(data[col]))
        
        str_placeholders = ', '.join(placeholders)
        
        # Determine update columns (all non-key columns)
        update_columns = [col for col in columns if col not in key_columns]
        
        # Get database-specific upsert clause from subclass
        upsert_clause = self._build_upsert_clause(key_columns, update_columns, for_values=True)
        
        sql = f"INSERT INTO {table} ({str_col}) VALUES ({str_placeholders}) {upsert_clause}"
        
        return sql, values

    def merge_select(self, source_table, target_table, insert_columns, key_columns,
                     order_by=None, conditions=None, source_select=None, update_columns=None):
        """
        Merge data from source table via SELECT into target table.
        
        Uses common logic for building INSERT...SELECT, delegates to _build_upsert_clause
        for database-specific conflict handling.
        
        Args:
            source_table: Name of the source table to select from (ignored if source_select is provided)
            target_table: Name of the target table to merge into
            insert_columns: List of column names to insert/update
            key_columns: List of column names that define uniqueness for matching records
            order_by: Optional ORDER BY clause for the source query (e.g., "id DESC")
            conditions: Optional WHERE conditions dict for filtering source data
            source_select: Optional custom SELECT statement to use instead of auto-generated one
            update_columns: Optional list of columns to update (defaults to insert_columns minus key_columns)
            
        Returns:
            Dictionary with:
                - status: 0 for success, -1 for failure
                - message: Descriptive message about the operation result
                - total: Number of rows affected (if available)
        """
        try:
            insert_column_str = ', '.join(insert_columns)
            
            # Determine update columns (default: insert_columns minus key_columns)
            if not update_columns:
                update_columns = [col for col in insert_columns if col not in key_columns]
            
            # Build the source SELECT query
            if source_select:
                source_sql = source_select
            else:
                where_clause = sql_utils.conditions_to_string(conditions) if conditions else ""
                order_clause = f" ORDER BY {order_by}" if order_by else ""
                source_sql = f"SELECT {insert_column_str} FROM {source_table}{where_clause}{order_clause}"
            
            # Get database-specific upsert clause from subclass
            upsert_clause = self._build_upsert_clause(key_columns, update_columns, for_values=False)
            
            sql = f"""INSERT INTO {target_table} ({insert_column_str})
                    {source_sql}
                    {upsert_clause}
                    """
            
            logger.debug(f"Merge Select SQL: {sql}")
            
            cur = self.get_cursor()
            cur.execute(sql)
            affected_rows = cur.rowcount
            self.commit()
            cur.close()
            
            message = f"MERGE into table {target_table} succeeded. {affected_rows} rows affected."
            logger.info(message)
            return {"status": 0, "message": message, "total": affected_rows}
            
        except Exception as e:
            message = f'Failed to execute merge_select statement: {e}'
            logger.error(message)
            logger.error(traceback.format_exc())
            return {"status": -1, "message": message}

    def run_query_from_file(self, sql_file, values=None):
        """
        Execute SQL query from a file.
        
        This method reads SQL from a file and executes it using the query method.
        Useful for running complex queries stored in external files.
        
        Args:
            sql_file: Path to the SQL file to execute
            values: Optional list of values to bind to the query placeholders
            
        Returns:
            Dictionary with query results (same as query method return format)
            
        Example:
            # Run a simple query file
            result = db.run_query_from_file("queries/get_users.sql")
            
            # Run with parameters
            result = db.run_query_from_file("queries/get_user_by_id.sql", [123])
        """
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql = f.read()
            return self.query(sql=sql, arr_values=values)

    def export_from_query(self, sql, csv_file, values=None, options=None):
        """
        Export query results to a CSV file.
        
        This method executes a SQL query and writes the results to a CSV file.
        It properly handles None/Null values, escaping of special characters,
        and optionally includes a header row.
        
        Note: Python's csv writer cannot differentiate None/Null and blank,
              so this method handles it properly by converting None to empty string.
        
        Args:
            sql: SQL query to execute
            csv_file: Path to the output CSV file
            values: Optional list of values to bind to query placeholders
            options: Optional dict with format control options:
                - header: 'Y' to include header row with column names (default: 'Y')
                - null_value: String to use for NULL values (default: '')
                
        Returns:
            Dictionary with:
                - status: 0 for success, -1 for failure
                - message: Descriptive message about the operation result
                - row_count: Number of rows exported
                
        Example:
            # Export query results with header
            result = db.export_from_query(
                "SELECT * FROM users WHERE status = %s",
                "output/users.csv",
                values=["active"],
                options={"header": "Y"}
            )
            
            # Export without header
            result = db.export_from_query(
                "SELECT name, email FROM users",
                "output/users_no_header.csv",
                options={"header": "N"}
            )
        """
        if options is None:
            options = {}
        if values is None:
            values = []
            
        row_count = 0
        
        try:
            # Use stream_query for memory-efficient processing
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                header_written = False
                
                for row_num, row, status in self.stream_query(sql, values):
                    if status != 0:
                        message = f"Query error at row {row_num}"
                        logger.error(message)
                        return {"status": -1, "message": message, "row_count": row_count}
                    
                    # Write header on first row if requested
                    if not header_written:
                        if options.get('header', 'Y') == 'Y':
                            header_row = data_utils.escape_csv_row(list(row.keys()))
                            f.write(','.join(header_row) + '\n')
                        header_written = True
                    
                    # Write data row
                    null_value = options.get('null_value', '')
                    escaped_row = data_utils.escape_csv_row(list(row.values()), null_value)
                    f.write(','.join(escaped_row) + '\n')
                    row_count += 1
            
            message = f"Query data export succeeded. {row_count} rows have been exported to {csv_file}"
            logger.info(message)
            return {"status": 0, "message": message, "row_count": row_count}
            
        except Exception as e:
            message = f'Failed to export data at row {row_count}: {e}'
            logger.error(message)
            logger.error(traceback.format_exc())
            return {"status": -1, "message": message, "row_count": row_count}
