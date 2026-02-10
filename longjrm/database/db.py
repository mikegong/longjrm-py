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
from abc import ABC, abstractmethod
from longjrm.config.runtime import get_config
from longjrm.database.placeholder_handler import PlaceholderHandler
from longjrm.connection.connectors import get_connector_class, unwrap_connection
from longjrm.utils import sql as sql_utils, data as data_utils


logger = logging.getLogger(__name__)

class Db(ABC):
    """
    Base database class for JSON Relational Mapping operations.
    
    Note: 
    self.conn is a raw DB-API connection (e.g., psycopg.Connection,
    pymysql.Connection), NOT a Connector wrapper.
    
    For driver-specific operations like autocommit, we use standalone functions
    from connector classes which handle different driver APIs.
    """

    def __init__(self, client):
        # client['conn'] is the raw DB-API connection from Connector.connect()
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
    
    @abstractmethod
    def get_cursor(self):
        """Get a cursor for executing queries. Subclasses must override."""
        ...
    
    @abstractmethod
    def get_stream_cursor(self):
        """Get a cursor optimized for streaming large result sets. Subclasses must override."""
        ...

    @abstractmethod
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
        ...

    # -------------------------------------------------------------------------
    # Transaction control - uses connector methods for driver-specific handling
    # -------------------------------------------------------------------------
    
    def set_autocommit(self, value):
        """Set autocommit mode using connector-specific logic."""
        if self.conn is None:
            return
        # Unwrap pooled connection wrappers (DBUtils/SQLAlchemy) to get actual DB-API connection
        actual_conn = unwrap_connection(self.conn)
        get_connector_class(self.database_type).set_dbapi_autocommit(actual_conn, value)
    
    def get_autocommit(self):
        """Get current autocommit state using connector-specific logic."""
        if self.conn is None:
            return True
        # Unwrap pooled connection wrappers (DBUtils/SQLAlchemy) to get actual DB-API connection
        actual_conn = unwrap_connection(self.conn)
        return get_connector_class(self.database_type).get_dbapi_autocommit(actual_conn)

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

    def bulk_load(self, table, load_info=None, *, command=None):
        """
        Bulk load data into table using database-specific high-performance method.
        
        Args:
            table: Target table name
            load_info: Data source or configuration (used if command is not provided).
                Can be:
                - Config dictionary (RECOMMENDED): {'source': '...', 'delimiter': ',', ...}
                - Source string: File path or SQL query (uses default options)
            command: Optional raw database-specific bulk load command. If provided,
                it takes precedence over load_info.
        Raises:
            NotImplementedError: If the database does not support bulk loading
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support bulk_load(). "
            f"Use insert() with a list for batch operations instead."
        )

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

        # Check for dynamic_param option, which may provide better performance in some scenarios
        inline = False
        if options and options.get('dynamic_param') == 'N':
            inline = True

        str_where, arr_values = sql_utils.where_parser(where, self.placeholder, inline=inline)
        str_order = ' order by ' + ', '.join(options.get('order_by', [])) if options.get('order_by') else ''
        limit = options.get('limit')
        
        select_query = self._construct_select_sql(table, str_column, str_where, str_order, limit)
        return select_query, arr_values

    def _construct_select_sql(self, table, str_column, str_where, str_order, limit):
        """
        Construct final SELECT SQL string.
        Can be overridden by subclasses (e.g. SqlServer uses TOP instead of LIMIT).
        """
        str_limit = '' if not limit or limit == 0 else ' limit ' + str(limit)
        return "select " + str_column + " from " + table + str_where + str_order + str_limit

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
            if processed_values:
                cur.execute(sql, processed_values)
            else:
                cur.execute(sql)
            
            rows = cur.fetchall()
            
            # Check if we have results
            columns = []
            final_rows = rows
            
            if cur.description:
                columns = [col[0].lower() for col in cur.description]
                
            if rows and len(rows) > 0:
                first_row = rows[0]
                # If rows are tuples/lists (standard DB-API), convert to dicts
                if not isinstance(first_row, dict):
                    if not columns:
                        # Should not happen for SELECTs
                        logger.warning("Rows returned but no cursor.description available")
                    else:
                        processed_rows = []
                        for row in rows:
                            processed_rows.append(dict(zip(columns, row)))
                        final_rows = processed_rows
                # If rows are already dict-like (e.g. RealDictRow), ensure columns are set if not already
                elif hasattr(first_row, 'keys') and not columns:
                     columns = list(first_row.keys())

            logger.info(f"Query completed successfully with {len(final_rows)} rows returned")
            return {"status": 0, "message": "Query completed successfully", "data": final_rows, "columns": columns, "count": len(final_rows)}

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
            
            if processed_values:
                cur.execute(sql, processed_values)
            else:
                cur.execute(sql)
            logger.debug(f"Stream query executed: {sql}")
            
            # Pre-fetch columns for tuple conversion if needed
            columns = []
            if cur.description:
                columns = [col[0].lower() for col in cur.description]

            # Fetch and yield rows one by one
            while True:
                try:
                    row = cur.fetchone()
                    if not row:
                        break
                    
                    row_number += 1
                    
                    row_dict = {}
                    if columns and not isinstance(row, dict):
                         row_dict = dict(zip(columns, row))
                    else:
                         # Assume dict-like (RealDictRow) or try conversion
                         row_dict = dict(row)
                         
                    yield row_number, row_dict, 0
                    
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

    def stream_query_batch(self, sql, arr_values=None, *, batch_size=1000, max_error_count=0):
        """
        Execute query and stream results in batches (buckets) row by row using generator.
        
        Args:
            sql: SQL statement to execute
            arr_values: values to bind to query
            batch_size: Number of rows per batch (bucket)
            max_error_count: Maximum number of errors to tolerate
            
        Yields:
             tuple of (row_number, batch_data, status):
                - row_number: Total rows yielded so far (at end of batch)
                - batch_data: List of row dictionaries
                - status: 0 for success, -1 for error
        """
        buck_data = []
        total_rows = 0
        last_status = 0
        
        try:
            for row_num, row, status in self.stream_query(sql, arr_values, max_error_count=max_error_count):
                total_rows = row_num
                last_status = status
                
                if status == 0:
                    buck_data.append(row)
                    
                    if len(buck_data) >= batch_size:
                        logger.info(f"Batch of {len(buck_data)} rows sent to downstream (Total: {total_rows})")
                        yield total_rows, buck_data, 0
                        buck_data = []
                else:
                    # Propagate error immediately, or handle as per policy.
                    # Legacy behavior yielded (row_number, {}, -1).
                    yield row_num, {}, -1
            
            # Yield remaining rows
            if buck_data:
                logger.info(f"Final batch of {len(buck_data)} rows sent to downstream (Total: {total_rows})")
                yield total_rows, buck_data, 0
                
        except Exception as e:
            logger.error(f"stream_query_batch failed: {e}", exc_info=True)
            yield total_rows, [], -1

    def _stream_transaction_handler(self, stream, operation_func, commit_count=10000, max_error_count=0, table_name="unknown"):
        """
        Generic handler for stream-based transactional operations (insert/update/merge).
        
        Args:
            stream: Iterator yielding rows
            operation_func: Callable(row, row_number) -> result_dict
            commit_count: Rows between commits (0 to disable manual commit control)
            max_error_count: Max errors allowed
            table_name: Name of table for logging
            
        Returns:
            Result dictionary
        """
        row_number = 0
        result = {}
        autocommit_was_enabled = True
        current_error_count = 0
        
        try:
            if commit_count != 0:
                autocommit_was_enabled = self.get_autocommit()
                self.set_autocommit(False)
            
            for stream_row in stream:
                # Normalize stream row format
                if len(stream_row) == 3:
                     row_number, row, row_status = stream_row
                else:
                     row_number, row = stream_row
                     row_status = 0
                
                # Check upstream status
                if row_status != 0:
                    current_error_count += 1
                    message = f"Upstream error at row {row_number} to table {table_name} (Error {current_error_count}/{max_error_count})"
                    logger.warning(message)
                    
                    if current_error_count > max_error_count:
                        if commit_count != 0: self.rollback()
                        return {"status": -1, "record_count": row_number, "message": message}
                    continue
                
                # Execute operation
                result = operation_func(row, row_number)
                
                if result.get('status') != 0:
                    current_error_count += 1
                    message = f"Failed to process row {row_number} in {table_name} (Error {current_error_count}/{max_error_count}): {result.get('message', 'Unknown error')}"
                    logger.warning(message)
                    
                    if current_error_count > max_error_count:
                        if commit_count != 0: self.rollback()
                        return {"status": -1, "record_count": row_number, "message": message}
                    continue
                
                # Periodic commit
                if commit_count != 0 and row_number > 0 and row_number % commit_count == 0:
                    self.commit()
                    logger.info(f"Committed {row_number} rows into {table_name}")
            
            # Final processing
            if row_number == 0:
                message = f"Incoming stream for {table_name} is empty"
                logger.info(message)
                return {"status": 0, "record_count": 0, "message": message}
                
            if commit_count != 0:
                self.commit()
                
            message = f"{row_number} rows processed into {table_name} successfully"
            logger.info(message)
            return {"status": 0, "record_count": row_number, "message": message}

        except Exception as e:
            error_message = f"Fatal database error at row {row_number}: {e}"
            logger.error(error_message, exc_info=True)
            if commit_count != 0:
                self.rollback()
            return {"status": -1, "record_count": row_number, "message": error_message}
            
        finally:
            if commit_count != 0:
                self.set_autocommit(autocommit_was_enabled)

    def stream_insert(self, stream, table, *, commit_count=10000, max_error_count=0):
        """
        Insert stream data into table with optional periodic commits.
        """
        def op(row, _):
            return self.insert(table, row)
            
        return self._stream_transaction_handler(
            stream, op, commit_count, max_error_count, table_name=table
        )

    def stream_update(self, stream, table, *, commit_count=10000, max_error_count=0):
        """
        Update table from stream data with optional periodic commits.
        """
        def op(row, row_num):
            if not isinstance(row, dict) or 'data' not in row or 'condition' not in row:
                return {"status": -1, "message": "Invalid row format: expected dict with 'data' and 'condition'"}
            return self.update(table, row['data'], row['condition'])
            
        return self._stream_transaction_handler(
            stream, op, commit_count, max_error_count, table_name=table
        )

    def stream_merge(self, stream, table, key_columns, *, commit_count=10000, max_error_count=0):
        """
        Merge (upsert) stream data into table with optional periodic commits.
        """
        def op(row, _):
            return self.merge(table, row, key_columns)
            
        return self._stream_transaction_handler(
            stream, op, commit_count, max_error_count, table_name=table
        )

    def insert(self, table, data, return_columns=None, bulk_size=1000):
        """
        Insert data in JSON format into table
        Args:
            table: target table name
            data: JSON data - either:
                  - Single record: {"col1": "val1", "col2": "val2"}
                  - Multiple records: [{"col1": "val1"}, {"col1": "val2"}]
            return_columns: optional list of columns to return from inserted records
            bulk_size: number of records per batch for bulk inserts (default 1000)
        Returns:
            dictionary with status, message, data (empty), and count (affected rows)
        """
        if return_columns is None:
            return_columns = []
            
        # Handle both single record and bulk insert
        if isinstance(data, list):
            return self._bulk_insert(table, data, return_columns, bulk_size)
        else:
            return self._single_insert(table, data, return_columns)

    def _single_insert(self, table, data, return_columns=None):
        """
        Internal method to handle single record insert.
        """
        # construct SQL and values, then use execute()
        insert_query, arr_values = self._single_insert_constructor(table, data, return_columns)
        if return_columns:
            return self.query(insert_query, arr_values)
        else:
            return self.execute(insert_query, arr_values)

    def _bulk_insert(self, table, data_list, return_columns=None, bulk_size=1000):
        """
        Internal method to handle bulk insert using executemany.
        """
        if not data_list:
            return {"status": 0, "message": "No data to insert", "data": [], "count": 0}

        row_count = len(data_list)
        
        # Validate uniform columns based on first record
        first_row = data_list[0]
        columns = list(first_row.keys())
        str_col = ', '.join(columns)
        placeholders = [self.placeholder for _ in columns]
        str_qm = ', '.join(placeholders)
        
        # Construct a standard single-row INSERT statement
        sql = f"INSERT INTO {table} ({str_col}) VALUES ({str_qm})"

        total_affected = 0
        cur = None
        try:
            cur = self.get_cursor()
            
            for batch in data_utils.datalist_to_dataseq(data_list, bulk_size=bulk_size):
                cur.executemany(sql, batch)
                
                # Try to accumulate affected rows if driver supports it
                # Use getattr to safely check for rowcount attribute without explicit try/catch for AttributeError
                row_count_val = getattr(cur, 'rowcount', -1)
                if row_count_val > 0:
                    total_affected += row_count_val
        
            if total_affected <= 0 and row_count > 0:
                total_affected = row_count # Fallback if driver doesn't report count
                
            message = f"BULK INSERT succeeded. {total_affected} rows affected."
            logger.info(message)
            return {"status": 0, "message": message, "data": [], "count": total_affected}
            
        except Exception as e:
            message = f'Failed to execute bulk insert: {e}'
            logger.error(message)
            logger.error(traceback.format_exc())
            return {"status": -1, "message": message}

        finally:
            if cur:
                cur.close()

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
        values_sql = f"({str_qm})"
        
        list_val = [self._process_value(data[k]) for k in columns]

        sql = self._construct_insert_sql(table, str_col, values_sql, return_columns)

        return sql, list_val

    def _construct_insert_sql(self, table, str_col, values_sql, return_columns):
        """
        Construct final INSERT SQL string.
        Can be overridden by subclasses (e.g. Db2) for databases with wrapping syntax.
        """
        if return_columns and self.supports_returning():
            returning_clause = self.get_returning_clause(return_columns)
            return f"INSERT INTO {table} ({str_col}) VALUES {values_sql}{returning_clause}"
        else:
            return f"INSERT INTO {table} ({str_col}) VALUES {values_sql}"

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
                # The driver will handle conversion (e.g., psycopg -> PostgreSQL arrays)
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
        Execute query with no return result set such as update, delete, and DDLs like create table, etc.
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
            if processed_values:
                cur.execute(sql, processed_values)
            else:
                cur.execute(sql)
            
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

    def bulk_update(self, table, data_list, key_columns, bulk_size=1000):
        """
        Update multiple rows in bulk using executemany for performance.
        
        Args:
            table: Target table name
            data_list: List of dictionaries containing data to update + key values.
                      All dictionaries must have the same keys.
            key_columns: List of column names to use in the WHERE clause.
            bulk_size: Number of records to process in each batch (default 1000)
            
        Returns:
            Dictionary with status, message, and total affected rows count.
        """
        if not data_list:
            return {"status": 0, "message": "No data to update", "count": 0}

        row_count = len(data_list)
        first_row = data_list[0]
        
        # Validate keys exist in data
        missing_keys = [k for k in key_columns if k not in first_row]
        if missing_keys:
            return {"status": -1, "message": f"Key columns {missing_keys} missing from data"}
            
        # Determine update columns (all keys in data that are not key_columns)
        update_columns = [k for k in first_row.keys() if k not in key_columns]
        
        if not update_columns:
            return {"status": 0, "message": "No columns to update found in data", "count": 0}

        # Construct SQL
        sql = self._construct_bulk_update_sql(table, update_columns, key_columns)
        
        total_affected = 0
        cur = None
        
        total_affected = 0
        cur = None
        
        try:
            cur = self.get_cursor()
            
            # Prepare data sequence: list of tuples (update_vals..., key_vals...)
            # We process this in batches to avoid huge memory usage for large lists
            # We implement manual batching to preserve dict structure for value extraction
            total_records = len(data_list)
            
            for i in range(0, total_records, bulk_size):
                batch = data_list[i : i + bulk_size]
                batch_params = []
                
                for row in batch:
                    # Extract values in correct order: update columns, then key columns
                    params = []
                    # Add update values
                    try:
                        for col in update_columns:
                             params.append(self._process_value(row.get(col)))
                        # Add key values
                        for col in key_columns:
                             params.append(self._process_value(row.get(col)))
                    except Exception as e:
                        logger.error(f"Error processing row for bulk update: {row}. Error: {e}")
                        continue
                        
                    batch_params.append(tuple(params))
                
                if batch_params:
                    cur.executemany(sql, batch_params)
                
                    # Accumulate affected rows if supported
                    row_count_val = getattr(cur, 'rowcount', -1)
                    if row_count_val > 0:
                        total_affected += row_count_val
            
            # If driver doesn't support rowcount for executemany, we can't be sure
            if total_affected <= 0 and row_count > 0:
                 # Many drivers (like psycopg) might return -1 or generic count for executemany
                 # We'll just report 0 or unknown if not explicitly positive
                 pass
                 
            message = f"BULK UPDATE succeeded. {total_affected} rows affected."
            logger.info(message)
            return {"status": 0, "message": message, "count": total_affected}

        except Exception as e:
            message = f'Failed to execute bulk update: {e}'
            logger.error(message)
            logger.error(traceback.format_exc())
            return {"status": -1, "message": message}
        finally:
            if cur:
                cur.close()

    def _construct_bulk_update_sql(self, table, update_columns, key_columns):
        """
        Construct SQL for bulk_update.
        Can be overridden by subclasses (e.g. OracleDb).
        """
        set_clause = ', '.join([f"{col} = {self.placeholder}" for col in update_columns])
        where_clause = ' AND '.join([f"{col} = {self.placeholder}" for col in key_columns])
        return f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

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

    def merge(self, table, data, key_columns, no_update=None):
        """
        Merge (upsert) data in JSON format into table
        This function performs an INSERT if the record doesn't exist based on key_columns,
        or UPDATE if it does exist.
        
        Args:
            table: target table name
            data: JSON data with column-value pairs {"col1": "val1", "col2": "val2"}
                  Can be a single record (dict) or list of records (list of dicts)
            key_columns: list of column names that define uniqueness for matching records
            no_update: if True, do nothing on conflict (effectively "INSERT OR IGNORE")
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
            return self._bulk_merge(table, data, key_columns, no_update=no_update)
        else:
            return self._single_merge(table, data, key_columns, no_update=no_update)

    def _single_merge(self, table, data, key_columns, no_update=None):
        """
        Merge a single record into the table
        """
        # construct merge/upsert SQL
        merge_query, arr_values = self._merge_constructor(table, data, key_columns, no_update=no_update)
        return self.execute(merge_query, arr_values)

    def _bulk_merge(self, table, data_list, key_columns, no_update=None):
        """
        Merge multiple records into the table using batch execution.
        
        Uses executemany() for efficient batch processing, allowing the database
        driver to optimize the execution of repeated statements with different values.
        
        Args:
            table: target table name
            data_list: list of records (dicts) to merge
            key_columns: list of column names that define uniqueness
            no_update: if True, do nothing on conflict
            
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
                sql, values = self._merge_constructor(table, record, key_columns, no_update=no_update)
                
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

    def _merge_constructor(self, table, data, key_columns, no_update=None):
        """
        Construct SQL MERGE/UPSERT statement from JSON data.
        
        Uses common logic for building INSERT part, delegates to _build_upsert_clause
        for database-specific conflict handling.
        
        Args:
            table: target table name
            data: JSON data with column-value pairs
            key_columns: list of column names for matching
            no_update: if True, do nothing on conflict
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
        if no_update:
            update_columns = []
        else:
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

    def execute_script(self, sql_script, transaction=False):
        """
        Execute a script containing multiple SQL statements separated by semicolons.
        
        Args:
            sql_script: String containing SQL statements separated by ;
            transaction: If True, wraps execution in a transaction (autocommit=False)
            
        Returns:
            Dictionary with status and message.
        """
        if not sql_script:
            return {"status": 0, "message": "SQL script is empty"}

        # Split by semicolon and filter empty strings
        sqls = [s.strip() for s in sql_script.split(';') if s.strip()]
        
        if not sqls:
            return {"status": 0, "message": "SQL script contains no executable statements"}

        autocommit_was_enabled = True
        success_count = 0
        
        try:
            if transaction:
                autocommit_was_enabled = self.get_autocommit()
                self.set_autocommit(False)
            
            for sql in sqls:
                # Reuse existing unescape logic if present in _prepare_sql, 
                # but legacy checked unescape_current_keyword explicitly.
                # Our self.execute() handles inject_current calls via _prepare_sql.
                
                # We can call self.execute() directly.
                # Note: execute() returns a dict result.
                
                result = self.execute(sql)
                if result['status'] != 0:
                    # If any statement fails, we stop and rollback if in transaction
                    if transaction:
                        self.rollback()
                    return {
                        "status": -1, 
                        "message": f"Script execution failed at statement {success_count + 1}: {result['message']}"
                    }
                success_count += 1
            
            if transaction:
                self.commit()
                
            return {
                "status": 0, 
                "message": f"Script execution succeeded. {success_count} statements executed."
            }

        except Exception as e:
            logger.error(f"Script execution exception: {e}")
            logger.error(traceback.format_exc())
            if transaction:
                self.rollback()
            return {"status": -1, "message": f"Script execution error: {e}"}
            
        finally:
            if transaction:
                self.set_autocommit(autocommit_was_enabled)

    def run_script_from_file(self, sql_file, transaction=False):
        """
        Execute multiple SQL statements from a file.
        
        Args:
            sql_file: Path to the SQL file
            transaction: If True, wraps execution in a transaction
            
        Returns:
            Dictionary with status and message
        """
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            return self.execute_script(sql_script, transaction=transaction)
        except Exception as e:
            logger.error(f"Failed to read script file {sql_file}: {e}")
            return {"status": -1, "message": f"Failed to read script file: {e}"}

    def stream_to_csv(self, sql, csv_file, values=None, options=None):
        """
        Stream query results to a CSV file.
        
        This method executes a SQL query and writes the results to a CSV file using streaming.
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
                - quotechar: 'Y' to enforce double quotes on all strings
                - abort_on_error: 'Y' (default) to abort on first error, 'N' to continue
                
        Returns:
            Dictionary with:
                - status: 0 for success, -1 for failure
                - message: Descriptive message about the operation result
                - row_count: Number of rows exported
                
        Example:
            # Export query results with header
            result = db.stream_to_csv(
                "SELECT * FROM users WHERE status = %s",
                "output/users.csv",
                values=["active"],
                options={"header": "Y"}
            )
            
            # Export without header
            result = db.stream_to_csv(
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
                        if options.get('abort_on_error', 'Y') != 'N':
                            message = f"Query error at row {row_num}"
                            logger.error(message)
                            return {"status": -1, "message": message, "row_count": row_count}
                        else:
                            logger.warning(f"Row {row_num} status is {status}, continue ...")
                            continue
                    
                    # Write header on first row if requested
                    if not header_written:
                        if options.get('header', 'Y') == 'Y':
                            header_row = data_utils.escape_csv_row(list(row.keys()))
                            f.write(','.join(header_row) + '\n')
                        header_written = True
                    
                    # Write data row
                    null_value = options.get('null_value', '')
                    quotechar = options.get('quotechar')
                    escaped_row = data_utils.escape_csv_row(list(row.values()), null_value, quotechar)
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
