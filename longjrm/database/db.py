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
import functools
import logging
import traceback
import warnings
from abc import ABC, abstractmethod
from longjrm.config.runtime import get_config
from longjrm.database.placeholder_handler import PlaceholderHandler
from longjrm.connection.connectors import get_connector_class, unwrap_connection
from longjrm.utils import sql as sql_utils, data as data_utils


logger = logging.getLogger(__name__)


class _ResultDict(dict):
    """A result dict that nudges callers off the deprecated count keys.

    Reading a deprecated old key (``count`` / ``record_count`` / ``reject_count``
    for the methods that now alias it) emits a ``DeprecationWarning`` pointing to its
    ``rows_*`` replacement. The old key STILL WORKS -- this is only the migration
    hint before the old vocabulary is unified away in a future release. Reads of the
    new ``rows_*`` keys (and any other key) are silent.
    """
    __slots__ = ("_aliases",)

    def __init__(self, data, aliases):
        super().__init__(data)
        self._aliases = aliases                  # deprecated old_key -> rows_* key

    def _nudge(self, key):
        new = self._aliases.get(key)
        if new is not None:
            warnings.warn(
                f"longjrm result key '{key}' is deprecated; use '{new}'. The old key "
                f"still works but will be removed in a future release.",
                DeprecationWarning, stacklevel=3)

    def __getitem__(self, key):
        self._nudge(key)
        return super().__getitem__(key)

    def get(self, key, default=None):
        self._nudge(key)
        return super().get(key, default)


def rows_alias(**mapping):
    """Decorator: expose data-engineering ``rows_*`` keys on a method's result dict.

    longjrm has historically returned ``count`` (rows affected/returned),
    ``record_count``/``reject_count`` (streams), and ``row_count`` (file ops).
    This additively copies each such key to its ``rows_*`` alias
    (``mapping`` is ``old_key -> new_key``) AFTER the wrapped method returns, so
    callers can read the standard names (``rows_read``, ``rows_inserted``,
    ``rows_updated``, ``rows_deleted``, ``rows_merged``, ``rows_rejected``, ...).

    The old keys remain fully functional, but reading one now emits a
    ``DeprecationWarning`` toward its ``rows_*`` replacement (see ``_ResultDict``) --
    the migration nudge so the old vocabulary can be unified away in a release or
    two. Cases without a single clear verb (``execute``'s affected count; the file
    ops' ``row_count``) are left unaliased -- and therefore NOT yet deprecated --
    until their ``rows_*`` names land.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)
            if isinstance(result, dict):
                for old, new in mapping.items():
                    # dict.__getitem__ bypasses the deprecation nudge: a method whose
                    # result is itself an already-wrapped _ResultDict (e.g. insert
                    # returning query's result) must copy the old key silently here.
                    if old in result and new not in result:
                        result[new] = dict.__getitem__(result, old)
                result = _ResultDict(result, dict(mapping))
            return result
        return wrapper
    return decorator

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
    # merge_select() SQL family
    # -------------------------------------------------------------------------
    # Backends that support INSERT ... ON CONFLICT / ON DUPLICATE KEY use the
    # base 'on_conflict' path. Backends that cannot (Db2, Oracle, SQL Server,
    # Spark) set _merge_select_style='merge_into' to use the shared
    # MERGE INTO ... USING (SELECT ...) builder below; the remaining knobs
    # absorb per-dialect differences.
    _merge_select_style = 'on_conflict'
    _merge_select_use_as = True           # 'AS target'/'AS source' vs bare (Oracle)
    _merge_select_set_target_prefix = ''  # UPDATE SET left side (Spark: 'target.')
    _merge_select_else_clause = ''        # Db2 appends 'ELSE IGNORE'
    _merge_select_terminator = ''         # SQL Server requires a trailing ';'
    _merge_select_supports_bind = True    # Spark can't bind here -> forces inline
    _upsert_select_needs_where = False    # SQLite needs a WHERE to disambiguate
                                          # INSERT...SELECT...ON CONFLICT

    def _build_merge_into_select_sql(self, target_table, insert_columns, key_columns,
                                     update_columns, source_sql):
        """Build a ``MERGE INTO ... USING (SELECT ...)`` statement.

        Shared by the 'merge_into' backends (Db2/Oracle/SQL Server/Spark);
        per-dialect differences are captured by the ``_merge_select_*`` class
        attributes. ``source_sql`` is the fully built source SELECT (already
        carrying any WHERE / ORDER BY / isolation clause).
        """
        insert_column_str = ', '.join(insert_columns)
        as_kw = 'AS ' if self._merge_select_use_as else ''
        match_str = ' AND '.join(f"target.{k} = source.{k}" for k in key_columns)
        insert_values_str = ', '.join(f"source.{c}" for c in insert_columns)

        sql = (f"MERGE INTO {target_table} {as_kw}target "
               f"USING ({source_sql}) {as_kw}source "
               f"ON ({match_str})")
        if update_columns:
            prefix = self._merge_select_set_target_prefix
            update_str = ', '.join(f"{prefix}{c} = source.{c}" for c in update_columns)
            sql += f" WHEN MATCHED THEN UPDATE SET {update_str}"
        sql += (f" WHEN NOT MATCHED THEN INSERT ({insert_column_str}) "
                f"VALUES ({insert_values_str})")
        if self._merge_select_else_clause:
            sql += f" {self._merge_select_else_clause}"
        if self._merge_select_terminator:
            sql += self._merge_select_terminator
        return sql

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
        Bulk load data into table.

        Engine subclasses override this with their fastest native channel (DB2 via the
        ADMIN_CMD stored procedure, Postgres via COPY, MySQL via LOAD DATA). This base
        implementation is the FALLBACK that makes every engine bulk-loadable with no
        external dependency -- the same principle as DB2 going through a stored
        procedure instead of the load utility: everything happens through the driver,
        like SQL. A query source becomes one in-engine INSERT INTO ... SELECT; a file
        source is parsed client-side and written in array-bound executemany batches,
        the standard high-performance path for engines (Oracle, SQL Server) whose
        native bulk channels want server-side files or external utilities.

        Args:
            table: Target table name, e.g. "my_table". Also supports
                "my_table(col1, col2)" naming the columns to load into.
            load_info: Config dictionary:
                - source: 'file path' or 'SELECT query' (required)
                - source_type: 'file' | 'cursor' (auto-detected when omitted)
                - columns: List of target columns (alternative to the table(...) form)
                - delimiter: Field delimiter (default ',')
                - quote: Quote character (default '"')
                - encoding: File encoding (default 'utf-8')
                - header: True when the file's first line is column names; it is
                  skipped, and used as the column list if none was given
                - null_value: The string that means NULL (default '': what
                  stream_to_csv writes for None)
                - bulk_size: Rows per executemany batch (default 10000)
            command: Optional raw engine-specific command (executed verbatim,
                bypassing load_info).
        """
        import csv

        if command:
            return self.execute(command)
        if not isinstance(load_info, dict):
            raise TypeError(
                f"bulk_load requires a configuration dictionary for 'load_info', "
                f"got {type(load_info).__name__}")

        columns = load_info.get('columns')
        if table and '(' in str(table):
            t_parts = table.split('(', 1)
            table = t_parts[0].strip()
            if not columns:
                columns = [c.strip() for c in t_parts[1].rstrip(')').split(',')]

        source = load_info.get('source')
        source_type = load_info.get('source_type')
        if source_type is None:
            if isinstance(source, str):
                upper_src = source.strip().upper()
                source_type = 'cursor' if upper_src.startswith(('SELECT', '(SELECT'))                     else 'file'
            else:
                source_type = 'file'          # a file-like object

        if source_type == 'cursor':
            # One statement inside the engine; requires the source to be readable on
            # THIS connection (same database, or a federated table making it look so).
            col_clause = f" ({', '.join(columns)})" if columns else ""
            return self.execute(f"INSERT INTO {table}{col_clause} {source}")

        if source_type != 'file':
            raise ValueError(f"Unknown source_type: {source_type}")

        delimiter = load_info.get('delimiter', ',')
        quote = load_info.get('quote', '"')
        null_value = load_info.get('null_value', '')
        bulk_size = int(load_info.get('bulk_size', 10000))
        has_header = bool(load_info.get('header', False))

        def _rows(reader):
            for raw in reader:
                if not raw:
                    continue                   # a blank line is not a row of NULLs
                yield [None if v == null_value else v for v in raw]

        file_handle = None
        try:
            if isinstance(source, str):
                file_handle = open(source, 'r', newline='',
                                   encoding=load_info.get('encoding', 'utf-8'))
                stream = file_handle
            else:
                stream = source
            reader = csv.reader(stream, delimiter=delimiter, quotechar=quote)

            if has_header:
                header_row = next(reader, None)
                if columns is None and header_row:
                    columns = [c.strip() for c in header_row]
            if not columns:
                raise ValueError(
                    "bulk_load from a file needs the target columns: pass 'columns', "
                    "use the table(col, ...) form, or set header=True on a file whose "
                    "first line names them")

            total = 0
            batch = []
            for values in _rows(reader):
                batch.append(dict(zip(columns, values)))
                if len(batch) >= bulk_size:
                    result = self.insert(table, batch)
                    if result.get('status', 0) != 0:
                        return result
                    total += len(batch)
                    batch = []
            if batch:
                result = self.insert(table, batch)
                if result.get('status', 0) != 0:
                    return result
                total += len(batch)

            message = f"Bulk load to {table} completed. {total} rows loaded."
            logger.info(message)
            return {"status": 0, "message": message, "data": [], "count": total}
        finally:
            if file_handle:
                file_handle.close()

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
            # Escape literal % only when values will actually be bound through a
            # pyformat driver (%s). qmark drivers (sqlite3, pyodbc, ibm_db_dbi)
            # and statements executed without parameters take % literally, so
            # doubling it there corrupts the SQL (e.g. '50%' stored as '50%%').
            if processed_values and self.placeholder == '%s':
                sql = self._escape_sql(sql)
        else:
            processed_values = arr_values

        return sql, processed_values

    def _escape_sql(self, sql):
        """
        Escape literal % as %% so it survives pyformat parameter substitution
        (psycopg/pymysql). Only applied when parameters are bound and the
        placeholder is %s — see _prepare_sql.

        Args:
            sql: SQL string to escape

        Returns:
            Escaped SQL string
        """
        # Replace standalone % with %% (escape them) but leave %s placeholders alone
        return re.sub(r'%(?!s)', '%%', sql)

    @rows_alias(count="rows_read")
    def query(self, sql, arr_values=None):
        """
        Execute query with small result set, return entire result set.

        Base SQL primitive: returns the result dict on success and raises on
        failure (does not return a ``status: -1`` dict). See the error
        contract in docs/database.md.

        Args:
            sql: SQL statement to execute
            arr_values: values to bind to query (supports positional and named placeholders)
        Returns:
            On success, dict with status (0), message, data, columns, count.
        Raises:
            Exception: the underlying driver error if the query fails.
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

    def stream_query(self, sql, arr_values=None, *, max_error_count=0, reject_sink=None):
        """
        Execute query and stream results row by row using generator.
        This is memory-efficient for large result sets as it doesn't load all rows at once.

        Args:
            sql: SQL statement to execute
            arr_values: values to bind to query (supports positional and named placeholders)
            max_error_count: Maximum number of errors to tolerate before failing (default 0)
            reject_sink: optional callable(row_number, row, reason) invoked when a row
                cannot be fetched. The row's data is unavailable at fetch failure, so the
                payload is {}; the sink still records the reason. Must not raise.

        Yields:
            tuple of (row_number, row_dict, status):
                - row_number: 1-indexed sequence number of the row (0 if error before any rows)
                - row_dict: dictionary with column-value pairs ({} if error)
                - status: 0 for success, -1 for error

        Raises:
            Exception: the underlying driver error once more than
                ``max_error_count`` row-level errors have occurred (hard
                failure). Errors during query setup/execution are reported as a
                single ``(0, {}, -1)`` yield instead, per the streaming
                partial-failure contract.
        """
        row_number = 0
        current_error_count = 0
        cur = None
        logger.debug(f"Query to stream: {sql}")

        try:
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
            except Exception as e:
                logger.error(f'stream_query failed to execute: {e}', exc_info=True)
                if reject_sink is not None:
                    reject_sink(row_number, {}, f"stream_query setup failed: {e}")
                yield row_number, {}, -1
                return

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
                        # Hard failure: tolerance exhausted, propagate to caller.
                        logger.error(f"Max error count ({max_error_count}) exceeded. Aborting.")
                        if reject_sink is not None:
                            reject_sink(row_number + 1, {}, f"fetch failed: {e}")
                        raise

                    # Yield error status for downstream handling if tolerating error
                    row_number += 1
                    if reject_sink is not None:
                        reject_sink(row_number, {}, f"fetch failed: {e}")
                    yield row_number, {}, -1

            logger.info(f"{row_number} rows sent to downstream successfully via stream")

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

        # Tolerated row errors arrive as status -1 tuples and are forwarded;
        # a hard failure (max_error_count exceeded) raises out of stream_query
        # and propagates to the caller.
        for row_num, row, status in self.stream_query(sql, arr_values, max_error_count=max_error_count):
            total_rows = row_num

            if status == 0:
                buck_data.append(row)

                if len(buck_data) >= batch_size:
                    logger.info(f"Batch of {len(buck_data)} rows sent to downstream (Total: {total_rows})")
                    yield total_rows, buck_data, 0
                    buck_data = []
            else:
                yield row_num, {}, -1

        # Yield remaining rows
        if buck_data:
            logger.info(f"Final batch of {len(buck_data)} rows sent to downstream (Total: {total_rows})")
            yield total_rows, buck_data, 0

    def _stream_transaction_handler(self, stream, operation_func, commit_count=10000, max_error_count=0, table_name="unknown", *, reject_sink=None):
        """
        Generic handler for stream-based transactional operations (insert/update/merge).

        Args:
            stream: Iterator yielding rows
            operation_func: Callable(row, row_number) -> result_dict
            commit_count: Rows between commits (0 to disable manual commit control)
            max_error_count: Max errors allowed
            table_name: Name of table for logging
            reject_sink: optional callable(row_number, row, reason) invoked for every
                rejected row (both tolerated and the final aborting one) so callers can
                persist rejects. It MUST NOT raise and MUST NOT write through this same
                connection (a rollback on max_error_count would erase its writes).

        Returns:
            Result dictionary (carries reject_count alongside record_count)
        """
        row_number = 0
        result = {}
        autocommit_was_enabled = True
        current_error_count = 0
        reject_count = 0
        
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
                    reject_count += 1
                    message = f"Upstream error at row {row_number} to table {table_name} (Error {current_error_count}/{max_error_count})"
                    logger.warning(message)
                    if reject_sink is not None:
                        reject_sink(row_number, row, message)

                    if current_error_count > max_error_count:
                        if commit_count != 0: self.rollback()
                        return {"status": -1, "record_count": row_number, "reject_count": reject_count, "message": message}
                    continue
                
                # Execute operation. By default a per-row driver error raises and
                # is handled as a fatal stream error below (original behavior). When
                # reject handling is requested (a sink, or max_error_count > 0), the
                # row is instead isolated by a SAVEPOINT so a bad row can be rolled
                # back and recorded as a per-row reject without poisoning the batch
                # transaction (Postgres aborts the whole tx on error; the savepoint
                # confines that). The savepoint path is opt-in, so the default load
                # keeps its exact behavior and adds no per-row round-trips.
                tolerant = reject_sink is not None or max_error_count > 0
                savepoint = None
                if tolerant and commit_count != 0:
                    savepoint = f"jrm_sp_{row_number}"
                    self.execute(f"SAVEPOINT {savepoint}")
                try:
                    result = operation_func(row, row_number)
                except Exception as op_error:
                    if not tolerant:
                        raise
                    result = {"status": -1, "message": str(op_error)}
                    if savepoint is not None:
                        try:
                            self.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                        except Exception:
                            pass
                else:
                    if savepoint is not None:
                        try:
                            self.execute(f"RELEASE SAVEPOINT {savepoint}")
                        except Exception:
                            pass

                if result.get('status') != 0:
                    current_error_count += 1
                    reject_count += 1
                    message = f"Failed to process row {row_number} in {table_name} (Error {current_error_count}/{max_error_count}): {result.get('message', 'Unknown error')}"
                    logger.warning(message)
                    if reject_sink is not None:
                        reject_sink(row_number, row, message)

                    if current_error_count > max_error_count:
                        if commit_count != 0: self.rollback()
                        return {"status": -1, "record_count": row_number, "reject_count": reject_count, "message": message}
                    continue
                
                # Periodic commit
                if commit_count != 0 and row_number > 0 and row_number % commit_count == 0:
                    self.commit()
                    logger.info(f"Committed {row_number} rows into {table_name}")
            
            # Final processing
            if row_number == 0:
                message = f"Incoming stream for {table_name} is empty"
                logger.info(message)
                return {"status": 0, "record_count": 0, "reject_count": reject_count, "message": message}

            if commit_count != 0:
                self.commit()

            message = f"{row_number} rows processed into {table_name} successfully"
            logger.info(message)
            return {"status": 0, "record_count": row_number, "reject_count": reject_count, "message": message}

        except Exception as e:
            error_message = f"Fatal database error at row {row_number}: {e}"
            logger.error(error_message, exc_info=True)
            if commit_count != 0:
                self.rollback()
            return {"status": -1, "record_count": row_number, "reject_count": reject_count, "message": error_message}
            
        finally:
            if commit_count != 0:
                self.set_autocommit(autocommit_was_enabled)

    def stream_select(self, table, columns=None, where=None, options=None, *, max_error_count=0):
        """
        Stream a SELECT row by row without buffering the whole result set -- the
        streaming counterpart of select(). Builds the same SQL as select() via
        _select_constructor, so the data_fetch_limit default still applies: pass
        options={"limit": 0} to stream an entire (large) table on purpose.

        Yields the same tuples as stream_query: (row_number, row_dict, status).
        """
        sql, arr_values = self._select_constructor(table, columns, where, options)
        return self.stream_query(sql, arr_values, max_error_count=max_error_count)

    @rows_alias(record_count="rows_read", reject_count="rows_rejected")
    def stream_insert(self, stream, table, *, commit_count=10000, max_error_count=0, reject_sink=None):
        """
        Insert stream data into table with optional periodic commits.
        """
        def op(row, _):
            return self.insert(table, row)

        return self._stream_transaction_handler(
            stream, op, commit_count, max_error_count, table_name=table, reject_sink=reject_sink
        )

    def stream_update(self, stream, table, *, commit_count=10000, max_error_count=0, reject_sink=None):
        """
        Update table from stream data with optional periodic commits.
        """
        def op(row, row_num):
            if not isinstance(row, dict) or 'data' not in row or 'condition' not in row:
                return {"status": -1, "message": "Invalid row format: expected dict with 'data' and 'condition'"}
            return self.update(table, row['data'], row['condition'])

        return self._stream_transaction_handler(
            stream, op, commit_count, max_error_count, table_name=table, reject_sink=reject_sink
        )

    @rows_alias(record_count="rows_read", reject_count="rows_rejected")
    def stream_merge(self, stream, table, key_columns, *, commit_count=10000, max_error_count=0, reject_sink=None):
        """
        Merge (upsert) stream data into table with optional periodic commits.
        """
        def op(row, _):
            return self.merge(table, row, key_columns)

        return self._stream_transaction_handler(
            stream, op, commit_count, max_error_count, table_name=table, reject_sink=reject_sink
        )

    @rows_alias(count="rows_inserted")
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
            On success, dict with status (0), message, data (empty), and count (affected rows).
        Raises:
            Exception: the underlying driver error if the insert fails.
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

        # The INSERT columns + placeholders are taken from the first row; the
        # remaining rows are bound positionally by executemany. We don't pre-scan
        # every row for column consistency -- a row with a different column count
        # makes the driver raise on bind anyway (propagated per the error
        # contract), so the extra O(rows) pass isn't worth it. Callers are
        # expected to pass uniformly-shaped rows: a row with the same count but
        # different/reordered keys would bind to the wrong columns without error.
        columns = list(data_list[0].keys())
        str_col = ', '.join(columns)
        placeholders = [self.placeholder for _ in columns]
        str_qm = ', '.join(placeholders)
        
        # Construct a standard single-row INSERT statement
        sql = f"INSERT INTO {table} ({str_col}) VALUES ({str_qm})"

        total_affected = 0
        cur = None
        try:
            cur = self.get_cursor()
            
            # process_value_fn keeps bulk inserts serializing values exactly
            # like single-row inserts on the same backend (lists, datetimes, ...).
            for batch in data_utils.datalist_to_dataseq(
                    data_list, bulk_size=bulk_size, process_value_fn=self._process_value):
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

        finally:
            # try/finally only — close the cursor; let any error propagate
            # (raise-on-failure contract), as execute()/query() do.
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

        # Raw expressions are rendered into the SQL here (no placeholder, no
        # bind); everything else gets a placeholder. Legacy backtick CURRENT
        # keywords still ride through as values for inject_current.
        placeholders = []
        list_val = []
        for k in columns:
            v = data[k]
            if isinstance(v, sql_utils.Raw):
                placeholders.append(v.text)
            else:
                placeholders.append(self.placeholder)
                list_val.append(self._process_value(v))
        str_qm = ', '.join(placeholders)
        values_sql = f"({str_qm})"

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
        if isinstance(value, sql_utils.Raw):
            # Raw is handled at SQL-construction time (insert/update/merge/
            # where). Reaching here means a positionally-bound bulk path
            # (bulk_update, list-form insert) that cannot inline expressions.
            raise TypeError(
                "Raw SQL expressions are not supported in this operation: "
                "bulk paths bind every value by position. Use per-record "
                "insert/update/merge, or a column DEFAULT, instead."
            )
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
        elif isinstance(value, datetime.datetime):
            # Must be checked before datetime.date: datetime is a date subclass.
            # Aware values keep their UTC offset -- dropping it makes the server
            # read the digits in its own session time zone and store a different
            # instant, with no error anywhere. See data_utils.serialize_datetime.
            return data_utils.serialize_datetime(value)
        elif isinstance(value, datetime.date):
            return str(value)
        elif isinstance(value, str):
            return value
        else:
            return value

    def execute(self, sql, arr_values=None):
        """
        Execute query with no return result set such as update, delete, and DDLs like create table, etc.

        Base SQL primitive: returns the result dict on success and raises on
        failure (does not return a ``status: -1`` dict). See the error
        contract in docs/database.md.

        Args:
            sql: SQL statement to execute
            arr_values: values that need to be bound to query (supports both positional and named placeholders)
        Returns:
            On success, dict with status (0), message, data (empty), and count (affected rows).
        Raises:
            Exception: the underlying driver error if the statement fails.
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

    @rows_alias(count="rows_updated")
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

    @rows_alias(count="rows_updated")
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
            On success, dict with status (0), message, and total affected rows count.
        Raises:
            Exception: the underlying driver error if the bulk update fails.
        """
        if not data_list:
            return {"status": 0, "message": "No data to update", "count": 0}

        row_count = len(data_list)
        first_row = data_list[0]
        
        # Validate keys exist in data (misuse -> raise, consistent with merge())
        missing_keys = [k for k in key_columns if k not in first_row]
        if missing_keys:
            raise ValueError(f"Key columns {missing_keys} missing from data")
            
        # Determine update columns (all keys in data that are not key_columns)
        update_columns = [k for k in first_row.keys() if k not in key_columns]
        
        if not update_columns:
            return {"status": 0, "message": "No columns to update found in data", "count": 0}

        # Construct SQL
        sql = self._construct_bulk_update_sql(table, update_columns, key_columns)
        
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
                    params = [self._process_value(row.get(col)) for col in update_columns]
                    params += [self._process_value(row.get(col)) for col in key_columns]
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

        finally:
            # try/finally only — close the cursor; let any error propagate.
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
            if isinstance(v, sql_utils.Raw):
                # Trusted SQL expression: rendered verbatim, never bound.
                update_str += ", " + k + " = " + v.text
                continue

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

    @rows_alias(count="rows_deleted")
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

    @staticmethod
    def _no_update_flag(no_update):
        """
        Normalize the historical ``no_update`` spellings to a boolean.
        Accepts True/False/None and the legacy 'Y'/'N' strings so every backend
        interprets the flag identically.
        """
        if isinstance(no_update, str):
            return no_update.upper() == 'Y'
        return bool(no_update)

    @rows_alias(count="rows_merged")
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
            no_update: if True (or legacy 'Y'), do nothing on conflict
                  (effectively "INSERT OR IGNORE")
        Returns:
            dictionary with status, message, data (empty), and count (affected rows)
        """
        no_update = self._no_update_flag(no_update)

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

            # Prepare SQL per row (placeholder conversion, CURRENT-keyword
            # injection, escaping). Injection rewrites the SQL itself, so every
            # row must produce the same prepared statement — otherwise rows would
            # bind against the wrong placeholders (or bind a `CURRENT ...` token
            # as literal data).
            prepared_sql = None
            prepared_rows = []
            for i, values in enumerate(all_values):
                row_sql, row_values = self._prepare_sql(sql_template, values)
                if prepared_sql is None:
                    prepared_sql = row_sql
                elif row_sql != prepared_sql:
                    raise ValueError(
                        f"Record {i} uses CURRENT keywords differently than previous records. "
                        f"Bulk merge requires consistent keyword usage across rows."
                    )
                prepared_rows.append(row_values)

            # Execute batch using executemany
            cur = self.get_cursor()
            cur.executemany(prepared_sql, prepared_rows)
            total_affected = cur.rowcount if cur.rowcount >= 0 else len(data_list)
            
            success_msg = f"Bulk merge completed successfully. {total_affected} rows affected."
            logger.info(success_msg)
            
            return {
                "status": 0,
                "message": success_msg,
                "data": [],
                "count": total_affected
            }

        finally:
            # try/finally only — close the cursor; let any error propagate.
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
            v = data[col]
            if isinstance(v, sql_utils.Raw):
                # Trusted SQL expression: rendered verbatim, never bound.
                placeholders.append(v.text)
            else:
                placeholders.append(self.placeholder)
                values.append(self._process_value(v))

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

    @rows_alias(count="rows_merged")
    def merge_select(self, source_table, target_table, insert_columns, key_columns,
                     order_by=None, conditions=None, source_select=None, update_columns=None,
                     isolation_clause='', dynamic_param='Y'):
        """
        Merge data from source table via SELECT into target table.
        
        Uses common logic for building INSERT...SELECT, delegates to _build_upsert_clause
        for database-specific conflict handling.
        
        Args:
            source_table: Name of the source table to select from (ignored if source_select is provided)
            target_table: Name of the target table to merge into
            insert_columns: List of column names to insert/update
            key_columns: List of column names that define uniqueness for matching records
            order_by: Optional ORDER BY clause for the source query (e.g., "id DESC").
                Ignored for the MERGE INTO backends (Db2/Oracle/SQL Server/Spark),
                where it can't affect merge semantics and is illegal in the USING
                subquery on SQL Server.
            conditions: Optional filter for the source data. Accepts a raw clause
                string (used verbatim), a dict (supports operators / IN / $and /
                $or, e.g. {"col": {">": x, "<=": y}}), or a list of condition dicts
                AND-ed together (e.g. [{"col": {">": x}}, {"col": {"<=": y}}]). See
                longjrm.utils.sql.build_where.
            source_select: Optional custom SELECT statement to use instead of auto-generated one
            update_columns: Optional list of columns to update (defaults to insert_columns minus key_columns)
            isolation_clause: Optional trailing isolation clause appended to the
                source SELECT (e.g. Db2 "WITH UR"). Empty for most backends.
            dynamic_param: 'Y' (default) binds condition values as parameters to
                avoid SQL injection; 'N' inlines them (quoted/escaped) instead.
                Backends that cannot bind in this context (Spark) always inline.

        Returns:
            On success, a dict with:
                - status: 0
                - message: Descriptive message about the operation result
                - count / total / merge_count: Number of rows affected (if available)
        Raises:
            Exception: the underlying driver error if the merge fails.

        Works across all backends: PostgreSQL/MySQL/SQLite use INSERT ... ON
        CONFLICT / ON DUPLICATE KEY; Db2/Oracle/SQL Server/Spark use
        MERGE INTO ... USING (SELECT ...).
        """
        insert_column_str = ', '.join(insert_columns)

        # Determine update columns (default: insert_columns minus key_columns)
        if not update_columns:
            update_columns = [col for col in insert_columns if col not in key_columns]

        # Conditions are parameterized by default (dynamic_param='Y') so
        # untrusted filter values are bound rather than inlined. Backends that
        # cannot bind here (Spark) force inline; pass dynamic_param='N' to
        # inline explicitly on any backend.
        inline = (dynamic_param == 'N') or (not self._merge_select_supports_bind)

        cond_values = []
        if source_select:
            source_sql = source_select
        else:
            where_clause, cond_values = sql_utils.build_where(
                conditions, self.placeholder, inline=inline)
            # SQLite can't parse INSERT ... SELECT ... ON CONFLICT when the
            # SELECT has no WHERE (it reads 'ON' as a join clause); a dummy
            # predicate disambiguates it.
            if self._upsert_select_needs_where and not where_clause:
                where_clause = " WHERE 1=1"
            # ORDER BY can't affect MERGE semantics and is illegal inside the
            # USING subquery on SQL Server, so it's only emitted for the
            # INSERT ... SELECT (on_conflict) family.
            if order_by and self._merge_select_style != 'merge_into':
                order_clause = f" ORDER BY {order_by}"
            else:
                order_clause = ""
            iso_clause = f" {isolation_clause}" if isolation_clause else ""
            source_sql = (f"SELECT {insert_column_str} FROM {source_table}"
                          f"{where_clause}{order_clause}{iso_clause}")

        # Two SQL families: MERGE INTO ... USING (SELECT ...) for backends that
        # can't do INSERT ... ON CONFLICT / ON DUPLICATE KEY, and the
        # INSERT ... <upsert clause> form for those that can.
        if self._merge_select_style == 'merge_into':
            sql = self._build_merge_into_select_sql(
                target_table, insert_columns, key_columns, update_columns, source_sql)
        else:
            upsert_clause = self._build_upsert_clause(key_columns, update_columns, for_values=False)
            sql = f"INSERT INTO {target_table} ({insert_column_str}) {source_sql} {upsert_clause}"

        logger.debug(f"Merge Select SQL: {sql}")

        # Route through execute() for consistent placeholder conversion,
        # CURRENT-keyword injection, escaping, binding, and result shape.
        # execute() raises on failure (error contract), so no local try/except.
        result = self.execute(sql, cond_values or None)
        if result.get('status') == 0:
            affected_rows = result.get('count')
            result['message'] = (f"MERGE into table {target_table} succeeded. "
                                 f"{affected_rows} rows affected.")
            # Back-compat aliases (base previously returned 'total').
            result['total'] = affected_rows
            result['merge_count'] = affected_rows
        return result

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
            On success, a dict with status (0) and message.
        Raises:
            Exception: the underlying driver error if a statement fails. When
                transaction=True the transaction is rolled back before raising.
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
                # execute() handles inject_current via _prepare_sql and raises on
                # failure; a failing statement propagates to the except below,
                # which rolls back (when transactional) before re-raising.
                self.execute(sql)
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
            raise

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
            On success, a dict with status (0) and message.
        Raises:
            Exception: a file-read error, or the underlying driver error if a
                statement fails.
        """
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        return self.execute_script(sql_script, transaction=transaction)

    def stream_to_csv(self, sql, csv_file, values=None, options=None):
        """
        Stream query results to a CSV file.
        
        This method executes a SQL query and writes the results to a CSV file using streaming.
        It properly handles None/Null values, escaping of special characters,
        and optionally includes a header row.
        
        Note: Python's csv writer cannot differentiate None/Null and blank,
              so this method handles it properly by converting None to empty string.

        Note: the header row is derived from the first data row, so a query
              returning zero rows produces an empty file even with header='Y'.

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
