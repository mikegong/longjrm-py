import logging
import json
from longjrm.database.db import Db
from longjrm.utils import sql as sql_utils
import traceback

logger = logging.getLogger(__name__)

class SqlServerDb(Db):
    """
    Microsoft SQL Server database implementation.
    """

    # merge_select(): SQL Server uses MERGE INTO ...; the statement must be
    # terminated with a semicolon.
    _merge_select_style = 'merge_into'
    _merge_select_terminator = ';'

    def __init__(self, client):
        super().__init__(client)
        # SQL Server (pyodbc) uses ? as placeholder
        self.placeholder = '?'

    def _process_value(self, value):
        """
        Process individual values for SQL Server.
        pyodbc doesn't automatically convert lists to JSON/String for VARCHAR columns,
        raising errors like "Cannot find data type...".
        """
        if isinstance(value, list):
            return json.dumps(value, ensure_ascii=False)
        return super()._process_value(value)

    def get_cursor(self):
        """Get a cursor for executing queries."""
        return self.conn.cursor()
    
    def get_stream_cursor(self):
        """Get a cursor optimized for streaming."""
        return self.conn.cursor()

    def supports_returning(self):
        """
        SQL Server supports returning generated keys via OUTPUT clause.
        We handle this in _construct_insert_sql.
        """
        return True

    def bulk_load(self, table, load_info=None, *, command=None):
        """Bulk load into SQL Server -- pure SQL / pure driver, no external utility.

        SQL Server's famous loader is bcp, an external program, and its in-engine
        statements (BULK INSERT, OPENROWSET(BULK ...)) read the file on the SERVER,
        which needs the file to live there and the caller to hold ADMINISTER BULK
        OPERATIONS. Neither is something a driver can assume. What it can always reach:

          cursor source -> INSERT INTO t (cols) SELECT ...      (one in-engine statement)
          file source   -> parameter-array binding via pyodbc's fast_executemany, which
                           sends a whole batch in one round trip instead of a statement
                           per row

        fast_executemany is the same trade Oracle's APPEND_VALUES makes: the fast write
        belongs to the driver, so it works wherever the connection does -- no server-side
        path, no extra permission.

        A deployment whose server CAN read the file gets the last increment through the
        ``command`` escape hatch, e.g.
        ``bulk_load(t, command="BULK INSERT t FROM '\\\\share\\f.csv' WITH (FORMAT='CSV')")``.

        load_info keys are the shared vocabulary (see Db.bulk_load): source, source_type,
        columns, delimiter, quote, encoding, header, null_value, bulk_size.
        """
        if command:
            logger.info(f"SQL Server bulk_load executing raw command: {command}")
            return self.execute(command)
        if not isinstance(load_info, dict):
            raise TypeError(
                f"SQL Server bulk_load requires a configuration dictionary for "
                f"'load_info', got {type(load_info).__name__}")

        columns = load_info.get('columns')
        target = table
        if table and '(' in str(table):
            t_parts = table.split('(', 1)
            target = t_parts[0].strip()
            if not columns:
                columns = [c.strip() for c in t_parts[1].rstrip(')').split(',')]

        source = load_info.get('source')
        source_type = load_info.get('source_type')
        if source_type is None and isinstance(source, str):
            source_type = 'cursor' if source.strip().upper().startswith(('SELECT', '(SELECT')) \
                else 'file'

        if source_type == 'cursor':
            col_clause = f" ({', '.join(columns)})" if columns else ""
            logger.info(f"SQL Server bulk load into {target} from a query")
            return self.execute(f"INSERT INTO {target}{col_clause} {source}")

        # File source: the base class owns the parsing (and the shared option names);
        # only the write is ours, through the _write_batch seam.
        def _array_bind(rows):
            cols = list(rows[0].keys())
            binds = ', '.join('?' for _ in cols)
            sql = f"INSERT INTO {target} ({', '.join(cols)}) VALUES ({binds})"
            cur = None
            try:
                cur = self.get_cursor()
                # The whole point: without it pyodbc sends one statement per row.
                cur.fast_executemany = True
                cur.executemany(sql, [[self._process_value(r[c]) for c in cols]
                                      for r in rows])
                if not getattr(self.conn, 'autocommit', False):
                    self.conn.commit()
                return {"status": 0, "message": f"{len(rows)} rows loaded",
                        "data": [], "count": len(rows)}
            except Exception as e:
                logger.error(f"SQL Server bulk load into {target} failed: {e}",
                             exc_info=True)
                return {"status": -1, "message": str(e), "data": [], "count": 0}
            finally:
                if cur:
                    cur.close()

        logger.info(f"SQL Server bulk load into {target} from a file")
        return super().bulk_load(table, dict(load_info, _write_batch=_array_bind))

    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        """
        SQL Server does not support ON CONFLICT. It requires MERGE.
        Raises NotImplementedError to force use of merge() method.
        """
        raise NotImplementedError("SQL Server does not support INSERT ... ON CONFLICT syntax. Use merge() method instead.")

    def _construct_insert_sql(self, table, str_col, values_sql, return_columns):
        """
        Override to implement OUTPUT inserted.* syntax which behaves like RETURNING
        but must be placed before VALUES.
        
        Standard: INSERT INTO table (col1) VALUES (val1)
        SQL Server: INSERT INTO table (col1) OUTPUT inserted.col1 VALUES (val1)
        """
        if return_columns:
            # SQL Server OUTPUT clause
            # We prefix columns with 'inserted.'
            
            # Handle special 'all' case or explicit list
            if return_columns == ['*']:
                out_cols = "inserted.*"
            else:
                out_cols = ', '.join([f"inserted.{c}" for c in return_columns])
            
            output_clause = f" OUTPUT {out_cols}"
            
            # Insert OUTPUT clause *before* VALUES
            return f"INSERT INTO {table} ({str_col}){output_clause} VALUES {values_sql}"
        else:
            return super()._construct_insert_sql(table, str_col, values_sql, return_columns)

    def _construct_select_sql(self, table, str_column, str_where, str_order, limit):
        """
        Override to use TOP syntax instead of LIMIT.
        """
        top_clause = ''
        if limit and limit > 0:
            top_clause = f"TOP {limit} "
            
        return f"select {top_clause}{str_column} from {table}{str_where}{str_order}"

    def merge(self, table, data, key_columns, no_update=None, *, update_columns=None, bulk_size=0):
        """
        Merge (Upsert) data into table for SQL Server using MERGE statement.

        Mirrors the base ``merge(table, data, key_columns, no_update)``
        signature (async delegation passes ``no_update`` positionally);
        SQL Server-specific extras are keyword-only.
        """
        no_update = self._no_update_flag(no_update)

        if not data:
            return {"status": 0, "message": "Merge data is empty", "data": [], "count": 0}

        is_bulk = isinstance(data, list)
        data_list = data if is_bulk else [data]
        
        first_row = data_list[0]
        data_keys = list(first_row.keys())

        # Validation
        if not key_columns:
            raise ValueError("key_columns cannot be empty for merge operation")
            
        for key in key_columns:
            if key not in data_keys:
                raise ValueError(f"Key column '{key}' not found in data")
        
        # Build Match Clause
        match_parts = [f"target.{k} = source.{k}" for k in key_columns]
        match_str = ' AND '.join(match_parts)

        # Build Update Clause
        if update_columns:
            upd_cols = update_columns
        else:
            upd_cols = [k for k in data_keys if k not in key_columns]
        
        update_set_str = ', '.join([f"target.{k} = source.{k}" for k in upd_cols])
        
        # Build Insert Clause
        insert_cols_str = ', '.join(data_keys)
        insert_vals_str = ', '.join([f"source.{k}" for k in data_keys])

        # Source Construction
        # SQL Server allows VALUES constructor in subquery:
        # MERGE INTO table target USING (VALUES (?, ?), (?, ?)) AS source (col1, col2) ON ...
        
        placeholders_row = f"({', '.join(['?'] * len(data_keys))})"
        
        # NOTE: For very large bulk inserts, constructing one massive SQL with many VALUES clauses
        # might hit parameter limits (2100 in SQL Server).
        # We should respect bulk_size.
        
        effective_batch_size = bulk_size if bulk_size > 0 else 1000
        # Check against parameter limit safety (e.g. 2000 params / cols_per_row)
        max_params = 2000
        cols_per_row = len(data_keys)
        safe_batch_size = max_params // cols_per_row
        if safe_batch_size < 1: safe_batch_size = 1
        
        if effective_batch_size > safe_batch_size:
            effective_batch_size = safe_batch_size
            
        
        total_affected = 0
        cur = None
        try:
            cur = self.get_cursor()
            
            # Process in batches
            for i in range(0, len(data_list), effective_batch_size):
                batch = data_list[i : i + effective_batch_size]
                
                # Build SQL for this specific batch size
                batch_placeholders = ', '.join([placeholders_row] * len(batch))
                
                sql = f"""
                MERGE INTO {table} AS target
                USING (VALUES {batch_placeholders}) AS source ({', '.join(data_keys)})
                ON ({match_str})
                """
                
                if not no_update and update_set_str:
                    sql += f" WHEN MATCHED THEN UPDATE SET {update_set_str}"
                    
                sql += f" WHEN NOT MATCHED THEN INSERT ({insert_cols_str}) VALUES ({insert_vals_str});" # SQL Server requires semi-colon for MERGE usually
                
                # Flatten values. Raw expressions skip _process_value and ride
                # through to inject_current, which replaces their placeholder
                # with the expression text (per row, so mixed rows are fine).
                params = []
                for row in batch:
                    for k in data_keys:
                        v = row.get(k)
                        params.append(v if isinstance(v, sql_utils.Raw) else self._process_value(v))

                # Inject Raw expressions / CURRENT keywords into the SQL
                sql, params = sql_utils.inject_current(sql, params, self.placeholder)

                cur.execute(sql, params)
                total_affected += cur.rowcount

            message = f"MERGE table {table} succeeded. {total_affected} rows affected."
            logger.info(message)
            return {"status": 0, "message": message, "data": [], "count": total_affected}

        except Exception as e:
            logger.error(f"Failed to merge SQL Server: {e}", exc_info=True)
            raise
        finally:
            if cur:
                cur.close()
