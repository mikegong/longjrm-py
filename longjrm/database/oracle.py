import logging
import re
import json
from longjrm.database.db import Db
from longjrm.utils import sql as sql_utils, data as data_utils

logger = logging.getLogger(__name__)

class OracleDb(Db):
    """
    Oracle database implementation.
    """

    # merge_select(): Oracle uses MERGE INTO ... USING (SELECT ...); Oracle does
    # not accept an AS keyword on the table/source aliases.
    _merge_select_style = 'merge_into'
    _merge_select_use_as = False

    def __init__(self, client):
        super().__init__(client)
        # Oracle uses :key or :1, :2 placeholders. 
        # We will convert standard %s (positional) to :1, :2, etc. dynamically. 

    def _process_value(self, value):
        """
        Process individual values for Oracle.
        Oracle driver doesn't automatically convert lists to JSON/String for VARCHAR columns,
        invalidly interpreting them as array binds. We must serialize lists to JSON.
        We also pass datetime objects natively to avoid ORA-01858 errors.
        """
        import datetime
        if isinstance(value, list):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, (datetime.date, datetime.datetime)):
            return value
        return super()._process_value(value)

    def get_cursor(self):
        """Get a cursor for executing queries."""
        return self.conn.cursor()
    
    def get_stream_cursor(self):
        """Get a cursor optimized for streaming."""
        # For Oracle, standard cursor with array size tuning is usually sufficient
        cur = self.conn.cursor()
        cur.arraysize = 500  # Reasonable default for fetching
        return cur

    def _bulk_insert(self, table, data_list, return_columns=None, bulk_size=1000):
        """
        Oracle bulk insert override to use :1, :2 placeholders.
        """
        if not data_list:
            return {"status": 0, "message": "No data to insert", "data": [], "count": 0}

        row_count = len(data_list)
        first_row = data_list[0]
        columns = list(first_row.keys())
        str_col = ', '.join(columns)
        
        # Oracle requires :1, :2, etc for positional args in executemany
        placeholders = [f":{i+1}" for i in range(len(columns))]
        str_qm = ', '.join(placeholders)
        
        sql = f"INSERT INTO {table} ({str_col}) VALUES ({str_qm})"

        # Columns come from the first row; rows are bound positionally. No
        # per-row consistency pre-scan -- the driver raises on a mismatched bind
        # (see Db._bulk_insert).
        total_affected = 0
        cur = None
        try:
            cur = self.get_cursor()
            
            # process_value_fn keeps bulk inserts serializing values exactly
            # like single-row inserts (lists -> JSON, datetimes passed natively)
            for batch in data_utils.datalist_to_dataseq(
                    data_list, bulk_size=bulk_size, process_value_fn=self._process_value):
                cur.executemany(sql, batch)
                if cur.rowcount > 0:
                    total_affected += cur.rowcount
            
            message = f"BULK INSERT succeeded. {total_affected} rows affected."
            logger.info(message)
            return {"status": 0, "message": message, "data": [], "count": total_affected}
            
        except Exception as e:
            logger.error(f'Failed to execute bulk insert: {e}', exc_info=True)
            raise

        finally:
            if cur:
                cur.close()

    def _construct_select_sql(self, table, str_column, str_where, str_order, limit):
        """
        Override to use FETCH FIRST syntax for Oracle (12c+).
        """
        str_limit = ''
        if limit and limit > 0:
            str_limit = f" FETCH FIRST {limit} ROWS ONLY"
            
        return f"select {str_column} from {table}{str_where}{str_order}{str_limit}"

    def bulk_load(self, table, load_info=None, *, command=None):
        """Bulk load into Oracle by DIRECT PATH -- pure SQL, no external utility.

        Oracle's fast loader is famously sqlldr, an external program, and its external
        tables want the file sitting on the database server behind a DIRECTORY object.
        Neither is usable from a driver. The direct path itself, though, is reachable
        from plain SQL through a hint, and that is what this uses:

          cursor source -> INSERT /*+ APPEND */ INTO t (cols) SELECT ...
          file source   -> INSERT /*+ APPEND_VALUES */ INTO t (cols) VALUES (:1, :2, ...)
                           with array binds, one round trip per batch

        Both write above the table's high-water mark, bypassing the buffer cache and
        the row-by-row insert path -- the same mechanism sqlldr's direct path uses,
        asked for in SQL instead of on a command line. The cost is the semantics that
        come with it: the load takes an exclusive lock on the table until commit, and
        it only ever APPENDS. That suits a bulk load and rules out an upsert, which is
        the same boundary every other engine's load channel has.

        load_info keys are the shared vocabulary (see Db.bulk_load): source,
        source_type, columns, delimiter, quote, encoding, header, null_value,
        bulk_size.
        """
        if command:
            return self.execute(command)
        if not isinstance(load_info, dict):
            raise TypeError(
                f"Oracle bulk_load requires a configuration dictionary for 'load_info', "
                f"got {type(load_info).__name__}")

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
            sql = f"INSERT /*+ APPEND */ INTO {target}{col_clause} {source}"
            logger.info(f"Oracle direct-path load into {target} from a query")
            return self.execute(sql)

        # File source: the base class owns the parsing (and the shared option names);
        # only the write is ours. _write_batch is the seam it offers for exactly this.
        def _direct_path(rows):
            cols = list(rows[0].keys())
            binds = ', '.join(f":{i + 1}" for i in range(len(cols)))
            sql = (f"INSERT /*+ APPEND_VALUES */ INTO {target} "
                   f"({', '.join(cols)}) VALUES ({binds})")
            cur = None
            try:
                cur = self.get_cursor()
                cur.executemany(sql, [[self._process_value(r[c]) for c in cols]
                                      for r in rows])
                # Direct path holds an exclusive lock until commit; with autocommit off
                # the next batch would block on our own uncommitted load.
                if not getattr(self.conn, 'autocommit', False):
                    self.conn.commit()
                return {"status": 0, "message": f"{len(rows)} rows loaded",
                        "data": [], "count": len(rows)}
            except Exception as e:
                logger.error(f"Oracle direct-path load into {target} failed: {e}",
                             exc_info=True)
                return {"status": -1, "message": str(e), "data": [], "count": 0}
            finally:
                if cur:
                    cur.close()

        logger.info(f"Oracle direct-path load into {target} from a file")
        return super().bulk_load(table, dict(load_info, _write_batch=_direct_path))

    def _construct_bulk_update_sql(self, table, update_columns, key_columns):
        """
        Override to use :1, :2 placeholders for Oracle bulk update.
        """
        # Determine total placeholders needed
        total_placeholders = len(update_columns) + len(key_columns)
        placeholders = [f":{i+1}" for i in range(total_placeholders)]
        
        # Split placeholders between set clause and where clause
        update_placeholders = placeholders[:len(update_columns)]
        key_placeholders = placeholders[len(update_columns):]
        
        set_clause = ', '.join([f"{col} = {ph}" for col, ph in zip(update_columns, update_placeholders)])
        where_clause = ' AND '.join([f"{col} = {ph}" for col, ph in zip(key_columns, key_placeholders)])
        
        return f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

    def _prepare_sql(self, sql, arr_values):
        """
        Prepare SQL for Oracle execution.
        Converts %s placeholders to :1, :2, etc.
        """
        if arr_values is None:
            arr_values = []

        processed_values = []
        if arr_values:
            # 1. Normalize positional placeholders to the internal %s style FIRST.
            #    Callers (and the framework itself, e.g. job.py windowing) use the
            #    standard '?' qmark; without this normalization '?' is left untouched
            #    and Oracle's driver reports zero binds for N provided values
            #    (DPY-4009). A query already written with %s is left unchanged.
            sql, arr_values = self.placeholder_handler.convert_to_positional(sql, arr_values, '%s')

            # 2. Inject current-time keywords (operates on the %s form).
            sql, processed_values = sql_utils.inject_current(sql, arr_values, '%s')

            # 3. Convert all %s to Oracle's positional :1, :2, :3 ... binds.
            parts = sql.split('%s')
            if len(parts) > 1:
                new_sql = ""
                for i, part in enumerate(parts[:-1]):
                    new_sql += f"{part}:{i+1}"
                new_sql += parts[-1]
                sql = new_sql

        else:
            processed_values = arr_values

        return sql, processed_values

    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        """
        Oracle uses MERGE INTO standard SQL for upserts.
        However, the base insert/upsert methods might try to append a clause to INSERT.
        Oracle does NOT support INSERT ... ON CONFLICT.
        
        This method raising NotImplementedError forces the caller to use the merge() method
        if they want upsert behavior, or we must refactor insert() to handle this.
        
        Current patterns suggests throwing NotImplementedError is correct for non-PG/MySQL/SQLite DBs
        that rely on the merge() method override.
        """
        raise NotImplementedError("Oracle does not support INSERT ... ON CONFLICT syntax. Use merge() method instead.")

    def merge(self, table, data, key_columns, no_update=None, *, update_columns=None):
        """
        Merge (Upsert) data into table for Oracle using MERGE INTO.

        Mirrors the base ``merge(table, data, key_columns, no_update)``
        signature (async delegation passes ``no_update`` positionally);
        ``update_columns`` is an Oracle-specific keyword-only extra.
        """
        no_update = self._no_update_flag(no_update)

        if not data:
            return {"status": 0, "message": "No data to merge", "data": [], "count": 0}

        if not key_columns:
            raise ValueError("key_columns cannot be empty for merge operation")

        # Normalize data to list
        is_bulk = isinstance(data, list)
        data_list = data if is_bulk else [data]
        
        first_row = data_list[0]
        data_keys = list(first_row.keys())
        
        # Verify all key_columns are present in data
        keys_set = set(data_keys)
        for k in key_columns:
            if k not in keys_set:
                raise ValueError(f"Key column '{k}' not found in data")
        
        # Build column strings
        column_list_str = ', '.join(data_keys)
        
        # Construct Match Clause
        match_parts = [f"target.{k} = source.{k}" for k in key_columns]
        match_str = ' AND '.join(match_parts)
        
        # Construct Update Clause
        if update_columns:
            upd_cols = update_columns
        else:
            upd_cols = [k for k in data_keys if k not in key_columns]
            
        update_set_str = ', '.join([f"{k} = source.{k}" for k in upd_cols])
        
        # Construct Insert Clause
        insert_cols_str = column_list_str
        insert_vals_str = ', '.join([f"source.{k}" for k in data_keys])
        
        # Prepare binds and identify raw SQL columns
        # If a value in first_row is a string wrapped in backticks, treat as raw SQL
        bind_keys = []
        source_select_parts = []
        bind_index = 1
        
        # The first row determines which columns are SQL expressions (Raw or
        # backtick CURRENT keywords); those columns are inlined for every row.
        for k in data_keys:
            val = first_row.get(k)
            if isinstance(val, sql_utils.Raw):
                # Trusted SQL expression: rendered verbatim, never bound.
                source_select_parts.append(f"{val.text} AS {k}")
            elif isinstance(val, str) and sql_utils.check_current_keyword(val):
                # Only the backtick-escaped CURRENT keywords are emitted as raw
                # SQL (consistent with the rest of the library); arbitrary
                # backtick-wrapped strings are bound like any other value.
                raw_sql = sql_utils.unescape_current_keyword(val)
                source_select_parts.append(f"{raw_sql} AS {k}")
            else:
                # Normal bind: use placeholder
                bind_keys.append(k)
                source_select_parts.append(f":{bind_index} AS {k}")
                bind_index += 1

        source_select = f"SELECT {', '.join(source_select_parts)} FROM DUAL"
        
        sql = f"""
        MERGE INTO {table} target
        USING ({source_select}) source
        ON ({match_str})
        """

        if not no_update and update_set_str:
            sql += f" WHEN MATCHED THEN UPDATE SET {update_set_str}"
            
        sql += f" WHEN NOT MATCHED THEN INSERT ({insert_cols_str}) VALUES ({insert_vals_str})"
        
        # Prepare parameters collecting ONLY bound columns
        batch_params = []
        for row in data_list:
            row_vals = [self._process_value(row.get(k)) for k in bind_keys]
            batch_params.append(row_vals)
            
        total_affected = 0
        cur = None
        try:
            cur = self.get_cursor()
            
            if is_bulk:
                # Use executemany
                cur.executemany(sql, batch_params)
                # oracledb executemany returns rowcount for the whole batch usually
                total_affected = cur.rowcount
            else:
                # Single execution
                cur.execute(sql, batch_params[0])
                total_affected = cur.rowcount
            
            message = f"MERGE table {table} succeeded. {total_affected} rows affected."
            logger.info(message)
            return {"status": 0, "message": message, "data": [], "count": total_affected}
            
        except Exception as e:
            logger.error(f"Failed to merge Oracle: {e}", exc_info=True)
            raise
        finally:
            if cur:
                cur.close()

    def supports_returning(self):
        """
        Oracle supports RETURNING ... INTO ...
        But simple SQL composition doesn't work because it requires OUT bind variables.
        We return False here to use standard separate SELECT if needed, 
        UNLESS we implement specialized insert logic.
        
        For now, let's keep it simple and safe: return False so the framework 
        doesn't try to append RETURNING clauses that crash.
        """
        return False
