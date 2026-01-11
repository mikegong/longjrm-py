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

    def _bulk_insert(self, table, data_list, return_columns=None, bulk_size=1000):
        """
        SQL Server specific bulk insert with validation.
        """
        if data_list:
            # Validate column consistency
            first_row = data_list[0]
            keys_set = set(first_row.keys())
            for i, row in enumerate(data_list):
                if set(row.keys()) != keys_set:
                    raise ValueError(f"Inconsistent columns at row {i}")
                    
        return super()._bulk_insert(table, data_list, return_columns, bulk_size)

    def supports_returning(self):
        """
        SQL Server supports returning generated keys via OUTPUT clause.
        We handle this in _construct_insert_sql.
        """
        return True

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

    def merge(self, table, data, key_columns, column_meta=None, update_columns=None, no_update='N', bulk_size=0):
        """
        Merge (Upsert) data into table for SQL Server using MERGE statement.
        """
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
                
                if no_update != 'Y' and update_set_str:
                    sql += f" WHEN MATCHED THEN UPDATE SET {update_set_str}"
                    
                sql += f" WHEN NOT MATCHED THEN INSERT ({insert_cols_str}) VALUES ({insert_vals_str});" # SQL Server requires semi-colon for MERGE usually
                
                # Flatten values
                params = []
                for row in batch:
                    for k in data_keys:
                        params.append(self._process_value(row.get(k)))
                
                # Inject current timestamp or other raw SQL literals
                sql, params = sql_utils.inject_current(sql, params, self.placeholder)

                cur.execute(sql, params)
                total_affected += cur.rowcount

            message = f"MERGE table {table} succeeded. {total_affected} rows affected."
            logger.info(message)
            return {"status": 0, "message": message, "data": [], "count": total_affected}

        except Exception as e:
            message = f"Failed to merge SQL Server: {e}"
            logger.error(message, exc_info=True)
            return {"status": -1, "message": message}
        finally:
            if cur:
                cur.close()
