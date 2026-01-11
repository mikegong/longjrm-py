"""
Spark SQL database implementation using PySpark SparkSession.

Supports:
- Standard SQL queries via Spark SQL
- Delta Lake for MERGE, UPDATE, DELETE operations
- Memory-efficient streaming via DataFrame.toLocalIterator()
"""
import logging
import json
from datetime import datetime
from longjrm.database.db import Db

logger = logging.getLogger(__name__)


class SparkDb(Db):
    """
    Spark SQL implementation using SparkSession.
    
    Note: Spark handles concurrency internally via a single session.
    Transaction methods (commit/rollback) are no-ops as Spark SQL
    doesn't support traditional transactions (use Delta Lake for ACID).
    """
    
    def __init__(self, client, use_parameterized=None):
        super().__init__(client)
        self.spark = client['conn']  # SparkSession
        self._delta_enabled = None  # Lazy check
        
        # Auto-detect parameterized query support (Spark 3.4+)
        if use_parameterized is None:
            try:
                version = tuple(int(x) for x in self.spark.version.split('.')[:2])
                self.use_parameterized = version >= (3, 4)
            except Exception:
                self.use_parameterized = False
        else:
            self.use_parameterized = use_parameterized
        
        if self.use_parameterized:
            logger.debug(f"Spark {self.spark.version}: Using native parameterized queries")
        else:
            logger.debug(f"Spark {self.spark.version}: Using inline value formatting")
    
    def _check_delta_support(self):
        """Check if Delta Lake is available."""
        if self._delta_enabled is None:
            try:
                from delta.tables import DeltaTable
                self._delta_enabled = True
            except ImportError:
                self._delta_enabled = False
                logger.warning("Delta Lake not available. MERGE/UPDATE/DELETE on Delta tables disabled.")
        return self._delta_enabled
    
    def get_cursor(self):
        """
        Spark doesn't use DB-API cursors.
        Return a wrapper that mimics cursor behavior.
        """
        return _SparkCursor(self.spark)
    
    def get_stream_cursor(self):
        """
        Get cursor optimized for streaming.
        Same as get_cursor for Spark (uses toLocalIterator internally).
        """
        return _SparkCursor(self.spark, streaming=True)
    
    def commit(self):
        """No-op: Spark SQL has no traditional transactions."""
        logger.debug("Spark: commit() called (no-op)")
    
    def rollback(self):
        """No-op: Spark SQL has no traditional transactions."""
        logger.debug("Spark: rollback() called (no-op)")
    
    def set_autocommit(self, value):
        """No-op: Spark SQL has no autocommit concept."""
        pass
    
    def get_autocommit(self):
        """Spark always behaves as autocommit."""
        return True
    
    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        """
        Spark SQL doesn't support INSERT ON CONFLICT.
        Use Delta Lake merge() instead.
        """
        raise NotImplementedError(
            "Spark SQL does not support INSERT...ON CONFLICT. "
            "Use merge() method with Delta Lake tables instead."
        )
    
    def _prepare_sql(self, sql, arr_values):
        """
        Prepare SQL for Spark execution.
        Replaces %s placeholders with properly formatted literal values.
        Handles all Python types including bool, datetime, dict, list.
        """
        if not arr_values:
            return sql, None
        
        def format_value(val):
            """Format a Python value for SQL literal embedding."""
            if val is None:
                return "NULL"
            elif isinstance(val, bool):
                return "TRUE" if val else "FALSE"
            elif isinstance(val, str):
                escaped = val.replace("'", "''")
                return f"'{escaped}'"
            elif isinstance(val, (int, float)):
                return str(val)
            elif isinstance(val, datetime):
                return f"TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S')}'"
            elif isinstance(val, (dict, list)):
                json_str = json.dumps(val, ensure_ascii=False).replace("'", "''")
                return f"'{json_str}'"
            else:
                return f"'{str(val)}'"
        
        # Replace %s placeholders with formatted values
        result_sql = sql
        for val in arr_values:
            result_sql = result_sql.replace('%s', format_value(val), 1)
        
        return result_sql, None
    
    def query(self, sql, arr_values=None):
        """
        Execute query and return results.
        """
        logger.debug(f"Spark SQL query: {sql}")
        
        try:
            sql, _ = self._prepare_sql(sql, arr_values)
            df = self.spark.sql(sql)
            rows = df.collect()
            
            # Convert Spark Rows to dicts
            columns = [field.name.lower() for field in df.schema.fields]
            result_rows = [row.asDict() for row in rows]
            
            # Lowercase column names in result
            final_rows = []
            for row in result_rows:
                final_rows.append({k.lower(): v for k, v in row.items()})
            
            logger.info(f"Spark query completed: {len(final_rows)} rows")
            return {
                "status": 0,
                "message": "Query completed successfully",
                "data": final_rows,
                "columns": columns,
                "count": len(final_rows)
            }
            
        except Exception as e:
            logger.error(f"Spark query failed: {e}", exc_info=True)
            return {
                "status": -1,
                "message": f"Query failed: {e}",
                "data": [],
                "columns": [],
                "count": 0
            }
    
    def stream_query(self, sql, arr_values=None, *, max_error_count=0):
        """
        Execute query and stream results row by row using toLocalIterator().
        Memory-efficient for large result sets.
        """
        row_number = 0
        logger.debug(f"Spark SQL stream query: {sql}")
        
        try:
            sql, _ = self._prepare_sql(sql, arr_values)
            df = self.spark.sql(sql)
            columns = [field.name.lower() for field in df.schema.fields]
            
            for row in df.toLocalIterator():
                row_number += 1
                row_dict = {k.lower(): v for k, v in row.asDict().items()}
                yield row_number, row_dict, 0
            
            logger.info(f"Spark stream query completed: {row_number} rows")
            
        except Exception as e:
            logger.error(f"Spark stream query failed at row {row_number}: {e}", exc_info=True)
            yield row_number, {}, -1
    
    def execute(self, sql, arr_values=None):
        """
        Execute DDL or DML statement (CREATE, DROP, INSERT, etc.)
        """
        logger.debug(f"Spark SQL execute: {sql}")
        
        try:
            sql, _ = self._prepare_sql(sql, arr_values)
            df = self.spark.sql(sql)
            
            # For statements that don't return data, df might be empty
            # Try to get affected rows if available
            affected_rows = -1
            
            return {
                "status": 0,
                "message": "SQL statement succeeded",
                "data": [],
                "count": affected_rows
            }
            
        except Exception as e:
            logger.error(f"Spark execute failed: {e}", exc_info=True)
            return {
                "status": -1,
                "message": f"Execute failed: {e}",
                "data": [],
                "count": 0
            }
    
    def _single_insert(self, table, data, return_columns=None):
        """
        Insert a single row using parameterized query (if supported).
        """
        if return_columns:
            logger.warning("Spark insert does not support return_columns")
        
        if not data:
            return {"status": 0, "message": "No data to insert", "data": [], "count": 0}
        
        columns = list(data.keys())
        column_list = ", ".join(columns)
        
        try:
            if self.use_parameterized:
                # Spark 3.4+: Use native parameterized queries
                placeholders = ", ".join([f":{col}" for col in columns])
                sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
                self.spark.sql(sql, data)
            else:
                # Fallback: Use execute() which handles _prepare_sql
                placeholders = ", ".join(['%s' for _ in columns])
                sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
                values = [data.get(col) for col in columns]
                self.execute(sql, values)
            
            message = f"INSERT succeeded. 1 row inserted into {table}."
            logger.info(message)
            return {"status": 0, "message": message, "data": [], "count": 1}
            
        except Exception as e:
            message = f"Spark insert failed: {e}"
            logger.error(message, exc_info=True)
            return {"status": -1, "message": message, "data": [], "count": 0}
    
    def _bulk_insert(self, table, data_list, return_columns=None, bulk_size=1000):
        """
        Bulk insert using multi-row VALUES syntax with batching.
        Uses inline value formatting (parameterized not practical for bulk).
        """
        if not data_list:
            return {"status": 0, "message": "No data to insert", "data": [], "count": 0}
        
        try:
            # Get column names from first row
            columns = list(data_list[0].keys())
            column_list = ", ".join(columns)
            row_placeholder = "(" + ", ".join(['%s' for _ in columns]) + ")"
            
            # Execute in batches
            total_inserted = 0
            for i in range(0, len(data_list), bulk_size):
                batch = data_list[i:i + bulk_size]
                # Build VALUES placeholders for this batch
                values_placeholders = ", ".join([row_placeholder for _ in batch])
                # Flatten all values for this batch
                all_values = []
                for row in batch:
                    all_values.extend([row.get(col) for col in columns])
                
                sql = f"INSERT INTO {table} ({column_list}) VALUES {values_placeholders}"
                self.execute(sql, all_values)
                total_inserted += len(batch)
            
            message = f"BULK INSERT succeeded. {total_inserted} rows inserted into {table}."
            logger.info(message)
            return {"status": 0, "message": message, "data": [], "count": total_inserted}
            
        except Exception as e:
            message = f"Spark bulk insert failed: {e}"
            logger.error(message, exc_info=True)
            return {"status": -1, "message": message, "data": [], "count": 0}
    
    def update(self, table, data, where=None):
        """
        Update data in Delta Lake table.
        Requires Delta Lake for UPDATE support.
        """
        if not self._check_delta_support():
            return {
                "status": -1,
                "message": "UPDATE requires Delta Lake. Install delta-spark package.",
                "data": [], "count": 0
            }
        
        try:
            from delta.tables import DeltaTable
            
            delta_table = DeltaTable.forName(self.spark, table)
            
            # Build condition string
            condition = self._build_condition_string(where) if where else "1=1"
            
            # Build update dict
            update_dict = {k: f"'{v}'" if isinstance(v, str) else str(v) 
                          for k, v in data.items()}
            
            delta_table.update(condition=condition, set=update_dict)
            
            message = f"UPDATE on {table} succeeded"
            logger.info(message)
            return {"status": 0, "message": message, "data": [], "count": -1}
            
        except Exception as e:
            message = f"Spark update failed: {e}"
            logger.error(message, exc_info=True)
            return {"status": -1, "message": message, "data": [], "count": 0}
    
    def delete(self, table, where=None):
        """
        Delete data from Delta Lake table.
        Requires Delta Lake for DELETE support.
        """
        if not self._check_delta_support():
            return {
                "status": -1,
                "message": "DELETE requires Delta Lake. Install delta-spark package.",
                "data": [], "count": 0
            }
        
        try:
            from delta.tables import DeltaTable
            
            delta_table = DeltaTable.forName(self.spark, table)
            
            # Build condition string
            condition = self._build_condition_string(where) if where else None
            
            if condition:
                delta_table.delete(condition=condition)
            else:
                delta_table.delete()  # Delete all
            
            message = f"DELETE on {table} succeeded"
            logger.info(message)
            return {"status": 0, "message": message, "data": [], "count": -1}
            
        except Exception as e:
            message = f"Spark delete failed: {e}"
            logger.error(message, exc_info=True)
            return {"status": -1, "message": message, "data": [], "count": 0}
    
    def merge(self, table, data, key_columns, column_meta=None, update_columns=None, 
              no_update='N', bulk_size=0):
        """
        Merge (upsert) data into Delta Lake table using pure SQL.
        Uses MERGE INTO ... USING (VALUES ...) syntax to avoid Python serialization.
        """
        if not self._check_delta_support():
            return {
                "status": -1,
                "message": "MERGE requires Delta Lake. Install delta-spark package.",
                "data": [], "count": 0
            }
        
        if not data:
            return {"status": 0, "message": "No data to merge", "data": [], "count": 0}
        
        if not key_columns:
            raise ValueError("key_columns cannot be empty for merge operation")
        
        try:
            # Normalize to list
            data_list = data if isinstance(data, list) else [data]
            
            # Get all column names from first row
            all_columns = list(data_list[0].keys())
            
            # Determine update columns
            if update_columns is None:
                update_columns = [k for k in all_columns if k not in key_columns]
            
            # Build VALUES clause placeholders for source data
            row_placeholder = "(" + ", ".join(['%s' for _ in all_columns]) + ")"
            values_placeholders = ", ".join([row_placeholder for _ in data_list])
            
            # Flatten all values
            all_values = []
            for row in data_list:
                all_values.extend([row.get(col) for col in all_columns])
            
            column_list = ", ".join(all_columns)
            
            # Build merge condition
            merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
            
            # Build UPDATE SET clause
            if no_update != 'Y' and update_columns:
                update_set = ", ".join([f"target.{col} = source.{col}" for col in update_columns])
                update_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"
            else:
                update_clause = ""
            
            # Build INSERT clause
            insert_columns = ", ".join(all_columns)
            insert_values = ", ".join([f"source.{col}" for col in all_columns])
            insert_clause = f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
            
            # Build complete MERGE SQL with placeholders
            sql = f"""
                MERGE INTO {table} AS target
                USING (
                    SELECT * FROM VALUES {values_placeholders} AS t({column_list})
                ) AS source
                ON {merge_condition}
                {update_clause}
                {insert_clause}
            """
            
            self.execute(sql, all_values)
            
            count = len(data_list)
            message = f"MERGE into {table} succeeded. {count} rows processed."
            logger.info(message)
            
            return {"status": 0, "message": message, "data": [], "count": count}
            
        except Exception as e:
            message = f"Spark merge failed: {e}"
            logger.error(message, exc_info=True)
            return {"status": -1, "message": message, "data": [], "count": 0}
    
    def _build_condition_string(self, where):
        """Build SQL condition string from where dict."""
        if not where:
            return None
        
        conditions = []
        for k, v in where.items():
            if v is None:
                conditions.append(f"{k} IS NULL")
            elif isinstance(v, dict):
                # Handle operators: {'LIKE': '%val%'}, {'>': 10}, etc.
                for op, val in v.items():
                    if isinstance(val, str):
                        escaped = val.replace("'", "''")
                        conditions.append(f"{k} {op} '{escaped}'")
                    else:
                        conditions.append(f"{k} {op} {val}")
            elif isinstance(v, str):
                escaped = v.replace("'", "''")
                conditions.append(f"{k} = '{escaped}'")
            else:
                conditions.append(f"{k} = {v}")
        
        return " AND ".join(conditions)
    
    def bulk_load(self, table, load_info=None, *, command=None):
        """
        Bulk load data into Spark table from files or queries.
        
        Args:
            table: Target table name. Can include column list,
                   e.g., "my_table" or "my_table(col1, col2, col3)"
            load_info: Dict with configuration:
                source: File path (CSV/Parquet/JSON) or SELECT query
                source_type: 'file' or 'cursor' (auto-detected if omitted)
                format: 'csv', 'parquet', 'json', 'orc' (default: 'csv', for file source)
                columns: Optional list of columns to load into
                delimiter: Field delimiter for CSV (default: ',')
                header: Whether file has header row (default: False)
                mode: Write mode - 'append', 'overwrite' (default: 'append')
                infer_schema: Whether to infer schema from file (default: True)
            command: Optional raw SQL command (bypasses load_info processing)
        
        Returns:
            Result dict with status, message, data, count
        
        Examples:
            # Load from CSV file
            db.bulk_load('my_table', {
                'source': '/path/to/data.csv',
                'source_type': 'file',
                'format': 'csv',
                'header': True,
                'columns': ['name', 'email', 'age']
            })
            
            # Load from query (cursor mode)
            db.bulk_load('target_table', {
                'source': 'SELECT name, email FROM source_table',
                'source_type': 'cursor',
                'columns': ['name', 'email']
            })
        """
        try:
            # 1. Handle raw command mode
            if command:
                logger.info(f"Spark bulk_load executing raw command: {command}")
                return self.execute(command)
            
            # 2. Validate load_info
            if not isinstance(load_info, dict):
                raise TypeError(
                    f"Spark bulk_load requires a configuration dictionary for 'load_info', "
                    f"got {type(load_info).__name__}"
                )
            
            source = load_info.get('source')
            if not source:
                raise ValueError("load_info must contain 'source' key")
            
            # 3. Parse table and columns
            columns = load_info.get('columns')
            if table and '(' in str(table):
                # Table specified as "table(col1, col2)"
                t_parts = table.split('(', 1)
                table = t_parts[0].strip()
                t_cols = t_parts[1].rstrip(')').split(',')
                columns = columns or [c.strip() for c in t_cols]
            
            # 4. Auto-detect source_type if not specified
            source_type = load_info.get('source_type')
            if source_type is None:
                upper_src = source.strip().upper()
                if upper_src.startswith('SELECT') or upper_src.startswith('(SELECT'):
                    source_type = 'cursor'
                else:
                    source_type = 'file'
            
            source_type = source_type.lower()
            
            # 5. Handle cursor/query mode: INSERT INTO ... SELECT
            if source_type in ('cursor', 'query'):
                logger.info(f"Spark bulk_load from query into {table}")
                
                if columns:
                    column_list = ', '.join(columns)
                    sql = f"INSERT INTO {table} ({column_list}) {source}"
                else:
                    sql = f"INSERT INTO {table} {source}"
                
                self.spark.sql(sql)
                
                # Try to get row count from target table (approximate)
                count_result = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()
                row_count = count_result[0]['cnt'] if count_result else -1
                
                message = f"Bulk load from query into {table} succeeded."
                logger.info(message)
                return {"status": 0, "message": message, "data": [], "count": row_count}
            
            # 6. Handle file mode: spark.read -> write to table
            elif source_type == 'file':
                logger.info(f"Spark bulk_load from file '{source}' into {table}")
                
                file_format = load_info.get('format', 'csv').lower()
                mode = load_info.get('mode', 'append')
                header = load_info.get('header', False)
                delimiter = load_info.get('delimiter', ',')
                infer_schema = load_info.get('infer_schema', True)
                
                # Build reader
                reader = self.spark.read.format(file_format)
                
                if file_format == 'csv':
                    reader = reader.option("header", str(header).lower())
                    reader = reader.option("delimiter", delimiter)
                    reader = reader.option("inferSchema", str(infer_schema).lower())
                
                # Read data
                df = reader.load(source)
                
                # Select only specified columns if provided
                if columns:
                    # Handle case where file has different column names
                    # Map by position if column count matches
                    file_cols = df.columns
                    if len(file_cols) == len(columns):
                        # Rename columns to match target
                        for old_name, new_name in zip(file_cols, columns):
                            df = df.withColumnRenamed(old_name, new_name)
                        df = df.select(columns)
                    else:
                        # Try to select by name
                        df = df.select(columns)
                
                # Count rows before writing (since saveAsTable consumes the DataFrame)
                row_count = df.count()
                
                # Write to table
                df.write.mode(mode).saveAsTable(table)
                
                message = f"Bulk load from file into {table} succeeded. {row_count} rows loaded."
                logger.info(message)
                return {"status": 0, "message": message, "data": [], "count": row_count}
            
            else:
                raise ValueError(f"Unknown source_type: {source_type}. Use 'file' or 'cursor'.")
                
        except Exception as e:
            message = f"Spark bulk_load failed: {e}"
            logger.error(message, exc_info=True)
            return {"status": -1, "message": message, "data": [], "count": 0}


class _SparkCursor:
    """
    Cursor-like wrapper for Spark SQL to match DB-API interface.
    Used internally by SparkDb to maintain API compatibility.
    """
    
    def __init__(self, spark, streaming=False):
        self.spark = spark
        self.streaming = streaming
        self._df = None
        self._iterator = None
        self._description = None
        self.rowcount = -1
    
    @property
    def description(self):
        """Return column descriptions like DB-API cursor."""
        return self._description
    
    def execute(self, sql, params=None):
        """Execute SQL statement."""
        self._df = self.spark.sql(sql)
        if self._df.schema:
            self._description = [(field.name, None, None, None, None, None, None) 
                                for field in self._df.schema.fields]
        if self.streaming:
            self._iterator = self._df.toLocalIterator()
    
    def fetchall(self):
        """Fetch all rows."""
        if self._df is None:
            return []
        rows = self._df.collect()
        return [row.asDict() for row in rows]
    
    def fetchone(self):
        """Fetch one row (for streaming)."""
        if self._iterator is None:
            if self._df is None:
                return None
            self._iterator = iter(self._df.toLocalIterator())
        
        try:
            row = next(self._iterator)
            return row.asDict()
        except StopIteration:
            return None
    
    def close(self):
        """Close cursor (no-op for Spark)."""
        self._df = None
        self._iterator = None
