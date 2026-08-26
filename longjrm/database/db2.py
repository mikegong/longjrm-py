
import logging
import traceback
import ibm_db
import datetime
import json
import ibm_db_dbi
from longjrm.database.db import Db
from longjrm.utils import sql as sql_utils
from longjrm.connection.connectors import unwrap_connection
from longjrm.utils.sql import unescape_current_keyword

logger = logging.getLogger(__name__)

class Db2Db(Db):
    """
    Db2 database implementation.
    """

    # merge_select(): Db2 uses MERGE INTO ... USING (SELECT ...) with ELSE IGNORE.
    _merge_select_style = 'merge_into'
    _merge_select_else_clause = 'ELSE IGNORE'

    def __init__(self, client):
        super().__init__(client)
        self.placeholder = '?' # Db2 uses ? as placeholder

    def get_cursor(self):
        """Get a cursor for executing queries."""
        return self.conn.cursor()
    
    def get_stream_cursor(self):
        """Get a cursor optimized for streaming."""
        # ibm_db_dbi typical cursor is fine, but we might want to check if specific flags are needed
        return self.conn.cursor()

    def supports_returning(self):
        """Db2 supports returning columns via SELECT ... FROM FINAL TABLE (INSERT ...)"""
        return True
    
    def _construct_select_sql(self, table, str_column, str_where, str_order, limit):
        """
        Override to use FETCH FIRST syntax for Db2 (LIMIT is not standard Db2 SQL).
        """
        str_limit = ''
        if limit and limit > 0:
            str_limit = f" FETCH FIRST {limit} ROWS ONLY"

        return f"select {str_column} from {table}{str_where}{str_order}{str_limit}"

    def _construct_insert_sql(self, table, str_col, values_sql, return_columns):
        """
        Db2 wrap syntax for returning columns: SELECT ... FROM FINAL TABLE (INSERT ...)
        """
        if return_columns:
            str_return = ', '.join(return_columns)
            return f"SELECT {str_return} FROM FINAL TABLE (INSERT INTO {table} ({str_col}) VALUES {values_sql})"
        else:
            return f"INSERT INTO {table} ({str_col}) VALUES {values_sql}"

    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        """Db2 uses MERGE INTO standard SQL for upserts."""
        raise NotImplementedError("Db2 does not support INSERT ... ON CONFLICT syntax. Use merge() method instead.")

    def merge(self, table, data, key_columns, no_update=None, *,
              column_meta=None, update_columns=None, bulk_size=0):
        """
        Merge (Upsert) data into table for DB2.
        Overrides base method to use DB2 specific MERGE syntax.

        Mirrors the base ``merge(table, data, key_columns, no_update)``
        signature (async delegation passes ``no_update`` positionally);
        DB2-specific extras are keyword-only.
        """
        no_update = self._no_update_flag(no_update)

        if not data:
            return {"status": 0, "message": "No data to merge", "data": [], "count": 0}

        if not key_columns:
             raise ValueError("key_columns cannot be empty for merge operation")

        # --- Bulk Merge (List) ---
        if isinstance(data, list):
            first_row = data[0]
            data_keys = list(first_row.keys())
            column_str = ','.join(data_keys)

            match_str = ''
            for k in key_columns:
                match_str += " AND A." + k + " = TMP." + k
            match_str = match_str[5:] if match_str else '1=0'

            # Build USING inputs using CAST if meta provided, else placeholders
            if column_meta:
                 values_parts = [f"CAST({self.placeholder} AS {column_meta.get(k, 'VARCHAR(255)')})" for k in data_keys]
                 values_clause = ','.join(values_parts)
            else:
                 values_clause = ','.join([self.placeholder] * len(data_keys))

            insert_col_str = ', '.join(data_keys)
            insert_val_str = ', '.join([f"TMP.{k}" for k in data_keys])

            update_set_str = ''
            if update_columns:
                update_set_str = ', '.join([f"{c} = TMP.{c}" for c in update_columns])
            else:
                update_set_str = ', '.join([f"{k} = TMP.{k}" for k in data_keys if k not in key_columns])

            # Construct SQL
            sql = f"MERGE INTO {table} AS A USING TABLE (VALUES ({values_clause})) AS TMP ({column_str}) ON ({match_str}) "

            if not no_update and update_set_str:
                sql += f"WHEN MATCHED THEN UPDATE SET {update_set_str} "
            
            sql += f"WHEN NOT MATCHED THEN INSERT ({insert_col_str}) VALUES ({insert_val_str}) ELSE IGNORE"

            logger.debug(f"DB2 Bulk Merge SQL: {sql}")
            
            total_affected = 0
            cur = None
            try:
                cur = self.get_cursor()
                batch_size = bulk_size if bulk_size > 0 else 1000
                total_records = len(data)
                
                for i in range(0, total_records, batch_size):
                    batch = data[i : i + batch_size]
                    batch_params = []
                    for row in batch:
                        params = [self._process_value(row.get(k)) for k in data_keys]
                        batch_params.append(tuple(params))
                    
                    if batch_params:
                        cur.executemany(sql, batch_params)
                        row_count = getattr(cur, 'rowcount', -1)
                        if row_count > 0:
                            total_affected += row_count
                            
                return {
                    "status": 0, 
                    "message": f"MERGE table {table} succeeded. {total_affected} rows affected.",
                    "data": [], 
                    "count": total_affected
                }
            except Exception as e:
                logger.error(f'Failed to execute bulk merge: {e}')
                logger.error(traceback.format_exc())
                raise
            finally:
                if cur:
                    cur.close()

        # --- Single Merge (Dict) ---
        else:
            for k in key_columns:
                if k not in data:
                    raise ValueError(f"Key column '{k}' not found in data")

            column_str = ','.join(data.keys())
            match_str = ''
            value_str = ''
            update_str = ''
            insert_str = ''

            # Pre-calculate literal values to simulate legacy behavior
            literals = {}
            for k, val in data.items():
                if isinstance(val, sql_utils.Raw):
                    # Trusted SQL expression: rendered verbatim.
                    literals[k] = val.text
                elif val is None:
                    literals[k] = 'NULL'
                elif isinstance(val, (dict, list)):
                     val_str = json.dumps(val, ensure_ascii=False)
                     escaped = val_str.replace("'", "''")
                     literals[k] = f"'{escaped}'"
                elif isinstance(val, str):
                    # Only the backtick-escaped CURRENT keywords are emitted as
                    # raw SQL (consistent with the rest of the library);
                    # arbitrary backtick-wrapped strings stay quoted literals.
                    if sql_utils.check_current_keyword(val):
                         literals[k] = sql_utils.unescape_current_keyword(val)
                    else:
                        escaped = val.replace("'", "''")
                        literals[k] = f"'{escaped}'"
                elif isinstance(val, (datetime.date, datetime.datetime)):
                     literals[k] = f"'{val}'"
                else:
                     literals[k] = str(val)

            # Build clauses
            for kc in key_columns:
                match_str += f" AND A.{kc} = TMP.{kc}"
            match_str = match_str[5:]
            
            value_str = ','.join([literals[k] for k in data.keys()])
            insert_str = ','.join([f"TMP.{k}" for k in data.keys()])
            
            update_parts = []
            for k in data.keys():
                # Legacy update all columns behavior
                update_parts.append(f"{k}={literals[k]}")
            update_str = ','.join(update_parts)

            if no_update:
                 sql = f"MERGE INTO {table} AS A USING TABLE (VALUES ({value_str})) AS TMP ({column_str}) ON ({match_str}) " \
                       f"WHEN NOT MATCHED THEN INSERT ({column_str}) VALUES ({insert_str}) ELSE IGNORE"
            else:
                 sql = f"MERGE INTO {table} AS A USING TABLE (VALUES ({value_str})) AS TMP ({column_str}) ON ({match_str}) " \
                       f"WHEN MATCHED THEN UPDATE SET {update_str} " \
                       f"WHEN NOT MATCHED THEN INSERT ({column_str}) VALUES ({insert_str}) ELSE IGNORE"
            
            return self.execute(sql)

    def bulk_load(self, table, load_info=None, *, command=None):
        """
        Bulk load data into table using DB2 ADMIN_CMD stored procedure.
        Thin wrapper around load_admin_cmd() for API consistency.
        
        Args:
            table: Target table name. Can include column list, 
                   e.g., "MYTABLE" or "MYTABLE(COL1, COL2, COL3)"
            load_info: Dict with keys as expected by load_admin_cmd:
                       {'source': 'file or SELECT query', 'filetype': 'DEL/IXF/CURSOR', 
                        'operation': 'INSERT/REPLACE', 'columns': ['C1', 'C2'], ...}
            command: Optional raw ADMIN_CMD string (bypasses load_info processing)
        """
        if command:
            logger.info(f"Executing raw ADMIN_CMD: {command}")
            return self.execute(command)

        if not isinstance(load_info, dict):
            raise TypeError(f"DB2 bulk_load requires a configuration dictionary for 'load_info', got {type(load_info).__name__}")

        # Ensure target is set but respect existing load_info['target']
        final_load_info = load_info.copy()
        if table and 'target' not in final_load_info:
            final_load_info['target'] = table

        # Accept the neutral 'source_type' the sibling drivers speak ('file'/'cursor'),
        # translated to DB2's filetype (DEL/CURSOR) -- so one calling vocabulary works
        # against every engine. An explicit filetype still wins: it can also say IXF/ASC,
        # which the neutral word cannot.
        if 'filetype' not in final_load_info and 'source_type' in final_load_info:
            kind = str(final_load_info.pop('source_type')).lower()
            final_load_info['filetype'] = 'CURSOR' if kind == 'cursor' else 'DEL'

        return self.load_admin_cmd(final_load_info)

    def load_admin_cmd(self, load_info=None, load_cmd=''):
        """
        Execute DB2 LOAD via ADMIN_CMD stored procedure.
        
        Args:
            load_info: Dict with configuration:
                source: 'file path' or 'SELECT query'
                filetype: 'ASC', 'DEL', 'IXF', or 'CURSOR'
                target: 'table name' or 'table(col1, col2)'
                operation: 'INSERT' or 'REPLACE' (default 'INSERT')
                columns: Optional list of columns to load into
                recovery: 'COPY YES TO /path' (optional)
                access: 'ALLOW READ ACCESS' (optional)
                warningcount: Max warnings before aborting (default 100)
            load_cmd: Optional raw command string to execute
        """
        # execute load via ADMIN_CMD stored procedure
        # if source is file, the file must exist on server
        # to be noted that delprioritychar is used for non-cursor type to revert delimiter priority to
        # character delimiter, record delimiter, column delimiter
        if load_info is None:
            load_info = {}
        if not load_cmd:
            if not isinstance(load_info, dict):
                raise TypeError(f"load_admin_cmd requires a configuration dictionary for 'load_info', got {type(load_info).__name__}")

            source = load_info.get('source')
            filetype = load_info.get('filetype', '').upper()
            target = load_info.get('target')
            operation = load_info.get('operation', 'INSERT')
            warningcount = load_info.get('warningcount', 100)
            recovery = load_info.get('recovery', '')
            access = load_info.get('access', '')
            columns = load_info.get('columns')

            # Support for column list in load_info
            if columns and '(' not in str(target):
                target = f"{target} ({', '.join(columns)})"

            if filetype == 'CURSOR':
                escaped_sql = unescape_current_keyword(source)
                load_cmd = f"LOAD FROM ({escaped_sql}) OF CURSOR " \
                    f"warningcount {warningcount} MESSAGES ON SERVER " \
                    f"{operation} INTO {target} " \
                    f"{recovery} {access}"
            else:
                load_cmd = f"LOAD FROM {source} OF {filetype} " \
                    f"modified by delprioritychar " \
                    f"warningcount {warningcount} MESSAGES ON SERVER " \
                    f"{operation} INTO {target} " \
                    f"{recovery} {access}"
            
        try:
            logger.info(f"Loading data from {load_info.get('filetype', '')} via ADMIN_CMD: {load_cmd}")
            # With DBUtils pooling, self.conn is wrapped. Unwrap to get ibm_db_dbi.Connection, then get conn_handler
            raw_conn = unwrap_connection(self.conn)
            conn_handler = raw_conn.conn_handler
            result = ibm_db.callproc(conn_handler, 'SYSPROC.ADMIN_CMD', (load_cmd,))
            if result:
                data = []
                stmt = result[0]
                row = ibm_db.fetch_assoc(stmt)
                if row:
                    data.append({'ROWS_READ': row['ROWS_READ'], 'ROWS_SKIPPED': row['ROWS_SKIPPED'],
                                 'ROWS_LOADED': row['ROWS_LOADED'], 'ROWS_REJECTED': row['ROWS_REJECTED'],
                                 'ROWS_DELETED': row['ROWS_DELETED'], 'ROWS_COMMITTED': row['ROWS_COMMITTED'],
                                 'ROWS_PARTITIONED': row['ROWS_PARTITIONED']})
                    logger.info(data[0])

                    
                    if row['ROWS_LOADED'] is not None:
                        if row['ROWS_REJECTED'] == 0 and row['ROWS_DELETED'] == 0:
                            status = 0
                            message = f"ADMIN_CMD stored procedure completed successfully."
                            logger.info(message)
                        elif row['ROWS_REJECTED'] == 0 and row['ROWS_DELETED'] > 0:
                            status = 1
                            message = f"ADMIN_CMD stored procedure completed with DELETED: {row['ROWS_DELETED']}"
                            logger.warning(message)
                        else:
                            status = -1
                            message = f"ADMIN_CMD stored procedure completed with REJECTED: {row['ROWS_REJECTED']}"
                            logger.error(message)
                    else:
                        status = -1
                        message = f"ADMIN_CMD stored procedure completed with NONE LOADED."
                        logger.error(message)

                    if row['MSG_RETRIEVAL']:
                        msg_result = self.query(row['MSG_RETRIEVAL'])
                        msg_result_text = ''
                        if msg_result['status'] == 0:
                            for load_message in msg_result['data']:
                                # Handle both uppercase and lowercase result keys
                                m_sqlcode = load_message.get('SQLCODE') or load_message.get('sqlcode')
                                m_msg = load_message.get('MSG') or load_message.get('msg')
                                msg_result_text = msg_result_text + f'\nSQLCODE: {m_sqlcode}, MSG: {m_msg}'
                                logger.info(f"SQLCODE: {m_sqlcode}, MSG: {m_msg}")
                            message = f"{message} \n\n {msg_result_text}"
                        else:
                            logger.error(f"Failed to get ADMIN_CMD message: {msg_result['message']}")
                    
                    if row['MSG_REMOVAL']:
                        msg_key = row['MSG_REMOVAL'].split("'")[1]
                        ibm_db.callproc(conn_handler, 'SYSPROC.ADMIN_REMOVE_MSGS', (msg_key,))
                else:
                    status = 1
                    message = 'No message returned'
                    logger.warning(message)
                return {"status": status, "data": data, "total": len(data),
                        "message": message}
            else:
                message = f"Failed to execute ADMIN_CMD stored procedure: {load_cmd}"
                logger.error(message)
                return {"status": -1, "message": message}
        except Exception as e:
            logger.error(f"Failed to execute ADMIN_CMD stored procedure: {load_cmd}. {e}")
            raise

    def export_admin_cmd(self, export_info=None, export_cmd=''):
        """Execute export via ADMIN_CMD stored procedure"""
        if export_info is None:
            export_info = {}
        if not export_cmd:
            export_cmd = f"EXPORT TO {export_info.get('target')} OF {export_info.get('filetype')} " \
                f"FROM ({export_info.get('source')}) MESSAGES ON SERVER "
        try:
            logger.info(f"Exporting data from source via ADMIN_CMD: {export_cmd}")
            raw_conn = unwrap_connection(self.conn)
            conn_handler = raw_conn.conn_handler
            result = ibm_db.callproc(conn_handler, 'SYSPROC.ADMIN_CMD', (export_cmd,))
            if result:
                data = []
                stmt = result[0]
                row = ibm_db.fetch_assoc(stmt)
                if row:
                    data.append({'ROWS_READ': row['ROWS_READ'], 'ROWS_SKIPPED': row['ROWS_SKIPPED'],
                                 'ROWS_LOADED': row['ROWS_LOADED'], 'ROWS_REJECTED': row['ROWS_REJECTED'],
                                 'ROWS_DELETED': row['ROWS_DELETED'], 'ROWS_COMMITTED': row['ROWS_COMMITTED'],
                                 'ROWS_PARTITIONED': row['ROWS_PARTITIONED']})
                    logger.info(data[0])
                    
                    if row['ROWS_LOADED'] is not None:
                        if row['ROWS_REJECTED'] == 0 and row['ROWS_DELETED'] == 0:
                            status = 0
                            message = f"ADMIN_CMD stored procedure completed successfully."
                            logger.info(message)
                        elif row['ROWS_REJECTED'] == 0 and row['ROWS_DELETED'] > 0:
                            status = 1
                            message = f"ADMIN_CMD stored procedure completed with DELETED: {row['ROWS_DELETED']}"
                            logger.warning(message)
                        else:
                            status = -1
                            message = f"ADMIN_CMD stored procedure completed with REJECTED: {row['ROWS_REJECTED']}"
                            logger.error(message)
                    else:
                        status = -1
                        message = f"ADMIN_CMD stored procedure completed with NONE LOADED."
                        logger.error(message)

                    if row['MSG_RETRIEVAL']:
                        msg_result = self.query(row['MSG_RETRIEVAL'])
                        if msg_result['status'] == 0:
                            for load_message in msg_result['data']:
                                logger.info(str(load_message.get('SQLCODE')) + ' ' + str(load_message.get('MSG')))
                            message = f"{message} \n\n {msg_result['data']}"
                        else:
                            logger.error(f"Failed to get ADMIN_CMD message: {msg_result['message']}")
                    
                    if row['MSG_REMOVAL']:
                        msg_key = row['MSG_REMOVAL'].split("'")[1]
                        ibm_db.callproc(conn_handler, 'SYSPROC.ADMIN_REMOVE_MSGS', (msg_key,))
                else:
                    status = 1
                    message = 'No message returned'
                    logger.warning(message)
                return {"status": status, "data": data, "total": len(data),
                        "message": message}
            else:
                message = f"Failed to execute ADMIN_CMD stored procedure: {export_cmd}"
                logger.error(message)
                return {"status": -1, "message": message}
        except Exception as e:
            logger.error(f"Failed to execute ADMIN_CMD stored procedure: {export_cmd}. {e}")
            raise

    def admin_cmd(self, params):
        """Generic method to execute ADMIN_CMD stored procedure"""
        try:
            logger.info(f"Starting ADMIN_CMD stored procedure: {params}")
            raw_conn = unwrap_connection(self.conn)
            conn_handler = raw_conn.conn_handler
            result = ibm_db.callproc(conn_handler, 'SYSPROC.ADMIN_CMD', params)
            if result:
                data = []
                stmt = result[0]
                row = ibm_db.fetch_assoc(stmt)
                while row:
                    data.append(row)
                    row = ibm_db.fetch_assoc(stmt)
                logger.info("ADMIN_CMD stored procedure completed successfully")
                return {"status": 0, "data": data, "total": len(data),
                        "message": f"ADMIN_CMD stored procedure completed successfully."}
            else:
                message = f"Failed to execute ADMIN_CMD stored procedure: {params}"
                logger.error(message)
                return {"status": -1, "message": message}
        except Exception as e:
            logger.error(f"Failed to execute ADMIN_CMD stored procedure: {params}. {e}")
            raise

    def add_partition(self, table, partition_name, boundary=None, in_tbs=None, idx_tbs=None):
        sql = f"alter table {table} add partition {partition_name} {boundary}"
        if in_tbs:
            sql = sql + ' in ' + in_tbs
        if idx_tbs:
            sql = sql + ' index in ' + idx_tbs
        return self.execute(sql)

    def attach_partition(self, table, partition_name, boundary, from_table, index_option=None):
        sql = f"alter table {table} attach partition {partition_name} {boundary} {from_table} {index_option}"
        return self.execute(sql)

    def detach_partition(self, table, partition_name, target_table):
        sql = f"alter table {table} detach partition {partition_name} into {target_table}"
        return self.execute(sql)

    def check_partition(self, schema, table, partition_name=None):
        if partition_name:
            sql = f"select status, access_mode from syscat.datapartitions " \
                f"where TABSCHEMA = '{schema}' and tabname = '{table}' and datapartitionname = '{partition_name}'"
        else:
            sql = f"select status, access_mode from syscat.datapartitions " \
                f"where TABSCHEMA = '{schema}' and tabname = '{table}'"

        # query() raises on failure (error contract), so only the row-count
        # outcomes need handling here.
        result = self.query(sql)

        if len(result['data']) == 1:
            message = f"Got {schema}.{table} {partition_name} partition status: {result['data'][0]['STATUS']}"
            logger.info(message)
            return {"status": 0, "data": result['data'], "message": message}
        elif len(result['data']) == 0:
            message = f"No partition is found for {schema}.{table} {partition_name}"
            logger.warning(message)
            return {"status": 1, "data": [], "message": message}
        else:
            message = f"Multiple partions are found for {schema}.{table}"
            logger.warning(message)
            return {"status": 1, "data": result['data'], "message": message}

    def drop_detached_partition(self, schema, table):

        status_result = self.check_partition(schema, table)

        # No partition found / multiple partitions: nothing to drop.
        if status_result['status'] != 0:
            return status_result

        if status_result['data'][0]['STATUS'] == '' and status_result['data'][0]['ACCESS_MODE'] == 'F':
            # execute() raises on failure (error contract).
            self.execute(f"drop table {schema}.{table}")
            message = f"Table {schema}.{table} has been dropped successfully."
            logger.info(message)
            return {"status": 0, "message": message}
        else:
            message = f"Partition status of {schema}.{table} is " \
                f"{status_result['data'][0]['STATUS']}, " \
                f"access mode is {status_result['data'][0]['ACCESS_MODE']}. It can not be dropped."
            logger.warning(message)
            return {"status": 1, "message": message}

    def runstats(self, table, custom_cmd=''):
        """
        Execute runstats on table.
        Default sampling 10%, allow write access.
        """
        # default sampling 10%, allow write access
        # use custom_cmd to define customized runstats command
        if not custom_cmd:
            sql = f"runstats on table {table} with distribution and indexes all allow write access " \
                f"tablesample system(10) indexsample system(10)"
        else:
            sql = custom_cmd
        return self.execute(sql)

