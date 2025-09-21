"""
Description:
JRM - This is a generic JSON Relational Mapping library that wrap crud sql statements via database api.
conn - database connection
table - the table that is populated
data - json data including column and value pairs
where condition - json data that defines where column and value pairs
*/
"""

import re
import json
import datetime
import logging
import traceback
from typing import Union, List, Tuple, Dict, Any
from longjrm.config.runtime import get_config
from longjrm.database.placeholder_handler import PlaceholderHandler


logger = logging.getLogger(__name__)

class Db:

    def __init__(self, client):
        self.conn = client['conn']
        self.database_type = client['database_type']
        self.database_name = client['database_name']
        if client['db_lib'] in ['psycopg2', 'pymysql']: 
            self.placeholder = '%s'  # placeholder for query value
        else:
            self.placeholder = '?'  # temporary placeholder for future database libraries
        jrm_cfg = get_config()
        self.data_fetch_limit = jrm_cfg.data_fetch_limit
        self.placeholder_handler = PlaceholderHandler()

    @staticmethod
    def check_current_keyword(string):
        """
             check if string contains reserved CURRENT SQL keyword below
                CURRENT DATE, CURRENT_DATE, CURRENT TIMESTAMP, CURRENT_TIMESTAMP
             1. If CURRENT keyword is found with prefix escape ` and suffix escape ` without escape character \\,
                then that is keyword and return True
        """

        upper_string = string.upper()
        keywords = ['`CURRENT DATE`', '`CURRENT_DATE`', '`CURRENT TIMESTAMP`', '`CURRENT_TIMESTAMP`']
        for keyword in keywords:
            if keyword in upper_string and '\\' + keyword not in upper_string:
                return True
        return False

    @staticmethod
    def case_insensitive_replace(string, search, replacement):
        return re.sub(re.escape(search), replacement, string, flags=re.IGNORECASE)

    @staticmethod
    def unescape_current_keyword(string):
        """
             unescape reserved CURRENT SQL keyword below, which is quoted by `
                CURRENT DATE, CURRENT_DATE, CURRENT TIMESTAMP, CURRENT_TIMESTAMP
        """

        keywords = ['`CURRENT DATE`', '`CURRENT_DATE`', '`CURRENT TIMESTAMP`', '`CURRENT_TIMESTAMP`']
        for keyword in keywords:
            string = Db.case_insensitive_replace(string, keyword, keyword.replace('`', ''))
        return string

    @staticmethod
    def escape_sql_for_database(sql, database_type, placeholder):
        """
        Escape SQL string for database-specific requirements
        
        Args:
            sql: SQL string to escape
            database_type: Type of database ('postgres', 'mysql', etc.)
            placeholder: Placeholder character used ('%s', '?', etc.)
        
        Returns:
            Escaped SQL string
        """
        if database_type in ['postgres', 'postgresql']:
            # For PostgreSQL, escape literal % characters that are not part of placeholders
            # Replace standalone % with %% (escape them) but leave %s placeholders alone
            import re
            # This regex matches % that are NOT followed by 's' (i.e., not %s placeholders)
            sql = re.sub(r'%(?!s)', '%%', sql)
            
        elif database_type in ['mysql']:
            # MySQL with PyMySQL also needs % characters escaped when using parameterized queries
            # Replace standalone % with %% (escape them) but leave %s placeholders alone
            import re
            # This regex matches % that are NOT followed by 's' (i.e., not %s placeholders)
            sql = re.sub(r'%(?!s)', '%%', sql)
            
        return sql

    @staticmethod
    def datalist_to_dataseq(datalist, bulk_size=0):
        # transform a list of data rows into tuple of data sequence(tuple)
        # that contains input parameters matching parameter markers
        # and return the result as data set stream of bulk size through generator

        dataseq = []

        for i in range(len(datalist)):
            row_data = []
            for k in datalist[i].keys():
                if isinstance(datalist[i][k], dict):
                    data_value = json.dumps(datalist[i][k], ensure_ascii=False)
                elif isinstance(datalist[i][k], list):
                    if len(datalist[i][k]) > 0:
                        if isinstance(datalist[i][k][0], dict):
                            data_value = json.dumps(datalist[i][k], ensure_ascii=False)
                        else:
                            data_value = '|'.join(datalist[i][k])
                    else:
                        data_value = '[]'
                elif isinstance(datalist[i][k], datetime.date):
                    data_value = str(datalist[i][k])
                elif isinstance(datalist[i][k], datetime.datetime):
                    data_value = datetime.datetime.strftime(datalist[i][k], '%Y-%m-%d %H:%M:%S.%f')
                elif isinstance(datalist[i][k], str):
                    # TO BE REVIEWED: CURRENT keyword may not be supported by the bind function of ibm_db2 anyway
                    if Db.check_current_keyword(datalist[i][k]):
                        # handle keywords like CURRENT DATE
                        data_value = Db.unescape_current_keyword(datalist[i][k])
                    else:
                        data_value = datalist[i][k]
                else:
                    data_value = datalist[i][k]

                row_data.append(data_value)

            dataseq.append(tuple(row_data))

            if i + 1 == len(datalist) or (bulk_size != 0 and divmod(i + 1, bulk_size)[1] == 0):
                yield tuple(dataseq)
                dataseq = []

    @staticmethod
    def simple_condition_parser(condition, param_index, placeholder):
        """
            Simple condition format is as below，
            input: {column: value}
            output: [arrCond, arrValues, paramIndex]
            Please be aware that the condition object here contains only one key/value pair
        """

        column = list(condition.keys())[0]
        value = list(condition.values())[0]
        arr_cond = []
        arr_values = []

        if isinstance(value, str):
            clean_value = value.replace("''", "'")
            if Db.check_current_keyword(clean_value):
                # CURRENT keyword cannot be put in placeholder
                arr_cond.append(f"{column} = {Db.unescape_current_keyword(clean_value)}")
            else:
                # Placeholder is on by default 'Y'
                param_index += 1
                arr_values.append(clean_value)
                arr_cond.append(f"{column} = {placeholder}")
        else:
            # Placeholder is on by default 'Y'
            param_index += 1
            arr_values.append(value)
            arr_cond.append(f"{column} = {placeholder}")

        return arr_cond, arr_values, param_index

    @staticmethod
    def replace_nth(string, old, new, n):
        if n <= 0:
            logger.error(f"replace_nth: invalid n={n} (must be >= 1)")
            return string
            
        new_string = ''
        items = string.split(old)
        if len(items) - 1 >= n:
            for i in range(len(items)):
                new_string += items[i]
                if i + 1 == n:
                    new_string += new
                else:
                    if i < len(items) - 1:
                        new_string += old
        else:
            logger.error(f"replace_nth: requested replacement #{n} but only found {len(items)-1} occurrences of '{old}' in '{string}'")
            return string
            
        return new_string

    @staticmethod
    def inject_current(sql, values, placeholder):
        # For query that contains placeholder such as question mark but values contains CURRENT keyword,
        # replace the question mark with the CURRENT value
        if not values:
            return Db.unescape_current_keyword(sql), values
            
        logger.debug(f"inject_current: processing {len(values)} values with placeholder '{placeholder}'")
        
        new_values = []
        placeholder_position = 1  # Track which placeholder we're working on (1-based for replace_nth)
        
        for i in range(len(values)):
            if isinstance(values[i], str) and Db.check_current_keyword(values[i]):
                logger.debug(f"Found CURRENT keyword: '{values[i]}', replacing placeholder #{placeholder_position}")
                # Replace placeholder with CURRENT keyword - don't increment position since we consumed a placeholder
                sql = Db.replace_nth(sql, placeholder, values[i], placeholder_position)
            else:
                # Keep this value and increment placeholder position
                new_values.append(values[i])
                placeholder_position += 1
        
        return Db.unescape_current_keyword(sql), new_values

    @staticmethod
    def regular_condition_parser(condition, param_index, placeholder):
        """
            Regular condition format is as below，
            input: {column: {operator1: value1, operator2: value2}}
            output: [arrCond, arrValues, paramIndex]
            Please be aware that the object of condition value here supports multiple key/value pairs(operators)
        """

        column = list(condition.keys())[0]
        cond_obj = list(condition.values())[0]
        arr_cond = []
        arr_values = []

        for operator, value in cond_obj.items():
            if isinstance(value, str):
                clean_value = value.replace("''", "'")
                if Db.check_current_keyword(clean_value):
                    # CURRENT keyword cannot be put in placeholder
                    arr_cond.append(f"{column} {operator} {Db.unescape_current_keyword(clean_value)}")
                else:
                    # Placeholder is on by default 'Y'
                    param_index += 1
                    arr_values.append(clean_value)
                    arr_cond.append(f"{column} {operator} {placeholder}")
            elif isinstance(value, list) and operator.upper() == 'IN':
                # Special handling for IN operator with list values
                placeholders = ', '.join([placeholder] * len(value))
                arr_cond.append(f"{column} {operator} ({placeholders})")
                for list_item in value:
                    param_index += 1
                    arr_values.append(list_item)
            else:
                param_index += 1
                arr_values.append(value)
                arr_cond.append(f"{column} {operator} {placeholder}")

        return arr_cond, arr_values, param_index

    @staticmethod
    def comprehensive_condition_parser(condition, param_index, placeholder):
        """
            Comprehensive condition format is as below，
            {column: {"operator": ">", "value": value, "placeholder": "N"}}
            Please be aware that the object of condition value contains only one filter operator
        """

        column = list(condition.keys())[0]
        cond_obj = list(condition.values())[0]
        operator = cond_obj['operator']
        value = cond_obj['value']
        arr_cond = []
        arr_values = []

        if isinstance(value, str):
            # escape single quote
            clean_value = value.replace("''", "'")
            if Db.check_current_keyword(clean_value):
                arr_cond.append(f"{column} {operator} {Db.unescape_current_keyword(clean_value)}")
            else:
                if cond_obj.get('placeholder', 'Y') == 'N':
                    arr_cond.append(f"{column} {operator} '{clean_value}'")
                else:
                    param_index += 1
                    arr_values.append(clean_value)
                    arr_cond.append(f"{column} {operator} {placeholder}")
        else:
            if cond_obj.get('placeholder', 'Y') == 'N':
                arr_cond.append(f"{column} {operator} {value}")
            else:
                param_index += 1
                arr_values.append(value)
                arr_cond.append(f"{column} {operator} {placeholder}")

        return arr_cond, arr_values, param_index

    @staticmethod
    def operator_condition_parser(condition, param_index, placeholder):
        # TODO
        return [], [], 1

    @staticmethod
    def where_parser(where, placeholder):
        """
        where is an JSON object that defines query conditions.
            {condition1, condition2}
        Conditions can be simple, regular, comprehensive, or logical operators.
        1. Simple condition
            {column: value}, which is equivalent to regular condition {column: {"=": value}}
            and comprehensive condition {column: {"operator": "=", "value": value, "placeholder": "Y"}}
        2. Regular condition
            {column: {operator1: value1, operator2: value2}}
        3. Comprehensive condition
            {column: {"operator": ">", "value": value, "placeholder": "N"}}
        4. Logical operators
            {$operator: [{column1: value1}, {column2: value2}]}

            Note: placeholder is optional and default to Y. If placeholder is N, then the value will be put
                  in the query statement directly without using placeholder.
        """

        parsed_cond = []
        parsed_values = []
        param_index = 0

        if not where:
            return '', []

        for column in where:
            condition = {column: where[column]}
            if not isinstance(where[column], dict):
                arr_cond, arr_values, param_index = Db.simple_condition_parser(condition, param_index, placeholder)
            elif isinstance(where[column], dict):
                keys = where[column].keys()
                if "operator" in keys and "value" in keys and "placeholder" in keys:
                    arr_cond, arr_values, param_index = Db.comprehensive_condition_parser(condition, param_index, placeholder)
                else:
                    arr_cond, arr_values, param_index = Db.regular_condition_parser(condition, param_index, placeholder)
            elif where[column].keys()[0].startswith('$') and where[column].values() > 0:
                arr_cond, arr_values, param_index = Db.operator_condition_parser(condition, param_index, placeholder)
            else:
                raise Exception('Invalid where condition')
            parsed_cond.extend(arr_cond)
            if arr_values is not None:
                parsed_values.extend(arr_values)

        return ' where ' + ' and '.join(parsed_cond), parsed_values

    def select(self, table, columns=None, where=None, options=None):
        try:
            if self.database_type in ['mongodb', 'mongodb+srv']:
                select_query = self.mongo_select_constructor(columns, where, options)
                return self.query(select_query, [], table)
            else:
                select_query, arr_values = self.select_constructor(table, columns, where, options)
                return self.query(select_query, arr_values)
        except Exception as e:
            logger.error(f"select method failed: {e}")
            raise

    def select_constructor(self, table, columns=None, where=None, options=None):
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

        str_where, arr_values = Db.where_parser(where, self.placeholder)
        str_order = ' order by ' + ', '.join(options.get('order_by', [])) if options.get('order_by') else ''
        str_limit = '' if not options.get('limit') or options.get('limit') == 0 else ' limit ' + str(options['limit'])

        select_query = "select " + str_column + " from " + table + str_where + str_order + str_limit
        return select_query, arr_values

    def mongo_select_constructor(self, columns=None, where=None, options=None):
        select_query = {}

        if where:
            select_query['filter'] = where

        if columns and len(columns) >= 1 and columns != ['*']:
            select_query['projection'] = {item: 1 for item in columns}

        if options and options.get('limit'):
            if options.get('limit') != 0:
                select_query['limit'] = options.get('limit')
        else:
            select_query['limit'] = self.data_fetch_limit

        if options and options.get('order_by'):
            select_query['sort'] = {item.split(' ')[0]: -1 if item.split(' ')[1] == 'desc' else 1 for item in options['order_by']}

        return select_query

    def query(self, sql, arr_values=None, collection_name=None):
        # execute query with small result set, return entire result set, etc.
        # arr_values: values that need to be bound to query (supports both positional and named placeholders)
        # collection_name is used for MongoDb only
        # for MongoDb query, sql will be a dictionary that contains query parameters, we just re-use the name of sql
        logger.debug(f"Query: {sql}")

        try:
            if self.database_type in ['mysql', 'postgres', 'postgresql']:
                # TODO: hard code to be improved
                # define the data format of return data set as a list of dictionary like [{"column": value}]
                if self.database_type in ['postgres', 'postgresql']:
                    import psycopg2.extras
                    cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                else:
                    import pymysql
                    cur = self.conn.cursor(pymysql.cursors.DictCursor)
                
                # Handle named placeholders by converting to positional
                if arr_values:
                    sql, converted_values = self.placeholder_handler.convert_to_positional(
                        sql, arr_values, self.placeholder
                    )
                    sql, processed_values = Db.inject_current(sql, converted_values, self.placeholder)
                else:
                    processed_values = arr_values
                    
                # Escape SQL for database-specific requirements
                sql = Db.escape_sql_for_database(sql, self.database_type, self.placeholder)
                    
                cur.execute(sql, processed_values)
                rows = cur.fetchall()
                columns = list(rows[0].keys()) if len(rows) > 0 else []
                cur.close()
                logger.info(f"Query completed successfully with {len(rows)} rows returned")
                return {"data": rows, "columns": columns, "count": len(rows)}

            elif self.database_type in ['mongodb', 'mongodb+srv']:
                return self._execute_mongo_query(sql, collection_name)

            else:
                raise ValueError(f"Unsupported database type: {self.database_type}")

        except Exception as e:
            logger.error(f"query method failed: {e}")
            raise

    def _execute_mongo_query(self, sql, collection_name):
        """
        Execute MongoDB query operations
        Args:
            sql: Dictionary containing query parameters or operation details
            collection_name: MongoDB collection name
        Returns:
            Dictionary with data, columns, and count
        """
        collection = self.conn[self.database_name][collection_name]
        
        # Check if this is an insert operation
        if isinstance(sql, dict) and "operation" in sql:
            if sql["operation"] == "insert_one":
                result = collection.insert_one(sql["document"])
                if result.acknowledged:
                    inserted_doc = collection.find_one({"_id": result.inserted_id})
                    logger.info(f"MongoDB INSERT succeeded. 1 document inserted with _id: {result.inserted_id}")
                    return {
                        "data": [inserted_doc] if inserted_doc else [],
                        "columns": list(inserted_doc.keys()) if inserted_doc else [],
                        "count": 1
                    }
                else:
                    raise Exception("MongoDB insert operation was not acknowledged")
            elif sql["operation"] == "insert_many":
                result = collection.insert_many(sql["documents"])
                if result.acknowledged:
                    inserted_docs = list(collection.find({"_id": {"$in": result.inserted_ids}}))
                    columns = list(inserted_docs[0].keys()) if inserted_docs else []
                    logger.info(f"MongoDB bulk INSERT succeeded. {len(result.inserted_ids)} documents inserted")
                    return {
                        "data": inserted_docs,
                        "columns": columns,
                        "count": len(result.inserted_ids)
                    }
                else:
                    raise Exception("MongoDB bulk insert operation was not acknowledged")
        else:
            # Standard find operation
            cur = collection.find(**sql)
            rows = []
            for row in cur:
                rows.append(row)
            columns = list(rows[0].keys()) if len(rows) > 0 else []
            logger.info(f"Query completed successfully with {len(rows)} documents returned")
            return {"data": rows, "columns": columns, "count": len(rows)}

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
            dictionary with status, message, data (empty), and total (affected rows)
        """
        if return_columns is None:
            return_columns = []
            
        try:
            if self.database_type in ['mongodb', 'mongodb+srv']:
                # MongoDB uses different approach - construct operation object
                insert_operation = self.mongo_insert_constructor(table, data)
                return self.execute(insert_operation)
            else:
                # SQL databases - construct SQL and values, then use execute()
                insert_query, arr_values = self.insert_constructor(table, data, return_columns)
                return self.execute(insert_query, arr_values)
        except Exception as e:
            logger.error(f"insert method failed: {e}")
            raise

    def insert_constructor(self, table, data, return_columns=None):
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
            
        str_col = ''
        str_qm = ''
        list_val = []

        try:
            for k in data.keys():
                str_col += ',' + k
                
                # Check for CURRENT keywords BEFORE processing
                if isinstance(data[k], str) and self.check_current_keyword(data[k]):
                    # CURRENT keyword: add directly to SQL, no placeholder needed
                    str_qm += ', ' + self.unescape_current_keyword(data[k])
                else:
                    # Regular value: process it and add placeholder
                    data_value = self._process_insert_value(data[k])
                    str_qm += ', ' + self.placeholder
                    list_val.append(data_value)

        except Exception as e:
            logger.error(f"Failed to construct insert statement: {e}")
            raise

        # Remove leading comma from column list and placeholder list
        str_col = str_col[1:] if str_col else ''
        str_qm = str_qm[2:] if str_qm else ''  # Remove ', ' prefix

        if return_columns:
            if self.database_type == 'postgres' or self.database_type == 'postgresql':
                # PostgreSQL uses RETURNING clause
                sql = f"INSERT INTO {table} ({str_col}) VALUES ({str_qm}) RETURNING {','.join(return_columns)}"
            else:
                # Default SQL standard INSERT
                sql = f"INSERT INTO {table} ({str_col}) VALUES ({str_qm})"
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
            
        try:
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
                    # Check for CURRENT keywords BEFORE processing
                    if isinstance(record[col], str) and self.check_current_keyword(record[col]):
                        # CURRENT keyword: add directly to SQL, no placeholder needed
                        record_placeholders.append(self.unescape_current_keyword(record[col]))
                    else:
                        # Regular value: process it and add placeholder
                        data_value = self._process_insert_value(record[col])
                        record_placeholders.append(self.placeholder)
                        all_values.append(data_value)
                
                values_clauses.append(f"({', '.join(record_placeholders)})")
            
            # Construct INSERT statement with multiple VALUES
            values_sql = ', '.join(values_clauses)
            
            if return_columns and (self.database_type == 'postgres' or self.database_type == 'postgresql'):
                sql = f"INSERT INTO {table} ({str_col}) VALUES {values_sql} RETURNING {','.join(return_columns)}"
            else:
                sql = f"INSERT INTO {table} ({str_col}) VALUES {values_sql}"
            
            return sql, all_values
            
        except Exception as e:
            logger.error(f"Failed to construct bulk insert statement: {e}")
            raise

    def mongo_insert_constructor(self, table, data):
        """
        Construct MongoDB insert operation object
        Args:
            table: collection name
            data: JSON data - either single record or list of records  
        Returns:
            dictionary for MongoDB insert operation
        """
        if isinstance(data, list):
            return {
                "collection": table,
                "method": "insert_many",
                "args": [data]
            }
        else:
            return {
                "collection": table,
                "method": "insert_one",
                "args": [data]
            }

    def _process_insert_value(self, value):
        """
        Process individual values for insert operations
        Handles different data types: dict, list, primitives, datetime objects
        """
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)
        elif isinstance(value, list):
            if len(value) > 0:
                if isinstance(value[0], dict):
                    return json.dumps(value, ensure_ascii=False)
                else:
                    return '|'.join(str(item) for item in value)
            else:
                return '[]'
        elif isinstance(value, datetime.date):
            return str(value)
        elif isinstance(value, datetime.datetime):
            return datetime.datetime.strftime(value, '%Y-%m-%d %H:%M:%S.%f')
        elif isinstance(value, str):
            if self.check_current_keyword(value):
                # CURRENT keyword should not use placeholder
                return value
            else:
                return value
        else:
            return value

    def query_prepared(self, sql, values_list, collection_name=None):
        """
        Execute prepared query with multiple value sets for performance
        Args:
            sql: SQL statement to prepare (for SQL databases) or operation dict (for MongoDB)
            values_list: list of value arrays for multiple executions (supports both positional and named placeholders)
            collection_name: MongoDB collection name (optional)
        Returns:
            list of dictionaries with data, columns, and count for each execution
        """
        if not values_list:
            values_list = [[]]
            
        logger.debug(f"Prepared Query: {sql}")
        logger.debug(f"Values count: {len(values_list)}")

        try:
            if self.database_type in ['mysql', 'postgres', 'postgresql']:
                # Handle named placeholders by converting to positional for all value sets
                converted_values_list = []
                converted_sql = sql
                for values in values_list:
                    if values:
                        converted_sql, converted_values = self.placeholder_handler.convert_to_positional(
                            sql, values, self.placeholder
                        )
                        converted_values_list.append(converted_values)
                    else:
                        converted_values_list.append(values)
                
                values_list = converted_values_list
                sql = converted_sql
                # SQL databases with prepared statements
                if self.database_type in ['postgres', 'postgresql']:
                    import psycopg2.extras
                    cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                else:
                    import pymysql
                    cur = self.conn.cursor(pymysql.cursors.DictCursor)

                # Prepare statement (database-specific)
                if self.database_type in ['postgres', 'postgresql']:
                    # PostgreSQL: Use server-side prepared statements
                    prepared_name = f"prep_stmt_{hash(sql) % 10000}"
                    cur.execute(f"PREPARE {prepared_name} AS {sql}")
                    
                    results = []
                    for values in values_list:
                        if values:
                            processed_sql, processed_values = Db.inject_current(f"EXECUTE {prepared_name}", values, self.placeholder)
                            # Convert placeholders for EXECUTE statement
                            execute_sql = f"EXECUTE {prepared_name}({', '.join(['%s'] * len(processed_values))})"
                            cur.execute(execute_sql, processed_values)
                        else:
                            cur.execute(f"EXECUTE {prepared_name}")
                        
                        rows = cur.fetchall()
                        columns = list(rows[0].keys()) if len(rows) > 0 else []
                        results.append({"data": rows, "columns": columns, "count": len(rows)})
                    
                    # Clean up prepared statement
                    cur.execute(f"DEALLOCATE {prepared_name}")
                    
                else:
                    # MySQL: Use client-side prepared statements
                    results = []
                    for values in values_list:
                        if values:
                            processed_sql, processed_values = Db.inject_current(sql, values, self.placeholder)
                            cur.execute(processed_sql, processed_values)
                        else:
                            cur.execute(sql)
                        
                        rows = cur.fetchall()
                        columns = list(rows[0].keys()) if len(rows) > 0 else []
                        results.append({"data": rows, "columns": columns, "count": len(rows)})
                
                cur.close()
                logger.info(f"Prepared query completed successfully with {len(results)} executions")
                return results

            elif self.database_type in ['mongodb', 'mongodb+srv']:
                # MongoDB: Execute operation multiple times
                results = []
                for values in values_list:
                    # For MongoDB, values would contain different operation parameters
                    operation = sql.copy() if isinstance(sql, dict) else sql
                    result = self._execute_mongo_query(operation, collection_name)
                    results.append(result)
                
                logger.info(f"Prepared MongoDB operations completed with {len(results)} executions")
                return results

            else:
                raise ValueError(f"Unsupported database type: {self.database_type}")

        except Exception as e:
            logger.error(f"query_prepared method failed: {e}")
            raise

    def execute_prepared(self, sql, values_list):
        """
        Execute prepared statement with multiple value sets for performance
        Args:
            sql: SQL statement to prepare (for SQL databases) or operation dict (for MongoDB)
            values_list: list of value arrays for multiple executions (supports both positional and named placeholders)
        Returns:
            list of dictionaries with status, message, data (empty), and total for each execution
        """
        if not values_list:
            values_list = [[]]
            
        logger.debug(f"Prepared Execute: {sql}")
        logger.debug(f"Values count: {len(values_list)}")

        try:
            if self.database_type in ['mysql', 'postgres', 'postgresql']:
                # Handle named placeholders by converting to positional for all value sets
                converted_values_list = []
                converted_sql = sql
                for values in values_list:
                    if values:
                        converted_sql, converted_values = self.placeholder_handler.convert_to_positional(
                            sql, values, self.placeholder
                        )
                        converted_values_list.append(converted_values)
                    else:
                        converted_values_list.append(values)
                
                values_list = converted_values_list
                sql = converted_sql
                
                # SQL databases with prepared statements
                cur = self.conn.cursor()

                if self.database_type in ['postgres', 'postgresql']:
                    # PostgreSQL: Use server-side prepared statements
                    prepared_name = f"prep_exec_{hash(sql) % 10000}"
                    cur.execute(f"PREPARE {prepared_name} AS {sql}")
                    
                    results = []
                    for values in values_list:
                        try:
                            if values:
                                processed_sql, processed_values = Db.inject_current(f"EXECUTE {prepared_name}", values, self.placeholder)
                                execute_sql = f"EXECUTE {prepared_name}({', '.join(['%s'] * len(processed_values))})"
                                cur.execute(execute_sql, processed_values)
                            else:
                                cur.execute(f"EXECUTE {prepared_name}")
                            
                            affected_rows = cur.rowcount
                            
                            success_msg = f"Prepared SQL statement succeeded. {affected_rows} rows affected."
                            results.append({
                                "status": 0,
                                "message": success_msg,
                                "data": [],
                                "total": affected_rows
                            })
                            
                        except Exception as e:
                            error_msg = f"Prepared execution failed: {e}"
                            results.append({
                                "status": -1,
                                "message": error_msg,
                                "data": [],
                                "total": 0
                            })
                    
                    # Clean up prepared statement
                    try:
                        cur.execute(f"DEALLOCATE {prepared_name}")
                    except:
                        pass  # Ignore cleanup errors
                        
                else:
                    # MySQL: Execute multiple times (client-side preparation)
                    results = []
                    for values in values_list:
                        try:
                            if values:
                                processed_sql, processed_values = Db.inject_current(sql, values, self.placeholder)
                                cur.execute(processed_sql, processed_values)
                            else:
                                cur.execute(sql)
                            
                            affected_rows = cur.rowcount
                            
                            success_msg = f"Prepared SQL statement succeeded. {affected_rows} rows affected."
                            results.append({
                                "status": 0,
                                "message": success_msg,
                                "data": [],
                                "total": affected_rows
                            })
                            
                        except Exception as e:
                            error_msg = f"Prepared execution failed: {e}"
                            results.append({
                                "status": -1,
                                "message": error_msg,
                                "data": [],
                                "total": 0
                            })
                
                cur.close()
                logger.info(f"Prepared execute completed with {len(results)} executions")
                return results

            elif self.database_type in ['mongodb', 'mongodb+srv']:
                # MongoDB: Execute operation multiple times
                results = []
                for values in values_list:
                    # For MongoDB, each execution could have different parameters
                    operation = sql.copy() if isinstance(sql, dict) else sql
                    result = self._execute_mongo_operation(operation)
                    results.append(result)
                
                logger.info(f"Prepared MongoDB operations completed with {len(results)} executions")
                return results

            else:
                error_msg = f"Unsupported database type: {self.database_type}"
                logger.error(error_msg)
                return [{"status": -1, "message": error_msg, "data": [], "total": 0}]

        except Exception as e:
            error_msg = f"execute_prepared method failed: {e}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return [{"status": -1, "message": error_msg, "data": [], "total": 0}]

    def execute(self, sql, values=None):
        """
        Execute query with no return result set such as insert, update, delete, etc.
        Args:
            sql: SQL statement to execute (for SQL databases) or operation dict (for MongoDB)
            values: values that need to be bound to query (supports both positional and named placeholders)
        Returns:
            dictionary with status, message, data (empty), and total (affected rows)
        """
        if values is None:
            values = []
            
        logger.debug(f"Execute SQL: {sql}")
        logger.debug(f"Execute values: {values}")

        try:
            if self.database_type in ['mysql', 'postgres', 'postgresql']:
                # Handle named placeholders by converting to positional
                if values:
                    sql, converted_values = self.placeholder_handler.convert_to_positional(
                        sql, values, self.placeholder
                    )
                    values = converted_values
                # Get appropriate cursor for the database type
                if self.database_type in ['postgres', 'postgresql']:
                    cur = self.conn.cursor()
                else:  # mysql
                    cur = self.conn.cursor()

                # Handle CURRENT keywords in values
                if values:
                    sql, processed_values = Db.inject_current(sql, values, self.placeholder)
                else:
                    processed_values = values

                # Escape SQL for database-specific requirements
                sql = Db.escape_sql_for_database(sql, self.database_type, self.placeholder)

                # Execute the statement
                cur.execute(sql, processed_values)
                
                # Get affected row count
                affected_rows = cur.rowcount
                
                cur.close()
                
                success_msg = f"SQL statement succeeded. {affected_rows} rows is affected."
                logger.info(success_msg)
                
                return {
                    "status": 0,
                    "message": success_msg,
                    "data": [],
                    "total": affected_rows
                }
                
            elif self.database_type in ['mongodb', 'mongodb+srv']:
                return self._execute_mongo_operation(sql)
                
            else:
                error_msg = f"Unsupported database type: {self.database_type}"
                logger.error(error_msg)
                return {"status": -1, "message": error_msg, "data": [], "total": 0}

        except Exception as e:
            error_msg = f"Failed to execute statement: {e}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return {"status": -1, "message": error_msg, "data": [], "total": 0}

    def _execute_mongo_operation(self, operation):
        """
        Execute MongoDB operation with no return result set
        Args:
            operation: Dictionary containing MongoDB operation details with method and args
        Returns:
            Dictionary with status, message, data (empty), and total (affected count)
        """
        try:
            if not isinstance(operation, dict):
                error_msg = "MongoDB execute requires operation to be a dict"
                logger.error(error_msg)
                return {"status": -1, "message": error_msg, "data": [], "total": 0}
            
            collection_name = operation.get("collection")
            method_name = operation.get("method")
            method_args = operation.get("args", [])
            method_kwargs = operation.get("kwargs", {})
            
            if not collection_name:
                error_msg = "MongoDB execute requires 'collection' in operation dict"
                logger.error(error_msg)
                return {"status": -1, "message": error_msg, "data": [], "total": 0}
                
            if not method_name:
                error_msg = "MongoDB execute requires 'method' in operation dict"
                logger.error(error_msg)
                return {"status": -1, "message": error_msg, "data": [], "total": 0}
            
            collection = self.conn[self.database_name][collection_name]
            
            # Get the method from collection object
            if not hasattr(collection, method_name):
                error_msg = f"MongoDB collection does not have method: {method_name}"
                logger.error(error_msg)
                return {"status": -1, "message": error_msg, "data": [], "total": 0}
            
            method = getattr(collection, method_name)
            
            # Execute the method with provided arguments
            result = method(*method_args, **method_kwargs)
            
            # Try to get affected count from result
            affected_count = 0
            if hasattr(result, 'modified_count'):
                affected_count = result.modified_count
            elif hasattr(result, 'deleted_count'):
                affected_count = result.deleted_count
            elif hasattr(result, 'inserted_count'):
                affected_count = result.inserted_count
            elif hasattr(result, 'acknowledged'):
                affected_count = 1 if result.acknowledged else 0
            elif result is not None:
                affected_count = 1
                
            success_msg = f"MongoDB {method_name.upper()} succeeded. {affected_count} rows is affected."
            logger.info(success_msg)
            
            return {
                "status": 0,
                "message": success_msg,
                "data": [],
                "total": affected_count
            }
            
        except Exception as e:
            error_msg = f"MongoDB execute failed: {e}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return {"status": -1, "message": error_msg, "data": [], "total": 0}

    def update(self, table, data, where=None):
        """
        Update data in JSON format to table
        Args:
            table: target table name
            data: JSON data with column-value pairs to update {"col1": "val1", "col2": "val2"}
            where: JSON where condition (optional)
        Returns:
            dictionary with status, message, data (empty), and total (affected rows)
        """
        try:
            # Handle empty update data
            if not data:
                logger.warning("Update called with empty data - no operation performed")
                return {
                    'status': 0,
                    'message': 'No data to update',
                    'data': [],
                    'total': 0
                }
            
            if self.database_type in ['mongodb', 'mongodb+srv']:
                # MongoDB uses different approach - construct operation object
                update_operation = self.mongo_update_constructor(table, data, where)
                return self.execute(update_operation)
            else:
                # SQL databases - construct SQL and values, then use execute()
                update_query, arr_values = self.update_constructor(table, data, where)
                return self.execute(update_query, arr_values)
        except Exception as e:
            logger.error(f"update method failed: {e}")
            raise

    def update_constructor(self, table, data, where=None):
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

        try:
            for k, v in data.items():
                data_value = self._process_update_value(v)
                
                if data_value is None:
                    update_str += ", " + k + " = NULL"
                elif isinstance(data_value, str) and self.check_current_keyword(data_value):
                    # Handle CURRENT keywords - they should not use placeholders
                    update_str += ", " + k + " = " + self.unescape_current_keyword(data_value)
                else:
                    update_str += ", " + k + " = " + self.placeholder
                    list_val.append(data_value)

        except Exception as e:
            logger.error(f"Failed to construct update statement: {e}")
            raise

        # Remove leading comma and space from update string
        update_str = update_str[2:] if update_str else ''

        # Parse where condition
        where_str, where_values = Db.where_parser(where, self.placeholder)
        
        # Combine update values and where values
        all_values = list_val + where_values

        sql = f"UPDATE {table} SET {update_str}{where_str}"
        return sql, all_values

    def mongo_update_constructor(self, table, data, where=None):
        """
        Construct MongoDB update operation object
        Args:
            table: collection name
            data: JSON data with column-value pairs to update
            where: JSON where condition (optional)
        Returns:
            dictionary for MongoDB update operation
        """
        # Process update data - convert to MongoDB $set format
        update_doc = {"$set": {}}
        for k, v in data.items():
            processed_value = self._process_update_value(v)
            update_doc["$set"][k] = processed_value

        return {
            "collection": table,
            "method": "update_many" if where else "update_many",  # Default to update_many
            "args": [where or {}, update_doc]
        }

    def _process_update_value(self, value):
        """
        Process individual values for update operations
        Handles different data types: dict, list, primitives, datetime objects, None
        """
        if value is None:
            return None
        elif isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)
        elif isinstance(value, list):
            if len(value) > 0:
                if isinstance(value[0], dict):
                    return json.dumps(value, ensure_ascii=False)
                else:
                    return '|'.join(str(item) for item in value)
            else:
                return '[]'
        elif isinstance(value, datetime.date):
            return str(value)
        elif isinstance(value, datetime.datetime):
            return datetime.datetime.strftime(value, '%Y-%m-%d %H:%M:%S.%f')
        elif isinstance(value, str):
            if self.check_current_keyword(value):
                # CURRENT keyword should not use placeholder
                return value
            else:
                return value
        else:
            return value

    def delete(self, table, where=None):
        """
        Delete data from table based on where conditions
        Args:
            table: target table name
            where: JSON where condition (optional, but recommended to avoid deleting all records)
        Returns:
            dictionary with status, message, data (empty), and total (affected rows)
        """
        try:
            if self.database_type in ['mongodb', 'mongodb+srv']:
                # MongoDB uses different approach - construct operation object
                delete_operation = self.mongo_delete_constructor(table, where)
                return self.execute(delete_operation)
            else:
                # SQL databases - construct SQL and values, then use execute()
                delete_query, arr_values = self.delete_constructor(table, where)
                return self.execute(delete_query, arr_values)
        except Exception as e:
            logger.error(f"delete method failed: {e}")
            raise

    def delete_constructor(self, table, where=None):
        """
        Construct SQL DELETE statement from JSON where condition
        Args:
            table: target table name
            where: JSON where condition (optional)
        Returns:
            tuple of (sql_query, values_array)
        """
        try:
            # Parse where condition
            where_str, where_values = Db.where_parser(where, self.placeholder)
            
            # Construct DELETE statement
            sql = f"DELETE FROM {table}{where_str}"
            return sql, where_values
            
        except Exception as e:
            logger.error(f"Failed to construct delete statement: {e}")
            raise

    def mongo_delete_constructor(self, table, where=None):
        """
        Construct MongoDB delete operation object
        Args:
            table: collection name
            where: JSON where condition (optional)
        Returns:
            dictionary for MongoDB delete operation
        """
        return {
            "collection": table,
            "method": "delete_many",
            "args": [where or {}]
        }

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
            dictionary with status, message, data (empty), and total (affected rows)
        """
        try:
            # Validate input parameters
            if not data:
                logger.warning("Merge called with empty data - no operation performed")
                return {
                    'status': 0,
                    'message': 'No data to merge',
                    'data': [],
                    'total': 0
                }
            
            if not key_columns:
                raise ValueError("key_columns cannot be empty for merge operation")
            
            # Handle both single record and bulk merge
            if isinstance(data, list):
                return self._bulk_merge(table, data, key_columns)
            else:
                return self._single_merge(table, data, key_columns)
                
        except Exception as e:
            logger.error(f"merge method failed: {e}")
            raise

    def _single_merge(self, table, data, key_columns):
        """
        Merge a single record into the table
        """
        try:
            if self.database_type in ['mongodb', 'mongodb+srv']:
                # MongoDB uses upsert functionality
                merge_operation = self.mongo_merge_constructor(table, data, key_columns)
                return self.execute(merge_operation)
            else:
                # SQL databases - construct merge/upsert SQL
                merge_query, arr_values = self.merge_constructor(table, data, key_columns)
                return self.execute(merge_query, arr_values)
        except Exception as e:
            logger.error(f"_single_merge failed: {e}")
            raise

    def _bulk_merge(self, table, data_list, key_columns):
        """
        Merge multiple records into the table
        For simplicity, this processes each record individually
        """
        try:
            total_affected = 0
            messages = []
            
            for i, record in enumerate(data_list):
                try:
                    result = self._single_merge(table, record, key_columns)
                    if result['status'] == 0:
                        total_affected += result['total']
                        messages.append(f"Record {i+1}: {result['message']}")
                    else:
                        messages.append(f"Record {i+1} failed: {result['message']}")
                        
                except Exception as e:
                    messages.append(f"Record {i+1} failed: {str(e)}")
            
            success_msg = f"Bulk merge completed. {total_affected} total rows affected. Details: {'; '.join(messages)}"
            logger.info(success_msg)
            
            return {
                "status": 0,
                "message": success_msg,
                "data": [],
                "total": total_affected
            }
            
        except Exception as e:
            logger.error(f"_bulk_merge failed: {e}")
            raise

    def merge_constructor(self, table, data, key_columns):
        """
        Construct SQL MERGE/UPSERT statement from JSON data
        Uses database-specific syntax for upsert operations
        
        Args:
            table: target table name
            data: JSON data with column-value pairs
            key_columns: list of column names for matching
        Returns:
            tuple of (sql_query, values_array)
        """
        try:
            # Validate key columns exist in data
            for key_col in key_columns:
                if key_col not in data:
                    raise ValueError(f"Key column '{key_col}' not found in data")
            
            if self.database_type == 'mysql':
                return self._mysql_merge_constructor(table, data, key_columns)
            elif self.database_type in ['postgres', 'postgresql']:
                return self._postgres_merge_constructor(table, data, key_columns)
            else:
                # Generic approach using SELECT/INSERT/UPDATE for other databases
                return self._generic_merge_constructor(table, data, key_columns)
                
        except Exception as e:
            logger.error(f"Failed to construct merge statement: {e}")
            raise

    def _mysql_merge_constructor(self, table, data, key_columns):
        """
        Construct MySQL INSERT ... ON DUPLICATE KEY UPDATE statement
        """
        # Build INSERT part
        columns = list(data.keys())
        str_col = ', '.join(columns)
        placeholders = []
        values = []
        
        for col in columns:
            if isinstance(data[col], str) and self.check_current_keyword(data[col]):
                placeholders.append(self.unescape_current_keyword(data[col]))
            else:
                placeholders.append(self.placeholder)
                values.append(self._process_insert_value(data[col]))
        
        str_placeholders = ', '.join(placeholders)
        
        # Build UPDATE part (exclude key columns from update to avoid conflicts)
        update_parts = []
        for col in columns:
            if col not in key_columns:
                if isinstance(data[col], str) and self.check_current_keyword(data[col]):
                    update_parts.append(f"{col} = {self.unescape_current_keyword(data[col])}")
                else:
                    update_parts.append(f"{col} = VALUES({col})")
        
        if not update_parts:
            # If all columns are key columns, just ignore duplicates
            update_clause = f"{key_columns[0]} = {key_columns[0]}"
        else:
            update_clause = ', '.join(update_parts)
        
        sql = f"INSERT INTO {table} ({str_col}) VALUES ({str_placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
        
        return sql, values

    def _postgres_merge_constructor(self, table, data, key_columns):
        """
        Construct PostgreSQL INSERT ... ON CONFLICT statement
        """
        # Build INSERT part
        columns = list(data.keys())
        str_col = ', '.join(columns)
        placeholders = []
        values = []
        
        for col in columns:
            if isinstance(data[col], str) and self.check_current_keyword(data[col]):
                placeholders.append(self.unescape_current_keyword(data[col]))
            else:
                placeholders.append(self.placeholder)
                values.append(self._process_insert_value(data[col]))
        
        str_placeholders = ', '.join(placeholders)
        
        # Build conflict columns
        conflict_cols = ', '.join(key_columns)
        
        # Build UPDATE part (exclude key columns from update)
        update_parts = []
        for col in columns:
            if col not in key_columns:
                if isinstance(data[col], str) and self.check_current_keyword(data[col]):
                    update_parts.append(f"{col} = {self.unescape_current_keyword(data[col])}")
                else:
                    update_parts.append(f"{col} = EXCLUDED.{col}")
        
        if not update_parts:
            # If all columns are key columns, just do nothing on conflict
            on_conflict_clause = f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        else:
            update_clause = ', '.join(update_parts)
            on_conflict_clause = f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
        
        sql = f"INSERT INTO {table} ({str_col}) VALUES ({str_placeholders}) {on_conflict_clause}"
        
        return sql, values

    def _generic_merge_constructor(self, table, data, key_columns):
        """
        Construct generic merge using a transaction-based approach
        This is a fallback for databases that don't support native MERGE/UPSERT
        """
        # Build WHERE condition for key columns
        key_conditions = {}
        for key_col in key_columns:
            key_conditions[key_col] = data[key_col]
        
        # First try UPDATE
        update_data = {k: v for k, v in data.items() if k not in key_columns}
        if update_data:
            update_sql, update_values = self.update_constructor(table, update_data, key_conditions)
        else:
            # If no non-key columns to update, create a dummy update that affects 0 rows
            update_sql = f"UPDATE {table} SET {key_columns[0]} = {key_columns[0]} WHERE 1=0"
            update_values = []
        
        # Then INSERT if UPDATE affected 0 rows (this would need application logic)
        insert_sql, insert_values = self.insert_constructor(table, data)
        
        # For generic databases, we'll use a simpler approach with INSERT and handle duplicates in application
        # This is not atomic but works for most cases
        sql = f"""
        INSERT INTO {table} ({', '.join(data.keys())}) 
        SELECT {', '.join([self.placeholder] * len(data))}
        WHERE NOT EXISTS (
            SELECT 1 FROM {table} WHERE {' AND '.join([f"{k} = {self.placeholder}" for k in key_columns])}
        )
        """
        
        # Combine values: data values + key values for EXISTS check
        all_values = [self._process_insert_value(v) for v in data.values()]
        all_values.extend([self._process_insert_value(data[k]) for k in key_columns])
        
        return sql, all_values

    def mongo_merge_constructor(self, table, data, key_columns):
        """
        Construct MongoDB upsert operation object
        Args:
            table: collection name
            data: JSON data with column-value pairs
            key_columns: list of column names for matching
        Returns:
            dictionary for MongoDB upsert operation
        """
        # Build filter from key columns
        filter_doc = {}
        for key_col in key_columns:
            if key_col in data:
                filter_doc[key_col] = data[key_col]
        
        # Build update document
        update_doc = {"$set": {}}
        for k, v in data.items():
            processed_value = self._process_update_value(v)
            update_doc["$set"][k] = processed_value

        return {
            "collection": table,
            "method": "update_one",
            "args": [filter_doc, update_doc],
            "kwargs": {"upsert": True}
        }
