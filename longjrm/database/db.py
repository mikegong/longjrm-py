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
from longjrm.config import JrmConfig, DatabaseConfig

import longjrm.load_env as jrm_env

logger = logging.getLogger(__name__)

class Db:

    def __init__(self, client):
        self.conn = client['conn']
        self.database_type = client['database_type']
        self.database_name = client['database_name']
        if client['db_lib'] == 'dbutils':  # universal database connection from dbutils lib
            self.placeholder = '%s'  # placeholder for query value
        else:
            self.placeholder = '?'  # temporary placeholder for future database libraries

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
        return new_string

    @staticmethod
    def inject_current(sql, values, placeholder):
        # For query that contains placeholder such as question mark but values contains CURRENT keyword,
        # replace the question mark with the CURRENT value
        new_values = []
        j = 0
        for i in range(len(values)):
            if isinstance(values[i], str) and Db.check_current_keyword(values[i]):
                sql = Db.replace_nth(sql, placeholder, values[i], j + 1)
                j -= 1
            else:
                new_values.append(values[i])
            j += 1
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
                select_query = Db.mongo_select_constructor(columns, where, options)
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
                "limit": jrm_env.config.get('DATABASE', 'DATA_FETCH_LIMIT'),
                "order_by": []
            }

        str_column = ', '.join(columns) if isinstance(columns, list) else None
        if not str_column:
            raise ValueError('Invalid columns')

        str_where, arr_values = Db.where_parser(where, self.placeholder)
        str_order = ' order by ' + ', '.join(options['order_by']) if options['order_by'] else ''
        str_limit = '' if not options.get('limit') or options.get('limit') == 0 else ' limit ' + str(options['limit'])

        select_query = "select " + str_column + " from " + table + str_where + str_order + str_limit
        return select_query, arr_values

    @staticmethod
    def mongo_select_constructor(columns=None, where=None, options=None):
        select_query = {}

        if where:
            select_query['filter'] = where

        if columns and len(columns) >= 1 and columns != ['*']:
            select_query['projection'] = {item: 1 for item in columns}

        if options and options.get('limit'):
            if options.get('limit') != 0:
                select_query['limit'] = options.get('limit')
        else:
            select_query['limit'] = int(jrm_env.config.get('DATABASE', 'DATA_FETCH_LIMIT'))

        if options and options.get('order_by'):
            select_query['sort'] = {item.split(' ')[0]: -1 if item.split(' ')[1] == 'desc' else 1 for item in options['order_by']}

        return select_query

    def query(self, sql, arr_values=None, collection_name=None):
        # collection_name is used for MongoDb only
        # for MongoDb query, sql will be a dictionary that contains query parameters, we just re-use the name of sql
        logger.debug(f"Query: {sql}")

        try:
            if self.database_type in ['mysql', 'postgres', 'postgresql']:
                # TODO: hard code to be improved
                # define the data format of return data set as a list of dictionary like [{"column": value}]
                if self.database_type in ['postgres', 'postgresql']:
                    import psycopg2.extras
                    cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                else:
                    import pymysql
                    cur = self.conn.cursor(pymysql.cursors.DictCursor)
                # scan query input values to exclude CURRENT keyword
                if arr_values:
                    sql, values = Db.inject_current(sql, arr_values, self.placeholder)

                cur.execute(sql, arr_values)
                rows = cur.fetchall()
                columns = list(rows[0].keys()) if len(rows) > 0 else []
                cur.close()
                logger.info(f"Query completed successfully with {len(rows)} rows returned")
                return {"data": rows, "columns": columns, "count": len(rows)}

            elif self.database_type in ['mongodb', 'mongodb+srv']:
                cur = self.conn[collection_name].find(**sql)
                rows = []
                for row in cur:
                    rows.append(row)
                columns = list(rows[0].keys()) if len(rows) > 0 else []
                logger.info(f"Query completed successfully with {len(rows)} documents returned")
                return {"data": rows, "columns": columns, "count": len(rows)}

            else:
                raise ValueError(f"Unsupported database type: {self.database_type}")

        except Exception as e:
            logger.error(f"query method failed: {e}")
            raise
