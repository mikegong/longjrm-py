"""
SQL utility functions for longjrm.

This module provides SQL-related utility functions for handling keywords,
escaping, and database-specific transformations.
"""

import logging
from longjrm.utils.data import case_insensitive_replace, replace_nth

logger = logging.getLogger(__name__)

# SQL CURRENT keywords that need special handling
CURRENT_KEYWORDS = [
    '`CURRENT DATE`', 
    '`CURRENT_DATE`', 
    '`CURRENT TIMESTAMP`', 
    '`CURRENT_TIMESTAMP`'
]


def check_current_keyword(string):
    """
    Check if string contains reserved CURRENT SQL keyword.
    
    Checks for: CURRENT DATE, CURRENT_DATE, CURRENT TIMESTAMP, CURRENT_TIMESTAMP
    These keywords are expected to be quoted with backticks (`) to indicate
    they should be treated as SQL keywords rather than string values.
    
    Args:
        string: String to check
        
    Returns:
        True if CURRENT keyword is found (not escaped with \\), False otherwise
    """
    upper_string = string.upper()
    for keyword in CURRENT_KEYWORDS:
        if keyword in upper_string and '\\\\' + keyword not in upper_string:
            return True
    return False


def unescape_current_keyword(string):
    """
    Unescape reserved CURRENT SQL keywords by removing backtick quotes.
    
    Converts `CURRENT DATE` to CURRENT DATE, etc.
    
    Args:
        string: String with quoted CURRENT keywords
        
    Returns:
        String with backticks removed from CURRENT keywords
    """
    for keyword in CURRENT_KEYWORDS:
        string = case_insensitive_replace(string, keyword, keyword.replace('`', ''))
    return string


def inject_current(sql, values, placeholder):
    """
    Handle CURRENT SQL keywords in parameterized queries.
    
    For queries with placeholders (like %s or ?) where values contain CURRENT
    keywords, replace the placeholder with the actual CURRENT keyword.
    
    Args:
        sql: SQL string with placeholders
        values: List of values for placeholders
        placeholder: Placeholder string ('%s', '?', etc.)
        
    Returns:
        Tuple of (modified_sql, filtered_values)
    """
    if not values:
        return unescape_current_keyword(sql), values
        
    logger.debug(f"inject_current: processing {len(values)} values with placeholder '{placeholder}'")
    
    new_values = []
    placeholder_position = 1  # Track which placeholder we're working on (1-based)
    
    for i in range(len(values)):
        if isinstance(values[i], str) and check_current_keyword(values[i]):
            logger.debug(f"Found CURRENT keyword: '{values[i]}', replacing placeholder #{placeholder_position}")
            # Replace placeholder with CURRENT keyword
            sql = replace_nth(sql, placeholder, values[i], placeholder_position)
        else:
            # Keep this value and increment placeholder position
            new_values.append(values[i])
            placeholder_position += 1
    
    return unescape_current_keyword(sql), new_values


def conditions_to_string(conditions):
    """
    Convert conditions dictionary to WHERE clause string.
    
    Args:
        conditions: Dictionary of column-value conditions
        
    Returns:
        WHERE clause string (including " WHERE " prefix if conditions exist)
    """
    if not conditions:
        return ""
    
    where_parts = []
    for col, val in conditions.items():
        if val is None:
            where_parts.append(f"{col} IS NULL")
        elif isinstance(val, str):
            # Escape single quotes in string values
            escaped_val = val.replace("'", "''")
            where_parts.append(f"{col} = '{escaped_val}'")
        elif isinstance(val, bool):
            where_parts.append(f"{col} = {str(val).upper()}")
        elif isinstance(val, (int, float)):
            where_parts.append(f"{col} = {val}")
        elif isinstance(val, dict):
            # Handle operator conditions like {">=": 10}
            for op, v in val.items():
                if isinstance(v, str):
                    escaped_v = v.replace("'", "''")
                    where_parts.append(f"{col} {op} '{escaped_v}'")
                else:
                    where_parts.append(f"{col} {op} {v}")
        else:
            where_parts.append(f"{col} = '{val}'")
    
    return " WHERE " + " AND ".join(where_parts)


# =============================================================================
# WHERE Clause Parser Functions
# =============================================================================
# These functions parse JSON-style where conditions into SQL WHERE clauses.
# Moved from Db class to enable reuse across the codebase.

# =============================================================================
# WHERE Clause Parser Functions
# =============================================================================
# These functions parse JSON-style where conditions into SQL WHERE clauses.
# Moved from Db class to enable reuse across the codebase.

def simple_condition_parser(condition, param_index, placeholder, inline=False):
    """
    Parse simple condition format: {column: value}
    
    Args:
        condition: Dictionary with single key-value pair
        param_index: Current parameter index
        placeholder: SQL placeholder string ('%s', '?', etc.)
        inline: If True, inline values directly into SQL
        
    Returns:
        Tuple of (arr_cond, arr_values, param_index)
    """
    column = list(condition.keys())[0]
    value = list(condition.values())[0]
    arr_cond = []
    arr_values = []

    if value is None:
        arr_cond.append(f"{column} is null")
    elif isinstance(value, str):
        clean_value = value.replace("''", "'")
        if check_current_keyword(clean_value):
            # CURRENT keyword cannot be put in placeholder
            arr_cond.append(f"{column} = {unescape_current_keyword(clean_value)}")
        elif inline:
             escaped_val = clean_value.replace("'", "''")
             arr_cond.append(f"{column} = '{escaped_val}'")
        else:
            param_index += 1
            arr_values.append(clean_value)
            arr_cond.append(f"{column} = {placeholder}")
    elif inline:
        arr_cond.append(f"{column} = {value}")
    else:
        param_index += 1
        arr_values.append(value)
        arr_cond.append(f"{column} = {placeholder}")

    return arr_cond, arr_values, param_index


def regular_condition_parser(condition, param_index, placeholder, inline=False):
    """
    Parse regular condition format: {column: {operator1: value1, operator2: value2}}
    
    Args:
        condition: Dictionary with column key and operator-value dict
        param_index: Current parameter index
        placeholder: SQL placeholder string
        inline: If True, inline values directly into SQL
        
    Returns:
        Tuple of (arr_cond, arr_values, param_index)
    """
    column = list(condition.keys())[0]
    cond_obj = list(condition.values())[0]
    arr_cond = []
    arr_values = []

    for operator, value in cond_obj.items():
        if isinstance(value, str):
            clean_value = value.replace("''", "'")
            if check_current_keyword(clean_value):
                arr_cond.append(f"{column} {operator} {unescape_current_keyword(clean_value)}")
            elif inline:
                escaped_val = clean_value.replace("'", "''")
                arr_cond.append(f"{column} {operator} '{escaped_val}'")
            else:
                param_index += 1
                arr_values.append(clean_value)
                arr_cond.append(f"{column} {operator} {placeholder}")
        elif isinstance(value, list) and operator.upper() == 'IN':
            # Special handling for IN operator with list values
            if inline:
                if not value: # empty list
                     arr_cond.append("1=0") # false condition
                else:
                    item_strs = []
                    for list_item in value:
                        if isinstance(list_item, str):
                             escaped_item = list_item.replace("'", "''")
                             item_strs.append(f"'{escaped_item}'")
                        else:
                             item_strs.append(str(list_item))
                    arr_cond.append(f"{column} {operator} ({', '.join(item_strs)})")
            else:
                placeholders = ', '.join([placeholder] * len(value))
                arr_cond.append(f"{column} {operator} ({placeholders})")
                for list_item in value:
                    param_index += 1
                    arr_values.append(list_item)
        elif inline:
             arr_cond.append(f"{column} {operator} {value}")
        else:
            param_index += 1
            arr_values.append(value)
            arr_cond.append(f"{column} {operator} {placeholder}")

    return arr_cond, arr_values, param_index


def comprehensive_condition_parser(condition, param_index, placeholder, inline=False):
    """
    Parse comprehensive condition format: {column: {"operator": ">", "value": value, "placeholder": "N"}}
    
    Args:
        condition: Dictionary with column key and operator/value/placeholder dict
        param_index: Current parameter index
        placeholder: SQL placeholder string
        inline: Global inline preference (overridden by local placeholder param)
        
    Returns:
        Tuple of (arr_cond, arr_values, param_index)
    """
    column = list(condition.keys())[0]
    cond_obj = list(condition.values())[0]
    operator = cond_obj['operator']
    value = cond_obj['value']
    arr_cond = []
    arr_values = []
    
    # Local placeholder override takes precedence over global inline
    # If placeholder='N', use inline. If placeholder='Y', use placeholder.
    # If placeholder not set, default 'Y' -> check inline arg.
    should_inline = cond_obj.get('placeholder', 'N' if inline else 'Y') == 'N'

    if isinstance(value, str):
        clean_value = value.replace("''", "'")
        if check_current_keyword(clean_value):
            arr_cond.append(f"{column} {operator} {unescape_current_keyword(clean_value)}")
        else:
            if should_inline:
                escaped_val = clean_value.replace("'", "''")
                arr_cond.append(f"{column} {operator} '{escaped_val}'")
            else:
                param_index += 1
                arr_values.append(clean_value)
                arr_cond.append(f"{column} {operator} {placeholder}")
    else:
        if should_inline:
            arr_cond.append(f"{column} {operator} {value}")
        else:
            param_index += 1
            arr_values.append(value)
            arr_cond.append(f"{column} {operator} {placeholder}")

    return arr_cond, arr_values, param_index


def parse_single_condition(condition, param_index, placeholder, inline=False):
    """
    Route a single condition to the appropriate parser.
    """
    column = list(condition.keys())[0]
    value = condition[column]
    
    if not isinstance(value, dict):
        return simple_condition_parser(condition, param_index, placeholder, inline)
    else:
        keys = value.keys()
        if "operator" in keys and "value" in keys and "placeholder" in keys:
            return comprehensive_condition_parser(condition, param_index, placeholder, inline)
        else:
            return regular_condition_parser(condition, param_index, placeholder, inline)


def operator_condition_parser(condition, param_index, placeholder, inline=False):
    """
    Parse logical operator conditions (MongoDB-style).
    
    Args:
        condition: Dictionary with $operator key
        param_index: Current parameter index
        placeholder: SQL placeholder string
        inline: If True, inline values directly into SQL
        
    Returns:
        Tuple of (arr_cond, arr_values, param_index)
    """
    operator = list(condition.keys())[0]
    operand = condition[operator]
    arr_cond = []
    arr_values = []
    
    op_upper = operator.upper()
    
    if op_upper == '$AND':
        if not isinstance(operand, list):
            raise ValueError(f"$and operator expects a list of conditions, got {type(operand)}")
        
        sub_conditions = []
        for sub_cond in operand:
            for col, val in sub_cond.items():
                sub_result = parse_single_condition({col: val}, param_index, placeholder, inline)
                sub_conditions.extend(sub_result[0])
                arr_values.extend(sub_result[1])
                param_index = sub_result[2]
        
        if sub_conditions:
            arr_cond.append('(' + ' AND '.join(sub_conditions) + ')')
            
    elif op_upper == '$OR':
        if not isinstance(operand, list):
            raise ValueError(f"$or operator expects a list of conditions, got {type(operand)}")
        
        sub_conditions = []
        for sub_cond in operand:
            for col, val in sub_cond.items():
                sub_result = parse_single_condition({col: val}, param_index, placeholder, inline)
                sub_conditions.extend(sub_result[0])
                arr_values.extend(sub_result[1])
                param_index = sub_result[2]
        
        if sub_conditions:
            arr_cond.append('(' + ' OR '.join(sub_conditions) + ')')
            
    elif op_upper == '$NOT':
        if not isinstance(operand, dict):
            raise ValueError(f"$not operator expects a condition dict, got {type(operand)}")
        
        sub_conditions = []
        for col, val in operand.items():
            sub_result = parse_single_condition({col: val}, param_index, placeholder, inline)
            sub_conditions.extend(sub_result[0])
            arr_values.extend(sub_result[1])
            param_index = sub_result[2]
        
        if sub_conditions:
            arr_cond.append('NOT (' + ' AND '.join(sub_conditions) + ')')
            
    elif op_upper == '$NIN':
        if not isinstance(operand, dict):
            raise ValueError(f"$nin operator expects {{column: [values]}}, got {type(operand)}")
        
        for col, values in operand.items():
            if not isinstance(values, list):
                raise ValueError(f"$nin values must be a list, got {type(values)}")
            
            if inline:
                if not values:
                     arr_cond.append("1=1") # NOT IN empty set is always true? No, typically "col NOT IN ()" syntax isn't standard or empty set means true. 
                     # Wait, col NOT IN (empty) is True. col IN (empty) is False.
                item_strs = []
                for v in values:
                    if isinstance(v, str):
                        escaped_v = v.replace("'", "''")
                        item_strs.append(f"'{escaped_v}'")
                    else:
                        item_strs.append(str(v))
                arr_cond.append(f"{col} NOT IN ({', '.join(item_strs)})")
            else:
                placeholders = ', '.join([placeholder] * len(values))
                arr_cond.append(f"{col} NOT IN ({placeholders})")
                for v in values:
                    param_index += 1
                    arr_values.append(v)
    else:
        raise ValueError(f"Unknown operator: {operator}")
    
    return arr_cond, arr_values, param_index


def where_parser(where, placeholder, inline=False):
    """
    Parse JSON where conditions into SQL WHERE clause.
    
    Args:
        where: Dictionary of conditions
        placeholder: SQL placeholder string ('%s', '?', etc.)
        inline: If True, inline values directly into SQL (default False)
        
    Returns:
        Tuple of (where_clause_sql, values_list)
    """
    parsed_cond = []
    parsed_values = []
    param_index = 0

    if not where:
        return '', []

    for column in where:
        condition = {column: where[column]}
        
        # Check for logical operators first (keys starting with $)
        if column.startswith('$'):
            arr_cond, arr_values, param_index = operator_condition_parser(condition, param_index, placeholder, inline)
        elif not isinstance(where[column], dict):
            arr_cond, arr_values, param_index = simple_condition_parser(condition, param_index, placeholder, inline)
        elif isinstance(where[column], dict):
            keys = where[column].keys()
            if "operator" in keys and "value" in keys and "placeholder" in keys:
                arr_cond, arr_values, param_index = comprehensive_condition_parser(condition, param_index, placeholder, inline)
            else:
                arr_cond, arr_values, param_index = regular_condition_parser(condition, param_index, placeholder, inline)
        else:
            raise Exception('Invalid where condition')
        parsed_cond.extend(arr_cond)
        if arr_values is not None:
            parsed_values.extend(arr_values)

    return ' where ' + ' and '.join(parsed_cond), parsed_values

