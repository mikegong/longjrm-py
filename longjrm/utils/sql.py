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


class Raw:
    """
    Marks a string as a trusted SQL expression rather than data.

    Values wrapped in ``Raw`` are rendered into the generated SQL verbatim at
    construction time — no placeholder is emitted and nothing is bound::

        db.insert("events", {"name": "boot", "created_at": Raw("CURRENT_TIMESTAMP")})
        db.update("users", {"updated_at": CURRENT_TIMESTAMP}, {"id": 1})
        db.select("logs", where={"ts": {">": Raw("CURRENT_DATE - 7")}})

    Security: ``Raw`` instances can only be constructed from Python code —
    data deserialized from JSON can never produce one, so untrusted input
    cannot escalate to SQL. Never wrap untrusted strings in ``Raw``.

    The legacy backtick form (``"`CURRENT TIMESTAMP`"`` as a plain string) is
    still recognized for backward compatibility, but ``Raw`` is the
    recommended mechanism: it is type-safe, works for any SQL expression (not
    just the four CURRENT keywords), and avoids post-hoc placeholder rewriting.

    Note: the expression text must not contain bind placeholders
    (``%s``, ``?``, ``:name``).
    """
    __slots__ = ("text",)

    def __init__(self, text):
        if isinstance(text, Raw):
            text = text.text
        if not isinstance(text, str) or not text.strip():
            raise TypeError(f"Raw expects a non-empty SQL string, got {text!r}")
        self.text = text

    def __repr__(self):
        return f"Raw({self.text!r})"

    def __str__(self):
        """The SQL text itself.

        A ``Raw`` IS its expression, so a caller that interpolates one into SQL it
        builds by hand (``f"... <= {cutoff}"``) must get ``CURRENT_DATE``, not the
        repr. Without this, such a value works when passed to insert/update/select
        -- which read ``.text`` -- and silently produces invalid SQL when formatted,
        which is the harder failure to trace.
        """
        return self.text

    def __eq__(self, other):
        return isinstance(other, Raw) and other.text == self.text

    def __hash__(self):
        return hash(("longjrm.Raw", self.text))


# Ready-made expressions for the common cases. Both spellings are standard
# SQL and accepted by every supported backend (DB2 also accepts the
# underscore forms).
CURRENT_TIMESTAMP = Raw("CURRENT_TIMESTAMP")
CURRENT_DATE = Raw("CURRENT_DATE")


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
    Handle SQL expression values (Raw) and CURRENT keywords in parameterized queries.

    For queries with placeholders (like %s or ?) where values contain Raw
    expressions or backtick-escaped CURRENT keywords, replace the placeholder
    with the expression / keyword text and drop the value from the bind list.

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
        if isinstance(values[i], Raw):
            logger.debug(f"Found Raw expression: {values[i]!r}, replacing placeholder #{placeholder_position}")
            sql = replace_nth(sql, placeholder, values[i].text, placeholder_position)
        elif isinstance(values[i], str) and check_current_keyword(values[i]):
            logger.debug(f"Found CURRENT keyword: '{values[i]}', replacing placeholder #{placeholder_position}")
            # Replace placeholder with CURRENT keyword
            sql = replace_nth(sql, placeholder, values[i], placeholder_position)
        else:
            # Keep this value and increment placeholder position
            new_values.append(values[i])
            placeholder_position += 1

    return unescape_current_keyword(sql), new_values


def build_where(conditions, placeholder="?", inline=False):
    """
    Build a WHERE clause from flexible conditions, returning (clause, values).

    Shared entry point for callers -- such as merge_select() -- that need a
    filter expressed the same way as select()/query(). Accepts several shapes:

      - None / empty            -> ("", []) (no filtering)
      - str                     -> (str, []) returned verbatim (assumed to already
                                   begin with WHERE); lets callers pass a hand-built
                                   clause for full control / backward compatibility
      - dict                    -> standard longjrm where mapping; supports
                                   operator conditions ({col: {">": x, "<=": y}}),
                                   IN lists, and logical $and/$or/$not operators
      - list of condition dicts -> implicitly AND-ed together, e.g.
                                   [{col: {">": x}}, {col: {"<=": y}}]

    When ``inline`` is False (default), value-bearing conditions emit placeholders
    and their bound values are returned in the second element, so the caller can
    pass them to execute()/query() and avoid inlining untrusted values. When
    ``inline`` is True, values are inlined (quoted/escaped) and the returned list
    is empty. Backtick-escaped CURRENT keywords are always emitted as SQL keywords
    (never bound). Returns the clause with a leading space and an uppercase WHERE.
    """
    if not conditions:
        return "", []
    # A pre-built clause string is used as-is (caller owns quoting/escaping).
    if isinstance(conditions, str):
        return conditions, []
    # A bare list of condition dicts is treated as an implicit AND so callers can
    # express repeated-column ranges (col > a AND col <= b) without colliding on
    # a single dict key.
    where = {"$and": conditions} if isinstance(conditions, list) else conditions
    clause, values = where_parser(where, placeholder, inline=inline)
    if not clause:
        return "", []
    # where_parser emits a lowercase ' where ' prefix; normalize the keyword.
    return clause.replace(" where ", " WHERE ", 1), (values or [])


def build_inline_where(conditions):
    """
    Build an inline (no bind parameters) WHERE clause from flexible conditions.

    Thin wrapper around build_where(inline=True) for callers that inline the
    filter directly into a SQL string. Accepts the same str/dict/list shapes;
    returns just the clause (a leading-space, uppercase ``WHERE ...``), or "".
    """
    clause, _ = build_where(conditions, "?", inline=True)
    return clause


# =============================================================================
# WHERE Clause Parser Functions
# =============================================================================
# These functions parse JSON-style where conditions into SQL WHERE clauses.
# Moved from Db class to enable reuse across the codebase.

# Operators that mean equality / inequality against NULL. SQL requires the
# `IS [NOT] NULL` form here — `col = NULL` / `col != NULL` always evaluate to
# UNKNOWN (never true), so a plain comparison silently matches no rows.
_NULL_IS_OPS = {'=', '==', 'IS'}
_NULL_IS_NOT_OPS = {'!=', '<>', 'IS NOT', 'NOT'}


def null_operator_clause(column, operator):
    """
    Translate ``operator`` applied to a ``None`` value into an ``IS NULL`` /
    ``IS NOT NULL`` predicate.

    Raises ValueError for operators that are undefined against NULL (``>``,
    ``<``, ``LIKE``, ...) — silently matching nothing would hide the mistake.
    """
    op = ' '.join(str(operator).split()).upper()  # normalize internal whitespace
    if op in _NULL_IS_OPS:
        return f"{column} IS NULL"
    if op in _NULL_IS_NOT_OPS:
        return f"{column} IS NOT NULL"
    raise ValueError(
        f"Operator {operator!r} cannot be used with a None value. "
        f"Use {{{column!r}: None}} (or {{'=': None}}) for IS NULL, "
        f"or {{'!=': None}} for IS NOT NULL."
    )


def in_clause(column, operator, values, placeholder, inline, param_index):
    """
    Build an ``IN`` / ``NOT IN`` predicate, correctly handling ``None`` members
    and empty lists.

    SQL ``IN``/``NOT IN`` can't match NULL via the value list, and a NULL inside
    a ``NOT IN`` list makes the whole predicate UNKNOWN for every row (the
    classic NOT-IN-NULL trap). So a ``None`` member is pulled out and expressed
    as a separate ``IS [NOT] NULL`` branch:

        col IN (a, b)  + None  ->  (col IN (a, b) OR col IS NULL)
        col NOT IN (a) + None  ->  (col NOT IN (a) AND col IS NOT NULL)

    Returns ``(clause, bind_values, param_index)``.
    """
    is_not = 'NOT' in str(operator).upper()
    has_null = any(v is None for v in values)
    items = [v for v in values if v is not None]
    binds = []

    if not items:
        # No non-null members.
        if has_null:
            clause = f"{column} IS NOT NULL" if is_not else f"{column} IS NULL"
        else:
            # Empty list: NOT IN () is always true, IN () always false.
            clause = "1=1" if is_not else "1=0"
        return clause, binds, param_index

    if inline:
        item_strs = []
        for it in items:
            if isinstance(it, str):
                item_strs.append("'" + it.replace("'", "''") + "'")
            else:
                item_strs.append(str(it))
        inner = ', '.join(item_strs)
    else:
        inner = ', '.join([placeholder] * len(items))
        binds = list(items)
        param_index += len(items)

    clause = f"{column} {operator} ({inner})"
    if has_null:
        if is_not:
            clause = f"({clause} AND {column} IS NOT NULL)"
        else:
            clause = f"({clause} OR {column} IS NULL)"
    return clause, binds, param_index


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

    if isinstance(value, Raw):
        # Trusted SQL expression: rendered verbatim, never bound.
        arr_cond.append(f"{column} = {value.text}")
    elif value is None:
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
        if isinstance(value, Raw):
            # Trusted SQL expression: rendered verbatim, never bound.
            arr_cond.append(f"{column} {operator} {value.text}")
        elif value is None:
            # `col = NULL` / `col != NULL` is never true in SQL; translate to
            # IS [NOT] NULL (raises for operators undefined against NULL).
            arr_cond.append(null_operator_clause(column, operator))
        elif isinstance(value, str):
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
        elif isinstance(value, list) and ' '.join(operator.upper().split()) in ('IN', 'NOT IN'):
            # IN / NOT IN. Handles empty lists and None members (see in_clause).
            clause, binds, param_index = in_clause(
                column, operator, value, placeholder, inline, param_index)
            arr_cond.append(clause)
            arr_values.extend(binds)
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

    if isinstance(value, Raw):
        # Trusted SQL expression: rendered verbatim, never bound.
        arr_cond.append(f"{column} {operator} {value.text}")
    elif value is None:
        # `col = NULL` / `col != NULL` is never true in SQL; translate to
        # IS [NOT] NULL (raises for operators undefined against NULL).
        arr_cond.append(null_operator_clause(column, operator))
    elif isinstance(value, list) and ' '.join(str(operator).upper().split()) in ('IN', 'NOT IN'):
        # IN / NOT IN. Handles empty lists and None members (see in_clause).
        # should_inline honors the per-condition placeholder override.
        clause, binds, param_index = in_clause(
            column, operator, value, placeholder, should_inline, param_index)
        arr_cond.append(clause)
        arr_values.extend(binds)
    elif isinstance(value, str):
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
            
    elif op_upper == '$IN':
        if not isinstance(operand, dict):
            raise ValueError(f"$in operator expects {{column: [values]}}, got {type(operand)}")

        for col, values in operand.items():
            if not isinstance(values, list):
                raise ValueError(f"$in values must be a list, got {type(values)}")

            # Handles empty lists and None members (see in_clause).
            clause, binds, param_index = in_clause(
                col, "IN", values, placeholder, inline, param_index)
            arr_cond.append(clause)
            arr_values.extend(binds)

    elif op_upper == '$NIN':
        if not isinstance(operand, dict):
            raise ValueError(f"$nin operator expects {{column: [values]}}, got {type(operand)}")

        for col, values in operand.items():
            if not isinstance(values, list):
                raise ValueError(f"$nin values must be a list, got {type(values)}")

            # Handles empty lists and None members (NOT-IN-NULL trap) — see in_clause.
            clause, binds, param_index = in_clause(
                col, "NOT IN", values, placeholder, inline, param_index)
            arr_cond.append(clause)
            arr_values.extend(binds)
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
        else:
            keys = where[column].keys()
            if "operator" in keys and "value" in keys and "placeholder" in keys:
                arr_cond, arr_values, param_index = comprehensive_condition_parser(condition, param_index, placeholder, inline)
            else:
                arr_cond, arr_values, param_index = regular_condition_parser(condition, param_index, placeholder, inline)
        parsed_cond.extend(arr_cond)
        if arr_values is not None:
            parsed_values.extend(arr_values)

    return ' where ' + ' and '.join(parsed_cond), parsed_values

