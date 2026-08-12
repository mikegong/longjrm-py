"""
Data manipulation utilities for longjrm.

This module provides generic data manipulation functions including:
- File loading (INI, JSON, YAML)
- String operations (case-insensitive replace, nth occurrence replace)
- CSV escaping and formatting
"""
import re
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# File Loading Functions
# =============================================================================

def load_ini(file):
    """
    Load an INI configuration file.
    
    Args:
        file: Path to the INI file
        
    Returns:
        ConfigParser object with loaded configuration
    """
    import configparser
    config = configparser.ConfigParser()
    with open(file) as f:
        config.read_file(f)
    return config


def load_json(file):
    """
    Load a JSON file.
    
    Args:
        file: Path to the JSON file
        
    Returns:
        Parsed JSON data (dict or list)
    """
    import json
    with open(file, 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config


def load_yaml(file):
    """
    Load a YAML file.
    
    Args:
        file: Path to the YAML file
        
    Returns:
        Parsed YAML data
    """
    import yaml
    with open(file, 'r') as f:
        config = yaml.safe_load(f)
    return config


# =============================================================================
# String Manipulation Functions
# =============================================================================


def case_insensitive_replace(string, search, replacement):
    """
    Replace all occurrences of a substring case-insensitively.
    
    Args:
        string: The string to search in
        search: The substring to find (case-insensitive)
        replacement: The replacement string
        
    Returns:
        String with all occurrences replaced
    """
    return re.sub(re.escape(search), replacement, string, flags=re.IGNORECASE)


def replace_nth(string, old, new, n):
    """
    Replace the nth occurrence of a substring.
    
    Args:
        string: The string to search in
        old: The substring to find
        new: The replacement string
        n: Which occurrence to replace (1-indexed)
        
    Returns:
        String with nth occurrence replaced, or original string if not found
    """
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


def escape_csv_row(values, null_value='', quotechar=None):
    """
    Escape a row of values for CSV output.
    
    Handles:
    - None values -> empty string (or custom null_value)
    - Strings with commas, quotes, or newlines -> quoted
    - Strings with leading/trailing spaces -> quoted
    - Double quotes -> escaped as ""
    
    Args:
        values: List of values to escape
        null_value: String to use for None/NULL values
        quotechar: If set to 'Y', enforce double quotes on all strings
        
    Returns:
        List of escaped string values
    """
    escaped_row = []
    for value in values:
        if value is None:
            escaped_value = null_value
        elif isinstance(value, str):
            escaped_value = value
            # Escape double quotes by doubling them
            if '"' in escaped_value:
                escaped_value = escaped_value.replace('"', '""')
            
            # Quote if contains comma, quote, newline, or is empty/whitespace,
            # or has leading/trailing spaces, or if forced quoting is enabled
            if (',' in escaped_value or '"' in value or '\n' in escaped_value or 
                escaped_value.strip() == '' or 
                (len(escaped_value) > 0 and (escaped_value[0] == ' ' or escaped_value[-1] == ' ')) or
                quotechar == 'Y'):
                escaped_value = '"' + escaped_value + '"'
        else:
            escaped_value = str(value)
        escaped_row.append(escaped_value)
    return escaped_row


# =============================================================================
# Database Data Transformation Functions
# =============================================================================

def serialize_datetime(value):
    """Serialize a ``datetime.datetime`` for binding as a SQL value.

    An **aware** datetime denotes an instant, and the offset is the only part
    of it the database cannot reconstruct on its own. Serializing it without
    the offset does not fail loudly -- the server reads the wall-clock digits
    in its own session time zone and stores a different instant, silently. So
    aware values are emitted in ISO 8601 with their offset
    (``2026-08-12 11:26:53.525447+00:00``), which every supported dialect
    parses back to the same instant regardless of session settings.

    **Naive** datetimes keep the historical ``%Y-%m-%d %H:%M:%S.%f`` form.
    They carry no offset to preserve, so there is nothing to fix and no reason
    to change what already works for existing callers.

    Args:
        value: a ``datetime.datetime`` (aware or naive)

    Returns:
        str: the value in a form the target database parses unambiguously
    """
    if value.tzinfo is not None and value.utcoffset() is not None:
        return value.isoformat(sep=' ')
    return value.strftime('%Y-%m-%d %H:%M:%S.%f')


def datalist_to_dataseq(datalist, bulk_size=0, check_current_fn=None, unescape_current_fn=None,
                        process_value_fn=None):
    """
    Transform a list of data rows into tuple of data sequences.

    Converts list of dictionaries to tuples suitable for bulk database operations.
    Yields batches of tuples based on bulk_size through generator.

    Args:
        datalist: List of dictionaries to transform
        bulk_size: Number of rows per batch (0 = all rows in single batch)
        check_current_fn: Function to check for CURRENT SQL keywords
        unescape_current_fn: Function to unescape CURRENT keywords
        process_value_fn: Optional per-value converter (e.g. Db._process_value).
            When provided it replaces the built-in conversion below, so bulk
            operations serialize values exactly like their single-row
            counterparts on the same backend.

    Yields:
        Tuple of row tuples for each batch
    """
    import json
    import datetime

    dataseq = []

    for i in range(len(datalist)):
        row_data = []
        for k in datalist[i].keys():
            value = datalist[i][k]

            if process_value_fn is not None:
                data_value = process_value_fn(value)
            elif isinstance(value, dict):
                data_value = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, list):
                if len(value) > 0:
                    if isinstance(value[0], dict):
                        data_value = json.dumps(value, ensure_ascii=False)
                    else:
                        data_value = '|'.join(str(v) for v in value)
                else:
                    data_value = '[]'
            elif isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                data_value = str(value)
            elif isinstance(value, datetime.datetime):
                data_value = serialize_datetime(value)
            elif isinstance(value, str):
                # Handle CURRENT SQL keywords if check function provided
                if check_current_fn and check_current_fn(value):
                    data_value = unescape_current_fn(value) if unescape_current_fn else value
                else:
                    data_value = value
            else:
                data_value = value

            row_data.append(data_value)

        dataseq.append(tuple(row_data))

        if i + 1 == len(datalist) or (bulk_size != 0 and divmod(i + 1, bulk_size)[1] == 0):
            yield dataseq
            dataseq = []
