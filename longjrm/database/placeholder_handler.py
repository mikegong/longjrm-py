"""
Placeholder handling for both positional and named parameters in SQL queries.
Automatically detects and converts named placeholders to positional format.
"""

import re
from enum import Enum
from typing import Dict, List, Tuple, Any, Union
import logging

logger = logging.getLogger(__name__)


class PlaceholderType(Enum):
    POSITIONAL = "positional"
    NAMED = "named"
    MIXED = "mixed"  # Error case


class PlaceholderHandler:
    """Handles conversion between named and positional SQL placeholders."""
    
    # Regex patterns for different placeholder styles
    NAMED_PATTERNS = {
        'colon': re.compile(r'(?<!:):(\w+)(?!\w|::)'),           # :name (but not ::type cast operators)
        'percent': re.compile(r'%\((\w+)\)s'),    # %(name)s
        'dollar': re.compile(r'\$(\w+)')          # $name
    }
    
    POSITIONAL_PATTERNS = {
        'percent_s': re.compile(r'%s'),           # %s
        'question': re.compile(r'\?')             # ?
    }
    
    @classmethod
    def detect_placeholder_type(cls, sql: str) -> Tuple[PlaceholderType, str]:
        """
        Detect which placeholder style is used in SQL.
        
        Args:
            sql: SQL query string
            
        Returns:
            Tuple of (PlaceholderType, style_name)
        """
        named_matches = []
        positional_matches = []
        
        # Check for named placeholders
        for style, pattern in cls.NAMED_PATTERNS.items():
            if pattern.search(sql):
                named_matches.append(style)
        
        # Check for positional placeholders
        for style, pattern in cls.POSITIONAL_PATTERNS.items():
            if pattern.search(sql):
                positional_matches.append(style)
        
        if named_matches and positional_matches:
            return PlaceholderType.MIXED, "mixed"
        elif named_matches:
            return PlaceholderType.NAMED, named_matches[0]
        elif positional_matches:
            return PlaceholderType.POSITIONAL, positional_matches[0]
        else:
            return PlaceholderType.POSITIONAL, "none"
    
    @classmethod
    def convert_to_positional(cls, sql: str, params: Union[List, Tuple, Dict, None], 
                            target_placeholder: str = '%s') -> Tuple[str, List[Any]]:
        """
        Convert named placeholders to positional format.
        
        Args:
            sql: SQL query string
            params: Parameters (list/tuple for positional, dict for named)
            target_placeholder: Target positional placeholder format (%s or ?)
            
        Returns:
            Tuple of (converted_sql, ordered_values_list)
        """
        if params is None:
            return sql, []
        
        # If params is already positional (list/tuple), return as-is
        # If params is already positional (list/tuple), return as-is, BUT check for placeholder mismatch
        if isinstance(params, (list, tuple)):
            # Simple conversion support for common positional patterns (%s <-> ?)
            if target_placeholder == '?' and '?' not in sql:
                # Convert %s to ?
                sql = cls.POSITIONAL_PATTERNS['percent_s'].sub('?', sql)
            elif target_placeholder == '%s' and '%s' not in sql:
                # Convert ? to %s
                sql = cls.POSITIONAL_PATTERNS['question'].sub('%s', sql)
                
            return sql, list(params)
        
        # Handle dict params - could be named placeholders
        if not isinstance(params, dict):
            raise ValueError(f"Unsupported parameter type: {type(params)}")
        
        placeholder_type, style = cls.detect_placeholder_type(sql)
        
        if placeholder_type == PlaceholderType.MIXED:
            raise ValueError("Cannot mix named and positional placeholders in the same query")
        
        if placeholder_type == PlaceholderType.POSITIONAL:
            # SQL has positional placeholders but params is dict
            # Convert dict values to list (order may not be guaranteed)
            logger.warning("Using dict parameters with positional placeholders - parameter order may be incorrect")
            return sql, list(params.values())
        
        if placeholder_type == PlaceholderType.NAMED:
            # Handle named placeholders
            pattern = cls.NAMED_PATTERNS.get(style)
            if not pattern:
                raise ValueError(f"Unsupported named placeholder style: {style}")
            
            # Find all named placeholders in order of appearance
            matches = list(pattern.finditer(sql))
            param_names = [match.group(1) for match in matches]
            
            # Check that all required parameters are provided
            missing_params = set(param_names) - set(params.keys())
            if missing_params:
                raise ValueError(f"Missing parameters: {missing_params}")
            
            # Replace named placeholders with positional ones
            converted_sql = pattern.sub(target_placeholder, sql)
            ordered_values = [params[name] for name in param_names]
            
            logger.debug(f"Converted named placeholders: {param_names} -> positional")
            return converted_sql, ordered_values
        
        # No placeholders found but dict provided - return empty
        return sql, []
    
    @classmethod
    def validate_parameters(cls, sql: str, params: Union[List, Tuple, Dict, None]) -> bool:
        """
        Validate that parameters match the placeholders in SQL.
        
        Args:
            sql: SQL query string
            params: Parameters to validate
            
        Returns:
            True if parameters are valid
        """
        try:
            cls.convert_to_positional(sql, params)
            return True
        except (ValueError, KeyError):
            return False