"""
dbconn.py - DEPRECATED module.

.. deprecated:: 1.0
    This module is deprecated and will be removed in a future version.
    Import directly from `longjrm.connection.connectors` instead.

Migration Guide:
    Old:
        from longjrm.connection.dbconn import JrmConnectionError
        from longjrm.connection.dbconn import get_connector_class
    
    New:
        from longjrm.connection.connectors import JrmConnectionError
        from longjrm.connection.connectors import get_connector_class

The `DatabaseConnection` class has been consolidated into the connector classes.
Use `get_connector_class(db_cfg.type)(db_cfg).connect()` instead.
"""
import warnings

from longjrm.connection.connectors import (
    JrmConnectionError,
    get_connector_class,
)

# Emit deprecation warning on import
warnings.warn(
    "longjrm.connection.dbconn is deprecated. "
    "Import from longjrm.connection.connectors instead.",
    DeprecationWarning,
    stacklevel=2
)

# Re-export for backwards compatibility
__all__ = ['JrmConnectionError', 'get_connector_class']
