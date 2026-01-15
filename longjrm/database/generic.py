import logging
import importlib
from longjrm.database.db import Db

logger = logging.getLogger(__name__)

class GenericDb(Db):
    """
    Generic database implementation for any DB-API 2.0 compliant driver.
    """
    
    def __init__(self, client):
        super().__init__(client)
        self._configure_placeholder(client.get('db_lib'))

    def _configure_placeholder(self, db_lib_name):
        """
        Attempt to detect parameter style from the driver module.
        Defaults to '%s' (format/pyformat) if detection fails or style is unsupported.
        """
        try:
            if db_lib_name:
                # db_lib_name comes from the client dict, usually populated by the connector
                module = importlib.import_module(db_lib_name)
                style = getattr(module, 'paramstyle', 'format')
                
                logger.debug(f"Detected paramstyle '{style}' for driver '{db_lib_name}'")
                
                if style == 'qmark':
                    self.placeholder = '?'
                elif style in ['numeric', 'named']:
                    # These require more complex handling (like Oracle's implementation)
                    # For generic usage, we stick to default %s and log a warning if likely to fail
                    logger.warning(f"Paramstyle '{style}' detected. GenericDb defaults to '%s', which may not work without variable conversion.")
                
        except Exception as e:
            logger.warning(f"Could not check paramstyle for {db_lib_name}: {e}")

    def get_cursor(self):
        """Get a standard cursor."""
        return self.conn.cursor()
    
    def get_stream_cursor(self):
        """Get a standard cursor (streaming optimizations depend on specific driver)."""
        return self.conn.cursor()

    def _build_upsert_clause(self, key_columns, update_columns, for_values=True):
        """
        Generic DB cannot know valid upsert syntax (ON CONFLICT vs MERGE vs DUPLICATE KEY).
        """
        raise NotImplementedError("GenericDb does not support automatic _build_upsert_clause. Please implement a specific subclass for this database type.")
