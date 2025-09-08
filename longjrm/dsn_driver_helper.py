# jrm/helpers.py
from .drivers import get_driver
from .config import DatabaseConfig, JrmConfigurationError


def as_dsn(db: DatabaseConfig) -> str:
    if db.dsn:
        return db.dsn
    drv = get_driver(db.type)
    if drv and drv.build_dsn:
        # build_dsn expects a mapping; dataclasses.asdict works too
        data = {
            "host": db.host, "port": db.port, "user": db.user,
            "password": db.password, "database": db.database, "query": db.options,
        }
        return drv.build_dsn(data)
    raise JrmConfigurationError(
        f"No DSN and no driver builder for type={db.type!r}. "
        f"Provide a DSN or register a driver."
    )
