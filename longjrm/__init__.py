# jrm/__init__.py
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

from .drivers import register_driver, get_driver  # re-export if you like

# Optional convenience: explicit plugin loader
_loaded_plugins = False

def load_plugins(
    group: str = "jrm.drivers",
    *,
    strict: bool = False,
    reload: bool = False,
    include: set[str] | None = None,   # ep names to allow; None = all
) -> int:
    """
    Discover and register third-party drivers via entry points.
    Returns the number of drivers loaded. Does nothing unless called.
    """
    global _loaded_plugins
    if _loaded_plugins and not reload:
        return 0

    try:
        from importlib.metadata import entry_points
        eps = entry_points().select(group=group)
    except Exception:
        eps = []

    from .drivers import register_driver
    loaded = 0
    for ep in eps:
        if include and ep.name not in include:
            continue
        try:
            drv = ep.load()  # expected to return a Driver instance
            register_driver(drv)
            loaded += 1
        except Exception as e:
            if strict:
                raise
            # stay silent or log at DEBUG via your namespaced logger
            # logging.getLogger(__name__).debug("plugin load failed: %s", e)
            continue

    _loaded_plugins = True
    return loaded
