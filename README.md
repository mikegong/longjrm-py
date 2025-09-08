Introducing JRM (JSON Relational Mapping) - a dynamic and high efficiency library designed to revolutionize the way databases are populated. Recognizing that the majority of databases don't naturally support object-oriented programming, JRM steps in as a game-changing solution, bridging the gap with finesse and efficiency.

At its core, JRM offers a hybrid adapter, meticulously crafted to facilitate a more seamless and intuitive connection between object-oriented programming (OOP) and various types of databases. This innovative approach circumvents the limitations often encountered with traditional Object-Relational Mapping (ORM) in non-object-oriented databases.

JRM's unique JSON Relational Mapping technique stands apart in the landscape of database interaction, offering a more adaptable and efficient way to manage and populate databases across a wide array of applications. Whether you're dealing with legacy systems or the latest database technologies, JRM provides a robust, flexible solution to integrate OOP paradigms into your database management strategy effectively.

This library is a JRM implementation for Python.

.env environment is only used when USE_DOTENV environment variable is set as true, and DOTENV_PATH is set for the file path. Suggest setting the two environment variables on OS level for development environment.

## Config

If you *do* want third-party drivers (via entry points), call a function to load them. To make this clearer, drop the `init(auto_plugins=True)` and expose a **single-purpose, explicit** API:

### Usage

**Built-ins only (default):**

```python
from jrm.config import JrmConfig
cfg = JrmConfig.from_files("app.json", "dbs.json")
db = cfg.require()
```

**With third-party drivers:**

```python
from jrm import load_plugins
from jrm.config import JrmConfig

load_plugins()                 # explicit, optional
cfg = JrmConfig.from_env()
db = cfg.require("analytics")
```

**Selective / strict loading:**

```python
load_plugins(strict=True, include={"cockroach", "clickhouse"})
```

---

### If you still want an `init()` helper

Make it a thin wrapper that defaults to **doing nothing** unless asked:

```python
def init(*, plugins: bool = False, **kwargs) -> None:
    if plugins:
        load_plugins(**kwargs)
```

This keeps semantics crystal-clear: nothing happens unless the app *explicitly* opts in.

---

**Bottom line:** keep import side-effects at zero, and make plugin discovery an explicit, opt-in call (`load_plugins()`), so the library works out-of-the-box with built-ins and remains deterministic unless the application chooses to load extras.

How to add a new database (without touching core)
# somewhere in your package (or a plugin)
from drivers import Driver, register_driver

clickhouse = Driver(
    name="clickhouse",
    default_port=9000,
    validate=lambda c: None,  # or require host/user/... as you prefer
    build_dsn=lambda c: (
        f"clickhouse://{_cred(c.get('user'), c.get('password'))}"
        f"{c['host']}{f':{_i(c.get('port'))}' if _i(c.get('port')) else ''}"
        f"/{c.get('database','')}{_qs(c.get('query'))}"
    ),
)
register_driver(clickhouse)

