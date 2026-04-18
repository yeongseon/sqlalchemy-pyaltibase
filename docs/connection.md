# Connection Guide

This page documents connection URL format, dialect defaults, and practical `create_engine()` configuration.

## URL format

```text
altibase://user:password@host:port/database
```

You can also pass query parameters recognized by `AltibaseDialect.create_connect_args()`:

- `driver` (default: `ALTIBASE_HDB_ODBC_64bit`)
- `dsn`
- `login_timeout` (integer)
- `nls_use`
- `long_data_compat` (`true` by default; false for `0|false|no`)

Example:

```text
altibase://sys:secret@db.example.com:20300/appdb?login_timeout=30&long_data_compat=true
```

## Dialect defaults

If omitted, these values are used:

| Field | Default |
|---|---|
| host | `localhost` |
| port | `20300` |
| user | `sys` |
| password | empty string |
| database | empty string |

## Basic `create_engine()` usage

```python
from sqlalchemy import create_engine

engine = create_engine(
    "altibase://sys:password@localhost:20300/mydb",
    pool_pre_ping=True,
)
```

## Connection pooling

SQLAlchemy pooling options work normally with this dialect.

```python
from sqlalchemy import create_engine

engine = create_engine(
    "altibase://sys:password@localhost:20300/mydb",
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800,
    pool_pre_ping=True,
)
```

`pool_pre_ping=True` is recommended for long-running services. The dialect implements `do_ping()` with `SELECT 1 FROM DUAL`.

## Connection lifecycle

```mermaid
sequenceDiagram
    participant App as Application
    participant SA as SQLAlchemy Engine
    participant D as AltibaseDialect
    participant P as pyaltibase
    participant DB as Altibase

    App->>SA: create_engine("altibase://...")
    SA->>D: create_connect_args(url)
    D->>P: connect(host, port, user, ...)
    P->>DB: open session
    SA->>D: on_connect()
    D->>DB: set autocommit=False / isolation level
```

## Async connection (aioodbc)

Install the async extra:

```bash
pip install "sqlalchemy-pyaltibase[aioodbc]"
```

Use `create_async_engine` with the `altibase+aioodbc://` URL scheme:

```python
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine(
    "altibase+aioodbc://sys:password@localhost:20300/mydb",
    echo=True,
)

async with engine.begin() as conn:
    result = await conn.execute(text("SELECT 1 FROM DUAL"))
    print(result.scalar())
```

The async dialect builds an ODBC DSN string internally using the same query parameters as the sync dialect (`driver`, `nls_use`, `long_data_compat`, `login_timeout`).

!!! tip "Isolation level at connect time"
    You can set a dialect-level isolation setting (for example `SERIALIZABLE`) and `on_connect()` applies it after each new DB-API connection is created.

!!! warning "Connection errors"
    The dialect marks many network/server failures as disconnects by matching known message patterns and common error codes (`-3113`, `-3114`, `-3135`, `-12157`, `-12170`, etc.).
