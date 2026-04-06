# sqlalchemy-pyaltibase

SQLAlchemy 2.0 dialect for the Altibase database, backed by `pyaltibase`.

`sqlalchemy-pyaltibase` integrates Altibase with SQLAlchemy Core and ORM workflows, including Altibase-specific SQL compilation, type support, schema reflection, and event-driven autoincrement sequence management.

## Key features

- SQLAlchemy 2.0 dialect implementation for Altibase.
- Built on top of the `pyaltibase` DB-API driver.
- Supports SQLAlchemy Core and ORM usage patterns.
- Lightweight developer workflow with lint and test targets.
- Altibase-focused type support including `SERIAL`, `BIT`, `VARBIT`, `BYTE`, `VARBYTE`, `NIBBLE`, `GEOMETRY`.
- Reflection support for tables, views, columns, PK/FK/index metadata, comments, and schemas.
- Altibase-specific SQL behavior handling such as 1-based `OFFSET` normalization.

## Quick install

```bash
pip install sqlalchemy-pyaltibase
```

```bash
pip install "sqlalchemy-pyaltibase[pyaltibase]"
```

## Minimal example

```python
from sqlalchemy import create_engine, text
engine = create_engine("altibase://user:password@localhost:20300/mydb")
with engine.connect() as conn:
    value = conn.execute(text("SELECT 1 FROM DUAL")).scalar()
    print(value)
```

## Why this dialect exists

Altibase has behavior and SQL syntax details that differ from other engines. This package adapts SQLAlchemy behavior where needed:

- Autoincrement integer PK columns are sequence-backed and managed through table event listeners.
- Pagination offsets are compiled as 1-based expressions (`OFFSET (n + 1)`).
- Dialect-specific type compilation and reflection map Altibase system catalog metadata to SQLAlchemy constructs.

See [Dialect Features](dialect-features.md) and [Limitations](limitations.md) for details.

## Architecture

```mermaid
flowchart TD
    app["Application"] --> sa["SQLAlchemy Core/ORM"]
    sa --> dialect["AltibaseDialect"]
    dialect --> dbapi["pyaltibase"]
    dbapi --> server["Altibase Server"]
```

## Documentation map

- Getting started
  - [Quick Start](quickstart.md)
  - [Connection Guide](connection.md)
- User guide
  - [Dialect Features](dialect-features.md)
  - [Type Mapping](types.md)
  - [Schema Reflection](reflection.md)
  - [DDL Generation](ddl.md)
  - [Limitations](limitations.md)
- Reference
  - [API Reference](api-reference.md)
- Contributor docs
  - [Development Guide](development.md)

## Quick architecture of dialect internals

```mermaid
flowchart LR
    A[AltibaseDialect] --> B[AltibaseCompiler]
    A --> C[AltibaseDDLCompiler]
    A --> D[AltibaseTypeCompiler]
    A --> E[AltibaseIdentifierPreparer]
    A --> F[AltibaseExecutionContext]
```

## Project links

- GitHub: https://github.com/yeongseon/sqlalchemy-pyaltibase
- PyPI: https://pypi.org/project/sqlalchemy-pyaltibase/
