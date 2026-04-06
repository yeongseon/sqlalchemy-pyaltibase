# sqlalchemy-pyaltibase

SQLAlchemy 2.0 dialect for the Altibase database, backed by `pyaltibase`.

## Key features

- SQLAlchemy 2.0 dialect implementation for Altibase.
- Built on top of the `pyaltibase` DB-API driver.
- Supports SQLAlchemy Core and ORM usage patterns.
- Lightweight developer workflow with lint and test targets.

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

## Architecture

```mermaid
flowchart TD
    app["Application"] --> sa["SQLAlchemy Core/ORM"]
    sa --> dialect["AltibaseDialect"]
    dialect --> dbapi["pyaltibase"]
    dbapi --> server["Altibase Server"]
```

## Project links

- GitHub: https://github.com/yeongseon/sqlalchemy-pyaltibase
- PyPI: https://pypi.org/project/sqlalchemy-pyaltibase/
