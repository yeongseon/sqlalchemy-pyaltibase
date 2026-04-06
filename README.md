# sqlalchemy-pyaltibase

[![PyPI version](https://img.shields.io/pypi/v/sqlalchemy-pyaltibase)](https://pypi.org/project/sqlalchemy-pyaltibase)
[![CI](https://github.com/yeongseon/sqlalchemy-pyaltibase/actions/workflows/ci.yml/badge.svg)](https://github.com/yeongseon/sqlalchemy-pyaltibase/actions/workflows/ci.yml)
[![license](https://img.shields.io/github/license/yeongseon/sqlalchemy-pyaltibase)](https://github.com/yeongseon/sqlalchemy-pyaltibase/blob/main/LICENSE)
[![docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://yeongseon.github.io/sqlalchemy-pyaltibase/)

SQLAlchemy 2.0 dialect for the Altibase database, backed by `pyaltibase`.

## Installation

```bash
pip install sqlalchemy-pyaltibase
```

With DB-API dependency:

```bash
pip install "sqlalchemy-pyaltibase[pyaltibase]"
```

## Quick Start

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

## Development

```bash
make lint
make test
```

## License

MIT
