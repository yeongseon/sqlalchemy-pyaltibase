# Quick Start

Get started with `sqlalchemy-pyaltibase` to use Altibase through SQLAlchemy Core or ORM.

## Requirements

!!! note "Before you begin"
    - Python 3.10+
    - SQLAlchemy 2.x
    - Altibase server reachable from your application host
    - `pyaltibase` driver installed (directly or via extras)

## Install

```bash
pip install sqlalchemy-pyaltibase
```

Optional extra to install the DB-API driver with the dialect package:

```bash
pip install "sqlalchemy-pyaltibase[pyaltibase]"
```

## Connection URL

Use the Altibase SQLAlchemy URL form:

```text
altibase://user:pass@host:port/db
```

Defaults used by the dialect when URL parts are omitted:

- `host`: `localhost`
- `port`: `20300`
- `user`: `sys`
- `database`: empty string (`""`)

## SQLAlchemy Core example

```python
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select

engine = create_engine("altibase://sys:password@localhost:20300/mydb")
metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(100), nullable=False),
)

metadata.create_all(engine)

with engine.begin() as conn:
    conn.execute(users.insert().values(name="alice"))

with engine.connect() as conn:
    rows = conn.execute(select(users.c.id, users.c.name)).all()
    print(rows)
```

## SQLAlchemy ORM example

```python
from sqlalchemy import Integer, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)


engine = create_engine("altibase://sys:password@localhost:20300/mydb")
Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add(User(name="alice"))
    session.commit()

with Session(engine) as session:
    users = session.scalars(select(User)).all()
    print(users)
```

## Runtime architecture

```mermaid
flowchart LR
    app[Application] --> sa[SQLAlchemy Core/ORM]
    sa --> dialect[sqlalchemy-pyaltibase\nAltibaseDialect]
    dialect --> dbapi[pyaltibase DB-API]
    dbapi --> db[Altibase]
```

!!! warning "Autoincrement behavior differs from some databases"
    For autoincrement integer primary keys, this dialect creates and drops implicit sequences via table event listeners. See [Dialect Features](dialect-features.md).
