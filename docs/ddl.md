# DDL Generation

DDL generation is implemented by `AltibaseDDLCompiler` and coordinated with dialect table events.

## CREATE TABLE generation

`get_column_specification()` composes each column definition.

- Non-autoincrement columns:
  - use `AltibaseTypeCompiler` output
  - include `DEFAULT ...` when server default exists
  - include `NOT NULL` when column is not nullable
- Autoincrement integer primary key columns (without explicit server default):
  - force type `INTEGER`
  - generate `DEFAULT <table>_<column>_SEQ.NEXTVAL`

Example generated shape:

```sql
CREATE TABLE users (
    id INTEGER DEFAULT users_id_SEQ.NEXTVAL NOT NULL,
    name VARCHAR(100) NOT NULL
)
```

## Comments

The compiler supports table and column comments:

- `visit_set_table_comment`
- `visit_drop_table_comment`
- `visit_set_column_comment`
- `post_create_table` (inline extra statement after CREATE TABLE)

Rendered forms:

```sql
COMMENT ON TABLE users IS 'table comment'
COMMENT ON COLUMN users.id IS 'identifier'
COMMENT ON TABLE users IS ''
```

## Implicit sequence lifecycle

Autoincrement sequence management is done by table event listeners in `dialect.py`:

- `before_create`: create implicit sequence
- `after_drop`: drop implicit sequence (errors ignored)

```mermaid
flowchart TD
    A[MetaData.create_all] --> B[before_create listener]
    B --> C[CREATE SEQUENCE table_col_SEQ]
    C --> D[AltibaseDDLCompiler CREATE TABLE]
    D --> E[column DEFAULT table_col_SEQ.NEXTVAL]
    F[MetaData.drop_all] --> G[after_drop listener]
    G --> H[DROP SEQUENCE table_col_SEQ]
```

!!! warning "Event-driven sequence creation"
    If DDL is generated and executed outside SQLAlchemy table events, you must create/drop sequences yourself.

## Example

```python
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine

engine = create_engine("altibase://sys:password@localhost:20300/mydb")
m = MetaData()

users = Table(
    "users",
    m,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(100), nullable=False, comment="user name"),
    comment="application users",
)

m.create_all(engine)
```
