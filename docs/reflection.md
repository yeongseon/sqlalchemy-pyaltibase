# Schema Reflection

`AltibaseDialect` implements reflection queries against Altibase system catalogs (`SYSTEM_.*`).

## Reflection flow

```mermaid
flowchart TD
    A[Inspector / MetaData.reflect] --> B[_effective_schema(schema)]
    B --> C[get_table_names / get_view_names]
    C --> D[get_columns]
    D --> E[get_pk_constraint]
    E --> F[get_foreign_keys]
    F --> G[get_indexes]
    G --> H[get_table_comment]
```

## Catalog and schema handling

- User-facing schema names are normalized to uppercase via `_effective_schema()`.
- If no schema is passed, dialect queries default schema via:
  - `SELECT USER_NAME() FROM DUAL`
- Reflection SQL joins `SYSTEM_.SYS_USERS_` with object catalogs (`SYS_TABLES_`, `SYS_COLUMNS_`, etc.).

## Table and view reflection

- `get_table_names()` returns tables where `TABLE_TYPE = 'T'`
- `get_view_names()` returns views where `TABLE_TYPE = 'V'`
- `get_view_definition()` reads `SYSTEM_.SYS_VIEWS_.VIEW_TEXT`

## Column reflection

`get_columns()` returns SQLAlchemy column dictionaries including:

- `name`
- `type` (resolved by `_resolve_column_type()`)
- `nullable`
- `default`
- `autoincrement` (true when reflected type is `SERIAL`)

Type resolution supports both textual types and integer type codes, with fallback to `NullType` and warning on unknown types.

## Primary key reflection

`get_pk_constraint()` reads `SYS_CONSTRAINTS_` (`CONSTRAINT_TYPE = 3`) and ordered columns from `SYS_CONSTRAINT_COLUMNS_`.

Return shape:

```python
{"name": "PK_TABLE", "constrained_columns": ["ID"]}
```

## Foreign key reflection

`get_foreign_keys()`:

1. Collects FK constraints (`CONSTRAINT_TYPE = 0`)
2. Collects constrained columns in order
3. Resolves referred table/schema by `REFERENCED_TABLE_ID`
4. Resolves referred columns from `REFERENCED_INDEX_ID`

Return shape:

```python
{
    "name": "FK_ORDERS_USERS",
    "constrained_columns": ["USER_ID"],
    "referred_schema": "APP",
    "referred_table": "USERS",
    "referred_columns": ["ID"],
}
```

## Index reflection

`get_indexes()` returns index dictionaries with:

- `name`
- `column_names`
- `unique`

Uniqueness is derived from `IS_UNIQUE` values such as `Y`, `1`, `TRUE`, `UNIQUE`.

## Unique/check/sequence reflection notes

The current dialect exposes:

- `has_sequence(sequence_name, schema=None)` for existence checks
- `has_index(...)` for index existence

It does **not** currently implement dedicated public reflection methods like:

- `get_unique_constraints()`
- `get_check_constraints()`
- `get_sequence_names()`

In practice, unique index metadata is available from `get_indexes()` (`unique=True`).

## Practical reflection example

```python
from sqlalchemy import create_engine, inspect

engine = create_engine("altibase://sys:password@localhost:20300/mydb")
insp = inspect(engine)

print(insp.get_schema_names())
print(insp.get_table_names(schema="APP"))
print(insp.get_columns("USERS", schema="APP"))
print(insp.get_pk_constraint("USERS", schema="APP"))
print(insp.get_foreign_keys("ORDERS", schema="APP"))
print(insp.get_indexes("USERS", schema="APP"))
```
