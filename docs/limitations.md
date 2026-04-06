# Limitations and Workarounds

This page summarizes important behavioral differences and known constraints in the current dialect implementation.

## 1-based OFFSET semantics

Altibase offset behavior is 1-based, while SQLAlchemy offset is 0-based. The compiler adjusts automatically using `(offset + 1)`.

```sql
-- SQLAlchemy offset(0)
... OFFSET (0 + 1)
```

Workaround guidance:

- Use standard SQLAlchemy `offset()` calls.
- Do not manually pre-adjust offsets.

## Autoincrement depends on implicit sequence events

Autoincrement integer PK handling requires SQLAlchemy table lifecycle events:

- Sequence created before table create
- Sequence dropped after table drop

If you bypass these events, create/drop sequence manually.

## RETURNING is not supported

The dialect sets:

- `insert_returning = False`
- `update_returning = False`
- `delete_returning = False`

Workaround guidance:

- Use follow-up `SELECT` queries.
- For inserted autoincrement IDs, rely on `lastrowid`/CURRVAL fallback behavior provided by execution context.

## Empty INSERT is not supported

`supports_empty_insert = False`.

Workaround guidance:

- Provide explicit column/value pairs.
- Avoid `INSERT INTO table DEFAULT VALUES` patterns.

## Distinct-from operator not supported

`supports_is_distinct_from = False`.

Workaround guidance:

- Use explicit null-safe predicate logic (`(a != b) OR (a IS NULL AND b IS NOT NULL) ...`) in query construction.

## Behavior comparison

```mermaid
flowchart LR
    subgraph Generic SQLAlchemy expectation
      A1[offset(0) -> OFFSET 0]
      A2[autoincrement often native identity]
      A3[RETURNING often available]
    end

    subgraph Altibase dialect behavior
      B1[offset(0) -> OFFSET (0 + 1)]
      B2[autoincrement -> implicit sequence + NEXTVAL]
      B3[RETURNING disabled]
    end

    A1 --> B1
    A2 --> B2
    A3 --> B3
```

!!! warning "Plan migrations with these differences"
    If you are moving from PostgreSQL/MySQL/Oracle dialect assumptions, validate pagination and insert identity workflows explicitly.
