# Changelog

## 0.3.0

- Added `aioodbc` async dialect (`altibase+aioodbc://`) using SQLAlchemy's built-in `aiodbcConnector` (#12)

## 0.2.0

- Removed duplicate `get_lastrowid` implementation in `base.py`
- Added `_normalize_default` to strip vendor quoting from column defaults
- Added Alembic migration support (`alembic_impl.py` + entry point registration)
- Extended type mapping: `NUMBER`, `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP WITH LOCAL TIME ZONE`

## 0.1.0

- Initial SQLAlchemy 2.0 dialect for Altibase backed by `pyaltibase`
