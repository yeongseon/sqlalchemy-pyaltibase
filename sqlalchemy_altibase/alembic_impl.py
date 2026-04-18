from __future__ import annotations

try:
    from alembic.ddl.impl import DefaultImpl
except ImportError:
    raise ImportError(
        "Alembic is required for migration support. Install it with: pip install sqlalchemy-pyaltibase[alembic]"
    ) from None


class AltibaseImpl(DefaultImpl):
    __dialect__: str = "altibase"
    transactional_ddl: bool = False  # Altibase auto-commits DDL
