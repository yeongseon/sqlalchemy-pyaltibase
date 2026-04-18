from __future__ import annotations

import pytest

alembic = pytest.importorskip("alembic", reason="alembic not installed")

from sqlalchemy_altibase.alembic_impl import AltibaseImpl  # noqa: E402


class TestAltibaseAlembicImpl:
    def test_import_and_attributes(self):
        assert AltibaseImpl.__dialect__ == "altibase"
        assert AltibaseImpl.transactional_ddl is False
