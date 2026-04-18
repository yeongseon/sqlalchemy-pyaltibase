from __future__ import annotations

from sqlalchemy_altibase.alembic_impl import AltibaseImpl


class TestAltibaseAlembicImpl:
    def test_import_and_attributes(self):
        assert AltibaseImpl.__dialect__ == "altibase"
        assert AltibaseImpl.transactional_ddl is False
