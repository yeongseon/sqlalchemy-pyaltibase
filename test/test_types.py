from __future__ import annotations

from unittest.mock import patch

from sqlalchemy.sql import sqltypes

from sqlalchemy_altibase.dialect import AltibaseDialect
from sqlalchemy_altibase.types import (
    BIGINT,
    BIT,
    BLOB,
    BYTE,
    CHAR,
    CLOB,
    DATE,
    DECIMAL,
    DOUBLE,
    FLOAT,
    GEOMETRY,
    INTEGER,
    NCHAR,
    NIBBLE,
    NUMERIC,
    NVARCHAR,
    REAL,
    SERIAL,
    SMALLINT,
    VARBIT,
    VARBYTE,
    VARCHAR,
)


class TestVisitNames:
    def test_all_visit_names(self):
        assert NUMERIC.__visit_name__ == "NUMERIC"
        assert DECIMAL.__visit_name__ == "DECIMAL"
        assert FLOAT.__visit_name__ == "FLOAT"
        assert REAL.__visit_name__ == "REAL"
        assert DOUBLE.__visit_name__ == "DOUBLE"
        assert SMALLINT.__visit_name__ == "SMALLINT"
        assert INTEGER.__visit_name__ == "INTEGER"
        assert BIGINT.__visit_name__ == "BIGINT"
        assert SERIAL.__visit_name__ == "SERIAL"
        assert VARCHAR.__visit_name__ == "VARCHAR"
        assert CHAR.__visit_name__ == "CHAR"
        assert NCHAR.__visit_name__ == "NCHAR"
        assert NVARCHAR.__visit_name__ == "NVARCHAR"
        assert CLOB.__visit_name__ == "CLOB"
        assert BLOB.__visit_name__ == "BLOB"
        assert DATE.__visit_name__ == "DATE"
        assert BIT.__visit_name__ == "BIT"
        assert VARBIT.__visit_name__ == "VARBIT"
        assert BYTE.__visit_name__ == "BYTE"
        assert VARBYTE.__visit_name__ == "VARBYTE"
        assert NIBBLE.__visit_name__ == "NIBBLE"
        assert GEOMETRY.__visit_name__ == "GEOMETRY"


class TestTypeInstantiation:
    def test_numeric_types(self):
        assert isinstance(NUMERIC(precision=10, scale=2), sqltypes.NUMERIC)
        assert isinstance(DECIMAL(precision=10, scale=2), sqltypes.DECIMAL)
        assert isinstance(FLOAT(precision=10), sqltypes.FLOAT)
        assert isinstance(REAL(), sqltypes.Float)
        assert isinstance(DOUBLE(), sqltypes.Float)
        assert isinstance(SMALLINT(), sqltypes.SMALLINT)
        assert isinstance(INTEGER(), sqltypes.INTEGER)
        assert isinstance(BIGINT(), sqltypes.BIGINT)
        assert isinstance(SERIAL(), sqltypes.INTEGER)

    def test_string_lob_and_other_types(self):
        assert isinstance(VARCHAR(100), sqltypes.VARCHAR)
        assert isinstance(CHAR(10), sqltypes.CHAR)
        assert isinstance(NCHAR(12), sqltypes.NCHAR)
        assert isinstance(NVARCHAR(15), sqltypes.NVARCHAR)
        assert isinstance(CLOB(), sqltypes.Text)
        assert isinstance(BLOB(), sqltypes.LargeBinary)
        assert isinstance(DATE(), sqltypes.DATE)
        assert isinstance(BIT(8), sqltypes.TypeEngine)
        assert isinstance(VARBIT(16), sqltypes.TypeEngine)
        assert isinstance(BYTE(16), sqltypes.LargeBinary)
        assert isinstance(VARBYTE(16), sqltypes.LargeBinary)
        assert isinstance(NIBBLE(8), sqltypes.TypeEngine)
        assert isinstance(GEOMETRY(), sqltypes.TypeEngine)


class TestRepr:
    def test_string_repr(self):
        r = repr(VARCHAR(length=255))
        assert "VARCHAR" in r
        assert "255" in r

    def test_repr_signature_error_path(self):
        with patch("sqlalchemy_altibase.types.inspect.signature", side_effect=ValueError("x")):
            assert repr(CHAR(length=8)) == "CHAR()"


class TestBindProcessor:
    def test_float_bind_processor_returns_none(self):
        assert FLOAT().bind_processor(None) is None


class TestDialectTypeResolution:
    def test_ischema_name_aliases_map_to_expected_types(self):
        assert AltibaseDialect.ischema_names["NUMBER"] is NUMERIC
        assert AltibaseDialect.ischema_names["TIMESTAMP"] is DATE

    def test_resolve_timestamp_variant(self):
        dialect = AltibaseDialect()

        resolved = dialect._resolve_column_type("TIMESTAMP", None, None)
        assert isinstance(resolved, DATE)
