# pyright: reportArgumentType=false
from __future__ import annotations

import sys
import types
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.engine import url

from sqlalchemy_altibase.base import AltibaseExecutionContext
from sqlalchemy_altibase.dialect import (
    AltibaseDialect,
    _create_implicit_sequences,
    _drop_implicit_sequences,
    _get_autoinc_column,
)


def _invoke_reflection(dialect, method_name, connection, *args, **kwargs):
    method = getattr(dialect, method_name)
    if hasattr(method, "__wrapped__"):
        return method.__wrapped__(dialect, connection, *args, **kwargs)
    return method(connection, *args, **kwargs)


class TestDialectBasics:
    def test_init_and_flags(self):
        d = AltibaseDialect()
        assert d.name == "altibase"
        assert d.driver == "pyaltibase"
        assert d.postfetch_lastrowid is True

    def test_import_dbapi_success(self):
        fake = types.ModuleType("pyaltibase")
        with patch.dict(sys.modules, {"pyaltibase": fake}):
            assert AltibaseDialect.import_dbapi() is fake
            assert AltibaseDialect.dbapi() is fake

    def test_import_dbapi_import_error(self):
        import builtins

        real_import = builtins.__import__

        def _fake(name, *args, **kwargs):
            if name == "pyaltibase":
                raise ImportError("driver missing")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_fake):
            with pytest.raises(ImportError, match="driver missing"):
                AltibaseDialect.import_dbapi()

    def test_create_connect_args(self):
        d = AltibaseDialect()
        parsed = url.make_url(
            "altibase://u:p@dbhost:20301/d1?driver=ALTIBASE_HDB_ODBC_64bit&login_timeout=30"
        )
        args, kwargs = d.create_connect_args(parsed)
        assert args == ()
        assert kwargs == {
            "host": "dbhost",
            "port": 20301,
            "database": "d1",
            "user": "u",
            "password": "p",
            "dsn": None,
            "driver": "ALTIBASE_HDB_ODBC_64bit",
            "login_timeout": 30,
            "nls_use": None,
            "long_data_compat": True,
        }

    def test_create_connect_args_defaults_and_none(self):
        d = AltibaseDialect()
        args, kwargs = d.create_connect_args(url.make_url("altibase://"))
        assert args == ()
        assert kwargs == {
            "host": "localhost",
            "port": 20300,
            "database": "",
            "user": "sys",
            "password": "",
            "dsn": None,
            "driver": "ALTIBASE_HDB_ODBC_64bit",
            "login_timeout": None,
            "nls_use": None,
            "long_data_compat": True,
        }
        with pytest.raises(ValueError, match="Unexpected database URL format"):
            d.create_connect_args(cast(Any, None))

    def test_on_connect(self):
        d = AltibaseDialect(isolation_level="SERIALIZABLE")
        d.set_isolation_level = MagicMock()
        conn = MagicMock()
        d.on_connect()(conn)
        assert conn.autocommit is False
        d.set_isolation_level.assert_called_once_with(conn, "SERIALIZABLE")


class TestIsolationLevelMethods:
    def test_get_set_reset_isolation_levels(self):
        d = AltibaseDialect()
        cursor = MagicMock()
        cursor.fetchone.return_value = ("read committed",)
        dbapi_conn = MagicMock()
        dbapi_conn.cursor.return_value = cursor

        assert d.get_isolation_level(dbapi_conn) == "READ COMMITTED"
        assert d.get_isolation_level_values() == [
            "READ COMMITTED",
            "REPEATABLE READ",
            "SERIALIZABLE",
        ]

        d.set_isolation_level(dbapi_conn, "SERIALIZABLE")
        cursor.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

        d.reset_isolation_level(dbapi_conn)
        cursor.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        assert cursor.close.call_count >= 3

    def test_set_isolation_level_invalid(self):
        d = AltibaseDialect()
        dbapi_conn = MagicMock()
        dbapi_conn.cursor.return_value = MagicMock()
        with pytest.raises(ValueError, match="Invalid isolation level"):
            d.set_isolation_level(dbapi_conn, "READ UNCOMMITTED")

    def test_get_isolation_level_default_when_empty(self):
        d = AltibaseDialect()
        cursor = MagicMock()
        cursor.fetchone.return_value = (None,)
        dbapi_conn = MagicMock()
        dbapi_conn.cursor.return_value = cursor
        assert d.get_isolation_level(dbapi_conn) == "READ COMMITTED"


class TestExistenceChecks:
    def test_has_table_and_sequence_and_index(self):
        d = AltibaseDialect()
        conn = MagicMock()
        conn.execute.return_value = MagicMock(scalar=lambda: 1)
        assert d.has_table(conn, "users", schema="APP") is True

        conn2 = MagicMock()
        conn2.execute.return_value = MagicMock(scalar=lambda: 0)
        assert d.has_table(conn2, "users", schema="APP") is False

        conn3 = MagicMock()
        conn3.execute.return_value = MagicMock(scalar=lambda: 1)
        assert d.has_sequence(conn3, "seq_users", schema="APP") is True
        assert d.has_index(conn3, "users", "ix_users_name", schema="APP") is True

        conn4 = MagicMock()
        conn4.execute.return_value = MagicMock(scalar=lambda: 0)
        assert d.has_sequence(conn4, "seq_users", schema="APP") is False
        assert d.has_index(conn4, "users", "ix_users_name", schema="APP") is False


class TestReflectionMethods:
    def test_get_columns(self):
        d = AltibaseDialect()
        conn = MagicMock()
        conn.info_cache = {}
        conn.dialect_options = {}
        conn.execute.return_value = [
            ("ID", 4, None, None, "N", None, 1),
            ("NAME", 12, 100, None, "Y", "'x'", 2),
            ("FLAG", "MYSTERY", None, None, "Y", None, 3),
        ]
        with patch("sqlalchemy.util.warn", create=True) as warn:
            cols = _invoke_reflection(d, "get_columns", conn, "USERS", schema="APP")
        assert cols[0]["name"] == "ID"
        assert cols[0]["nullable"] is False
        assert cols[1]["type"].length == 100
        assert cols[2]["type"].__class__.__name__ == "NullType"
        warn.assert_called_once()

    def test_row_get_and_effective_schema_and_type_resolution_helpers(self):
        d = AltibaseDialect()
        assert d._row_get({"A": 1}, "A", 0) == 1

        mapping_row = MagicMock()
        mapping_row._mapping = {"A": 2}
        assert d._row_get(mapping_row, "A", 0) == 2

        class BadRow:
            def __getitem__(self, idx):
                raise IndexError()

        assert d._row_get(BadRow(), "A", 0, default=9) == 9

        conn = MagicMock()
        conn.execute.return_value = MagicMock(scalar=lambda: "app")
        assert d._effective_schema(conn, "x") == "X"
        assert d._effective_schema(conn, None) == "APP"
        conn.execute.return_value = MagicMock(scalar=lambda: None)
        assert d._effective_schema(conn, None) is None

        assert d._resolve_column_type(2, 5, None).__class__.__name__ == "NUMERIC"
        assert d._resolve_column_type("DATE", None, None).__class__.__name__ == "DATE"

    def test_get_pk_constraint(self):
        d = AltibaseDialect()
        conn = MagicMock()
        conn.info_cache = {}
        conn.dialect_options = {}
        conn.execute.return_value = [("PK_USERS", "ID")]
        pk = _invoke_reflection(d, "get_pk_constraint", conn, "USERS", schema="APP")
        assert pk == {"name": "PK_USERS", "constrained_columns": ["ID"]}

    def test_get_foreign_keys(self):
        d = AltibaseDialect()
        conn = MagicMock()
        conn.info_cache = {}
        conn.dialect_options = {}

        fk_rows = [(101, "FK_ORDERS_USERS", 200, 300)]
        col_rows = [("USER_ID",), ("TENANT_ID",)]
        ref_table_row = MagicMock(fetchone=lambda: ("USERS", "APP"))
        ref_col_rows = [("ID",), ("TENANT_ID",)]

        conn.execute.side_effect = [
            fk_rows,
            col_rows,
            ref_table_row,
            ref_col_rows,
        ]
        fks = _invoke_reflection(d, "get_foreign_keys", conn, "ORDERS", schema="APP")
        assert fks[0]["name"] == "FK_ORDERS_USERS"
        assert fks[0]["constrained_columns"] == ["USER_ID", "TENANT_ID"]
        assert fks[0]["referred_columns"] == ["ID", "TENANT_ID"]
        assert fks[0]["referred_table"] == "USERS"
        assert fks[0]["referred_schema"] == "APP"

    def test_get_foreign_keys_empty(self):
        d = AltibaseDialect()
        conn = MagicMock()
        conn.info_cache = {}
        conn.dialect_options = {}
        conn.execute.return_value = []
        fks = _invoke_reflection(d, "get_foreign_keys", conn, "ORDERS", schema="APP")
        assert fks == []

    def test_get_table_names_and_views_and_definition(self):
        d = AltibaseDialect()
        conn = MagicMock()
        conn.info_cache = {}
        conn.dialect_options = {}
        conn.execute.side_effect = [
            [("USERS",), ("ORDERS",)],
            [("ACTIVE_USERS",)],
            MagicMock(fetchone=lambda: ("SELECT * FROM USERS",)),
        ]
        assert _invoke_reflection(d, "get_table_names", conn, schema="APP") == ["USERS", "ORDERS"]
        assert _invoke_reflection(d, "get_view_names", conn, schema="APP") == ["ACTIVE_USERS"]
        assert (
            _invoke_reflection(d, "get_view_definition", conn, "ACTIVE_USERS", schema="APP")
            == "SELECT * FROM USERS"
        )

    def test_get_view_definition_none_and_empty_pk(self):
        d = AltibaseDialect()
        conn = MagicMock()
        conn.info_cache = {}
        conn.dialect_options = {}
        conn.execute.return_value = MagicMock(fetchone=lambda: None)
        assert _invoke_reflection(d, "get_view_definition", conn, "V1", schema="APP") is None

        conn2 = MagicMock()
        conn2.info_cache = {}
        conn2.dialect_options = {}
        conn2.execute.return_value = []
        assert _invoke_reflection(d, "get_pk_constraint", conn2, "T1", schema="APP") == {
            "name": None,
            "constrained_columns": [],
        }

    def test_get_indexes_comments_schemas(self):
        d = AltibaseDialect()
        conn = MagicMock()
        conn.info_cache = {}
        conn.dialect_options = {}
        conn.execute.side_effect = [
            [
                ("IX_USERS_NAME", "N", "NAME"),
                ("UX_USERS_EMAIL", "Y", "EMAIL"),
            ],
            MagicMock(fetchone=lambda: ("User table",)),
            [("APP",), ("SYS",)],
        ]
        idx = _invoke_reflection(d, "get_indexes", conn, "USERS", schema="APP")
        comment = _invoke_reflection(d, "get_table_comment", conn, "USERS", schema="APP")
        schemas = d.get_schema_names(conn)
        assert idx[0]["name"] == "IX_USERS_NAME"
        assert any(item["name"] == "UX_USERS_EMAIL" for item in idx)
        assert comment == {"text": "User table"}
        assert schemas == ["APP", "SYS"]


class TestIsDisconnect:
    def test_disconnect_message_patterns(self):
        d = AltibaseDialect()
        assert d.is_disconnect(Exception("connection is closed"), None, None) is True
        assert d.is_disconnect(Exception("Socket Error happened"), None, None) is True

    def test_disconnect_by_error_code(self):
        d = AltibaseDialect()
        assert d.is_disconnect(Exception(-3113, "x"), None, None) is True
        assert d.is_disconnect(Exception("3114 lost"), None, None) is True

        e = Exception("db error")
        e.errno = -3114
        assert d.is_disconnect(e, None, None) is True

    def test_non_disconnect(self):
        d = AltibaseDialect()
        assert d.is_disconnect(Exception("syntax error"), None, None) is False

    def test_extract_error_code_cases(self):
        d = AltibaseDialect()
        assert d._extract_error_code(Exception()) is None
        assert d._extract_error_code(Exception(-1, "x")) == -1
        assert d._extract_error_code(Exception("  -3113 closed")) == -3113
        assert d._extract_error_code(Exception("oops")) is None


class TestDoPing:
    def test_do_ping_success_and_exception(self):
        d = AltibaseDialect()
        cursor = MagicMock()
        dbapi_conn = MagicMock()
        dbapi_conn.cursor.return_value = cursor
        assert d.do_ping(dbapi_conn) is True
        cursor.execute.assert_called_once_with("SELECT 1 FROM DUAL")
        cursor.close.assert_called_once()

        bad_cursor = MagicMock()
        bad_cursor.execute.side_effect = RuntimeError("boom")
        bad_conn = MagicMock()
        bad_conn.cursor.return_value = bad_cursor
        with pytest.raises(RuntimeError, match="boom"):
            d.do_ping(bad_conn)


class TestPostfetchLastRowId:
    def test_postfetch_flag_and_execution_context(self):
        assert AltibaseDialect.postfetch_lastrowid is True
        ctx = object.__new__(AltibaseExecutionContext)
        ctx.cursor = MagicMock(lastrowid=55)
        ctx.compiled = None
        assert ctx.get_lastrowid() == 55

    def test_lastrowid_currval_fallback(self):
        ctx = object.__new__(AltibaseExecutionContext)
        ctx.cursor = MagicMock(lastrowid=None)
        ctx.cursor.fetchone.return_value = (42,)

        m = MetaData()
        t = Table("t", m, Column("id", Integer, primary_key=True, autoincrement=True))
        compiled = MagicMock()
        compiled.statement = MagicMock()
        compiled.statement.table = t
        ctx.compiled = compiled

        result = ctx.get_lastrowid()
        assert result == 42
        ctx.cursor.execute.assert_called_once()

    def test_lastrowid_no_compiled(self):
        ctx = object.__new__(AltibaseExecutionContext)
        ctx.cursor = MagicMock(lastrowid=None)
        ctx.compiled = None
        assert ctx.get_lastrowid() is None

    def test_lastrowid_no_autoincrement(self):
        ctx = object.__new__(AltibaseExecutionContext)
        ctx.cursor = MagicMock(lastrowid=None)

        m = MetaData()
        t = Table("t", m, Column("id", Integer, primary_key=True, autoincrement=False))
        compiled = MagicMock()
        compiled.statement = MagicMock()
        compiled.statement.table = t
        ctx.compiled = compiled

        assert ctx.get_lastrowid() is None


class TestDisconnectMessages:
    def test_disconnect_message_tuple(self):
        msgs = AltibaseDialect._disconnect_messages
        assert isinstance(msgs, tuple)
        assert len(msgs) > 0
        assert all(m == m.lower() for m in msgs)


class TestVersionAndSchemaQueries:
    def test_version_and_default_schema_methods(self):
        d = AltibaseDialect()
        conn = MagicMock()
        conn.execute.return_value = MagicMock(scalar=lambda: "Altibase Server 7.3.0.1")
        assert d._get_server_version_info(conn) == (7, 3, 0, 1)
        conn.execute.return_value = MagicMock(scalar=lambda: "unknown")
        assert d._get_server_version_info(conn) is None
        conn.execute.return_value = MagicMock(scalar=lambda: None)
        assert d._get_server_version_info(conn) is None

        conn.execute.return_value = MagicMock(scalar=lambda: "APP")
        assert d._get_default_schema_name(conn) == "APP"


class TestResolveColumnTypeExtended:
    def test_float_without_scale(self):
        d = AltibaseDialect()
        col = d._resolve_column_type("FLOAT", 38, 0)
        assert col.__class__.__name__ == "FLOAT"
        assert col.precision == 38

    def test_float_no_precision(self):
        d = AltibaseDialect()
        col = d._resolve_column_type("FLOAT", None, None)
        assert col.__class__.__name__ == "FLOAT"

    def test_float_with_inline_precision(self):
        d = AltibaseDialect()
        col = d._resolve_column_type("FLOAT(24)", None, None)
        assert col.__class__.__name__ == "FLOAT"
        assert col.precision == 24

    def test_numeric_with_precision_and_scale(self):
        d = AltibaseDialect()
        col = d._resolve_column_type("NUMERIC", 10, 2)
        assert col.__class__.__name__ == "NUMERIC"
        assert col.precision == 10
        assert col.scale == 2

    def test_numeric_inline_precision_only(self):
        d = AltibaseDialect()
        col = d._resolve_column_type("NUMERIC(12)", None, None)
        assert col.__class__.__name__ == "NUMERIC"
        assert col.precision == 12

    def test_numeric_inline_precision_and_scale(self):
        d = AltibaseDialect()
        col = d._resolve_column_type("NUMERIC(10, 3)", None, None)
        assert col.__class__.__name__ == "NUMERIC"
        assert col.precision == 10
        assert col.scale == 3

    def test_numeric_no_args(self):
        d = AltibaseDialect()
        col = d._resolve_column_type("NUMERIC", None, None)
        assert col.__class__.__name__ == "NUMERIC"

    def test_numeric_precision_only_from_metadata(self):
        d = AltibaseDialect()
        col = d._resolve_column_type("NUMERIC", 8, None)
        assert col.__class__.__name__ == "NUMERIC"
        assert col.precision == 8

    def test_varchar_inline_length(self):
        d = AltibaseDialect()
        col = d._resolve_column_type("VARCHAR(200)", None, None)
        assert col.__class__.__name__ == "VARCHAR"
        assert col.length == 200

    def test_varchar_bare(self):
        d = AltibaseDialect()
        col = d._resolve_column_type("VARCHAR", None, None)
        assert col.__class__.__name__ == "VARCHAR"


class TestImplicitSequenceEvents:
    def test_get_autoinc_column_with_autoincrement(self):
        m = MetaData()
        t = Table("t", m, Column("id", Integer, primary_key=True, autoincrement=True))
        assert _get_autoinc_column(t) is t.c.id

    def test_get_autoinc_column_no_autoincrement(self):
        m = MetaData()
        t = Table("t", m, Column("id", Integer, primary_key=True, autoincrement=False))
        assert _get_autoinc_column(t) is None

    def test_get_autoinc_column_with_server_default(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("id", Integer, primary_key=True, autoincrement=True, server_default="1"),
        )
        assert _get_autoinc_column(t) is None

    def test_create_sequence_event_altibase(self):
        m = MetaData()
        t = Table(
            "users",
            m,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(50)),
        )
        conn = MagicMock()
        conn.dialect = MagicMock(name="altibase")
        conn.dialect.name = "altibase"
        _create_implicit_sequences(t, conn)
        conn.execute.assert_called_once()
        sql_arg = str(conn.execute.call_args[0][0])
        assert "CREATE SEQUENCE users_id_SEQ" in sql_arg

    def test_create_sequence_event_skips_non_altibase(self):
        m = MetaData()
        t = Table(
            "users",
            m,
            Column("id", Integer, primary_key=True, autoincrement=True),
        )
        conn = MagicMock()
        conn.dialect = MagicMock()
        conn.dialect.name = "sqlite"
        _create_implicit_sequences(t, conn)
        conn.execute.assert_not_called()

    def test_create_sequence_event_skips_no_autoincrement(self):
        m = MetaData()
        t = Table("users", m, Column("id", Integer, primary_key=True, autoincrement=False))
        conn = MagicMock()
        conn.dialect = MagicMock()
        conn.dialect.name = "altibase"
        _create_implicit_sequences(t, conn)
        conn.execute.assert_not_called()

    def test_drop_sequence_event_altibase(self):
        m = MetaData()
        t = Table(
            "users",
            m,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(50)),
        )
        conn = MagicMock()
        conn.dialect = MagicMock()
        conn.dialect.name = "altibase"
        _drop_implicit_sequences(t, conn)
        conn.execute.assert_called_once()
        sql_arg = str(conn.execute.call_args[0][0])
        assert "DROP SEQUENCE users_id_SEQ" in sql_arg

    def test_drop_sequence_event_skips_non_altibase(self):
        m = MetaData()
        t = Table(
            "users",
            m,
            Column("id", Integer, primary_key=True, autoincrement=True),
        )
        conn = MagicMock()
        conn.dialect = MagicMock()
        conn.dialect.name = "postgresql"
        _drop_implicit_sequences(t, conn)
        conn.execute.assert_not_called()

    def test_drop_sequence_event_ignores_error(self):
        m = MetaData()
        t = Table(
            "users",
            m,
            Column("id", Integer, primary_key=True, autoincrement=True),
        )
        conn = MagicMock()
        conn.dialect = MagicMock()
        conn.dialect.name = "altibase"
        conn.execute.side_effect = RuntimeError("sequence not found")
        _drop_implicit_sequences(t, conn)
