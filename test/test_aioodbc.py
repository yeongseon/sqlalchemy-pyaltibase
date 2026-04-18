"""Offline tests for the aioodbc async dialect."""

from __future__ import annotations

import pytest

aioodbc = pytest.importorskip("aioodbc")  # noqa: E402

from sqlalchemy.engine import make_url  # noqa: E402

from sqlalchemy_altibase.aioodbc import AltibaseDialectAsync_aioodbc  # noqa: E402


class TestAltibaseDialectAsyncAioodbc:
    def setup_method(self):
        self.dialect = AltibaseDialectAsync_aioodbc()

    def test_is_async(self):
        assert self.dialect.is_async is True

    def test_driver(self):
        assert self.dialect.driver == "aioodbc"

    def test_supports_statement_cache(self):
        assert AltibaseDialectAsync_aioodbc.supports_statement_cache is True

    def test_create_connect_args_basic(self):
        url = make_url("altibase+aioodbc://user:pwd@myhost:20300/mydb")
        args, kwargs = self.dialect.create_connect_args(url)
        dsn = kwargs["dsn"]
        assert "DRIVER={ALTIBASE_HDB_ODBC_64bit}" in dsn
        assert "Server=myhost" in dsn
        assert "PORT=20300" in dsn
        assert "Database=mydb" in dsn
        assert "UID=user" in dsn
        assert "PWD=pwd" in dsn
        assert "LongDataCompat=on" in dsn

    def test_create_connect_args_defaults(self):
        url = make_url("altibase+aioodbc://@localhost/")
        args, kwargs = self.dialect.create_connect_args(url)
        dsn = kwargs["dsn"]
        assert "Server=localhost" in dsn
        assert "DRIVER={ALTIBASE_HDB_ODBC_64bit}" in dsn
        assert "LongDataCompat=on" in dsn

    def test_create_connect_args_no_database(self):
        url = make_url("altibase+aioodbc://user:pwd@host:20300/")
        args, kwargs = self.dialect.create_connect_args(url)
        dsn = kwargs["dsn"]
        assert "Database=" not in dsn

    def test_create_connect_args_nls_use(self):
        url = make_url("altibase+aioodbc://user:pwd@host:20300/db?nls_use=UTF8")
        args, kwargs = self.dialect.create_connect_args(url)
        dsn = kwargs["dsn"]
        assert "NLS_USE=UTF8" in dsn

    def test_create_connect_args_long_data_compat_off(self):
        url = make_url("altibase+aioodbc://u:p@h:20300/db?long_data_compat=false")
        args, kwargs = self.dialect.create_connect_args(url)
        dsn = kwargs["dsn"]
        assert "LongDataCompat=off" in dsn

    def test_create_connect_args_custom_driver(self):
        url = make_url("altibase+aioodbc://u:p@h:20300/db?driver=MyDriver")
        args, kwargs = self.dialect.create_connect_args(url)
        dsn = kwargs["dsn"]
        assert "DRIVER={MyDriver}" in dsn

    def test_entry_point(self):
        from importlib.metadata import entry_points

        eps = entry_points(group="sqlalchemy.dialects", name="altibase.aioodbc")
        assert len(list(eps)) >= 1

    def test_name_inherited(self):
        assert self.dialect.name == "altibase"
