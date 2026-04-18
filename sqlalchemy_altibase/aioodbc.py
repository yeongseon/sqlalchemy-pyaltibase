# pyright: reportMissingImports=false
"""aioodbc async dialect for Altibase."""

from __future__ import annotations

from sqlalchemy.connectors.aioodbc import aiodbcConnector

from sqlalchemy_altibase.dialect import AltibaseDialect


class AltibaseDialectAsync_aioodbc(aiodbcConnector, AltibaseDialect):
    driver = "aioodbc"
    supports_statement_cache = True

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user", database="database")
        query = dict(url.query)

        host = opts.get("host", "localhost")
        port = opts.get("port", 20300)
        database = opts.get("database", "")
        user = opts.get("user", "sys")
        password = opts.get("password", "")
        driver_name = query.get("driver", "ALTIBASE_HDB_ODBC_64bit")
        nls_use = query.get("nls_use")
        long_data_compat = query.get("long_data_compat", "true").lower() not in {"0", "false", "no"}

        parts = [f"DRIVER={{{driver_name}}}"]
        # Case-sensitive keys matching pyaltibase/protocol.py exactly:
        parts.append(f"Server={host}")  # capital-S, lowercase-erver
        parts.append(f"PORT={port}")
        if database:
            parts.append(f"Database={database}")  # capital-D, lowercase-atabase
        if user:
            parts.append(f"UID={user}")
        if password:
            parts.append(f"PWD={password}")
        if nls_use:
            parts.append(f"NLS_USE={nls_use}")
        parts.append(f"LongDataCompat={'on' if long_data_compat else 'off'}")

        # Pass through extra query params as DSN attributes
        skip_keys = {"driver", "nls_use", "long_data_compat", "login_timeout", "dsn"}
        login_timeout = query.get("login_timeout")
        if login_timeout:
            parts.append(f"LoginTimeout={login_timeout}")
        for key, value in query.items():
            if key.lower() not in skip_keys:
                parts.append(f"{key}={value}")

        dsn = ";".join(parts)
        return ([], {"dsn": dsn})


dialect = AltibaseDialectAsync_aioodbc
