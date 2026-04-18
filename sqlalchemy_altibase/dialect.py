# pyright: reportIncompatibleMethodOverride=false, reportAssignmentType=false, reportMissingImports=false, reportCallIssue=false
from __future__ import annotations

import re

import sqlalchemy as sa
from sqlalchemy import event, types as sqltypes
from sqlalchemy import util
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import text

from sqlalchemy_altibase.base import AltibaseExecutionContext, AltibaseIdentifierPreparer
from sqlalchemy_altibase.compiler import (
    AltibaseCompiler,
    AltibaseDDLCompiler,
    AltibaseTypeCompiler,
    autoinc_seq_name,
)
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

_RE_TYPE_BASE = re.compile(r"^([A-Z ]+)")
_RE_LENGTH = re.compile(r"\((\d+)\)")
_RE_PRECISION_SCALE = re.compile(r"\((\d+)(?:\s*,\s*(\d+))?\)")
_RE_VERSION = re.compile(r"(\d+)\.(\d+)(?:\.(\d+))?(?:\.(\d+))?")


def _normalize_default(raw_default):
    if raw_default is None:
        return None

    value = str(raw_default).strip()
    if not value or value.upper() == "NULL":
        return None

    def _has_wrapping_parentheses(expr):
        if len(expr) < 2 or expr[0] != "(" or expr[-1] != ")":
            return False

        depth = 0
        for index, char in enumerate(expr):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0 and index != len(expr) - 1:
                    return False
            if depth < 0:
                return False
        return depth == 0

    while _has_wrapping_parentheses(value):
        value = value[1:-1].strip()

    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        value = value[1:-1]

    value = value.strip()
    if not value or value.upper() == "NULL":
        return None
    return value


colspecs = {
    sqltypes.Numeric: NUMERIC,
    sqltypes.Float: FLOAT,
    sqltypes.Integer: INTEGER,
}

ischema_names = {
    "NUMBER": NUMERIC,
    "NUMERIC": NUMERIC,
    "DECIMAL": DECIMAL,
    "FLOAT": FLOAT,
    "REAL": REAL,
    "DOUBLE": DOUBLE,
    "SMALLINT": SMALLINT,
    "INTEGER": INTEGER,
    "INT": INTEGER,
    "BIGINT": BIGINT,
    "SERIAL": SERIAL,
    "VARCHAR": VARCHAR,
    "CHAR": CHAR,
    "NCHAR": NCHAR,
    "NVARCHAR": NVARCHAR,
    "CLOB": CLOB,
    "BLOB": BLOB,
    "DATE": DATE,
    "TIMESTAMP": DATE,
    "BYTE": BYTE,
    "NIBBLE": NIBBLE,
    "BIT": BIT,
    "VARBIT": VARBIT,
    "VARBYTE": VARBYTE,
    "GEOMETRY": GEOMETRY,
}

_type_code_names = {
    1: "CHAR",
    12: "VARCHAR",
    2: "NUMERIC",
    4: "INTEGER",
    5: "SMALLINT",
    6: "FLOAT",
    7: "REAL",
    8: "DOUBLE",
    9: "DATE",
    20: "BLOB",
    40: "CLOB",
    -8: "NCHAR",
    -9: "NVARCHAR",
    -5: "BIGINT",
    30: "BYTE",
    31: "NIBBLE",
    32: "BIT",
    33: "VARBIT",
    34: "VARBYTE",
}


class AltibaseDialect(default.DefaultDialect):
    name = "altibase"
    driver = "pyaltibase"
    supports_statement_cache = True

    statement_compiler = AltibaseCompiler
    ddl_compiler = AltibaseDDLCompiler
    type_compiler = AltibaseTypeCompiler
    preparer = AltibaseIdentifierPreparer
    execution_ctx_cls = AltibaseExecutionContext

    default_paramstyle = "qmark"

    colspecs = colspecs
    ischema_names = ischema_names

    max_identifier_length = 128
    max_index_name_length = 128
    max_constraint_name_length = 128
    requires_name_normalize = True

    supports_native_enum = False
    supports_native_boolean = False
    supports_native_decimal = True

    supports_sequences = True
    sequences_optional = True

    supports_alter = True
    supports_comments = True
    inline_comments = False

    supports_default_values = False
    supports_default_metavalue = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_is_distinct_from = False

    insert_returning = False
    update_returning = False
    delete_returning = False

    postfetch_lastrowid = True

    _disconnect_messages = (
        "connection is closed",
        "lost connection",
        "server has gone away",
        "connection reset",
        "broken pipe",
        "connection timed out",
        "connection refused",
        "failed to connect",
        "not connected",
        "socket error",
    )

    def __init__(self, isolation_level=None, **kwargs):
        super().__init__(**kwargs)
        self.isolation_level = isolation_level

    @classmethod
    def import_dbapi(cls):
        import pyaltibase

        return pyaltibase

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()

    def create_connect_args(self, url):
        if url is None:
            raise ValueError("Unexpected database URL format")

        opts = url.translate_connect_args(username="user", database="database")
        query = dict(url.query)

        kwargs = {
            "host": opts.get("host", "localhost"),
            "port": opts.get("port", 20300),
            "database": opts.get("database", ""),
            "user": opts.get("user", "sys"),
            "password": opts.get("password", ""),
            "dsn": query.get("dsn"),
            "driver": query.get("driver", "ALTIBASE_HDB_ODBC_64bit"),
            "login_timeout": int(query["login_timeout"]) if "login_timeout" in query else None,
            "nls_use": query.get("nls_use"),
            "long_data_compat": query.get("long_data_compat", "true").lower()
            not in {"0", "false", "no"},
        }
        return (), kwargs

    def on_connect(self):
        isolation_level = self.isolation_level

        def connect(conn):
            conn.autocommit = False
            if isolation_level is not None:
                self.set_isolation_level(conn, isolation_level)

        return connect

    def get_isolation_level(self, dbapi_conn):
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute("SELECT ISOLATION_LEVEL FROM SYSTEM_.V$SESSION")
            row = cursor.fetchone()
            if not row or not row[0]:
                return "READ COMMITTED"
            return str(row[0]).upper()
        finally:
            cursor.close()

    def get_isolation_level_values(self):
        return ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]

    def set_isolation_level(self, dbapi_conn, level):
        normalized = str(level).upper()
        if normalized not in self.get_isolation_level_values():
            raise ValueError(f"Invalid isolation level: {level!r}")
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {normalized}")
        finally:
            cursor.close()

    def reset_isolation_level(self, dbapi_conn):
        self.set_isolation_level(dbapi_conn, "READ COMMITTED")

    def _get_server_version_info(self, connection):
        version = connection.execute(text("SELECT PRODUCT_VERSION FROM V$VERSION")).scalar()
        if version is None:
            return None
        match = _RE_VERSION.search(str(version))
        if not match:
            return None
        nums = [int(p) for p in match.groups() if p is not None]
        return tuple(nums)

    def _get_default_schema_name(self, connection):
        return connection.execute(text("SELECT USER_NAME() FROM DUAL")).scalar()

    def _effective_schema(self, connection, schema):
        if schema:
            return schema.upper()
        default_schema = self._get_default_schema_name(connection)
        if default_schema:
            return str(default_schema).upper()
        return None

    def _row_get(self, row, key, index, default=None):
        if isinstance(row, dict):
            return row.get(key, default)
        mapping = getattr(row, "_mapping", None)
        if mapping is not None and key in mapping:
            return mapping[key]
        try:
            return row[index]
        except Exception:
            return default

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        effective = self._effective_schema(connection, schema)
        result = connection.execute(
            text(
                "SELECT T.TABLE_NAME FROM SYSTEM_.SYS_TABLES_ T "
                "JOIN SYSTEM_.SYS_USERS_ U ON T.USER_ID = U.USER_ID "
                "WHERE U.USER_NAME = :schema AND T.TABLE_TYPE = 'T' ORDER BY T.TABLE_NAME"
            ),
            {"schema": effective},
        )
        return [row[0] for row in result]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        effective = self._effective_schema(connection, schema)
        result = connection.execute(
            text(
                "SELECT T.TABLE_NAME FROM SYSTEM_.SYS_TABLES_ T "
                "JOIN SYSTEM_.SYS_USERS_ U ON T.USER_ID = U.USER_ID "
                "WHERE U.USER_NAME = :schema AND T.TABLE_TYPE = 'V' ORDER BY T.TABLE_NAME"
            ),
            {"schema": effective},
        )
        return [row[0] for row in result]

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        effective = self._effective_schema(connection, schema)
        result = connection.execute(
            text(
                "SELECT VIEW_TEXT FROM SYSTEM_.SYS_VIEWS_ "
                "WHERE USER_NAME = :schema AND VIEW_NAME = :name"
            ),
            {"schema": effective, "name": view_name.upper()},
        )
        row = result.fetchone()
        return row[0] if row else None

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        effective = self._effective_schema(connection, schema)
        result = connection.execute(
            text(
                "SELECT C.COLUMN_NAME, C.DATA_TYPE, C.PRECISION, C.SCALE, "
                "C.IS_NULLABLE, C.DEFAULT_VAL, C.COLUMN_ORDER "
                "FROM SYSTEM_.SYS_COLUMNS_ C "
                "JOIN SYSTEM_.SYS_TABLES_ T ON C.TABLE_ID = T.TABLE_ID "
                "JOIN SYSTEM_.SYS_USERS_ U ON T.USER_ID = U.USER_ID "
                "WHERE U.USER_NAME = :schema AND T.TABLE_NAME = :table "
                "ORDER BY C.COLUMN_ORDER"
            ),
            {"schema": effective, "table": table_name.upper()},
        )

        columns = []
        for row in result:
            col_name = self._row_get(row, "COLUMN_NAME", 0)
            data_type = self._row_get(row, "DATA_TYPE", 1, "")
            data_precision = self._row_get(row, "PRECISION", 2)
            data_scale = self._row_get(row, "SCALE", 3)
            nullable_raw = self._row_get(row, "IS_NULLABLE", 4)
            data_default = self._row_get(row, "DEFAULT_VAL", 5)

            coltype = self._resolve_column_type(
                data_type=data_type,
                data_precision=data_precision,
                data_scale=data_scale,
            )

            nullable = str(nullable_raw).upper() in {"Y", "1", "TRUE"}

            columns.append(
                {
                    "name": col_name,
                    "type": coltype,
                    "nullable": nullable,
                    "default": _normalize_default(data_default),
                    "autoincrement": isinstance(coltype, SERIAL),
                }
            )
        return columns

    def _resolve_column_type(self, data_type, data_precision, data_scale):
        if isinstance(data_type, int):
            data_type = _type_code_names.get(data_type, str(data_type))
        data_type = str(data_type).upper().strip()

        length_match = _RE_LENGTH.search(data_type)
        precision_match = _RE_PRECISION_SCALE.search(data_type)
        base_match = _RE_TYPE_BASE.search(data_type)
        base_type = base_match.group(1).strip() if base_match else data_type

        type_cls = self.ischema_names.get(base_type)
        if type_cls is None:
            util.warn(f"Did not recognize type '{data_type}'")
            return sqltypes.NULLTYPE

        if base_type in {
            "VARCHAR",
            "NVARCHAR",
            "CHAR",
            "NCHAR",
            "BYTE",
            "VARBYTE",
            "BIT",
            "VARBIT",
            "NIBBLE",
        }:
            if length_match:
                return type_cls(length=int(length_match.group(1)))
            if data_precision is not None and int(data_precision) > 0:
                return type_cls(length=int(data_precision))
            return type_cls()

        if base_type in {"NUMERIC", "DECIMAL"}:
            if precision_match:
                precision = int(precision_match.group(1))
                scale = precision_match.group(2)
                if scale is not None:
                    return type_cls(precision=precision, scale=int(scale))
                return type_cls(precision=precision)
            if data_precision is not None and data_scale is not None:
                return type_cls(precision=int(data_precision), scale=int(data_scale))
            if data_precision is not None:
                return type_cls(precision=int(data_precision))
            return type_cls()

        if base_type == "FLOAT":
            if precision_match:
                return type_cls(precision=int(precision_match.group(1)))
            if data_precision is not None:
                return type_cls(precision=int(data_precision))
            return type_cls()

        return type_cls() if callable(type_cls) else type_cls

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        effective = self._effective_schema(connection, schema)
        result = connection.execute(
            text(
                "SELECT C.CONSTRAINT_NAME, COL.COLUMN_NAME "
                "FROM SYSTEM_.SYS_CONSTRAINTS_ C "
                "JOIN SYSTEM_.SYS_CONSTRAINT_COLUMNS_ CC ON C.CONSTRAINT_ID = CC.CONSTRAINT_ID "
                "JOIN SYSTEM_.SYS_COLUMNS_ COL ON CC.COLUMN_ID = COL.COLUMN_ID "
                "AND CC.TABLE_ID = COL.TABLE_ID "
                "JOIN SYSTEM_.SYS_TABLES_ T ON C.TABLE_ID = T.TABLE_ID "
                "JOIN SYSTEM_.SYS_USERS_ U ON T.USER_ID = U.USER_ID "
                "WHERE U.USER_NAME = :schema AND T.TABLE_NAME = :table "
                "AND C.CONSTRAINT_TYPE = 3 "
                "ORDER BY CC.CONSTRAINT_COL_ORDER"
            ),
            {"schema": effective, "table": table_name.upper()},
        )
        rows = list(result)
        if not rows:
            return {"name": None, "constrained_columns": []}
        return {
            "name": rows[0][0],
            "constrained_columns": [r[1] for r in rows],
        }

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        effective = self._effective_schema(connection, schema)
        fk_result = connection.execute(
            text(
                "SELECT C.CONSTRAINT_ID, C.CONSTRAINT_NAME, "
                "C.REFERENCED_TABLE_ID, C.REFERENCED_INDEX_ID "
                "FROM SYSTEM_.SYS_CONSTRAINTS_ C "
                "JOIN SYSTEM_.SYS_TABLES_ T ON C.TABLE_ID = T.TABLE_ID "
                "JOIN SYSTEM_.SYS_USERS_ U ON T.USER_ID = U.USER_ID "
                "WHERE U.USER_NAME = :schema AND T.TABLE_NAME = :table "
                "AND C.CONSTRAINT_TYPE = 0 "
                "ORDER BY C.CONSTRAINT_NAME"
            ),
            {"schema": effective, "table": table_name.upper()},
        )
        fk_rows = list(fk_result)
        if not fk_rows:
            return []

        fkeys = []
        for fk_row in fk_rows:
            constraint_id = fk_row[0]
            constraint_name = fk_row[1]
            ref_table_id = fk_row[2]
            ref_index_id = fk_row[3]

            col_result = connection.execute(
                text(
                    "SELECT COL.COLUMN_NAME "
                    "FROM SYSTEM_.SYS_CONSTRAINT_COLUMNS_ CC "
                    "JOIN SYSTEM_.SYS_COLUMNS_ COL ON CC.COLUMN_ID = COL.COLUMN_ID "
                    "AND CC.TABLE_ID = COL.TABLE_ID "
                    "WHERE CC.CONSTRAINT_ID = :cid "
                    "ORDER BY CC.CONSTRAINT_COL_ORDER"
                ),
                {"cid": constraint_id},
            )
            constrained_columns = [r[0] for r in col_result]

            ref_result = connection.execute(
                text(
                    "SELECT T.TABLE_NAME, U.USER_NAME "
                    "FROM SYSTEM_.SYS_TABLES_ T "
                    "JOIN SYSTEM_.SYS_USERS_ U ON T.USER_ID = U.USER_ID "
                    "WHERE T.TABLE_ID = :tid"
                ),
                {"tid": ref_table_id},
            )
            ref_row = ref_result.fetchone()
            ref_table = ref_row[0] if ref_row else None
            ref_schema = ref_row[1] if ref_row else None

            ref_col_result = connection.execute(
                text(
                    "SELECT COL.COLUMN_NAME "
                    "FROM SYSTEM_.SYS_INDEX_COLUMNS_ IC "
                    "JOIN SYSTEM_.SYS_COLUMNS_ COL ON IC.COLUMN_ID = COL.COLUMN_ID "
                    "AND IC.TABLE_ID = COL.TABLE_ID "
                    "WHERE IC.INDEX_ID = :iid "
                    "ORDER BY IC.INDEX_COL_ORDER"
                ),
                {"iid": ref_index_id},
            )
            referred_columns = [r[0] for r in ref_col_result]

            fkeys.append(
                {
                    "name": constraint_name,
                    "constrained_columns": constrained_columns,
                    "referred_schema": ref_schema,
                    "referred_table": ref_table,
                    "referred_columns": referred_columns,
                }
            )
        return fkeys

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        effective = self._effective_schema(connection, schema)
        result = connection.execute(
            text(
                "SELECT I.INDEX_NAME, I.IS_UNIQUE, COL.COLUMN_NAME "
                "FROM SYSTEM_.SYS_INDICES_ I "
                "JOIN SYSTEM_.SYS_INDEX_COLUMNS_ IC ON I.INDEX_ID = IC.INDEX_ID "
                "JOIN SYSTEM_.SYS_COLUMNS_ COL ON IC.COLUMN_ID = COL.COLUMN_ID "
                "AND IC.TABLE_ID = COL.TABLE_ID "
                "JOIN SYSTEM_.SYS_TABLES_ T ON I.TABLE_ID = T.TABLE_ID "
                "JOIN SYSTEM_.SYS_USERS_ U ON T.USER_ID = U.USER_ID "
                "WHERE U.USER_NAME = :schema AND T.TABLE_NAME = :table "
                "ORDER BY I.INDEX_NAME, IC.INDEX_COL_ORDER"
            ),
            {"schema": effective, "table": table_name.upper()},
        )
        idict = {}
        for row in result:
            name = row[0]
            item = idict.setdefault(
                name,
                {
                    "name": name,
                    "column_names": [],
                    "unique": str(row[1]).upper() in {"Y", "1", "TRUE", "UNIQUE"},
                },
            )
            item["column_names"].append(row[2])
        return list(idict.values())

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        effective = self._effective_schema(connection, schema)
        result = connection.execute(
            text(
                "SELECT COMMENTS FROM SYSTEM_.SYS_COMMENTS_ "
                "WHERE USER_NAME = :schema AND TABLE_NAME = :table AND COLUMN_NAME IS NULL"
            ),
            {"schema": effective, "table": table_name.upper()},
        )
        row = result.fetchone()
        return {"text": row[0] if row and row[0] else None}

    def get_schema_names(self, connection, **kw):
        result = connection.execute(
            text("SELECT USER_NAME FROM SYSTEM_.SYS_USERS_ ORDER BY USER_NAME")
        )
        return [row[0] for row in result]

    def has_table(self, connection, table_name, schema=None, **kw):
        effective = self._effective_schema(connection, schema)
        table_count = connection.execute(
            text(
                "SELECT COUNT(*) FROM SYSTEM_.SYS_TABLES_ T "
                "JOIN SYSTEM_.SYS_USERS_ U ON T.USER_ID = U.USER_ID "
                "WHERE U.USER_NAME = :schema AND T.TABLE_NAME = :name AND T.TABLE_TYPE IN ('T', 'V')"
            ),
            {"schema": effective, "name": table_name.upper()},
        ).scalar()
        return bool(table_count and table_count > 0)

    def has_index(self, connection, table_name, index_name, schema=None):
        effective = self._effective_schema(connection, schema)
        count = connection.execute(
            text(
                "SELECT COUNT(*) FROM SYSTEM_.SYS_INDICES_ I "
                "JOIN SYSTEM_.SYS_TABLES_ T ON I.TABLE_ID = T.TABLE_ID "
                "JOIN SYSTEM_.SYS_USERS_ U ON T.USER_ID = U.USER_ID "
                "WHERE U.USER_NAME = :schema AND T.TABLE_NAME = :table AND I.INDEX_NAME = :name"
            ),
            {"schema": effective, "table": table_name.upper(), "name": index_name.upper()},
        ).scalar()
        return bool(count and count > 0)

    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        effective = self._effective_schema(connection, schema)
        count = connection.execute(
            text(
                "SELECT COUNT(*) FROM SYSTEM_.SYS_SEQUENCES_ S "
                "JOIN SYSTEM_.SYS_USERS_ U ON S.USER_ID = U.USER_ID "
                "WHERE U.USER_NAME = :schema AND S.SEQUENCE_NAME = :name"
            ),
            {"schema": effective, "name": sequence_name.upper()},
        ).scalar()
        return bool(count and count > 0)

    def is_disconnect(self, e, connection, cursor):
        msg = str(e).lower()
        for pattern in self._disconnect_messages:
            if pattern in msg:
                return True
        code = self._extract_error_code(e)
        if code in {-3113, -3114, -3135, -12157, -12170, 3113, 3114}:
            return True
        return False

    @staticmethod
    def _extract_error_code(exception):
        if getattr(exception, "errno", None) is not None:
            return int(exception.errno)
        if not getattr(exception, "args", None):
            return None
        first = exception.args[0]
        if isinstance(first, int):
            return first
        if isinstance(first, str):
            match = re.match(r"\s*(-?\d+)", first)
            if match:
                return int(match.group(1))
        return None

    def do_ping(self, dbapi_connection):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT 1 FROM DUAL")
            return True
        finally:
            cursor.close()


def _get_autoinc_column(table):
    col = table._autoincrement_column
    if col is not None and col.server_default is None:
        return col
    return None


@event.listens_for(sa.Table, "before_create")
def _create_implicit_sequences(target, connection, **kw):
    if connection.dialect.name != "altibase":
        return
    col = _get_autoinc_column(target)
    if col is None:
        return
    seq = autoinc_seq_name(target.name, col.name)
    connection.execute(text(f"CREATE SEQUENCE {seq} START WITH 1 INCREMENT BY 1"))


@event.listens_for(sa.Table, "after_drop")
def _drop_implicit_sequences(target, connection, **kw):
    if connection.dialect.name != "altibase":
        return
    col = _get_autoinc_column(target)
    if col is None:
        return
    seq = autoinc_seq_name(target.name, col.name)
    try:
        connection.execute(text(f"DROP SEQUENCE {seq}"))
    except Exception:
        pass


dialect = AltibaseDialect
