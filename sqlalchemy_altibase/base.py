# pyright: reportIncompatibleMethodOverride=false
from __future__ import annotations

import re

from sqlalchemy.engine import default
from sqlalchemy.sql import compiler


AUTOCOMMIT_REGEXP = re.compile(
    r"\s*(?:UPDATE|INSERT|CREATE|DELETE|DROP|ALTER|MERGE|TRUNCATE)", re.I | re.UNICODE
)

RESERVED_WORDS = frozenset(
    {
        "access",
        "add",
        "all",
        "alter",
        "and",
        "any",
        "as",
        "at",
        "between",
        "by",
        "cascade",
        "case",
        "check",
        "column",
        "connect",
        "constraint",
        "create",
        "cross",
        "current",
        "cursor",
        "database",
        "date",
        "decimal",
        "default",
        "delete",
        "desc",
        "distinct",
        "drop",
        "each",
        "else",
        "end",
        "escape",
        "exception",
        "exec",
        "exists",
        "float",
        "for",
        "foreign",
        "from",
        "full",
        "grant",
        "group",
        "having",
        "if",
        "in",
        "index",
        "inner",
        "insert",
        "integer",
        "intersect",
        "into",
        "is",
        "join",
        "key",
        "left",
        "level",
        "like",
        "limit",
        "lock",
        "merge",
        "minus",
        "modify",
        "not",
        "null",
        "number",
        "of",
        "on",
        "open",
        "or",
        "order",
        "outer",
        "primary",
        "prior",
        "privileges",
        "public",
        "raw",
        "references",
        "rename",
        "replace",
        "return",
        "revoke",
        "right",
        "row",
        "rowcount",
        "rownum",
        "rows",
        "select",
        "sequence",
        "session",
        "set",
        "some",
        "start",
        "step",
        "table",
        "then",
        "to",
        "trigger",
        "truncate",
        "union",
        "unique",
        "update",
        "user",
        "using",
        "values",
        "varchar",
        "view",
        "when",
        "where",
        "with",
    }
)


class AltibaseIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

    def __init__(
        self,
        dialect,
        initial_quote='"',
        final_quote=None,
        escape_quote='"',
        omit_schema=False,
    ):
        super().__init__(dialect, initial_quote, final_quote, escape_quote, omit_schema)

    def _quote_free_identifiers(self, *ids):
        return tuple(self.quote_identifier(i) for i in ids if i is not None)


class AltibaseExecutionContext(default.DefaultExecutionContext):
    def should_autocommit_text(self, statement):
        return AUTOCOMMIT_REGEXP.match(statement)

    def get_lastrowid(self):
        lastrowid = getattr(self.cursor, "lastrowid", None)
        if lastrowid is not None:
            return lastrowid

        if self.compiled is None or not hasattr(self.compiled, "statement"):
            return None

        statement = self.compiled.statement
        if statement is None or not hasattr(statement, "table"):
            return None

        table = statement.table
        col = table._autoincrement_column
        if col is None or col.server_default is not None:
            return None

        from sqlalchemy_altibase.compiler import autoinc_seq_name

        seq = autoinc_seq_name(table.name, col.name)
        try:
            self.cursor.execute(f"SELECT {seq}.CURRVAL FROM DUAL")
            row = self.cursor.fetchone()
            return row[0] if row else None
        except Exception:
            return None
