# pyright: reportIncompatibleMethodOverride=false, reportArgumentType=false, reportUnusedParameter=false
from __future__ import annotations

from sqlalchemy.sql import compiler
from sqlalchemy.sql import sqltypes


def autoinc_seq_name(table_name, column_name):
    return f"{table_name}_{column_name}_SEQ"


class AltibaseCompiler(compiler.SQLCompiler):
    def visit_sysdate_func(self, fn, **kw):
        return "SYSDATE"

    def visit_dual_func(self, fn, **kw):
        return "DUAL"

    def default_from(self):
        return " FROM DUAL"

    def visit_cast(self, cast, **kw):
        type_ = self.process(cast.typeclause)
        if type_ is None:
            return self.process(cast.clause.self_group())
        return f"CAST({self.process(cast.clause)} AS {type_})"

    def render_literal_value(self, value, type_):
        value = super().render_literal_value(value, type_)
        return value.replace("\\", "\\\\")

    def get_select_precolumns(self, select, **kw):
        if bool(select._distinct):
            return "DISTINCT "
        return ""

    def visit_join(self, join, asfrom=False, **kwargs):
        return "".join(
            (
                self.process(join.left, asfrom=True, **kwargs),
                (join.isouter and " LEFT OUTER JOIN " or " INNER JOIN "),
                self.process(join.right, asfrom=True, **kwargs),
                " ON ",
                self.process(join.onclause, **kwargs),
            )
        )

    def for_update_clause(self, select, **kw):
        if select._for_update_arg is None:
            return ""
        text = " FOR UPDATE"
        if select._for_update_arg.of:
            text += " OF " + ", ".join(self.process(col, **kw) for col in select._for_update_arg.of)
        wait = getattr(select._for_update_arg, "wait", None)
        if wait is not None:
            text += f" WAIT {int(wait)}"
        elif select._for_update_arg.nowait:
            text += " NOWAIT"
        return text

    def limit_clause(self, select, **kw):
        limit_clause = select._limit_clause
        offset_clause = select._offset_clause
        if limit_clause is None and offset_clause is None:
            return ""

        # Altibase OFFSET is 1-based (OFFSET 1 = first row).
        # SQLAlchemy uses 0-based, so always emit (offset + 1).
        has_offset = offset_clause is not None

        if has_offset:
            offset_expr = "(%s + 1)" % self.process(offset_clause, **kw)
        else:
            offset_expr = None

        if limit_clause is None:
            if offset_expr is None:
                return ""
            return "\n LIMIT 9223372036854775807 OFFSET %s" % offset_expr
        if offset_expr is None:
            return "\n LIMIT %s" % self.process(limit_clause, **kw)
        return "\n LIMIT %s OFFSET %s" % (
            self.process(limit_clause, **kw),
            offset_expr,
        )


class AltibaseDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kw):
        colspec = [self.preparer.format_column(column)]

        is_autoinc = (
            column.table is not None
            and column is column.table._autoincrement_column
            and column.server_default is None
        )

        if is_autoinc:
            colspec.append("INTEGER")
            seq_name = autoinc_seq_name(column.table.name, column.name)
            colspec.append(f"DEFAULT {seq_name}.NEXTVAL")
        else:
            colspec.append(
                self.dialect.type_compiler_instance.process(column.type, type_expression=column)
            )
            default = self.get_column_default_string(column)
            if default is not None:
                colspec.append("DEFAULT " + default)

        if not column.nullable:
            colspec.append("NOT NULL")

        return " ".join(colspec)

    def post_create_table(self, table):
        if table.comment is None:
            return ""
        literal = self.sql_compiler.render_literal_value(table.comment, sqltypes.String())
        return f"\nCOMMENT ON TABLE {self.preparer.format_table(table)} IS {literal}"

    def visit_set_table_comment(self, create, **kw):
        return "COMMENT ON TABLE %s IS %s" % (
            self.preparer.format_table(create.element),
            self.sql_compiler.render_literal_value(create.element.comment, sqltypes.String()),
        )

    def visit_drop_table_comment(self, drop, **kw):
        return "COMMENT ON TABLE %s IS ''" % (self.preparer.format_table(drop.element),)

    def visit_set_column_comment(self, create, **kw):
        return "COMMENT ON COLUMN %s.%s IS %s" % (
            self.preparer.format_table(create.element.table),
            self.preparer.format_column(create.element),
            self.sql_compiler.render_literal_value(create.element.comment, sqltypes.String()),
        )


class AltibaseTypeCompiler(compiler.GenericTypeCompiler):
    def visit_BOOLEAN(self, type_, **kw):
        return "SMALLINT"

    def visit_NUMERIC(self, type_, **kw):
        if type_.precision is None:
            return "NUMERIC"
        if type_.scale is None:
            return f"NUMERIC({type_.precision})"
        return f"NUMERIC({type_.precision}, {type_.scale})"

    def visit_DECIMAL(self, type_, **kw):
        if type_.precision is None:
            return "DECIMAL"
        if type_.scale is None:
            return f"DECIMAL({type_.precision})"
        return f"DECIMAL({type_.precision}, {type_.scale})"

    def visit_FLOAT(self, type_, **kw):
        if type_.precision is None:
            return "FLOAT"
        return f"FLOAT({type_.precision})"

    def visit_REAL(self, type_, **kw):
        return "REAL"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_SMALLINT(self, type_, **kw):
        return "SMALLINT"

    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_SERIAL(self, type_, **kw):
        return "SERIAL"

    def visit_VARCHAR(self, type_, **kw):
        if type_.length:
            return f"VARCHAR({type_.length})"
        return "VARCHAR"

    def visit_CHAR(self, type_, **kw):
        if getattr(type_, "national", False):
            return self.visit_NCHAR(type_, **kw)
        if type_.length:
            return f"CHAR({type_.length})"
        return "CHAR"

    def visit_NCHAR(self, type_, **kw):
        if type_.length:
            return f"NCHAR({type_.length})"
        return "NCHAR"

    def visit_NVARCHAR(self, type_, **kw):
        if type_.length:
            return f"NVARCHAR({type_.length})"
        return "NVARCHAR"

    def visit_CLOB(self, type_, **kw):
        return "CLOB"

    def visit_BLOB(self, type_, **kw):
        return "BLOB"

    def visit_BIT(self, type_, **kw):
        if getattr(type_, "length", None):
            return f"BIT({type_.length})"
        return "BIT"

    def visit_VARBIT(self, type_, **kw):
        if getattr(type_, "length", None):
            return f"VARBIT({type_.length})"
        return "VARBIT"

    def visit_BYTE(self, type_, **kw):
        if getattr(type_, "length", None):
            return f"BYTE({type_.length})"
        return "BYTE"

    def visit_VARBYTE(self, type_, **kw):
        if getattr(type_, "length", None):
            return f"VARBYTE({type_.length})"
        return "VARBYTE"

    def visit_NIBBLE(self, type_, **kw):
        if getattr(type_, "length", None):
            return f"NIBBLE({type_.length})"
        return "NIBBLE"

    def visit_GEOMETRY(self, type_, **kw):
        return "GEOMETRY"

    def visit_DATE(self, type_, **kw):
        return "DATE"

    def visit_large_binary(self, type_, **kw):
        return "BLOB"

    def visit_text(self, type_, **kw):
        return "CLOB"

    def visit_datetime(self, type_, **kw):
        return "DATE"
