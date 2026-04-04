# pyright: reportUnsafeMultipleInheritance=false, reportMissingTypeArgument=false
from __future__ import annotations

import inspect

from sqlalchemy.sql import sqltypes


class _NumericType:
    def __init__(self, **kw):
        super().__init__(**kw)


class _FloatType(_NumericType, sqltypes.Float):
    def __init__(self, precision=None, **kw):
        super().__init__(precision=precision, **kw)


class _IntegerType(_NumericType, sqltypes.Integer):
    def __init__(self, **kw):
        super().__init__(**kw)


class _StringType(sqltypes.String):
    def __init__(self, national=False, values=None, **kw):
        self.national = national
        self.values = values
        super().__init__(**kw)

    def __repr__(self):
        try:
            sig = inspect.signature(self.__class__.__init__)
            attributes = [p.name for p in sig.parameters.values() if p.name != "self"]
        except (ValueError, TypeError):
            attributes = []

        params = {}
        for attr in attributes:
            val = getattr(self, attr, None)
            if val is not None and val is not False:
                params[attr] = val

        return "{}({})".format(
            self.__class__.__name__,
            ", ".join(f"{k}={v!r}" for k, v in params.items()),
        )


class NUMERIC(_NumericType, sqltypes.NUMERIC):
    __visit_name__ = "NUMERIC"

    def __init__(self, precision=None, scale=None, **kw):
        super().__init__(precision=precision, scale=scale, **kw)


class DECIMAL(_NumericType, sqltypes.DECIMAL):
    __visit_name__ = "DECIMAL"

    def __init__(self, precision=None, scale=None, **kw):
        super().__init__(precision=precision, scale=scale, **kw)


class FLOAT(_FloatType, sqltypes.FLOAT):
    __visit_name__ = "FLOAT"

    def __init__(self, precision=None, **kw):
        super().__init__(precision=precision, **kw)

    def bind_processor(self, dialect):
        return None


class REAL(_FloatType):
    __visit_name__ = "REAL"


class DOUBLE(_FloatType):
    __visit_name__ = "DOUBLE"


class SMALLINT(_IntegerType, sqltypes.SMALLINT):
    __visit_name__ = "SMALLINT"


class INTEGER(_IntegerType, sqltypes.INTEGER):
    __visit_name__ = "INTEGER"


class BIGINT(_IntegerType, sqltypes.BIGINT):
    __visit_name__ = "BIGINT"


class SERIAL(_IntegerType, sqltypes.INTEGER):
    __visit_name__ = "SERIAL"


class VARCHAR(_StringType, sqltypes.VARCHAR):
    __visit_name__ = "VARCHAR"

    def __init__(self, length=None, **kwargs):
        super().__init__(length=length, **kwargs)


class CHAR(_StringType, sqltypes.CHAR):
    __visit_name__ = "CHAR"

    def __init__(self, length=None, **kwargs):
        super().__init__(length=length, **kwargs)


class NCHAR(_StringType, sqltypes.NCHAR):
    __visit_name__ = "NCHAR"

    def __init__(self, length=None, **kwargs):
        kwargs["national"] = True
        super().__init__(length=length, **kwargs)


class NVARCHAR(_StringType, sqltypes.NVARCHAR):
    __visit_name__ = "NVARCHAR"

    def __init__(self, length=None, **kwargs):
        kwargs["national"] = True
        super().__init__(length=length, **kwargs)


class CLOB(sqltypes.Text):
    __visit_name__ = "CLOB"


class BLOB(sqltypes.LargeBinary):
    __visit_name__ = "BLOB"


class DATE(sqltypes.DATE):
    __visit_name__ = "DATE"


class BIT(sqltypes.TypeEngine):
    __visit_name__ = "BIT"

    def __init__(self, length=None, **kw):
        self.length = length
        super().__init__(**kw)


class VARBIT(sqltypes.TypeEngine):
    __visit_name__ = "VARBIT"

    def __init__(self, length=None, **kw):
        self.length = length
        super().__init__(**kw)


class BYTE(sqltypes.LargeBinary):
    __visit_name__ = "BYTE"

    def __init__(self, length=None, **kw):
        self.length = length
        super().__init__(length=length, **kw)


class VARBYTE(sqltypes.LargeBinary):
    __visit_name__ = "VARBYTE"

    def __init__(self, length=None, **kw):
        self.length = length
        super().__init__(length=length, **kw)


class NIBBLE(sqltypes.TypeEngine):
    __visit_name__ = "NIBBLE"

    def __init__(self, length=None, **kw):
        self.length = length
        super().__init__(**kw)


class GEOMETRY(sqltypes.TypeEngine):
    __visit_name__ = "GEOMETRY"
