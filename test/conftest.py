import sys

if "--dburi" in sys.argv or any(a.startswith("--dburi=") for a in sys.argv):
    from sqlalchemy.dialects import registry

    registry.register("altibase", "sqlalchemy_altibase.dialect", "AltibaseDialect")
    registry.register("altibase.pyaltibase", "sqlalchemy_altibase.dialect", "AltibaseDialect")

    import pytest

    pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

    from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: E402, F401, F403
