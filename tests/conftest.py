import pytest

from apollo.integrations.db import tsql_base_db_proxy_client


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_odbc_driver_lookup: skip the autouse Driver-17-installed fixture for this test",
    )


@pytest.fixture(autouse=True)
def _assume_driver_17_installed(request):
    """Pin ODBC driver detection to Driver 17 so existing client tests' exact connection
    strings don't depend on which ODBC drivers are actually installed on the test host.
    """
    if request.node.get_closest_marker("real_odbc_driver_lookup"):
        yield
        return
    tsql_base_db_proxy_client._installed_odbc_drivers.cache_clear()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            tsql_base_db_proxy_client,
            "_installed_odbc_drivers",
            lambda: frozenset({"ODBC Driver 17 for SQL Server"}),
        )
        yield
    tsql_base_db_proxy_client._installed_odbc_drivers.cache_clear()
