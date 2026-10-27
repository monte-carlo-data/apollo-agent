"""
Retired runtime patches for databricks-sql-connector.

4.2.6–4.7.x rejected Arrow results with duplicate column names; 4.8.0 fixed it upstream
(databricks-sql-python#989), so the patch is gone. Other packages (data-collector) still
call ``install_connector_patches()``, so it stays as a no-op until they drop the call.
"""


def install_connector_patches() -> None:
    pass
