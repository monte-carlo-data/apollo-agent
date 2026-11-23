"""
``install_connector_patches()`` is a retired no-op: databricks-sql-connector 4.8.0 fixed
the duplicate-column-name bug it patched (databricks-sql-python#989). data-collector and
hermes-agent still call it, so delete it only after both drop the call.
"""


def install_connector_patches() -> None:
    pass
