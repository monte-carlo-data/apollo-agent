import logging
from typing import Any, Dict, Optional

from jinja2 import Template
from jinja2.sandbox import ImmutableSandboxedEnvironment

from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.custom.custom_integration_loader import (
    load_capabilities,
    load_integration_module,
    load_templates,
)

logger = logging.getLogger(__name__)

_ATTR_CONNECT_ARGS = "connect_args"

# Templates used by CustomProxyClient methods — only these are compiled at init
# time to avoid parsing all ~100 templates. The full set is still available as
# raw strings via get_templates().
_COMPILED_TEMPLATE_NAMES = frozenset(
    {
        "get_databases_query_template.j2",
        "get_schemas_query_template.j2",
        "get_tables_query_template.j2",
        "get_columns_query_template.j2",
        "get_query_logs_query_template.j2",
    }
)


class CustomProxyClient(BaseProxyClient):
    """
    Proxy client for custom database integrations loaded from
    /opt/custom-integrations/{name}/.

    The integration module is expected to define a BaseIntegration class
    with methods: create_connection, create_cursor, execute_query,
    fetch_all_results, close_connection.
    """

    def __init__(
        self,
        credentials: Optional[Dict],
        integration_dir: str,
        **kwargs: Any,
    ):
        module = load_integration_module(integration_dir)
        self._integration = module.BaseIntegration()

        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Custom-integration agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        self._integration.credentials = credentials[_ATTR_CONNECT_ARGS]
        self._integration.connection = self._integration.create_connection()
        self._integration.cursor = self._integration.create_cursor()

        jinja_env = ImmutableSandboxedEnvironment(
            trim_blocks=True,
            lstrip_blocks=True,
        )
        raw_templates = load_templates(integration_dir)
        self._templates = raw_templates
        self._compiled_templates: Dict[str, Template] = {
            name: jinja_env.from_string(content)
            for name, content in raw_templates.items()
            if name in _COMPILED_TEMPLATE_NAMES
        }
        self._capabilities = load_capabilities(integration_dir)

        logger.info("Opened custom integration connection from %s", integration_dir)

    @property
    def wrapped_client(self):
        return self._integration.connection

    def test_connection(self) -> Dict[str, bool]:
        """Connection is established in __init__; if we got here it succeeded."""
        return {"success": True}

    def fetch_databases(self) -> Dict[str, Any]:
        """Fetch the list of databases."""
        return self._render_and_execute("get_databases_query_template.j2")

    def fetch_schemas(self, database_name: str) -> Dict[str, Any]:
        """Fetch schemas for a given database."""
        return self._render_and_execute(
            "get_schemas_query_template.j2", database_name=database_name
        )

    def fetch_tables(
        self,
        database_name: str,
        schemas: str,
        offset: int,
        limit: int,
        tables: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch table metadata for given database, schemas, offset and limit."""
        return self._render_and_execute(
            "get_tables_query_template.j2",
            database_name=database_name,
            schemas=schemas,
            tables=tables,
            offset=offset,
            limit=limit,
        )

    def fetch_columns(
        self, database_name: str, schemas: str, tables: str
    ) -> Dict[str, Any]:
        """Fetch columns (schema) for a given list of tables"""
        return self._render_and_execute(
            "get_columns_query_template.j2",
            database_name=database_name,
            schemas=schemas,
            tables=tables,
        )

    def fetch_query_logs(
        self, start_time: str, end_time: str, limit: int, offset: int
    ) -> Dict[str, Any]:
        """Fetch query logs for a given time range with pagination."""
        return self._render_and_execute(
            "get_query_logs_query_template.j2",
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            offset=offset,
        )

    def execute_sql_query(self, query: str) -> Dict[str, Any]:
        """Execute arbitrary SQL and return results."""
        return self._execute_and_collect(query)

    def get_templates(self) -> Dict[str, str]:
        """Return all loaded .j2 templates as {filename: content}."""
        return self._templates

    def get_capabilities(self) -> Dict:
        """Return the capabilities.json contents."""
        return self._capabilities

    def _render_and_execute(
        self, template_name: str, **template_vars: Any
    ) -> Dict[str, Any]:
        """Render a named .j2 template and execute the resulting SQL."""
        template = self._compiled_templates.get(template_name)
        if template is None:
            raise ValueError(
                f"Unknown template: {template_name}. "
                f"Available: {list(self._compiled_templates.keys())}"
            )
        query = template.render(**template_vars)
        return self._execute_and_collect(query)

    def _execute_and_collect(self, query: str) -> Dict[str, Any]:
        """Execute a query and collect results with metadata."""
        self._integration.execute_query(query)
        all_results = self._integration.fetch_all_results()

        cursor = self._integration.cursor
        description = None
        if hasattr(cursor, "description") and cursor.description:
            description = [
                [col[0], col[1], col[2], col[3], col[4], col[5], col[6]]
                for col in cursor.description
            ]

        rowcount = None
        if hasattr(cursor, "rowcount"):
            rowcount = cursor.rowcount

        return {
            "all_results": all_results,
            "description": description,
            "rowcount": rowcount,
        }

    def close(self):
        try:
            self._integration.close_connection()
            logger.info("Closed custom integration connection")
        except Exception:
            logger.exception("Error closing custom integration connection")
