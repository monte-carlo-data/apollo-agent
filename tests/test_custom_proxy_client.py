import json
import os
import tempfile
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock

from apollo.integrations.custom.custom_integration_loader import (
    _discover_custom_integrations,
    load_integration_module,
    load_templates,
    load_capabilities,
)
from apollo.integrations.custom.custom_proxy_client import CustomProxyClient


def _create_mock_integration_dir(
    tmp_dir,
    name,
    connection_type,
    templates=None,
    capabilities=None,
):
    """Helper to create a mock integration directory structure."""
    integration_dir = os.path.join(tmp_dir, name)
    os.makedirs(integration_dir, exist_ok=True)

    # manifest.json
    manifest = {"connection_type": connection_type, "name": name}
    with open(os.path.join(integration_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f)

    # integration.py
    integration_code = """
class BaseIntegration:
    credentials = {}
    connection = None
    cursor = None

    def create_connection(self):
        return "mock_connection"

    def create_cursor(self):
        return "mock_cursor"

    def execute_query(self, query):
        pass

    def fetch_all_results(self):
        return []

    def close_connection(self):
        pass
"""
    with open(os.path.join(integration_dir, "integration.py"), "w") as f:
        f.write(integration_code)

    # templates/
    if templates:
        templates_dir = os.path.join(integration_dir, "templates")
        os.makedirs(templates_dir, exist_ok=True)
        for filename, content in templates.items():
            with open(os.path.join(templates_dir, filename), "w") as f:
                f.write(content)

    # capabilities.json
    if capabilities:
        with open(os.path.join(integration_dir, "capabilities.json"), "w") as f:
            json.dump(capabilities, f)

    return integration_dir


class TestCustomIntegrationDiscovery(TestCase):
    def test_discovery_reads_manifests(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_integration_dir(tmp_dir, "mydb", "custom-integration-abc1234")

            with patch(
                "apollo.integrations.custom.custom_integration_loader._CUSTOM_INTEGRATIONS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_integrations()

            self.assertIn("custom-integration-abc1234", registry)
            self.assertEqual(
                registry["custom-integration-abc1234"],
                os.path.join(tmp_dir, "mydb"),
            )

    def test_multiple_integrations(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_integration_dir(tmp_dir, "db1", "custom-integration-aaa")
            _create_mock_integration_dir(tmp_dir, "db2", "custom-integration-bbb")

            with patch(
                "apollo.integrations.custom.custom_integration_loader._CUSTOM_INTEGRATIONS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_integrations()

            self.assertEqual(len(registry), 2)
            self.assertIn("custom-integration-aaa", registry)
            self.assertIn("custom-integration-bbb", registry)

    def test_discovery_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "apollo.integrations.custom.custom_integration_loader._CUSTOM_INTEGRATIONS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_integrations()

            self.assertEqual(registry, {})

    def test_discovery_missing_directory(self):
        with patch(
            "apollo.integrations.custom.custom_integration_loader._CUSTOM_INTEGRATIONS_BASE_PATH",
            "/nonexistent/path",
        ):
            registry = _discover_custom_integrations()

        self.assertEqual(registry, {})

    def test_discovery_skips_missing_manifest(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create directory without manifest
            os.makedirs(os.path.join(tmp_dir, "bad_integration"))

            with patch(
                "apollo.integrations.custom.custom_integration_loader._CUSTOM_INTEGRATIONS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_integrations()

            self.assertEqual(registry, {})


class TestLoadTemplates(TestCase):
    def test_load_templates(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            templates_dir = os.path.join(tmp_dir, "templates")
            os.makedirs(templates_dir)
            with open(os.path.join(templates_dir, "get_tables.j2"), "w") as f:
                f.write("SELECT * FROM {{ schema }}.tables")
            with open(os.path.join(templates_dir, "get_columns.j2"), "w") as f:
                f.write("SELECT * FROM columns WHERE table = '{{ table }}'")

            result = load_templates(tmp_dir)

            self.assertEqual(len(result), 2)
            self.assertIn("get_tables.j2", result)
            self.assertIn("get_columns.j2", result)

    def test_load_templates_no_directory(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = load_templates(tmp_dir)
            self.assertEqual(result, {})


class TestLoadCapabilities(TestCase):
    def test_load_capabilities(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            caps = {"capabilities": {"supports_metadata": True}}
            with open(os.path.join(tmp_dir, "capabilities.json"), "w") as f:
                json.dump(caps, f)

            result = load_capabilities(tmp_dir)

            self.assertEqual(result, caps)

    def test_load_capabilities_no_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = load_capabilities(tmp_dir)
            self.assertEqual(result, {})


class TestCustomProxyClient(TestCase):
    def setUp(self):
        self._mock_module = MagicMock()
        self._mock_integration = MagicMock()
        self._mock_module.BaseIntegration.return_value = self._mock_integration
        self._mock_integration.create_connection.return_value = Mock()
        self._mock_integration.create_cursor.return_value = Mock()

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={"capabilities": {"supports_metadata": True}},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_test_connection(
        self, mock_load_module, mock_load_templates, mock_load_caps
    ):
        mock_load_module.return_value = self._mock_module

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            integration_dir="/opt/custom-integrations/mydb",
        )
        result = client.test_connection()

        self.assertEqual(result, {"success": True})
        self._mock_integration.create_connection.assert_called_once()
        self._mock_integration.create_cursor.assert_called_once()

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_execute_sql_query(
        self, mock_load_module, mock_load_templates, mock_load_caps
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_integration.fetch_all_results.return_value = [
            ["db1"],
            ["db2"],
        ]
        cursor = self._mock_integration.create_cursor.return_value
        cursor.description = [
            ("name", "varchar", None, None, None, None, None),
        ]
        cursor.rowcount = 2
        self._mock_integration.cursor = cursor

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            integration_dir="/opt/custom-integrations/mydb",
        )
        result = client.execute_sql_query("SELECT name FROM databases")

        self._mock_integration.execute_query.assert_called_once_with(
            "SELECT name FROM databases"
        )
        self.assertEqual(result["all_results"], [["db1"], ["db2"]])
        self.assertEqual(result["rowcount"], 2)

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={
            "get_databases_query_template.j2": "SELECT datname FROM pg_database",
        },
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_fetch_databases(
        self, mock_load_module, mock_load_templates, mock_load_caps
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_integration.fetch_all_results.return_value = [["db1"], ["db2"]]
        cursor = self._mock_integration.create_cursor.return_value
        cursor.description = [("datname", "varchar", None, None, None, None, None)]
        cursor.rowcount = 2
        self._mock_integration.cursor = cursor

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            integration_dir="/opt/custom-integrations/mydb",
        )
        result = client.fetch_databases()

        self._mock_integration.execute_query.assert_called_once_with(
            "SELECT datname FROM pg_database"
        )
        self.assertEqual(result["all_results"], [["db1"], ["db2"]])

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={
            "get_schemas_query_template.j2": "SELECT schema_name FROM schemas WHERE db = '{{ database_name }}'",
        },
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_fetch_schemas(self, mock_load_module, mock_load_templates, mock_load_caps):
        mock_load_module.return_value = self._mock_module
        self._mock_integration.fetch_all_results.return_value = [
            ["public"],
            ["information_schema"],
        ]
        cursor = self._mock_integration.create_cursor.return_value
        cursor.description = [
            ("schema_name", "varchar", None, None, None, None, None),
        ]
        cursor.rowcount = 2
        self._mock_integration.cursor = cursor

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            integration_dir="/opt/custom-integrations/mydb",
        )
        result = client.fetch_schemas(database_name="mydb")

        self._mock_integration.execute_query.assert_called_once_with(
            "SELECT schema_name FROM schemas WHERE db = 'mydb'"
        )
        self.assertEqual(result["all_results"], [["public"], ["information_schema"]])

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={
            "get_tables_query_template.j2": "SELECT * FROM tables WHERE db = '{{ database_name }}' LIMIT {{ limit }} OFFSET {{ offset }}",
        },
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_fetch_metadata(
        self, mock_load_module, mock_load_templates, mock_load_caps
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_integration.fetch_all_results.return_value = [["t1"], ["t2"]]
        cursor = self._mock_integration.create_cursor.return_value
        cursor.description = [("table_name", "varchar", None, None, None, None, None)]
        cursor.rowcount = 2
        self._mock_integration.cursor = cursor

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            integration_dir="/opt/custom-integrations/mydb",
        )
        result = client.fetch_metadata(
            database_name="mydb", schemas="public", offset=0, limit=100
        )

        self._mock_integration.execute_query.assert_called_once_with(
            "SELECT * FROM tables WHERE db = 'mydb' LIMIT 100 OFFSET 0"
        )
        self.assertEqual(result["all_results"], [["t1"], ["t2"]])

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={
            "get_query_logs_query_template.j2": "SELECT * FROM query_log WHERE ts BETWEEN '{{ start_time }}' AND '{{ end_time }}' LIMIT {{ limit }} OFFSET {{ offset }}",
        },
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_fetch_query_logs(
        self, mock_load_module, mock_load_templates, mock_load_caps
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_integration.fetch_all_results.return_value = [["q1"], ["q2"]]
        cursor = self._mock_integration.create_cursor.return_value
        cursor.description = [("query", "varchar", None, None, None, None, None)]
        cursor.rowcount = 2
        self._mock_integration.cursor = cursor

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            integration_dir="/opt/custom-integrations/mydb",
        )
        result = client.fetch_query_logs(
            start_time="2024-01-01", end_time="2024-01-02", limit=1000, offset=0
        )

        self._mock_integration.execute_query.assert_called_once_with(
            "SELECT * FROM query_log WHERE ts BETWEEN '2024-01-01' AND '2024-01-02' LIMIT 1000 OFFSET 0"
        )
        self.assertEqual(result["all_results"], [["q1"], ["q2"]])

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={"get_tables.j2": "SELECT * FROM tables"},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_get_templates(self, mock_load_module, mock_load_templates, mock_load_caps):
        mock_load_module.return_value = self._mock_module

        client = CustomProxyClient(
            credentials=None,
            integration_dir="/opt/custom-integrations/mydb",
        )
        result = client.get_templates()

        self.assertEqual(result, {"get_tables.j2": "SELECT * FROM tables"})

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={"capabilities": {"supports_metadata": True}},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_get_capabilities(
        self, mock_load_module, mock_load_templates, mock_load_caps
    ):
        mock_load_module.return_value = self._mock_module

        client = CustomProxyClient(
            credentials=None,
            integration_dir="/opt/custom-integrations/mydb",
        )
        result = client.get_capabilities()

        self.assertEqual(result, {"capabilities": {"supports_metadata": True}})

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_missing_template_raises(
        self, mock_load_module, mock_load_templates, mock_load_caps
    ):
        mock_load_module.return_value = self._mock_module

        client = CustomProxyClient(
            credentials=None,
            integration_dir="/opt/custom-integrations/mydb",
        )

        with self.assertRaises(ValueError) as ctx:
            client.fetch_databases()
        self.assertIn("Unknown template", str(ctx.exception))

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_close(self, mock_load_module, mock_load_templates, mock_load_caps):
        mock_load_module.return_value = self._mock_module

        client = CustomProxyClient(
            credentials=None,
            integration_dir="/opt/custom-integrations/mydb",
        )
        client.close()

        self._mock_integration.close_connection.assert_called_once()

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_capabilities",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_integration_module")
    def test_wrapped_client_returns_connection(
        self, mock_load_module, mock_load_templates, mock_load_caps
    ):
        mock_load_module.return_value = self._mock_module
        mock_conn = Mock()
        self._mock_integration.create_connection.return_value = mock_conn

        client = CustomProxyClient(
            credentials=None,
            integration_dir="/opt/custom-integrations/mydb",
        )

        self.assertEqual(client.wrapped_client, mock_conn)
