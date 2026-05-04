import json
import os
import tempfile
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock

from apollo.integrations.custom.custom_connector_loader import (
    _discover_custom_connectors,
    load_connector_module,
    load_manifest,
    load_templates,
)
from apollo.agent.agent import Agent
from apollo.integrations.custom.custom_proxy_client import CustomProxyClient


def _create_mock_connector_dir(
    tmp_dir,
    name,
    connection_type,
    templates=None,
    capabilities=None,
    metrics=None,
):
    """Helper to create a mock connector directory structure."""
    connector_dir = os.path.join(tmp_dir, name)
    os.makedirs(connector_dir, exist_ok=True)

    # manifest.json — capabilities and metrics live inside the manifest
    manifest = {
        "connection_type": connection_type,
        "connection_name": name,
    }
    if capabilities is not None:
        manifest["capabilities"] = capabilities
    if metrics is not None:
        manifest["metrics"] = metrics
    with open(os.path.join(connector_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f)

    # connector.py
    connector_code = """
class BaseConnector:
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
    with open(os.path.join(connector_dir, "connector.py"), "w") as f:
        f.write(connector_code)

    # templates/
    if templates:
        templates_dir = os.path.join(connector_dir, "templates")
        os.makedirs(templates_dir, exist_ok=True)
        for filename, content in templates.items():
            with open(os.path.join(templates_dir, filename), "w") as f:
                f.write(content)

    return connector_dir


class TestCustomConnectorDiscovery(TestCase):
    def test_discovery_reads_manifests(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_connector_dir(tmp_dir, "mydb", "custom-connector-abc1234")

            with patch(
                "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_connectors()

            self.assertIn("custom-connector-abc1234", registry)
            self.assertEqual(
                registry["custom-connector-abc1234"],
                os.path.join(tmp_dir, "mydb"),
            )

    def test_multiple_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_connector_dir(tmp_dir, "db1", "custom-connector-aaa")
            _create_mock_connector_dir(tmp_dir, "db2", "custom-connector-bbb")

            with patch(
                "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_connectors()

            self.assertEqual(len(registry), 2)
            self.assertIn("custom-connector-aaa", registry)
            self.assertIn("custom-connector-bbb", registry)

    def test_discovery_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_connectors()

            self.assertEqual(registry, {})

    def test_discovery_missing_directory(self):
        with patch(
            "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
            "/nonexistent/path",
        ):
            registry = _discover_custom_connectors()

        self.assertEqual(registry, {})

    def test_discovery_skips_missing_manifest(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create directory without manifest
            os.makedirs(os.path.join(tmp_dir, "bad_integration"))

            with patch(
                "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_connectors()

            self.assertEqual(registry, {})


class TestLoadConnectorModule(TestCase):
    def test_successful_load(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_connector_dir(tmp_dir, "mydb", "custom-connector-abc")
            connector_dir = os.path.join(tmp_dir, "mydb")

            module = load_connector_module(connector_dir)

            self.assertTrue(hasattr(module, "BaseConnector"))
            connector = module.BaseConnector()
            self.assertEqual(connector.create_connection(), "mock_connection")

    def test_missing_connector_py(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Directory exists but no connector.py
            with self.assertRaises(FileNotFoundError) as ctx:
                load_connector_module(tmp_dir)
            self.assertIn("connector.py not found", str(ctx.exception))

    def test_syntax_error_in_connector(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            connector_path = os.path.join(tmp_dir, "connector.py")
            with open(connector_path, "w") as f:
                f.write("class BaseConnector:\n    def bad_method(self\n")

            with self.assertRaises(SyntaxError):
                load_connector_module(tmp_dir)


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


class TestLoadManifest(TestCase):
    def test_load_manifest(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest = {"connection_type": "custom-connector-abc", "name": "mydb"}
            with open(os.path.join(tmp_dir, "manifest.json"), "w") as f:
                json.dump(manifest, f)

            result = load_manifest(tmp_dir)

            self.assertEqual(result, manifest)

    def test_load_manifest_no_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = load_manifest(tmp_dir)
            self.assertEqual(result, {})


class TestCustomProxyClient(TestCase):
    def setUp(self):
        self._mock_module = MagicMock()
        self._mock_connector = MagicMock()
        self._mock_module.BaseConnector.return_value = self._mock_connector
        self._mock_connector.create_connection.return_value = Mock()
        self._mock_connector.create_cursor.return_value = Mock()

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={"capabilities": {"supports_metadata": True}},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_test_connection(
        self, mock_load_module, mock_load_templates, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            connector_dir="/opt/custom-connectors/mydb",
        )
        result = client.test_connection()

        self.assertEqual(result, {"success": True})
        self._mock_connector.create_connection.assert_called_once()
        self._mock_connector.create_cursor.assert_called_once()

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_execute_sql_query(
        self, mock_load_module, mock_load_templates, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.fetch_all_results.return_value = [
            ["db1"],
            ["db2"],
        ]
        cursor = self._mock_connector.create_cursor.return_value
        cursor.description = [
            ("name", "varchar", None, None, None, None, None),
        ]
        cursor.rowcount = 2
        self._mock_connector.cursor = cursor

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            connector_dir="/opt/custom-connectors/mydb",
        )
        result = client.execute_sql_query("SELECT name FROM databases")

        self._mock_connector.execute_query.assert_called_once_with(
            "SELECT name FROM databases"
        )
        self.assertEqual(result["all_results"], [["db1"], ["db2"]])
        self.assertEqual(result["rowcount"], 2)

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={
            "get_databases_query_template.j2": "SELECT datname FROM pg_database",
        },
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_fetch_databases(
        self, mock_load_module, mock_load_templates, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.fetch_all_results.return_value = [["db1"], ["db2"]]
        cursor = self._mock_connector.create_cursor.return_value
        cursor.description = [("datname", "varchar", None, None, None, None, None)]
        cursor.rowcount = 2
        self._mock_connector.cursor = cursor

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            connector_dir="/opt/custom-connectors/mydb",
        )
        result = client.fetch_databases()

        self._mock_connector.execute_query.assert_called_once_with(
            "SELECT datname FROM pg_database"
        )
        self.assertEqual(result["all_results"], [["db1"], ["db2"]])

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={
            "get_schemas_query_template.j2": "SELECT schema_name FROM schemas WHERE db = '{{ database_name }}'",
        },
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_fetch_schemas(
        self, mock_load_module, mock_load_templates, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.fetch_all_results.return_value = [
            ["public"],
            ["information_schema"],
        ]
        cursor = self._mock_connector.create_cursor.return_value
        cursor.description = [
            ("schema_name", "varchar", None, None, None, None, None),
        ]
        cursor.rowcount = 2
        self._mock_connector.cursor = cursor

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            connector_dir="/opt/custom-connectors/mydb",
        )
        result = client.fetch_schemas(database_name="mydb")

        self._mock_connector.execute_query.assert_called_once_with(
            "SELECT schema_name FROM schemas WHERE db = 'mydb'"
        )
        self.assertEqual(result["all_results"], [["public"], ["information_schema"]])

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={
            "get_tables_query_template.j2": "SELECT * FROM tables WHERE db = '{{ database_name }}' LIMIT {{ limit }} OFFSET {{ offset }}",
        },
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_fetch_tables(
        self, mock_load_module, mock_load_templates, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.fetch_all_results.return_value = [["t1"], ["t2"]]
        cursor = self._mock_connector.create_cursor.return_value
        cursor.description = [("table_name", "varchar", None, None, None, None, None)]
        cursor.rowcount = 2
        self._mock_connector.cursor = cursor

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            connector_dir="/opt/custom-connectors/mydb",
        )
        result = client.fetch_tables(
            database_name="mydb", schemas="public", offset=0, limit=100
        )

        self._mock_connector.execute_query.assert_called_once_with(
            "SELECT * FROM tables WHERE db = 'mydb' LIMIT 100 OFFSET 0"
        )
        self.assertEqual(result["all_results"], [["t1"], ["t2"]])

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={
            "get_query_logs_query_template.j2": "SELECT * FROM query_log WHERE ts BETWEEN '{{ start_time }}' AND '{{ end_time }}' LIMIT {{ limit }} OFFSET {{ offset }}",
        },
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_fetch_query_logs(
        self, mock_load_module, mock_load_templates, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.fetch_all_results.return_value = [["q1"], ["q2"]]
        cursor = self._mock_connector.create_cursor.return_value
        cursor.description = [("query", "varchar", None, None, None, None, None)]
        cursor.rowcount = 2
        self._mock_connector.cursor = cursor

        client = CustomProxyClient(
            credentials={"connect_args": {"host": "localhost"}},
            connector_dir="/opt/custom-connectors/mydb",
        )
        result = client.fetch_query_logs(
            start_time="2024-01-01", end_time="2024-01-02", limit=1000, offset=0
        )

        self._mock_connector.execute_query.assert_called_once_with(
            "SELECT * FROM query_log WHERE ts BETWEEN '2024-01-01' AND '2024-01-02' LIMIT 1000 OFFSET 0"
        )
        self.assertEqual(result["all_results"], [["q1"], ["q2"]])

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={"get_tables.j2": "SELECT * FROM tables"},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_get_templates(
        self, mock_load_module, mock_load_templates, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module

        client = CustomProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-connectors/mydb",
        )
        result = client.get_templates()

        self.assertEqual(result, {"get_tables.j2": "SELECT * FROM tables"})

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={"capabilities": {"supports_metadata": True}},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_get_capabilities(
        self, mock_load_module, mock_load_templates, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module

        client = CustomProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-connectors/mydb",
        )
        result = client.get_capabilities()

        self.assertEqual(result, {"supports_metadata": True})

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_missing_template_raises(
        self, mock_load_module, mock_load_templates, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module

        client = CustomProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-connectors/mydb",
        )

        with self.assertRaises(ValueError) as ctx:
            client.fetch_databases()
        self.assertIn("Unknown template", str(ctx.exception))

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_close(self, mock_load_module, mock_load_templates, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        client = CustomProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-connectors/mydb",
        )
        client.close()

        self._mock_connector.close_connection.assert_called_once()

    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.load_templates",
        return_value={},
    )
    @patch("apollo.integrations.custom.custom_proxy_client.load_connector_module")
    def test_wrapped_client_returns_connection(
        self, mock_load_module, mock_load_templates, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        mock_conn = Mock()
        self._mock_connector.create_connection.return_value = mock_conn

        client = CustomProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-connectors/mydb",
        )

        self.assertEqual(client.wrapped_client, mock_conn)


class TestGetConnectionManifests(TestCase):
    def test_returns_all_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_connector_dir(
                tmp_dir,
                "db1",
                "custom-connector-aaa",
                templates={"get_tables.j2": "SELECT * FROM tables"},
                capabilities={"supports_metadata": True},
            )
            _create_mock_connector_dir(
                tmp_dir,
                "db2",
                "custom-connector-bbb",
                templates={"get_schemas.j2": "SELECT * FROM schemas"},
                capabilities={"supports_query_logs": False},
            )

            with patch(
                "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom.custom_connector_loader._custom_connector_registry",
                None,
            ):
                result = CustomProxyClient.get_connection_manifests()

            self.assertEqual(len(result), 2)

            self.assertIn("custom-connector-aaa", result)
            aaa = result["custom-connector-aaa"]
            self.assertEqual(aaa["manifest"]["connection_type"], "custom-connector-aaa")
            self.assertEqual(aaa["manifest"]["connection_name"], "db1")
            self.assertEqual(
                aaa["manifest"]["capabilities"], {"supports_metadata": True}
            )
            self.assertNotIn("capabilities", aaa)
            self.assertEqual(
                aaa["templates"], {"get_tables.j2": "SELECT * FROM tables"}
            )

            self.assertIn("custom-connector-bbb", result)
            bbb = result["custom-connector-bbb"]
            self.assertEqual(bbb["manifest"]["connection_type"], "custom-connector-bbb")
            self.assertEqual(
                bbb["templates"], {"get_schemas.j2": "SELECT * FROM schemas"}
            )

    def test_returns_empty_when_no_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom.custom_connector_loader._custom_connector_registry",
                None,
            ):
                result = CustomProxyClient.get_connection_manifests()

            self.assertEqual(result, {})

    def test_handles_missing_templates(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_connector_dir(
                tmp_dir,
                "db1",
                "custom-connector-aaa",
            )

            with patch(
                "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom.custom_connector_loader._custom_connector_registry",
                None,
            ):
                result = CustomProxyClient.get_connection_manifests()

            aaa = result["custom-connector-aaa"]
            self.assertEqual(aaa["manifest"]["connection_type"], "custom-connector-aaa")
            self.assertNotIn("capabilities", aaa)
            self.assertEqual(aaa["templates"], {})


class TestAgentGetConnectionManifests(TestCase):
    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        return_value={},
    )
    def test_returns_ok_response(self, _mock_registry):
        agent = Agent(None)
        response = agent.get_connection_manifests(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_result__", response.result)
        self.assertEqual(response.result["__mcd_result__"], {})
        self.assertEqual(response.trace_id, "test-trace")

    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        side_effect=RuntimeError("boom"),
    )
    def test_returns_error_on_failure(self, _mock_registry):
        agent = Agent(None)
        response = agent.get_connection_manifests(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_error__", response.result)


class TestGetCustomConnectorTypes(TestCase):
    def test_returns_all_types(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_connector_dir(tmp_dir, "Acme CRM", "acme-crm")
            _create_mock_connector_dir(tmp_dir, "Internal API", "internal-api")

            with patch(
                "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom.custom_connector_loader._custom_connector_registry",
                None,
            ):
                result = CustomProxyClient.get_custom_connector_types()

            self.assertEqual(len(result), 2)
            types_by_id = {entry["type"]: entry["name"] for entry in result}
            self.assertEqual(types_by_id["acme-crm"], "Acme CRM")
            self.assertEqual(types_by_id["internal-api"], "Internal API")

    def test_returns_empty_list_when_no_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom.custom_connector_loader._custom_connector_registry",
                None,
            ):
                result = CustomProxyClient.get_custom_connector_types()

            self.assertEqual(result, [])

    def test_falls_back_to_type_when_no_connection_name(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a connector dir with a manifest that has no connection_name
            connector_dir = os.path.join(tmp_dir, "noname")
            os.makedirs(connector_dir)
            with open(os.path.join(connector_dir, "manifest.json"), "w") as f:
                json.dump({"connection_type": "no-name-connector"}, f)

            with patch(
                "apollo.integrations.custom.custom_connector_loader._CUSTOM_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom.custom_connector_loader._custom_connector_registry",
                None,
            ):
                result = CustomProxyClient.get_custom_connector_types()

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["type"], "no-name-connector")
            self.assertEqual(result[0]["name"], "no-name-connector")


class TestAgentGetSupportedConnectorTypes(TestCase):
    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        return_value={},
    )
    def test_returns_native_and_custom(self, _mock_registry):
        agent = Agent(None)
        response = agent.get_supported_connector_types(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_result__", response.result)
        result = response.result["__mcd_result__"]
        self.assertIn("connector_types", result)
        self.assertIn("native", result["connector_types"])
        self.assertIn("custom", result["connector_types"])
        # native should contain known built-in types
        native = result["connector_types"]["native"]
        self.assertIn("bigquery", native)
        self.assertIn("snowflake", native)
        self.assertIn("postgres", native)
        # native list should be sorted
        self.assertEqual(native, sorted(native))
        # custom is empty because registry is mocked empty
        self.assertEqual(result["connector_types"]["custom"], [])
        self.assertEqual(response.trace_id, "test-trace")

    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        side_effect=RuntimeError("boom"),
    )
    def test_returns_error_on_failure(self, _mock_registry):
        agent = Agent(None)
        response = agent.get_supported_connector_types(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_error__", response.result)
