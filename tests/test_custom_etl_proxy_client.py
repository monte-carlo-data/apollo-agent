import json
import os
import tempfile
from datetime import timedelta
from unittest import TestCase
from unittest.mock import patch, MagicMock

from apollo.integrations.custom_etl.custom_etl_connector_loader import (
    _discover_custom_etl_connectors,
    load_connector_module,
    load_manifest,
)
from apollo.agent.agent import Agent
from apollo.integrations.custom_etl.custom_etl_proxy_client import (
    CustomEtlProxyClient,
    _serialize,
)


class _FakeModel:
    """Lightweight stand-in for connector model objects (EtlAsset / EtlRunEvent)."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _create_mock_etl_connector_dir(
    tmp_dir,
    name,
    connection_type,
    terminology=None,
    icon_url=None,
):
    """Helper to create a mock ETL connector directory structure."""
    connector_dir = os.path.join(tmp_dir, name)
    os.makedirs(connector_dir, exist_ok=True)

    manifest = {
        "connection_type": connection_type,
        "name": name,
    }
    if terminology is not None:
        manifest["terminology"] = terminology
    if icon_url is not None:
        manifest["icon_url"] = icon_url
    with open(os.path.join(connector_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f)

    # connector.py — mock Connector class mimicking BaseEtlConnector interface
    connector_code = """
class Connector:
    credentials = {}

    def setup_connection(self):
        pass

    def close_connection(self):
        pass

    def fetch_metadata(self, limit=1000, offset=0):
        return []

    def fetch_run_details(self, run_ids=None, lookback=None, limit=100, offset=0):
        return []
"""
    with open(os.path.join(connector_dir, "connector.py"), "w") as f:
        f.write(connector_code)

    return connector_dir


# ---------------------------------------------------------------------------
# Loader tests
# ---------------------------------------------------------------------------


class TestCustomEtlConnectorDiscovery(TestCase):
    def test_discovery_reads_manifests(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_etl_connector_dir(
                tmp_dir, "adf", "custom-etl-connector-de8d7c2"
            )

            with patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_etl_connectors()

            self.assertIn("custom-etl-connector-de8d7c2", registry)
            self.assertEqual(
                registry["custom-etl-connector-de8d7c2"],
                os.path.join(tmp_dir, "adf"),
            )

    def test_multiple_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_etl_connector_dir(tmp_dir, "adf", "custom-etl-connector-aaa")
            _create_mock_etl_connector_dir(
                tmp_dir, "airflow", "custom-etl-connector-bbb"
            )

            with patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_etl_connectors()

            self.assertEqual(len(registry), 2)
            self.assertIn("custom-etl-connector-aaa", registry)
            self.assertIn("custom-etl-connector-bbb", registry)

    def test_discovery_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_etl_connectors()

            self.assertEqual(registry, {})

    def test_discovery_missing_directory(self):
        with patch(
            "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
            "/nonexistent/path",
        ):
            registry = _discover_custom_etl_connectors()

        self.assertEqual(registry, {})

    def test_discovery_skips_missing_manifest(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            os.makedirs(os.path.join(tmp_dir, "bad_integration"))

            with patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_etl_connectors()

            self.assertEqual(registry, {})


class TestLoadConnectorModule(TestCase):
    def test_successful_load(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_etl_connector_dir(tmp_dir, "adf", "custom-etl-connector-abc")
            connector_dir = os.path.join(tmp_dir, "adf")

            module = load_connector_module(connector_dir)

            self.assertTrue(hasattr(module, "Connector"))
            connector = module.Connector()
            # Should not raise
            connector.setup_connection()

    def test_missing_connector_py(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with self.assertRaises(FileNotFoundError) as ctx:
                load_connector_module(tmp_dir)
            self.assertIn("connector.py not found", str(ctx.exception))

    def test_syntax_error_in_connector(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            connector_path = os.path.join(tmp_dir, "connector.py")
            with open(connector_path, "w") as f:
                f.write("class Connector:\n    def bad_method(self\n")

            with self.assertRaises(SyntaxError):
                load_connector_module(tmp_dir)


class TestLoadManifest(TestCase):
    def test_load_manifest(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest = {
                "connection_type": "custom-etl-connector-abc",
                "name": "adf",
                "terminology": {"group": "Factory", "job": "Pipeline"},
            }
            with open(os.path.join(tmp_dir, "manifest.json"), "w") as f:
                json.dump(manifest, f)

            result = load_manifest(tmp_dir)

            self.assertEqual(result, manifest)

    def test_load_manifest_no_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = load_manifest(tmp_dir)
            self.assertEqual(result, {})


# ---------------------------------------------------------------------------
# Serialization tests
# ---------------------------------------------------------------------------


class TestSerialize(TestCase):
    def test_serialize_dict(self):
        self.assertEqual(_serialize({"a": 1, "b": "c"}), {"a": 1, "b": "c"})

    def test_serialize_strips_none_from_dataclass(self):
        import dataclasses

        @dataclasses.dataclass
        class Sample:
            name: str
            value: int = None

        obj = Sample(name="test", value=None)
        result = _serialize(obj)
        self.assertEqual(result, {"name": "test"})

    def test_serialize_nested_dataclass(self):
        import dataclasses

        @dataclasses.dataclass
        class Inner:
            key: str
            value: str

        @dataclasses.dataclass
        class Outer:
            name: str
            tags: list

        obj = Outer(name="job1", tags=[Inner(key="env", value="prod")])
        result = _serialize(obj)
        self.assertEqual(
            result, {"name": "job1", "tags": [{"key": "env", "value": "prod"}]}
        )

    def test_serialize_plain_object(self):
        class Obj:
            def __init__(self):
                self.name = "test"
                self._private = "hidden"
                self.empty = None

        result = _serialize(Obj())
        self.assertEqual(result, {"name": "test"})

    def test_serialize_primitives(self):
        self.assertEqual(_serialize("hello"), "hello")
        self.assertEqual(_serialize(42), 42)
        self.assertIsNone(_serialize(None))


# ---------------------------------------------------------------------------
# Proxy client tests
# ---------------------------------------------------------------------------


class TestCustomEtlProxyClient(TestCase):
    def setUp(self):
        self._mock_module = MagicMock()
        self._mock_connector = MagicMock()
        self._mock_module.Connector.return_value = self._mock_connector

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={
            "connection_type": "custom-etl-connector-abc",
            "name": "adf",
        },
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_test_connection(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        client = CustomEtlProxyClient(
            credentials={"connect_args": {"tenant_id": "abc"}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )
        result = client.test_connection()

        self.assertEqual(result, {"success": True})
        self._mock_connector.setup_connection.assert_called_once()

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_missing_connect_args_raises(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        with self.assertRaises(ValueError) as ctx:
            CustomEtlProxyClient(
                credentials={"key": "value"},
                connector_dir="/opt/custom-etl-connectors/adf",
            )
        self.assertIn("connect_args", str(ctx.exception))

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_none_credentials_raises(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        with self.assertRaises(ValueError):
            CustomEtlProxyClient(
                credentials=None,
                connector_dir="/opt/custom-etl-connectors/adf",
            )

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_fetch_etl_assets(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module
        asset1 = _FakeModel(job_source_id="job-1", name="Daily load")
        asset2 = _FakeModel(job_source_id="job-2", name="Hourly sync")
        self._mock_connector.fetch_metadata.return_value = [asset1, asset2]

        client = CustomEtlProxyClient(
            credentials={"connect_args": {"tenant_id": "abc"}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )
        result = client.fetch_etl_assets(limit=100, offset=0)

        self._mock_connector.fetch_metadata.assert_called_once_with(limit=100, offset=0)
        self.assertEqual(len(result["all_results"]), 2)
        self.assertEqual(result["all_results"][0]["job_source_id"], "job-1")
        self.assertEqual(result["all_results"][1]["job_source_id"], "job-2")

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_fetch_etl_assets_empty(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.fetch_metadata.return_value = []

        client = CustomEtlProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )
        result = client.fetch_etl_assets(limit=100, offset=0)

        self.assertEqual(result, {"all_results": []})

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_fetch_etl_runs(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module
        run1 = _FakeModel(
            job_source_id="job-1", run_source_id="run-1", status="success"
        )
        run2 = _FakeModel(job_source_id="job-1", run_source_id="run-2", status="failed")
        self._mock_connector.fetch_run_details.return_value = [run1, run2]

        client = CustomEtlProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )
        result = client.fetch_etl_runs(lookback_min=1440, limit=100, offset=0)

        self._mock_connector.fetch_run_details.assert_called_once_with(
            run_ids=None,
            lookback=timedelta(minutes=1440),
            limit=100,
            offset=0,
        )
        self.assertEqual(len(result["all_results"]), 2)
        self.assertEqual(result["all_results"][0]["run_source_id"], "run-1")
        self.assertEqual(result["all_results"][1]["status"], "failed")

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_fetch_etl_runs_with_job_run_ids(
        self, mock_load_module, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        run1 = _FakeModel(
            job_source_id="job-1", run_source_id="run-1", status="success"
        )
        self._mock_connector.fetch_run_details.return_value = [run1]

        client = CustomEtlProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )
        result = client.fetch_etl_runs(
            lookback_min=720,
            job_run_ids=["run-1"],
            limit=50,
            offset=10,
        )

        self._mock_connector.fetch_run_details.assert_called_once_with(
            run_ids=["run-1"],
            lookback=timedelta(minutes=720),
            limit=50,
            offset=10,
        )
        self.assertEqual(len(result["all_results"]), 1)

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_fetch_etl_runs_filters_by_job_ids(
        self, mock_load_module, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        run1 = _FakeModel(
            job_source_id="job-1", run_source_id="run-1", status="success"
        )
        run2 = _FakeModel(
            job_source_id="job-2", run_source_id="run-2", status="success"
        )
        self._mock_connector.fetch_run_details.return_value = [run1, run2]

        client = CustomEtlProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )
        result = client.fetch_etl_runs(
            lookback_min=1440,
            job_ids=["job-1"],
            limit=100,
            offset=0,
        )

        # Only run1 should be included (job_ids filter)
        self.assertEqual(len(result["all_results"]), 1)
        self.assertEqual(result["all_results"][0]["job_source_id"], "job-1")

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_fetch_etl_runs_empty(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.fetch_run_details.return_value = []

        client = CustomEtlProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )
        result = client.fetch_etl_runs(lookback_min=1440, limit=100, offset=0)

        self.assertEqual(result, {"all_results": []})

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={
            "connection_type": "custom-etl-connector-abc",
            "name": "adf",
            "terminology": {"group": "Factory"},
        },
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_get_manifest(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        client = CustomEtlProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )
        result = client.get_manifest()

        self.assertEqual(result["name"], "adf")
        self.assertEqual(result["terminology"], {"group": "Factory"})

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_close(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        client = CustomEtlProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )
        client.close()

        self._mock_connector.close_connection.assert_called_once()

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_wrapped_client_returns_connector(
        self, mock_load_module, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module

        client = CustomEtlProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )

        self.assertEqual(client.wrapped_client, self._mock_connector)


# ---------------------------------------------------------------------------
# Discovery static method tests
# ---------------------------------------------------------------------------


class TestGetConnectionManifests(TestCase):
    def test_returns_all_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_etl_connector_dir(
                tmp_dir,
                "adf",
                "custom-etl-connector-aaa",
                terminology={"group": "Factory", "job": "Pipeline"},
                icon_url="https://example.com/icon.png",
            )
            _create_mock_etl_connector_dir(
                tmp_dir,
                "airflow",
                "custom-etl-connector-bbb",
            )

            with patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._custom_etl_connector_registry",
                None,
            ):
                result = CustomEtlProxyClient.get_connection_manifests()

            self.assertEqual(len(result), 2)

            self.assertIn("custom-etl-connector-aaa", result)
            aaa = result["custom-etl-connector-aaa"]
            self.assertEqual(
                aaa["manifest"]["connection_type"], "custom-etl-connector-aaa"
            )
            self.assertEqual(aaa["manifest"]["name"], "adf")
            self.assertEqual(
                aaa["manifest"]["terminology"],
                {"group": "Factory", "job": "Pipeline"},
            )

            self.assertIn("custom-etl-connector-bbb", result)

    def test_returns_empty_when_no_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._custom_etl_connector_registry",
                None,
            ):
                result = CustomEtlProxyClient.get_connection_manifests()

            self.assertEqual(result, {})


class TestGetCustomEtlConnectorTypes(TestCase):
    def test_returns_all_types(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_etl_connector_dir(tmp_dir, "adf", "custom-etl-connector-aaa")
            _create_mock_etl_connector_dir(
                tmp_dir, "airflow", "custom-etl-connector-bbb"
            )

            with patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._custom_etl_connector_registry",
                None,
            ):
                result = CustomEtlProxyClient.get_custom_etl_connector_types()

            self.assertEqual(len(result), 2)
            types_by_id = {entry["type"]: entry["name"] for entry in result}
            self.assertEqual(types_by_id["custom-etl-connector-aaa"], "adf")
            self.assertEqual(types_by_id["custom-etl-connector-bbb"], "airflow")

    def test_returns_empty_list_when_no_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._custom_etl_connector_registry",
                None,
            ):
                result = CustomEtlProxyClient.get_custom_etl_connector_types()

            self.assertEqual(result, [])

    def test_falls_back_to_type_when_no_name(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            connector_dir = os.path.join(tmp_dir, "noname")
            os.makedirs(connector_dir)
            with open(os.path.join(connector_dir, "manifest.json"), "w") as f:
                json.dump({"connection_type": "custom-etl-connector-xyz"}, f)

            with patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._custom_etl_connector_registry",
                None,
            ):
                result = CustomEtlProxyClient.get_custom_etl_connector_types()

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["type"], "custom-etl-connector-xyz")
            self.assertEqual(result[0]["name"], "custom-etl-connector-xyz")


# ---------------------------------------------------------------------------
# Agent integration tests
# ---------------------------------------------------------------------------


class TestAgentGetConnectionManifests(TestCase):
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.get_custom_etl_connector_registry",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        return_value={},
    )
    def test_returns_ok_response(self, _mock_custom, _mock_etl):
        a = Agent(None)
        response = a.get_connection_manifests(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_result__", response.result)
        self.assertEqual(response.result["__mcd_result__"], {})

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.get_custom_etl_connector_registry",
        side_effect=RuntimeError("boom"),
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        return_value={},
    )
    def test_returns_error_on_failure(self, _mock_custom, _mock_etl):
        a = Agent(None)
        response = a.get_connection_manifests(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_error__", response.result)


class TestAgentGetSupportedConnectorTypes(TestCase):
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.get_custom_etl_connector_registry",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        return_value={},
    )
    def test_returns_native_custom_and_custom_etl(self, _mock_custom, _mock_etl):
        a = Agent(None)
        response = a.get_supported_connector_types(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_result__", response.result)
        result = response.result["__mcd_result__"]
        self.assertIn("connector_types", result)
        self.assertIn("native", result["connector_types"])
        self.assertIn("custom", result["connector_types"])
        self.assertIn("custom_etl", result["connector_types"])
        # native should contain known built-in types
        native = result["connector_types"]["native"]
        self.assertIn("bigquery", native)
        self.assertIn("snowflake", native)
        # custom_etl is empty because registry is mocked empty
        self.assertEqual(result["connector_types"]["custom_etl"], [])

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.get_custom_etl_connector_registry",
        side_effect=RuntimeError("boom"),
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        return_value={},
    )
    def test_returns_error_on_failure(self, _mock_custom, _mock_etl):
        a = Agent(None)
        response = a.get_supported_connector_types(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_error__", response.result)


# ---------------------------------------------------------------------------
# Factory integration test
# ---------------------------------------------------------------------------


class TestProxyClientFactory(TestCase):
    @patch(
        "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
        "/nonexistent",
    )
    @patch.dict(os.environ, {"MCD_CUSTOM_CONNECTORS_ENABLED": "true"})
    def test_custom_etl_connector_lookup(self):
        """Factory routes custom-etl-connector types to CustomEtlProxyClient."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_etl_connector_dir(
                tmp_dir, "adf", "custom-etl-connector-de8d7c2"
            )

            with patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._CUSTOM_ETL_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._custom_etl_connector_registry",
                None,
            ), patch(
                "apollo.integrations.custom.custom_connector_loader._custom_connector_registry",
                {},
            ):
                from apollo.agent.proxy_client_factory import ProxyClientFactory

                client = ProxyClientFactory._create_proxy_client(
                    connection_type="custom-etl-connector-de8d7c2",
                    credentials={"connect_args": {"tenant_id": "abc"}},
                    platform="generic",
                )

            self.assertIsInstance(client, CustomEtlProxyClient)
            client.close()

    @patch.dict(os.environ, {"MCD_CUSTOM_CONNECTORS_ENABLED": "false"})
    def test_custom_etl_connector_disabled(self):
        """When custom connectors are disabled, custom ETL types raise."""
        from apollo.agent.proxy_client_factory import ProxyClientFactory
        from apollo.common.agent.models import AgentError

        with self.assertRaises(AgentError):
            ProxyClientFactory._create_proxy_client(
                connection_type="custom-etl-connector-de8d7c2",
                credentials={"connect_args": {}},
                platform="generic",
            )
