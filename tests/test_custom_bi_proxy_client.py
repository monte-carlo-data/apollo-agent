import json
import os
import tempfile
from unittest import TestCase
from unittest.mock import patch, MagicMock

from apollo.integrations.custom_bi.custom_bi_connector_loader import (
    _discover_custom_bi_connectors,
    load_connector_module,
    load_manifest,
)
from apollo.agent.agent import Agent
from apollo.integrations.custom_bi.custom_bi_proxy_client import (
    CustomBiProxyClient,
    _serialize,
)


class _FakeModel:
    """Lightweight stand-in for connector model objects (BiAsset / BiOwner / BiAssetRef)."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _create_mock_bi_connector_dir(
    tmp_dir,
    name,
    connection_type,
    terminology=None,
    icon_url=None,
):
    """Helper to create a mock BI connector directory structure."""
    connector_dir = os.path.join(tmp_dir, name)
    os.makedirs(connector_dir, exist_ok=True)

    manifest = {
        "connection_type": connection_type,
        "connection_name": name,
    }
    if terminology is not None:
        manifest["terminology"] = terminology
    if icon_url is not None:
        manifest["icon_url"] = icon_url
    with open(os.path.join(connector_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f)

    # connector.py — mock Connector class mimicking the BI connector interface
    connector_code = """
class Connector:
    credentials = {}

    def setup_connection(self):
        pass

    def close_connection(self):
        pass

    def fetch_metadata(self, limit=1000, offset=0):
        return []
"""
    with open(os.path.join(connector_dir, "connector.py"), "w") as f:
        f.write(connector_code)

    return connector_dir


# ---------------------------------------------------------------------------
# Loader tests
# ---------------------------------------------------------------------------


class TestCustomBiConnectorDiscovery(TestCase):
    def tearDown(self):
        import apollo.integrations.custom_bi.custom_bi_connector_loader as loader

        loader._custom_bi_connector_registry = None

    def test_discovery_reads_manifests(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_bi_connector_dir(
                tmp_dir, "tableau", "custom-bi-connector-de8d7c2"
            )

            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_bi_connectors()

            self.assertIn("custom-bi-connector-de8d7c2", registry)
            self.assertEqual(
                registry["custom-bi-connector-de8d7c2"],
                os.path.join(tmp_dir, "tableau"),
            )

    def test_multiple_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_bi_connector_dir(tmp_dir, "tableau", "custom-bi-connector-aaa")
            _create_mock_bi_connector_dir(tmp_dir, "powerbi", "custom-bi-connector-bbb")

            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_bi_connectors()

            self.assertEqual(len(registry), 2)
            self.assertIn("custom-bi-connector-aaa", registry)
            self.assertIn("custom-bi-connector-bbb", registry)

    def test_discovery_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_bi_connectors()

            self.assertEqual(registry, {})

    def test_discovery_missing_directory(self):
        with patch(
            "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
            "/nonexistent/path",
        ):
            registry = _discover_custom_bi_connectors()

        self.assertEqual(registry, {})

    def test_discovery_skips_missing_manifest(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            os.makedirs(os.path.join(tmp_dir, "bad_integration"))

            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_bi_connectors()

            self.assertEqual(registry, {})

    def test_discovery_skips_missing_connection_type(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            connector_dir = os.path.join(tmp_dir, "no_type")
            os.makedirs(connector_dir)
            with open(os.path.join(connector_dir, "manifest.json"), "w") as f:
                json.dump({"connection_name": "no_type"}, f)

            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ):
                registry = _discover_custom_bi_connectors()

            self.assertEqual(registry, {})


class TestLoadConnectorModule(TestCase):
    def test_successful_load(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_bi_connector_dir(tmp_dir, "tableau", "custom-bi-connector-abc")
            connector_dir = os.path.join(tmp_dir, "tableau")

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

    def test_module_caching(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_bi_connector_dir(
                tmp_dir, "cached", "custom-bi-connector-cache"
            )
            connector_dir = os.path.join(tmp_dir, "cached")

            module1 = load_connector_module(connector_dir)
            module2 = load_connector_module(connector_dir)

            self.assertIs(module1, module2)


class TestLoadManifest(TestCase):
    def test_load_manifest(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            manifest = {
                "connection_type": "custom-bi-connector-abc",
                "connection_name": "tableau",
                "terminology": {"asset": "Workbook"},
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

        obj = Outer(name="asset1", tags=[Inner(key="env", value="prod")])
        result = _serialize(obj)
        self.assertEqual(
            result, {"name": "asset1", "tags": [{"key": "env", "value": "prod"}]}
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

    def test_serialize_enum_uses_value(self):
        import enum

        class Kind(enum.Enum):
            WORKBOOK = "workbook"

        self.assertEqual(_serialize(Kind.WORKBOOK), "workbook")

    def test_serialize_str_enum_uses_value(self):
        import enum

        class AssetType(str, enum.Enum):
            DASHBOARD = "DASHBOARD"

        # str-Enum is also an Enum — must emit the bare value, not the member.
        self.assertEqual(_serialize(AssetType.DASHBOARD), "DASHBOARD")

    def test_serialize_datetime_isoformat(self):
        from datetime import datetime, timezone

        dt = datetime(2026, 6, 8, 12, 30, 0, tzinfo=timezone.utc)
        self.assertEqual(_serialize(dt), "2026-06-08T12:30:00+00:00")

    def test_serialize_date_isoformat(self):
        from datetime import date

        self.assertEqual(_serialize(date(2026, 6, 8)), "2026-06-08")

    def test_serialize_nested_dict_and_list_preserved(self):
        obj = {"a": {"b": [1, 2, {"c": None}]}, "d": None}
        self.assertEqual(_serialize(obj), {"a": {"b": [1, 2, {}]}})


# ---------------------------------------------------------------------------
# Proxy client tests
# ---------------------------------------------------------------------------


class TestCustomBiProxyClient(TestCase):
    def setUp(self):
        self._mock_module = MagicMock()
        self._mock_connector = MagicMock()
        self._mock_module.Connector.return_value = self._mock_connector

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={
            "connection_type": "custom-bi-connector-abc",
            "connection_name": "tableau",
        },
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_test_connection(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        client = CustomBiProxyClient(
            credentials={"connect_args": {"site_id": "abc"}},
            connector_dir="/opt/custom-bi-connectors/tableau",
        )
        result = client.test_connection()

        self.assertEqual(result, {"success": True})
        self._mock_connector.setup_connection.assert_called_once()

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={},
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_missing_connect_args_raises(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        with self.assertRaises(ValueError) as ctx:
            CustomBiProxyClient(
                credentials={"key": "value"},
                connector_dir="/opt/custom-bi-connectors/tableau",
            )
        self.assertIn("connect_args", str(ctx.exception))

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={},
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_none_credentials_raises(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        with self.assertRaises(ValueError):
            CustomBiProxyClient(
                credentials=None,
                connector_dir="/opt/custom-bi-connectors/tableau",
            )

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={},
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_fetch_bi_metadata(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module
        asset1 = _FakeModel(asset_source_id="asset-1", name="Sales dashboard")
        asset2 = _FakeModel(asset_source_id="asset-2", name="Exec workbook")
        self._mock_connector.fetch_metadata.return_value = [asset1, asset2]

        client = CustomBiProxyClient(
            credentials={"connect_args": {"site_id": "abc"}},
            connector_dir="/opt/custom-bi-connectors/tableau",
        )
        result = client.fetch_bi_metadata(limit=100, offset=0)

        self._mock_connector.fetch_metadata.assert_called_once_with(limit=100, offset=0)
        self.assertEqual(len(result["all_results"]), 2)
        self.assertEqual(result["all_results"][0]["asset_source_id"], "asset-1")
        self.assertEqual(result["all_results"][1]["asset_source_id"], "asset-2")

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={},
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_fetch_bi_metadata_empty(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.fetch_metadata.return_value = []

        client = CustomBiProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-bi-connectors/tableau",
        )
        result = client.fetch_bi_metadata(limit=100, offset=0)

        self.assertEqual(result, {"all_results": []})

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={
            "connection_type": "custom-bi-connector-abc",
            "connection_name": "tableau",
            "terminology": {"asset": "Workbook"},
        },
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_get_manifest(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        client = CustomBiProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-bi-connectors/tableau",
        )
        result = client.get_manifest()

        self.assertEqual(result["connection_name"], "tableau")
        self.assertEqual(result["terminology"], {"asset": "Workbook"})

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={},
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_close(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        client = CustomBiProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-bi-connectors/tableau",
        )
        client.close()

        self._mock_connector.close_connection.assert_called_once()

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={},
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_setup_connection_failure_propagates(
        self, mock_load_module, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.setup_connection.side_effect = RuntimeError("auth failed")

        with self.assertRaises(RuntimeError) as ctx:
            CustomBiProxyClient(
                credentials={"connect_args": {"site_id": "abc"}},
                connector_dir="/opt/custom-bi-connectors/tableau",
            )
        self.assertIn("auth failed", str(ctx.exception))

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={},
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_module_import_error_propagates(self, mock_load_module, mock_load_manifest):
        mock_load_module.side_effect = ImportError("bad connector module")

        with self.assertRaises(ImportError):
            CustomBiProxyClient(
                credentials={"connect_args": {}},
                connector_dir="/opt/custom-bi-connectors/tableau",
            )

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={},
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_close_suppresses_exception(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        client = CustomBiProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-bi-connectors/tableau",
        )
        self._mock_connector.close_connection.side_effect = RuntimeError(
            "disconnect failed"
        )

        # close() should not raise
        client.close()
        self._mock_connector.close_connection.assert_called_once()

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={},
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_wrapped_client_returns_connector(
        self, mock_load_module, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module

        client = CustomBiProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-bi-connectors/tableau",
        )

        self.assertEqual(client.wrapped_client, self._mock_connector)

    def test_proxy_method_is_named_fetch_bi_metadata(self):
        """The data-collector invokes this reflective method by exact name."""
        self.assertTrue(hasattr(CustomBiProxyClient, "fetch_bi_metadata"))

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={},
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_fetch_bi_metadata_delegates_to_fetch_metadata(
        self, mock_load_module, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.fetch_metadata.return_value = []

        client = CustomBiProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-bi-connectors/tableau",
        )
        client.fetch_bi_metadata(limit=25, offset=5)

        self._mock_connector.fetch_metadata.assert_called_once_with(limit=25, offset=5)


# ---------------------------------------------------------------------------
# Discovery static method tests
# ---------------------------------------------------------------------------


class TestGetConnectionManifests(TestCase):
    def tearDown(self):
        import apollo.integrations.custom_bi.custom_bi_connector_loader as loader

        loader._custom_bi_connector_registry = None

    def test_returns_all_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_bi_connector_dir(
                tmp_dir,
                "tableau",
                "custom-bi-connector-aaa",
                terminology={"asset": "Workbook"},
                icon_url="https://example.com/icon.png",
            )
            _create_mock_bi_connector_dir(
                tmp_dir,
                "powerbi",
                "custom-bi-connector-bbb",
            )

            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._custom_bi_connector_registry",
                None,
            ):
                result = CustomBiProxyClient.get_connection_manifests()

            self.assertEqual(len(result), 2)

            self.assertIn("custom-bi-connector-aaa", result)
            aaa = result["custom-bi-connector-aaa"]
            self.assertEqual(
                aaa["manifest"]["connection_type"], "custom-bi-connector-aaa"
            )
            self.assertEqual(aaa["manifest"]["connection_name"], "tableau")
            self.assertEqual(
                aaa["manifest"]["terminology"],
                {"asset": "Workbook"},
            )

            self.assertIn("custom-bi-connector-bbb", result)

    def test_strips_credentials_schema_from_manifest(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            connector_dir = os.path.join(tmp_dir, "tableau")
            os.makedirs(connector_dir, exist_ok=True)
            manifest = {
                "connection_type": "custom-bi-connector-aaa",
                "connection_name": "tableau",
                "credentials_schema": {"connect_args": {"type": "dict"}},
            }
            with open(os.path.join(connector_dir, "manifest.json"), "w") as f:
                json.dump(manifest, f)
            # connector.py needed for discovery
            with open(os.path.join(connector_dir, "connector.py"), "w") as f:
                f.write("class Connector: pass\n")

            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._custom_bi_connector_registry",
                None,
            ):
                result = CustomBiProxyClient.get_connection_manifests()

            aaa = result["custom-bi-connector-aaa"]
            self.assertNotIn("credentials_schema", aaa["manifest"])
            self.assertEqual(
                aaa["manifest"]["connection_type"], "custom-bi-connector-aaa"
            )

    def test_returns_empty_when_no_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._custom_bi_connector_registry",
                None,
            ):
                result = CustomBiProxyClient.get_connection_manifests()

            self.assertEqual(result, {})


class TestGetManifestStripsCredentialsSchema(TestCase):
    """Verify get_manifest() strips credentials_schema from the returned dict."""

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.load_manifest",
        return_value={
            "connection_type": "custom-bi-connector-abc",
            "connection_name": "tableau",
            "credentials_schema": {"connect_args": {"type": "dict"}},
        },
    )
    @patch("apollo.integrations.custom_bi.custom_bi_proxy_client.load_connector_module")
    def test_get_manifest_strips_credentials_schema(
        self, mock_load_module, mock_load_manifest
    ):
        mock_module = MagicMock()
        mock_module.Connector.return_value = MagicMock()
        mock_load_module.return_value = mock_module

        client = CustomBiProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-bi-connectors/tableau",
        )
        result = client.get_manifest()

        self.assertNotIn("credentials_schema", result)
        self.assertEqual(result["connection_name"], "tableau")


class TestGetCustomBiConnectorTypes(TestCase):
    def tearDown(self):
        import apollo.integrations.custom_bi.custom_bi_connector_loader as loader

        loader._custom_bi_connector_registry = None

    def test_returns_all_types(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_bi_connector_dir(tmp_dir, "tableau", "custom-bi-connector-aaa")
            _create_mock_bi_connector_dir(tmp_dir, "powerbi", "custom-bi-connector-bbb")

            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._custom_bi_connector_registry",
                None,
            ):
                result = CustomBiProxyClient.get_custom_bi_connector_types()

            self.assertEqual(len(result), 2)
            types_by_id = {entry["type"]: entry["name"] for entry in result}
            self.assertEqual(types_by_id["custom-bi-connector-aaa"], "tableau")
            self.assertEqual(types_by_id["custom-bi-connector-bbb"], "powerbi")

    def test_returns_empty_list_when_no_connectors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._custom_bi_connector_registry",
                None,
            ):
                result = CustomBiProxyClient.get_custom_bi_connector_types()

            self.assertEqual(result, [])

    def test_falls_back_to_type_when_no_name(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            connector_dir = os.path.join(tmp_dir, "noname")
            os.makedirs(connector_dir)
            with open(os.path.join(connector_dir, "manifest.json"), "w") as f:
                json.dump({"connection_type": "custom-bi-connector-xyz"}, f)

            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._custom_bi_connector_registry",
                None,
            ):
                result = CustomBiProxyClient.get_custom_bi_connector_types()

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["type"], "custom-bi-connector-xyz")
            self.assertEqual(result[0]["name"], "custom-bi-connector-xyz")


# ---------------------------------------------------------------------------
# Agent integration tests
# ---------------------------------------------------------------------------


class TestAgentGetConnectionManifests(TestCase):
    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.get_custom_bi_connector_registry",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.get_custom_etl_connector_registry",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        return_value={},
    )
    def test_returns_ok_response(self, _mock_custom, _mock_etl, _mock_bi):
        a = Agent(None)
        response = a.get_connection_manifests(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_result__", response.result)
        self.assertEqual(response.result["__mcd_result__"], {})

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.get_custom_bi_connector_registry",
        side_effect=RuntimeError("boom"),
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.get_custom_etl_connector_registry",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        return_value={},
    )
    def test_returns_error_on_failure(self, _mock_custom, _mock_etl, _mock_bi):
        a = Agent(None)
        response = a.get_connection_manifests(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_error__", response.result)


class TestAgentGetSupportedConnectorTypes(TestCase):
    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.get_custom_bi_connector_registry",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.get_custom_etl_connector_registry",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        return_value={},
    )
    def test_returns_native_custom_etl_and_bi(self, _mock_custom, _mock_etl, _mock_bi):
        a = Agent(None)
        response = a.get_supported_connector_types(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_result__", response.result)
        result = response.result["__mcd_result__"]
        self.assertIn("connector_types", result)
        self.assertIn("native", result["connector_types"])
        self.assertIn("custom", result["connector_types"])
        self.assertIn("custom_etl", result["connector_types"])
        self.assertIn("custom_bi", result["connector_types"])
        # native should contain known built-in types
        native = result["connector_types"]["native"]
        self.assertIn("bigquery", native)
        self.assertIn("snowflake", native)
        # custom_bi is empty because registry is mocked empty
        self.assertEqual(result["connector_types"]["custom_bi"], [])

    @patch(
        "apollo.integrations.custom_bi.custom_bi_proxy_client.get_custom_bi_connector_registry",
        side_effect=RuntimeError("boom"),
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.get_custom_etl_connector_registry",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom.custom_proxy_client.get_custom_connector_registry",
        return_value={},
    )
    def test_returns_error_on_failure(self, _mock_custom, _mock_etl, _mock_bi):
        a = Agent(None)
        response = a.get_supported_connector_types(trace_id="test-trace")

        self.assertEqual(response.status_code, 200)
        self.assertIn("__mcd_error__", response.result)


# ---------------------------------------------------------------------------
# Factory integration test
# ---------------------------------------------------------------------------


class TestProxyClientFactory(TestCase):
    def tearDown(self):
        import apollo.integrations.custom_bi.custom_bi_connector_loader as loader

        loader._custom_bi_connector_registry = None

    @patch.dict(os.environ, {"MCD_CUSTOM_CONNECTORS_ENABLED": "true"})
    def test_custom_bi_connector_lookup(self):
        """Factory routes custom-bi-connector types to CustomBiProxyClient."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            _create_mock_bi_connector_dir(
                tmp_dir, "tableau", "custom-bi-connector-de8d7c2"
            )

            with patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._CUSTOM_BI_CONNECTORS_BASE_PATH",
                tmp_dir,
            ), patch(
                "apollo.integrations.custom_bi.custom_bi_connector_loader._custom_bi_connector_registry",
                None,
            ), patch(
                "apollo.integrations.custom.custom_connector_loader._custom_connector_registry",
                {},
            ), patch(
                "apollo.integrations.custom_etl.custom_etl_connector_loader._custom_etl_connector_registry",
                {},
            ):
                from apollo.agent.proxy_client_factory import ProxyClientFactory

                client = ProxyClientFactory._create_proxy_client(
                    connection_type="custom-bi-connector-de8d7c2",
                    credentials={"connect_args": {"site_id": "abc"}},
                    platform="generic",
                )

            self.assertIsInstance(client, CustomBiProxyClient)
            client.close()

    @patch.dict(os.environ, {"MCD_CUSTOM_CONNECTORS_ENABLED": "false"})
    def test_custom_bi_connector_disabled(self):
        """When custom connectors are disabled, custom BI types raise."""
        from apollo.agent.proxy_client_factory import ProxyClientFactory
        from apollo.common.agent.models import AgentError

        with self.assertRaises(AgentError):
            ProxyClientFactory._create_proxy_client(
                connection_type="custom-bi-connector-de8d7c2",
                credentials={"connect_args": {}},
                platform="generic",
            )
