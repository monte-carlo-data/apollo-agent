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
    def tearDown(self):
        import apollo.integrations.custom_etl.custom_etl_connector_loader as loader

        loader._custom_etl_connector_registry = None

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

    def test_serialize_enum_uses_value(self):
        import enum

        class Kind(enum.Enum):
            CRON = "cron"

        self.assertEqual(_serialize(Kind.CRON), "cron")

    def test_serialize_str_enum_uses_value(self):
        import enum

        class Role(str, enum.Enum):
            INPUT = "INPUT"

        # str-Enum is also an Enum — must emit the bare value, not the member.
        self.assertEqual(_serialize(Role.INPUT), "INPUT")

    def test_serialize_datetime_isoformat(self):
        from datetime import datetime, timezone

        dt = datetime(2026, 6, 8, 12, 30, 0, tzinfo=timezone.utc)
        self.assertEqual(_serialize(dt), "2026-06-08T12:30:00+00:00")

    def test_serialize_date_isoformat(self):
        from datetime import date

        self.assertEqual(_serialize(date(2026, 6, 8)), "2026-06-08")


# ---------------------------------------------------------------------------
# ETL wire-contract serialization (pycarlo PR #1527 nested shape)
#
# These dataclasses mirror the connector-side model shape (which lives in the
# runtime-loaded connector bundle, not this repo). They pin the wire shape the
# proxy emits: a nested ``group`` object and a ``tasks`` array — NOT flat
# ``group_*`` keys — with ``*_source_ids`` reference fields, ``event_trigger``
# as a dict, and ``asset_ref`` without a ``metadata`` field.
# ---------------------------------------------------------------------------


class TestEtlWireContractSerialization(TestCase):
    def _build_asset(self):
        import dataclasses
        import enum
        from datetime import datetime, timezone
        from typing import Dict, List, Optional

        class ScheduleKind(str, enum.Enum):
            CRON = "cron"

        class AssetType(str, enum.Enum):
            TABLE = "TABLE"

        class Role(str, enum.Enum):
            INPUT = "INPUT"
            OUTPUT = "OUTPUT"

        @dataclasses.dataclass
        class AssetRef:
            asset_type: AssetType
            role: Role
            mcon: Optional[str] = None
            fully_qualified_name: Optional[str] = None

        @dataclasses.dataclass
        class EtlGroup:
            source_id: str
            name: Optional[str] = None
            group_type: Optional[str] = None
            schedule: Optional[str] = None
            attributes: Optional[Dict] = None

        @dataclasses.dataclass
        class EtlSchedule:
            kind: ScheduleKind
            cron_expression: Optional[str] = None
            timezone: Optional[str] = None
            next_run_at: Optional[datetime] = None
            event_trigger: Optional[Dict] = None
            upstream_job_source_ids: Optional[List[str]] = None
            raw: Optional[Dict] = None

        @dataclasses.dataclass
        class EtlTask:
            task_source_id: str
            name: str
            task_type: Optional[str] = None
            inputs: Optional[List[AssetRef]] = None
            outputs: Optional[List[AssetRef]] = None
            upstream_task_source_ids: Optional[List[str]] = None
            triggered_job_source_ids: Optional[List[str]] = None
            attributes: Optional[Dict] = None

        @dataclasses.dataclass
        class EtlAsset:
            job_source_id: str
            name: str
            group: EtlGroup
            schedule: Optional[EtlSchedule] = None
            inputs: Optional[List[AssetRef]] = None
            outputs: Optional[List[AssetRef]] = None
            triggered_job_source_ids: Optional[List[str]] = None
            tasks: Optional[List[EtlTask]] = None

        return EtlAsset(
            job_source_id="job-1",
            name="Daily load",
            group=EtlGroup(
                source_id="grp-1",
                name="Factory A",
                group_type="factory",
                attributes={"region": "us-east"},
            ),
            schedule=EtlSchedule(
                kind=ScheduleKind.CRON,
                cron_expression="0 * * * *",
                timezone="UTC",
                next_run_at=datetime(2026, 6, 8, 12, 0, 0, tzinfo=timezone.utc),
                event_trigger={"type": "blob_created", "path": "/in"},
                upstream_job_source_ids=["job-0"],
            ),
            inputs=[
                AssetRef(asset_type=AssetType.TABLE, role=Role.INPUT, mcon="mcon-in")
            ],
            outputs=[
                AssetRef(
                    asset_type=AssetType.TABLE,
                    role=Role.OUTPUT,
                    fully_qualified_name="db.schema.out",
                )
            ],
            triggered_job_source_ids=["job-2"],
            tasks=[
                EtlTask(
                    task_source_id="task-1",
                    name="copy activity",
                    task_type="copy",
                    inputs=[
                        AssetRef(asset_type=AssetType.TABLE, role=Role.INPUT, mcon="m1")
                    ],
                    outputs=[
                        AssetRef(
                            asset_type=AssetType.TABLE, role=Role.OUTPUT, mcon="m2"
                        )
                    ],
                    upstream_task_source_ids=["task-0"],
                    triggered_job_source_ids=["job-3"],
                    attributes={"retry": 3},
                )
            ],
        )

    def test_group_is_nested_not_flattened(self):
        result = _serialize(self._build_asset())

        # Nested group object — NOT flat group_source_id / group_name / group_type.
        self.assertIn("group", result)
        self.assertNotIn("group_source_id", result)
        self.assertNotIn("group_name", result)
        self.assertNotIn("group_type", result)
        self.assertEqual(result["group"]["source_id"], "grp-1")
        self.assertEqual(result["group"]["name"], "Factory A")
        self.assertEqual(result["group"]["group_type"], "factory")
        self.assertEqual(result["group"]["attributes"], {"region": "us-east"})

    def test_tasks_array_preserved_with_nested_fields(self):
        result = _serialize(self._build_asset())

        self.assertIn("tasks", result)
        self.assertEqual(len(result["tasks"]), 1)
        task = result["tasks"][0]
        self.assertEqual(task["task_source_id"], "task-1")
        self.assertEqual(task["name"], "copy activity")
        self.assertEqual(task["upstream_task_source_ids"], ["task-0"])
        self.assertEqual(task["triggered_job_source_ids"], ["job-3"])
        self.assertEqual(task["attributes"], {"retry": 3})
        # Nested asset refs inside the task come through too.
        self.assertEqual(task["inputs"][0]["mcon"], "m1")
        self.assertEqual(task["outputs"][0]["role"], "OUTPUT")

    def test_source_id_reference_fields(self):
        result = _serialize(self._build_asset())

        self.assertEqual(result["triggered_job_source_ids"], ["job-2"])
        self.assertEqual(result["schedule"]["upstream_job_source_ids"], ["job-0"])

    def test_schedule_kind_enum_and_event_trigger_dict(self):
        result = _serialize(self._build_asset())

        schedule = result["schedule"]
        # kind enum -> bare value
        self.assertEqual(schedule["kind"], "cron")
        # event_trigger is a dict (was a str in the prior contract)
        self.assertEqual(
            schedule["event_trigger"], {"type": "blob_created", "path": "/in"}
        )
        # datetime -> ISO-8601
        self.assertEqual(schedule["next_run_at"], "2026-06-08T12:00:00+00:00")

    def test_asset_ref_has_no_metadata_and_uses_enums(self):
        result = _serialize(self._build_asset())

        out_ref = result["outputs"][0]
        self.assertNotIn("metadata", out_ref)
        self.assertEqual(out_ref["asset_type"], "TABLE")
        self.assertEqual(out_ref["role"], "OUTPUT")
        self.assertEqual(out_ref["fully_qualified_name"], "db.schema.out")
        # ≥1 of mcon/fqn present; absent one is stripped (None).
        self.assertNotIn("mcon", out_ref)

    def test_fetch_etl_assets_emits_contract_shape(self):
        with patch(
            "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
            return_value={},
        ), patch(
            "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
        ) as mock_load_module:
            module = MagicMock()
            connector = MagicMock()
            module.Connector.return_value = connector
            mock_load_module.return_value = module
            connector.fetch_metadata.return_value = [self._build_asset()]

            client = CustomEtlProxyClient(
                credentials={"connect_args": {}},
                connector_dir="/opt/custom-etl-connectors/adf",
            )
            result = client.fetch_etl_assets(limit=10, offset=0)

        asset = result["all_results"][0]
        self.assertEqual(asset["group"]["source_id"], "grp-1")
        self.assertEqual(asset["tasks"][0]["task_source_id"], "task-1")
        self.assertEqual(asset["schedule"]["kind"], "cron")

    def test_run_event_requires_error_message_and_nested_tasks(self):
        import dataclasses
        import enum
        from typing import List, Optional

        class RunStatus(str, enum.Enum):
            FAILED = "failed"

        @dataclasses.dataclass
        class EtlError:
            message: str  # REQUIRED in the updated runs contract

        @dataclasses.dataclass
        class EtlTaskRun:
            task_source_id: str
            status: RunStatus
            error: Optional[EtlError] = None

        @dataclasses.dataclass
        class EtlRunEvent:
            job_source_id: str
            run_source_id: str
            status: RunStatus
            error: Optional[EtlError] = None
            tasks: Optional[List[EtlTaskRun]] = None

        run = EtlRunEvent(
            job_source_id="job-1",
            run_source_id="run-1",
            status=RunStatus.FAILED,
            error=EtlError(message="boom"),
            tasks=[
                EtlTaskRun(
                    task_source_id="task-1",
                    status=RunStatus.FAILED,
                    error=EtlError(message="task boom"),
                )
            ],
        )

        result = _serialize(run)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error"]["message"], "boom")
        self.assertEqual(result["tasks"][0]["error"]["message"], "task boom")


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
        self._mock_connector.fetch_run_details.return_value = [
            {"job_source_id": "job-1", "run_source_id": "run-1", "status": "success"},
            {"job_source_id": "job-2", "run_source_id": "run-2", "status": "success"},
        ]

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
    def test_setup_connection_failure_propagates(
        self, mock_load_module, mock_load_manifest
    ):
        mock_load_module.return_value = self._mock_module
        self._mock_connector.setup_connection.side_effect = RuntimeError("auth failed")

        with self.assertRaises(RuntimeError) as ctx:
            CustomEtlProxyClient(
                credentials={"connect_args": {"tenant_id": "abc"}},
                connector_dir="/opt/custom-etl-connectors/adf",
            )
        self.assertIn("auth failed", str(ctx.exception))

    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_manifest",
        return_value={},
    )
    @patch(
        "apollo.integrations.custom_etl.custom_etl_proxy_client.load_connector_module"
    )
    def test_close_suppresses_exception(self, mock_load_module, mock_load_manifest):
        mock_load_module.return_value = self._mock_module

        client = CustomEtlProxyClient(
            credentials={"connect_args": {}},
            connector_dir="/opt/custom-etl-connectors/adf",
        )
        self._mock_connector.close_connection.side_effect = RuntimeError(
            "disconnect failed"
        )

        # close() should not raise
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
    def tearDown(self):
        import apollo.integrations.custom_etl.custom_etl_connector_loader as loader

        loader._custom_etl_connector_registry = None

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
    def tearDown(self):
        import apollo.integrations.custom_etl.custom_etl_connector_loader as loader

        loader._custom_etl_connector_registry = None

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
    def tearDown(self):
        import apollo.integrations.custom_etl.custom_etl_connector_loader as loader

        loader._custom_etl_connector_registry = None

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
