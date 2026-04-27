from unittest import TestCase

from apollo.credentials.base import BaseCredentialsService
from apollo.integrations.ctp.defaults.passthrough import PASSTHROUGH_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestMergeConnectArgs(TestCase):
    def setUp(self):
        self.svc = BaseCredentialsService()

    def test_plain_credentials_returned_unchanged(self):
        creds = {"connect_args": {"host": "h", "port": 5432}}
        result = self.svc.get_credentials(creds)
        self.assertEqual({"connect_args": {"host": "h", "port": 5432}}, result)

    def test_only_incoming_has_connect_args(self):
        """External creds without connect_args get the incoming connect_args injected."""
        incoming = {"connect_args": {"username": "user"}}
        external = {"host": "db.example.com"}
        result = self.svc._merge_connect_args(
            incoming_credentials=incoming, external_credentials=external
        )
        self.assertEqual(
            {"host": "db.example.com", "connect_args": {"username": "user"}}, result
        )

    def test_only_external_has_connect_args(self):
        """When incoming has no connect_args, external is returned as-is."""
        incoming = {"aws_secret": "my-secret"}
        external = {"connect_args": {"host": "db.example.com", "password": "secret"}}
        result = self.svc._merge_connect_args(
            incoming_credentials=incoming, external_credentials=external
        )
        self.assertEqual(
            {"connect_args": {"host": "db.example.com", "password": "secret"}}, result
        )

    def test_both_have_connect_args_external_wins_on_conflict(self):
        """Keys present in both are resolved in favour of the external (secret) value."""
        incoming = {"connect_args": {"username": "override", "ssl": True}}
        external = {"connect_args": {"username": "db-user", "password": "secret"}}
        result = self.svc._merge_connect_args(
            incoming_credentials=incoming, external_credentials=external
        )
        self.assertEqual(
            {
                "connect_args": {
                    "username": "db-user",
                    "password": "secret",
                    "ssl": True,
                }
            },
            result,
        )

    def test_non_dict_connect_args_returns_external_unchanged(self):
        """If either connect_args is a connection string (non-dict), external is returned as-is."""
        incoming = {"connect_args": "host=db.example.com password=secret"}
        external = {"connect_args": "host=other.example.com"}
        result = self.svc._merge_connect_args(
            incoming_credentials=incoming, external_credentials=external
        )
        self.assertEqual({"connect_args": "host=other.example.com"}, result)


class TestPassthroughCtp(TestCase):
    def test_passthrough_pipeline_returns_raw(self):
        raw = {"host": "h", "port": 5432, "user": "u", "password": "p"}
        result = CtpPipeline().execute(PASSTHROUGH_CTP, raw)
        self.assertEqual(raw, result)

    def test_passthrough_registered_connector_wraps_in_connect_args(self):
        CtpRegistry.register("_test_passthrough", PASSTHROUGH_CTP)
        result = CtpRegistry.resolve("_test_passthrough", {"host": "h", "port": 5432})
        self.assertIn("connect_args", result)
        self.assertEqual("h", result["connect_args"]["host"])
