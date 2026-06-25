from unittest import TestCase

from apollo.integrations.ctp.defaults.gcp_dataform import GCP_DATAFORM_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry

_SA_INFO = {
    "type": "service_account",
    "project_id": "test-project",
    "private_key_id": "key-id",
    "private_key": "-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----\n",
    "client_email": "sa@test-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(GCP_DATAFORM_DEFAULT_CTP, credentials)


class TestGcpDataformCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("gcp-dataform"))

    # ── Full credentials ─────────────────────────────────────────────

    def test_full_credentials_mapped(self):
        args = _resolve(
            {
                "project_id": "my-project",
                "service_account_info": _SA_INFO,
                "locations": ["us-central1", "europe-west1"],
            }
        )
        self.assertEqual(args["project_id"], "my-project")
        self.assertEqual(args["service_account_info"], _SA_INFO)
        self.assertEqual(args["locations"], ["us-central1", "europe-west1"])

    # ── Optional locations ───────────────────────────────────────────

    def test_locations_absent_defaults_to_empty_list(self):
        args = _resolve(
            {
                "project_id": "my-project",
                "service_account_info": _SA_INFO,
            }
        )
        self.assertEqual(args["project_id"], "my-project")
        self.assertEqual(args["service_account_info"], _SA_INFO)
        self.assertEqual(args["locations"], [])

    # ── DC pre-shaped credentials (connect_args wrapper) ─────────────

    def test_dc_preshaped_credentials(self):
        """When DC sends credentials wrapped in connect_args, CtpRegistry.resolve unwraps them."""
        result = CtpRegistry.resolve(
            "gcp-dataform",
            {
                "connect_args": {
                    "project_id": "my-project",
                    "service_account_info": _SA_INFO,
                    "locations": ["us-central1"],
                }
            },
        )
        args = result["connect_args"]
        self.assertEqual(args["project_id"], "my-project")
        self.assertEqual(args["service_account_info"], _SA_INFO)
        self.assertEqual(args["locations"], ["us-central1"])
