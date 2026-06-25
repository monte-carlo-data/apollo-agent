from unittest import TestCase
from unittest.mock import patch, MagicMock

from apollo.integrations.gcp_dataform.gcp_dataform_proxy_client import (
    GcpDataformProxyClient,
)

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

_PATCH_PREFIX = "apollo.integrations.gcp_dataform.gcp_dataform_proxy_client"


class GcpDataformClientTests(TestCase):
    # ── Constructor ──────────────────────────────────────────────────

    @patch(f"{_PATCH_PREFIX}.dataform_v1.DataformClient")
    @patch(f"{_PATCH_PREFIX}.Credentials.from_service_account_info")
    def test_init_with_connect_args(self, mock_from_sa, mock_dataform_client):
        mock_from_sa.return_value = MagicMock()

        credentials = {
            "connect_args": {
                "project_id": "my-project",
                "service_account_info": _SA_INFO,
                "locations": ["us-central1"],
            }
        }
        client = GcpDataformProxyClient(credentials=credentials)

        self.assertEqual(client._project_id, "my-project")
        self.assertEqual(client._locations, ["us-central1"])
        mock_from_sa.assert_called_once()
        mock_dataform_client.assert_called_once()

    @patch(f"{_PATCH_PREFIX}.dataform_v1.DataformClient")
    @patch(f"{_PATCH_PREFIX}.Credentials.from_service_account_info")
    def test_init_locations_defaults_to_empty_list(
        self, mock_from_sa, mock_dataform_client
    ):
        mock_from_sa.return_value = MagicMock()

        credentials = {
            "connect_args": {
                "project_id": "my-project",
                "service_account_info": _SA_INFO,
            }
        }
        client = GcpDataformProxyClient(credentials=credentials)

        self.assertEqual(client._locations, [])

    @patch(f"{_PATCH_PREFIX}.dataform_v1.DataformClient")
    @patch(f"{_PATCH_PREFIX}.Credentials.from_service_account_info")
    def test_init_locations_none_becomes_empty_list(
        self, mock_from_sa, mock_dataform_client
    ):
        """When CTP emits locations=None, constructor should treat as empty list."""
        mock_from_sa.return_value = MagicMock()

        credentials = {
            "connect_args": {
                "project_id": "my-project",
                "service_account_info": _SA_INFO,
                "locations": None,
            }
        }
        client = GcpDataformProxyClient(credentials=credentials)

        self.assertEqual(client._locations, [])

    def test_init_missing_service_account_info_raises(self):
        credentials = {
            "connect_args": {
                "project_id": "my-project",
            }
        }
        with self.assertRaises(ValueError) as ctx:
            GcpDataformProxyClient(credentials=credentials)
        self.assertIn("service_account_info", str(ctx.exception))

    def test_init_no_connect_args_raises(self):
        """When connect_args is absent, should raise due to missing SA info."""
        with self.assertRaises(ValueError):
            GcpDataformProxyClient(credentials={"project_id": "x"})

    # ── get_connection_metadata ──────────────────────────────────────

    @patch(f"{_PATCH_PREFIX}.dataform_v1.DataformClient")
    @patch(f"{_PATCH_PREFIX}.Credentials.from_service_account_info")
    def test_get_connection_metadata(self, mock_from_sa, mock_dataform_client):
        mock_from_sa.return_value = MagicMock()

        credentials = {
            "connect_args": {
                "project_id": "my-project",
                "service_account_info": _SA_INFO,
                "locations": ["us-central1", "europe-west1"],
            }
        }
        client = GcpDataformProxyClient(credentials=credentials)
        metadata = client.get_connection_metadata()

        self.assertEqual(metadata["project_id"], "my-project")
        self.assertEqual(metadata["locations"], ["us-central1", "europe-west1"])

    # ── test_connection ──────────────────────────────────────────────

    @patch(f"{_PATCH_PREFIX}.dataform_v1.DataformClient")
    @patch(f"{_PATCH_PREFIX}.Credentials.from_service_account_info")
    def test_test_connection_success(self, mock_from_sa, mock_dataform_client):
        mock_from_sa.return_value = MagicMock()
        mock_client_instance = MagicMock()
        mock_dataform_client.return_value = mock_client_instance
        mock_client_instance.list_repositories.return_value = []

        credentials = {
            "connect_args": {
                "project_id": "my-project",
                "service_account_info": _SA_INFO,
                "locations": ["us-central1"],
            }
        }
        client = GcpDataformProxyClient(credentials=credentials)
        result = client.test_connection()

        self.assertEqual(result, {"success": True})
        mock_client_instance.list_repositories.assert_called_once_with(
            parent="projects/my-project/locations/us-central1"
        )

    @patch(f"{_PATCH_PREFIX}.dataform_v1.DataformClient")
    @patch(f"{_PATCH_PREFIX}.Credentials.from_service_account_info")
    def test_test_connection_empty_locations_raises(
        self, mock_from_sa, mock_dataform_client
    ):
        mock_from_sa.return_value = MagicMock()

        credentials = {
            "connect_args": {
                "project_id": "my-project",
                "service_account_info": _SA_INFO,
            }
        }
        client = GcpDataformProxyClient(credentials=credentials)

        with self.assertRaises(ValueError) as ctx:
            client.test_connection()
        self.assertIn("location", str(ctx.exception).lower())

    # ── list_repositories ────────────────────────────────────────────

    @patch(f"{_PATCH_PREFIX}.dataform_v1.DataformClient")
    @patch(f"{_PATCH_PREFIX}.Credentials.from_service_account_info")
    def test_list_repositories(self, mock_from_sa, mock_dataform_client):
        mock_from_sa.return_value = MagicMock()
        mock_client_instance = MagicMock()
        mock_dataform_client.return_value = mock_client_instance

        mock_repo = MagicMock()
        type(mock_repo).to_dict = MagicMock(
            return_value={"name": "projects/p/locations/l/repositories/r"}
        )
        mock_client_instance.list_repositories.return_value = [mock_repo]

        credentials = {
            "connect_args": {
                "project_id": "my-project",
                "service_account_info": _SA_INFO,
            }
        }
        client = GcpDataformProxyClient(credentials=credentials)
        result = client.list_repositories(
            parent="projects/my-project/locations/us-central1"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {"name": "projects/p/locations/l/repositories/r"})

    # ── wrapped_client ───────────────────────────────────────────────

    @patch(f"{_PATCH_PREFIX}.dataform_v1.DataformClient")
    @patch(f"{_PATCH_PREFIX}.Credentials.from_service_account_info")
    def test_wrapped_client(self, mock_from_sa, mock_dataform_client):
        mock_from_sa.return_value = MagicMock()
        mock_client_instance = MagicMock()
        mock_dataform_client.return_value = mock_client_instance

        credentials = {
            "connect_args": {
                "project_id": "my-project",
                "service_account_info": _SA_INFO,
            }
        }
        client = GcpDataformProxyClient(credentials=credentials)

        self.assertIs(client.wrapped_client, mock_client_instance)
