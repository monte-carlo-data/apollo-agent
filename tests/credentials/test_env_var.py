import json
import os
from unittest import TestCase
from unittest.mock import patch, Mock

from apollo.credentials.env_var import EnvVarCredentialsService
from apollo.credentials.decryption.base import BaseCredentialDecryptionService


class TestEnvVarCredentialsService(TestCase):
    def setUp(self):
        self.service = EnvVarCredentialsService()

    def test_get_credentials_missing_env_var_name(self):
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials({})

        self.assertEqual(
            "Missing expected environment variable name in credentials",
            str(context.exception),
        )

    @patch.dict(os.environ, {}, clear=True)
    def test_get_credentials_missing_env_var(self):
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials({"env_var_name": "TEST_CREDS"})

        self.assertEqual(
            "Missing expected environment variable: TEST_CREDS", str(context.exception)
        )

    @patch.dict(
        os.environ, {"TEST_CREDS": '{"username": "test", "password": "secret"}'}
    )
    def test_get_credentials_valid_json(self):
        credentials = self.service.get_credentials({"env_var_name": "TEST_CREDS"})

        self.assertEqual({"username": "test", "password": "secret"}, credentials)

    @patch.dict(os.environ, {"TEST_CREDS": "invalid json"})
    def test_get_credentials_invalid_json(self):
        with self.assertRaises(ValueError):
            self.service.get_credentials({"env_var_name": "TEST_CREDS"})

    @patch.dict(os.environ, {"TEST_CREDS": "encrypted_value"})
    @patch(
        "apollo.credentials.decryption.factory.CredentialDecryptionFactory.get_credentials_decryption_service"
    )
    def test_get_credentials_with_decryption(self, mock_get_decryption: Mock):
        # Setup mock decryption service
        mock_decryption = BaseCredentialDecryptionService()
        mock_get_decryption.return_value = mock_decryption

        # Mock the decrypt method to return valid JSON
        with patch.object(
            mock_decryption, "decrypt", return_value='{"key": "decrypted_value"}'
        ):
            credentials = self.service.get_credentials(
                {"env_var_name": "TEST_CREDS", "encryption": {"type": "test"}}
            )

            self.assertEqual({"key": "decrypted_value"}, credentials)

    def test_get_credentials_merge_connect_args(self):
        with patch.dict(
            os.environ, {"TEST_CREDS": '{"connect_args": {"password": "secret"}}'}
        ):
            credentials = self.service.get_credentials(
                {"env_var_name": "TEST_CREDS", "connect_args": {"username": "test"}}
            )
            self.assertEqual(
                {"connect_args": {"username": "test", "password": "secret"}},
                credentials,
            )
        # test merge credentials when connect_args is a string
        with patch.dict(
            os.environ, {"TEST_CREDS": '{"connect_args": "external connect args"}'}
        ):
            credentials = self.service.get_credentials(
                {"env_var_name": "TEST_CREDS", "connect_args": "incoming connect args"}
            )
            self.assertEqual(
                {"connect_args": "external connect args"},
                credentials,
            )
        # test merge credentials when internal connect_args is not provided
        with patch.dict(
            os.environ, {"TEST_CREDS": '{"connect_args": {"password": "secret"}}'}
        ):
            credentials = self.service.get_credentials({"env_var_name": "TEST_CREDS"})
            self.assertEqual(
                {"connect_args": {"password": "secret"}},
                credentials,
            )
