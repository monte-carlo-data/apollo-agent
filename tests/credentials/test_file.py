import json
import tempfile
from unittest import TestCase
from unittest.mock import patch, mock_open

from apollo.credentials.file import FileCredentialsService


class TestFileCredentialsService(TestCase):
    def setUp(self):
        self.service = FileCredentialsService()

    def test_get_credentials_missing_file_path(self):
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials({})

        self.assertEqual(
            "Missing expected file path in credentials",
            str(context.exception),
        )

    def test_get_credentials_success(self):
        # Setup
        credentials_data = {"username": "test", "password": "secret"}
        file_content = json.dumps(credentials_data)

        with patch("builtins.open", mock_open(read_data=file_content)):
            credentials = {"file_path": "/path/to/credentials.json"}

            # Execute
            result = self.service.get_credentials(credentials)

            # Verify
            self.assertEqual({"username": "test", "password": "secret"}, result)

    def test_get_credentials_file_not_found(self):
        # Setup
        credentials = {"file_path": "/nonexistent/path/credentials.json"}

        # Execute & Verify
        with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
            with self.assertRaises(FileNotFoundError):
                self.service.get_credentials(credentials)

    def test_get_credentials_invalid_json(self):
        # Setup
        file_content = "invalid json content"

        with patch("builtins.open", mock_open(read_data=file_content)):
            credentials = {"file_path": "/path/to/credentials.json"}

            # Execute & Verify
            with self.assertRaises(ValueError) as exc:
                self.service.get_credentials(credentials)
            self.assertIn("Invalid JSON in credentials file", str(exc.exception))

    def test_get_credentials_merge_connect_args(self):
        # Setup
        credentials_data = {"connect_args": {"password": "secret"}}
        file_content = json.dumps(credentials_data)

        with patch("builtins.open", mock_open(read_data=file_content)):
            credentials = {
                "file_path": "/path/to/credentials.json",
                "connect_args": {"username": "test"},
            }

            # Execute
            result = self.service.get_credentials(credentials)

            # Verify
            self.assertEqual(
                {"connect_args": {"username": "test", "password": "secret"}}, result
            )

    def test_get_credentials_with_real_temp_file(self):
        # Setup - create a real temporary file
        credentials_data = {"username": "test_user", "password": "test_pass"}

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            json.dump(credentials_data, f)
            temp_file_path = f.name

        try:
            credentials = {"file_path": temp_file_path}

            # Execute
            result = self.service.get_credentials(credentials)

            # Verify
            self.assertEqual({"username": "test_user", "password": "test_pass"}, result)
        finally:
            # Cleanup
            import os

            os.unlink(temp_file_path)

    def test_get_credentials_empty_file(self):
        # Setup
        file_content = "{}"

        with patch("builtins.open", mock_open(read_data=file_content)):
            credentials = {"file_path": "/path/to/credentials.json"}

            # Execute
            result = self.service.get_credentials(credentials)

            # Verify
            self.assertEqual({}, result)

    def test_get_credentials_complex_structure(self):
        # Setup
        credentials_data = {
            "username": "test",
            "password": "secret",
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "connect_args": {"ssl": True, "timeout": 30},
        }
        file_content = json.dumps(credentials_data)

        with patch("builtins.open", mock_open(read_data=file_content)):
            credentials = {"file_path": "/path/to/credentials.json"}

            # Execute
            result = self.service.get_credentials(credentials)

            # Verify
            self.assertEqual(credentials_data, result)
