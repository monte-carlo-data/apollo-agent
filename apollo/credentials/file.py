import json

from apollo.credentials.base import BaseCredentialsService


class FileCredentialsService(BaseCredentialsService):
    """
    Credentials service that fetches credentials from a file.
    """

    def _load_external_credentials(self, credentials: dict) -> dict:
        file_path = credentials.get("file_path")
        if not file_path:
            raise ValueError("Missing expected file path in credentials")
        with open(file_path, "r") as f:
            return json.load(f)
