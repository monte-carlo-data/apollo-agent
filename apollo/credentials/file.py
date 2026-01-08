import json
import logging

from apollo.credentials.base import BaseCredentialsService

logger = logging.getLogger(__name__)


class FileCredentialsService(BaseCredentialsService):
    """
    Credentials service that fetches credentials from a file.
    """

    def _load_external_credentials(self, credentials: dict) -> dict:
        file_path = credentials.get("file_path")
        if not file_path:
            raise ValueError("Missing expected file path in credentials")
        with open(file_path, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError as je:
                logger.error(f"Invalid JSON in file: {file_path}: {je}", exc_info=True)
                raise ValueError(
                    f"Invalid JSON in credentials file: {file_path} ({je})"
                )
            except Exception as e:
                logger.error(f"Error reading file: {file_path}: {e}", exc_info=True)
                raise ValueError(f"Error reading credentials file: {file_path} ({e})")
