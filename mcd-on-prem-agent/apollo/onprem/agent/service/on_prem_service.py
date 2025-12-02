import logging
from typing import Dict, Any

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.integrations.storage.base_storage_client import BaseStorageClient
from apollo.credentials.factory import CredentialsFactory
from apollo.egress.agent.config.config_manager import ConfigurationManager
from apollo.egress.agent.service.base_egress_service import BaseEgressAgentService
from apollo.egress.agent.service.logs_service import LogsService
from apollo.egress.agent.service.storage_service import StorageService

logger = logging.getLogger(__name__)


class OnPremService(BaseEgressAgentService):
    def __init__(
        self,
        config_manager: ConfigurationManager,
        storage_client: BaseStorageClient,
        logging_utils: LoggingUtils,
    ):
        super().__init__(
            platform="OnPrem",
            service_name="On Prem",
            config_manager=config_manager,
            logs_service=LogsService(),
            storage_service=StorageService(
                client=storage_client,
            ),
        )
        self._agent = Agent(logging_utils)

    def _internal_execute_agent_operation(
        self, event: Dict[str, Any]
    ) -> Dict[str, Any]:
        credentials = self._extract_credentials_in_request(event.get("credentials", {}))
        operation = event.get("operation")
        path = event.get("path")
        if not path or not path.startswith("/api/v1/agent/execute/"):
            raise ValueError(f"Invalid path: {path}")
        connection_type, operation_name = path.split("/")[5:7]

        return self._agent.execute_operation(
            connection_type, operation_name, operation, credentials
        ).result

    @staticmethod
    def _extract_credentials_in_request(credentials: Dict) -> Dict:
        credential_service = CredentialsFactory.get_credentials_service(credentials)
        return credential_service.get_credentials(credentials)
