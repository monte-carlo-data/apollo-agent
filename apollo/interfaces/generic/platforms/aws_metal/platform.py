import os
from typing import Dict, Optional

import logging

from apollo.agent.constants import PLATFORM_AWS_METAL
from apollo.agent.env_vars import (
    STORAGE_BUCKET_NAME_ENV_VAR,
    GUNICORN_WORKERS_ENV_VAR,
    GUNICORN_THREADS_ENV_VAR,
    GUNICORN_TIMEOUT_ENV_VAR,
    MCD_AGENT_CLOUD_PLATFORM_ENV_VAR,
)
from apollo.agent.models import AgentConfigurationError
from apollo.interfaces.lambda_function.platform import AwsPlatformProvider

logger = logging.getLogger(__name__)


class AwsMetalPlatformProvider(AwsPlatformProvider):
    """
    AWS Metal Platform provider
    """

    def __init__(self, platform_info: Optional[Dict] = None, **kwargs):  # type: ignore
        super().__init__(**kwargs)
        self._platform_info = platform_info or {
            STORAGE_BUCKET_NAME_ENV_VAR: os.getenv(STORAGE_BUCKET_NAME_ENV_VAR),
            GUNICORN_WORKERS_ENV_VAR: os.getenv(GUNICORN_WORKERS_ENV_VAR),
            GUNICORN_THREADS_ENV_VAR: os.getenv(GUNICORN_THREADS_ENV_VAR),
            GUNICORN_TIMEOUT_ENV_VAR: os.getenv(GUNICORN_TIMEOUT_ENV_VAR),
            MCD_AGENT_CLOUD_PLATFORM_ENV_VAR: os.getenv(
                MCD_AGENT_CLOUD_PLATFORM_ENV_VAR
            ),
        }

    @property
    def platform_info(self) -> Dict:
        return self._platform_info

    @property
    def platform(self) -> str:
        return PLATFORM_AWS_METAL

    @property
    def updater(self) -> None:
        # automatic updates are not supported for AWS Metal deployments
        return None

    def get_infra_details(self) -> Dict:
        return {}

    def filter_log_events(
        self,
        pattern: Optional[str],
        start_time_str: Optional[str],
        end_time_str: Optional[str],
        limit: int,
    ) -> Dict | None:
        try:
            return super().filter_log_events(
                pattern, start_time_str, end_time_str, limit
            )
        except AgentConfigurationError:
            # if env var is not set, log collection is disabled for AWS Metal
            logger.info("Log collection is disabled for this agent")
            return None

    def start_logs_query(
        self,
        query: str,
        start_time_str: Optional[str],
        end_time_str: Optional[str],
        limit: int,
    ) -> Dict | None:
        """
        Returns a dictionary with a "query_id" with the ID of the query, results can be obtained using
        get_logs_query_results.
        """
        try:
            return super().start_logs_query(query, start_time_str, end_time_str, limit)
        except AgentConfigurationError:
            # if env var is not set, log collection is disabled for AWS Metal
            logger.info("Log collection is disabled for this agent")
            return None
