import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, cast

import boto3
import logging

from apollo.agent.constants import PLATFORM_AWS_METAL
from apollo.agent.env_vars import (
    CLOUDWATCH_LOG_GROUP_ID_ENV_VAR,
    AGENT_WRAPPER_TYPE_ENV_VAR,
    WRAPPER_TYPE_CLOUDFORMATION,
)
from apollo.agent.models import AgentConfigurationError
from apollo.agent.platform import AgentPlatformProvider
from apollo.interfaces.lambda_function.platform import AwsPlatformProvider
from apollo.agent.updater import AgentUpdater
from apollo.interfaces.generic.utils import AgentPlatformUtils

logger = logging.getLogger(__name__)


class AwsMetalPlatformProvider(AwsPlatformProvider):
    """
    AWS Metal Platform provider
    """
    def __init__(self, **kwargs): # type: ignore
        super().__init__(**kwargs)

    _epoch = datetime.utcfromtimestamp(0).astimezone(timezone.utc)

    @property
    def platform_info(self) -> Dict:
        # TODO
        return {}

    @property
    def platform(self) -> str:
        return PLATFORM_AWS_METAL


    @property
    def updater(self) -> None:
        # automatic updates are not supported for AWS Metal deployments
        return None

    def get_infra_details(self) -> Dict:
        # TODO
        return {}

    def filter_log_events(
        self,
        pattern: Optional[str],
        start_time_str: Optional[str],
        end_time_str: Optional[str],
        limit: int,
    ) -> Dict | None:
        try:
            return super().filter_log_events(pattern, start_time_str, end_time_str, limit)
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
