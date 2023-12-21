import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, cast

import boto3

from apollo.agent.constants import PLATFORM_AWS
from apollo.agent.env_vars import (
    CLOUDWATCH_LOG_GROUP_ID_ENV_VAR,
    AGENT_WRAPPER_TYPE_ENV_VAR,
    WRAPPER_TYPE_CLOUDFORMATION,
)
from apollo.agent.models import AgentConfigurationError
from apollo.agent.agent_platform import AgentPlatformProvider
from apollo.agent.updater import AgentUpdater
from apollo.interfaces.generic.utils import AgentPlatformUtils
from apollo.interfaces.lambda_function.cf_updater import LambdaCFUpdater
from apollo.interfaces.lambda_function.cf_utils import CloudFormationUtils
from apollo.interfaces.lambda_function.direct_updater import LambdaDirectUpdater


class AwsPlatformProvider(AgentPlatformProvider):
    """
    AWS Platform provider that supports:
    - Access to CloudWatch logs.
    - CloudFormation Updater if MCD_AGENT_WRAPPER_TYPE env var is "CLOUDFORMATION"
    - Direct Updater if MCD_AGENT_WRAPPER_TYPE env var is not "CLOUDFORMATION".
    """

    _epoch = datetime.utcfromtimestamp(0).astimezone(timezone.utc)

    @property
    def platform_info(self) -> Dict:
        return {}

    @property
    def platform(self) -> str:
        return PLATFORM_AWS

    @property
    def is_cloudformation(self) -> bool:
        wrapper_type = os.getenv(AGENT_WRAPPER_TYPE_ENV_VAR)
        return wrapper_type == WRAPPER_TYPE_CLOUDFORMATION

    @property
    def updater(self) -> AgentUpdater:
        return LambdaCFUpdater() if self.is_cloudformation else LambdaDirectUpdater()

    def get_infra_details(self) -> Dict:
        """
        Returns a dictionary with infrastructure information, the dictionary contains the following attributes:
        - template: the TemplateBody from the CloudFormation template. Only returned when CloudFormation is in use.
        - parameters: the "Parameters" attribute from the CloudFormation stack details if CloudFormation is in use, if
            not it returns the value for MemorySize and ConcurrentExecutions from the lambda settings.
        """
        if self.is_cloudformation:
            return CloudFormationUtils.get_infra_details()
        else:
            return LambdaDirectUpdater.get_infra_details()

    def filter_log_events(
        self,
        pattern: Optional[str],
        start_time_str: Optional[str],
        end_time_str: Optional[str],
        limit: int,
    ) -> Dict:
        """
        Returns a dictionary with an "events" attribute containing all events returned by CloudWatch
        with the specified restrictions.
        """
        log_group_arn = os.getenv(CLOUDWATCH_LOG_GROUP_ID_ENV_VAR)
        if not log_group_arn:
            raise AgentConfigurationError(
                f"Missing {CLOUDWATCH_LOG_GROUP_ID_ENV_VAR} environment variable"
            )

        start_time = AgentPlatformUtils.parse_datetime(
            start_time_str, datetime.now(timezone.utc) - timedelta(minutes=10)
        )
        end_time = AgentPlatformUtils.parse_datetime(end_time_str)

        filter_params = {
            "logGroupIdentifier": log_group_arn,
            "limit": limit,
        }
        if pattern:
            filter_params["filterPattern"] = pattern
        if start_time:
            filter_params["startTime"] = self._millis_since_1970(start_time)
        if end_time:
            filter_params["endTime"] = self._millis_since_1970(end_time)

        logs_client = boto3.client("logs")
        all_events: List[Dict] = []
        while len(all_events) < limit:
            result = logs_client.filter_log_events(**filter_params)
            events = result.get("events")
            if events:
                all_events.extend(events)
            next_token = result.get("nextToken")
            if next_token:
                filter_params["nextToken"] = next_token
            else:
                break
        return {
            "events": all_events,
        }

    def start_logs_query(
        self,
        query: str,
        start_time_str: Optional[str],
        end_time_str: Optional[str],
        limit: int,
    ) -> Dict:
        """
        Returns a dictionary with a "query_id" with the ID of the query, results can be obtained using
        get_logs_query_results.
        """
        log_group_arn = os.getenv(CLOUDWATCH_LOG_GROUP_ID_ENV_VAR)
        if not log_group_arn:
            raise AgentConfigurationError(
                f"Missing {CLOUDWATCH_LOG_GROUP_ID_ENV_VAR} environment variable"
            )

        start_time = cast(
            datetime,
            AgentPlatformUtils.parse_datetime(
                start_time_str, datetime.now(timezone.utc) - timedelta(minutes=10)
            ),
        )
        end_time: datetime = cast(
            datetime,
            AgentPlatformUtils.parse_datetime(end_time_str, datetime.now(timezone.utc)),
        )

        start_query_params = {
            "queryString": query,
            "logGroupIdentifiers": [log_group_arn],
            "limit": limit,
            "startTime": self._millis_since_1970(start_time),
            "endTime": self._millis_since_1970(end_time),
        }

        logs_client = boto3.client("logs")
        result = logs_client.start_query(**start_query_params)
        return {
            "query_id": result.get("queryId"),
        }

    def stop_logs_query(
        self,
        query_id: str,
    ) -> Dict:
        """
        Stops the query with the given ID previously started with `start_logs_query`.
        Returns a dictionary with a single boolean attribute "success", for more information see:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs/client/stop_query.html
        """
        logs_client = boto3.client("logs")
        result = logs_client.stop_query(queryId=query_id)
        return result

    def get_logs_query_results(
        self,
        query_id: str,
    ) -> Dict:
        """
        Returns a dictionary with:
         - an "events" attribute containing the "results" field returned by
            CloudWatchLogs.Client.get_query_results with the specified query ID.
         - a "status" attribute as returned by `get_query_results`, one of: Scheduled, Running, Complete,
            Failed, Cancelled, Timeout, Unknown
        """
        logs_client = boto3.client("logs")
        result = logs_client.get_query_results(queryId=query_id)
        events = result.get("results") or []
        # each result is an array of fields containing "field" and "value", convert that into a regular dictionary
        return {
            "events": [
                {
                    log_field.get("field"): log_field.get("value")
                    for log_field in log_fields
                }
                for log_fields in events
            ],
            "status": result.get("status"),
        }

    @classmethod
    def _millis_since_1970(cls, dt: datetime) -> int:
        return int((dt - cls._epoch).total_seconds() * 1000)
