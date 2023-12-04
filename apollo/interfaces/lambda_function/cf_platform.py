import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List

import boto3

from apollo.agent.constants import PLATFORM_AWS
from apollo.agent.env_vars import CLOUDFORMATION_LOG_GROUP_ID_ENV_VAR
from apollo.agent.models import AgentConfigurationError
from apollo.agent.platform import AgentPlatformProvider
from apollo.agent.updater import AgentUpdater
from apollo.interfaces.lambda_function.cf_updater import LambdaCFUpdater
from apollo.interfaces.lambda_function.cf_utils import CloudFormationUtils


class CFPlatformProvider(AgentPlatformProvider):
    _epoch = datetime.utcfromtimestamp(0).astimezone(timezone.utc)

    @property
    def platform_info(self) -> Dict:
        return {}

    @property
    def platform(self) -> str:
        return PLATFORM_AWS

    @property
    def updater(self) -> AgentUpdater:
        return LambdaCFUpdater()

    def get_infra_details(self) -> Dict:
        """
        Returns a dictionary with infrastructure information, containing the following attributes:
        - template: the TemplateBody from the CloudFormation template.
        - parameters: the "Parameters" attribute from the CloudFormation stack details.
        """
        client = CloudFormationUtils.get_cloudformation_client()
        stack_id = CloudFormationUtils.get_stack_id()

        template = client.get_template(StackName=stack_id).get("TemplateBody")
        parameters = CloudFormationUtils.get_stack_parameters(client)
        return {
            "template": template,
            "parameters": parameters,
        }

    @classmethod
    def millis_since_1970(cls, dt: datetime) -> int:
        return int((dt - cls._epoch).total_seconds() * 1000)

    @classmethod
    def parse_datetime(
        cls, dt_str: Optional[str], default_value: Optional[datetime] = None
    ) -> Optional[datetime]:
        if not dt_str:
            return default_value
        dt = datetime.fromisoformat(dt_str)
        if not dt.tzinfo:
            dt = dt.astimezone(timezone.utc)  # make it offset-aware
        return dt

    def filter_log_events(
        self,
        pattern: Optional[str],
        start_time_str: Optional[str],
        end_time_str: Optional[str],
        limit: int,
    ) -> List[Dict]:
        log_group_arn = os.getenv(CLOUDFORMATION_LOG_GROUP_ID_ENV_VAR)
        if not log_group_arn:
            raise AgentConfigurationError(
                f"Missing {CLOUDFORMATION_LOG_GROUP_ID_ENV_VAR} environment variable"
            )

        start_time = self.parse_datetime(
            start_time_str, datetime.now(timezone.utc) - timedelta(minutes=10)
        )
        end_time = self.parse_datetime(end_time_str)

        filter_params = {
            "logGroupIdentifier": log_group_arn,
            "limit": limit,
        }
        if pattern:
            filter_params["filterPattern"] = pattern
        if start_time:
            filter_params["startTime"] = self.millis_since_1970(start_time)
        if end_time:
            filter_params["endTime"] = self.millis_since_1970(end_time)

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
        return all_events
