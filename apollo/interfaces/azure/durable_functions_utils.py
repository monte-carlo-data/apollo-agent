import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, cast, Optional, List

from azure.durable_functions.models import (
    DurableOrchestrationClient,
    OrchestrationRuntimeStatus,
)
from dataclasses_json import dataclass_json, DataClassJsonMixin

from apollo.interfaces.generic.utils import AgentPlatformUtils


@dataclass_json
@dataclass
class AzureDurableFunctionsRequest(DataClassJsonMixin):
    created_time_from: Optional[str] = None
    created_time_to: Optional[str] = None


@dataclass_json
@dataclass
class AzureDurableFunctionsCleanupRequest(AzureDurableFunctionsRequest):
    include_pending: Optional[bool] = False
    include_running: Optional[bool] = False


class AzureDurableFunctionsUtils:
    @classmethod
    async def cleanup_durable_functions_instances(
        cls,
        request: AzureDurableFunctionsCleanupRequest,
        client: DurableOrchestrationClient,
    ) -> int:
        """
        Cleanup Durable Functions data, body supports the following attributes:
        - created_time_from: the oldest instance to purge, default is 10 years ago
        - created_time_to: the newest instance to purge, default is 10 minutes ago
        - include_pending: whether to terminate and purge pending instances that were not executed
            yet, defaults to False.
        Returns the number of deleted instances.
        """
        created_time_from, created_time_to = cls._parse_created_times(
            request,
            default_created_from=datetime.now(timezone.utc) - timedelta(days=365 * 10),
            default_created_to=datetime.now(timezone.utc) - timedelta(minutes=10),
        )
        status_list: List[OrchestrationRuntimeStatus] = []
        if request.include_pending is not None and request.include_pending:
            status_list.append(OrchestrationRuntimeStatus.Pending)
        if request.include_running is not None and request.include_running:
            status_list.append(OrchestrationRuntimeStatus.Running)
        if status_list:
            terminated_instances = await cls._terminate_instances(
                client=client,
                created_time_from=created_time_from,
                created_time_to=created_time_to,
                status_list=status_list,
            )
        else:
            terminated_instances = 0
        purged_instances = await cls.purge_instances(
            client=client,
            created_time_from=created_time_from,
            created_time_to=created_time_to,
            status_list=status_list,
        )
        return terminated_instances + purged_instances

    @classmethod
    async def get_durable_functions_info(
        cls,
        request: AzureDurableFunctionsRequest,
        client: DurableOrchestrationClient,
    ) -> Dict[str, int]:
        created_time_from, created_time_to = cls._parse_created_times(
            request,
            default_created_from=datetime.now(timezone.utc) - timedelta(days=1),
            default_created_to=datetime.now(timezone.utc),
        )
        instances = await client.get_status_by(
            created_time_from=created_time_from,
            created_time_to=created_time_to,
            runtime_status=[
                OrchestrationRuntimeStatus.Completed,
                OrchestrationRuntimeStatus.Pending,
                OrchestrationRuntimeStatus.Running,
            ],
        )
        status_count: Dict[str, int] = defaultdict(lambda: 0)
        for i in instances:
            status_count[i.runtime_status.value] = status_count[i.runtime_status.value] + 1  # type: ignore
        return status_count

    @staticmethod
    async def _terminate_instances(
        client: DurableOrchestrationClient,
        created_time_from: datetime,
        created_time_to: datetime,
        status_list: List[OrchestrationRuntimeStatus],
    ) -> int:
        instances = await client.get_status_by(
            created_time_from=created_time_from,
            created_time_to=created_time_to,
            runtime_status=status_list,
        )
        terminated_instances = 0
        for instance in instances:
            if not instance.instance_id:
                logging.warning("instance_id not found, skipping termination")
                continue
            try:
                await client.terminate(instance.instance_id, reason="Agent Cleanup")
                terminated_instances += 1
            except Exception as ex:
                logging.error(
                    f"Failed to terminate instance: {instance.instance_id}: {ex}"
                )
        return terminated_instances

    @staticmethod
    async def purge_instances(
        client: DurableOrchestrationClient,
        created_time_from: datetime,
        created_time_to: datetime,
        status_list: List[OrchestrationRuntimeStatus],
    ) -> int:
        runtime_statuses = [
            OrchestrationRuntimeStatus.Canceled,
            OrchestrationRuntimeStatus.Completed,
            OrchestrationRuntimeStatus.Failed,
            OrchestrationRuntimeStatus.Terminated,
        ]
        runtime_statuses.extend(status_list)

        logging.info(
            f'Purging instances older than {created_time_to.isoformat(timespec="seconds")}'
            f", including: {status_list}"
        )

        try:
            result = await client.purge_instance_history_by(
                created_time_from=created_time_from,
                created_time_to=created_time_to,
                runtime_status=runtime_statuses,
            )
            logging.info(
                f"Purge completed, deleted instances: {result.instances_deleted}"
            )
            return result.instances_deleted
        except Exception as ex:
            logging.error(f"Failed to purge Durable Functions data: {ex}")
            return -1

    @staticmethod
    def _parse_created_times(
        request: AzureDurableFunctionsRequest,
        default_created_from: datetime,
        default_created_to: datetime,
    ) -> Tuple[datetime, datetime]:
        created_time_from_str = request.created_time_from
        created_time_to_str = request.created_time_to

        created_time_from = cast(
            datetime,
            AgentPlatformUtils.parse_datetime(
                created_time_from_str, default_created_from
            ),
        )
        created_time_to = cast(
            datetime,
            AgentPlatformUtils.parse_datetime(created_time_to_str, default_created_to),
        )
        return created_time_from, created_time_to
