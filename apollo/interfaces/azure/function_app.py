import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, cast

from azure.durable_functions.models.Task import TimerTask
from azure.monitor.opentelemetry import configure_azure_monitor

from apollo.agent.constants import ATTRIBUTE_NAME_ERROR
from apollo.agent.env_vars import (
    DEBUG_ENV_VAR,
    ORCHESTRATION_ACTIVITY_TIMEOUT_ENV_VAR,
    ORCHESTRATION_ACTIVITY_TIMEOUT_DEFAULT_VALUE,
)
from apollo.interfaces.azure.durable_functions_utils import (
    AzureDurableFunctionsUtils,
    AzureDurableFunctionsRequest,
    AzureDurableFunctionsCleanupRequest,
)
from apollo.interfaces.azure.log_context import AzureLogContext

# remove default handlers to prevent duplicate log messages
# https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-opentelemetry-readme?view=azure-python#logging-issues
root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

is_debug = os.getenv(DEBUG_ENV_VAR, "false").lower() == "true"
root_logger.setLevel(logging.DEBUG if is_debug else logging.INFO)

# configure the Azure Log Monitor, it gets the Instrumentation Key
# from APPINSIGHTS_INSTRUMENTATIONKEY env var
configure_azure_monitor()

# configure the log context to include the agent context in all log messages
log_context = AzureLogContext()
log_context.install()

# disable annoying logs every time OT logs are sent
disable_loggers = [
    "azure.monitor.opentelemetry.exporter.export._base",
    "azure.core.pipeline.policies",
]
for logger_name in disable_loggers:
    logging.getLogger(logger_name).setLevel(logging.ERROR)

# intentionally imported here after log is initialized
import azure.functions as func
import azure.durable_functions as df
from azure.durable_functions import (
    DurableOrchestrationContext,
    DurableOrchestrationClient,
    OrchestrationRuntimeStatus,
)
from azure.functions import WsgiMiddleware

from apollo.interfaces.azure.azure_platform import AzurePlatformProvider
from apollo.interfaces.azure import main

_ACTIVITY_TIMEOUT_SECONDS = int(
    os.getenv(
        ORCHESTRATION_ACTIVITY_TIMEOUT_ENV_VAR,
        ORCHESTRATION_ACTIVITY_TIMEOUT_DEFAULT_VALUE,
    )
)
main.agent.platform_provider = AzurePlatformProvider()
main.agent.log_context = log_context
wsgi_middleware = WsgiMiddleware(main.app.wsgi_app)

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="async/api/v1/agent/execute/{connection_type}/{operation_name}")
@app.durable_client_input(client_name="client")
async def execute_async_operation(
    req: func.HttpRequest, client: DurableOrchestrationClient
):
    """
    Entry point for triggering async operations, please note the path is the same but prefixed with async.
    It returns the id for the request in the attribute "__mcd_request_id__", this id can be be used to check
    the status with the `async/api/v1/status` endpoint.
    """
    connection_type = req.route_params.get("connection_type")
    operation_name = req.route_params.get("operation_name")
    payload = req.get_json()
    client_input = {
        "connection_type": connection_type,
        "operation_name": operation_name,
        "payload": payload,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    instance_id = await client.start_new(
        "agent_operation_orchestrator", client_input=client_input
    )
    trace_id = payload.get("operation", {}).get("trace_id")
    root_logger.info(
        f"Started async operation: {instance_id}",
        extra={"instance_id": instance_id, "mcd_trace_id": trace_id},
    )

    response_payload = {
        "__mcd_request_id__": instance_id,
    }
    return func.HttpResponse(
        status_code=202,
        body=json.dumps(response_payload),
        headers={
            "Content-Type": "application/json",
        },
    )


@app.route(route="async/api/v1/status/{instance_id}")
@app.durable_client_input(client_name="client")
async def get_async_operation_status(
    req: func.HttpRequest, client: DurableOrchestrationClient
):
    """
    Uses the Azure Durable Functions runtime to get the status for a given request.
    The only required path parameter is "instance_id" that is expected to be the value returned by a
    request to `async/api/v1/agent/execute` in __mcd_request_id__.
    """
    instance_id = req.route_params.get("instance_id", "")
    status = await client.get_status(instance_id=instance_id)
    response_payload = {
        "__mcd_status__": (
            status.runtime_status.name if status.runtime_status else "unknown"
        ),
        "__mcd_created__": (
            status.created_time.isoformat() if status.created_time else None
        ),
        "__mcd_last_updated__": (
            status.last_updated_time.isoformat() if status.last_updated_time else None
        ),
    }
    if status.runtime_status == OrchestrationRuntimeStatus.Completed and status.output:
        if isinstance(status.output, Dict):
            response_payload.update(status.output)
        else:
            response_payload["__mcd_result__"] = status.output

    return func.HttpResponse(
        status_code=200,
        body=json.dumps(response_payload),
        headers={
            "Content-Type": "application/json",
        },
    )


@app.route(route="async/api/v1/cleanup")
@app.durable_client_input(client_name="client")
async def cleanup_durable_functions_instances(
    req: func.HttpRequest, client: DurableOrchestrationClient
):
    """
    Endpoint to manually cleanup Durable Functions data, use a POST sending a JSON body with:
    - created_time_from: the oldest instance to purge, default is 10 years ago
    - created_time_to: the newest instance to purge, default is 10 minutes ago
    - include_pending: whether to purge pending instances that were not executed yet,
        defaults to False
    """
    deleted_instances = (
        await AzureDurableFunctionsUtils.cleanup_durable_functions_instances(
            request=AzureDurableFunctionsCleanupRequest.from_dict(req.get_json()),
            client=client,
        )
    )
    return func.HttpResponse(
        status_code=200,
        body=json.dumps(
            {
                "deleted_instances": deleted_instances,
            }
        ),
        headers={
            "Content-Type": "application/json",
        },
    )


@app.route(route="async/api/v1/queue/info")
@app.durable_client_input(client_name="client")
async def get_durable_functions_info(
    req: func.HttpRequest, client: DurableOrchestrationClient
):
    """
    Endpoint that returns information about the instances completed and pending during
     the specified period, use a POST sending a JSON body with:
    - created_time_from: the oldest instance to purge, default is 10 years ago
    - created_time_to: the newest instance to purge, default is 10 minutes ago
    """
    pending_instances, completed_instances = (
        await AzureDurableFunctionsUtils.get_durable_functions_info(
            request=AzureDurableFunctionsRequest.from_dict(req.get_json()),
            client=client,
        )
    )
    return func.HttpResponse(
        status_code=200,
        body=json.dumps(
            {
                "pending_instances": pending_instances,
                "completed_instances": completed_instances,
            }
        ),
        headers={
            "Content-Type": "application/json",
        },
    )


@app.orchestration_trigger(context_name="context")
def agent_operation_orchestrator(context: DurableOrchestrationContext):
    client_input = context.get_input()
    if isinstance(client_input, Dict):
        log_extra = {
            "mcd_trace_id": client_input.get("payload", {})
            .get("operation", {})
            .get("trace_id"),
            "operation_name": client_input.get("operation_name"),
            "connection_type": client_input.get("connection_type"),
        }
    else:
        log_extra = {}
    log_extra["instance_id"] = context.instance_id
    log_extra["timeout"] = _ACTIVITY_TIMEOUT_SECONDS

    root_logger.info(
        f"Running orchestrator for operation: {context.instance_id}", extra=log_extra
    )
    deadline = context.current_utc_datetime + timedelta(
        seconds=_ACTIVITY_TIMEOUT_SECONDS
    )
    activity_task = context.call_activity("agent_operation", client_input)
    timeout_task: TimerTask = cast(TimerTask, context.create_timer(deadline))

    # "Abandon" feature, the activity is abandoned after 14:45 minutes so it is not retried
    # by the Durable Functions framework. Based on Azure docs:
    # https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-error-handling?tabs=python#function-timeouts
    winner = yield context.task_any([activity_task, timeout_task])
    if winner == activity_task:
        timeout_task.cancel()
        return activity_task.result

    root_logger.info(
        f"Orchestrator activity: {context.instance_id} timed out", extra=log_extra
    )
    return {
        ATTRIBUTE_NAME_ERROR: f"Activity timed out after {_ACTIVITY_TIMEOUT_SECONDS} seconds."
    }


@app.activity_trigger(input_name="body")
def agent_operation(body: Dict):
    """
    Called by the Azure Durable Functions runtime to perform the operation.
    """
    # first check how long the activity has been waiting to be executed
    # it doesn't make sense to start running a task when nobody is waiting for its result
    log_extra = {
        "mcd_trace_id": body.get("payload", {}).get("operation", {}).get("trace_id"),
        "operation_name": body.get("operation_name"),
        "connection_type": body.get("connection_type"),
    }
    timestamp_str = body.get("timestamp")
    if timestamp_str:
        timestamp = datetime.fromisoformat(timestamp_str)
        seconds_since_triggered = (
            datetime.now(timezone.utc) - timestamp
        ).total_seconds()
        if seconds_since_triggered > _ACTIVITY_TIMEOUT_SECONDS:
            root_logger.warning(
                f"Activity expired after {seconds_since_triggered} seconds.",
                extra=log_extra,
            )
            return {
                ATTRIBUTE_NAME_ERROR: f"Activity expired after {seconds_since_triggered} seconds."
            }
    else:
        root_logger.warning("No timestamp in orchestrator request", extra=log_extra)

    agent_response = main.execute_agent_operation(
        connection_type=body["connection_type"],
        operation_name=body["operation_name"],
        json_request=body["payload"],
    )
    return agent_response.result


@app.http_type(http_type="wsgi")
@app.route(route="/api/{*route}")
def agent_api(req: func.HttpRequest, context: func.Context):
    """
    Endpoint to execute sync operations.
    """
    return wsgi_middleware.handle(req, context)


@app.schedule(
    schedule="0 0 3,15 * * *", arg_name="timer", run_on_startup=False
)  # run every day at 3 AM/PM
@app.durable_client_input(client_name="client")
async def cleanup_durable_functions_data(
    timer: func.TimerRequest, client: DurableOrchestrationClient
) -> None:
    created_time_from = datetime.now(timezone.utc) - timedelta(
        days=365 * 10
    )  # datetime.min or None not supported
    created_time_to = datetime.now(timezone.utc) - timedelta(days=1)
    await AzureDurableFunctionsUtils.purge_instances(
        client, created_time_from, created_time_to, include_pending=False
    )
