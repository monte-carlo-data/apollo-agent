import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict

from azure.monitor.opentelemetry import configure_azure_monitor

from apollo.agent.env_vars import DEBUG_ENV_VAR
from apollo.interfaces.azure.log_context import AzureLogContext

# remove default handlers to prevent duplicate log messages
# https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-opentelemetry-readme?view=azure-python#logging-issues
root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

is_debug = os.getenv(DEBUG_ENV_VAR, "false").lower() == "true"
root_logger.setLevel(logging.DEBUG if is_debug else logging.INFO)

# configure the Azure Log Monitor, it gets the Instrumentation Key from APPINSIGHTS_INSTRUMENTATIONKEY env var
try:
    configure_azure_monitor()
except Exception as exc:
    root_logger.error(f"Failed to initialize logging: {exc}")

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
from azure.functions import WsgiMiddleware, AuthLevel

from apollo.interfaces.azure.azure_platform import AzurePlatformProvider
from apollo.interfaces.azure import main

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
    client_input = {
        "connection_type": connection_type,
        "operation_name": operation_name,
        "payload": req.get_json(),
    }
    instance_id = await client.start_new(
        "agent_operation_orchestrator", client_input=client_input
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
        "__mcd_status__": status.runtime_status.name
        if status.runtime_status
        else "unknown"
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


@app.orchestration_trigger(context_name="context")
def agent_operation_orchestrator(context: DurableOrchestrationContext):
    client_input = context.get_input()
    result = yield context.call_activity("agent_operation", client_input)
    return result


@app.activity_trigger(input_name="body")
def agent_operation(body: Dict):
    """
    Called by the Azure Durable Functions runtime to perform the operation.
    """
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
    try:
        return wsgi_middleware.handle(req, context)
    except Exception as exc:
        return func.HttpResponse(
            status_code=500,
            body=str(exc),
            headers={
                "Content-Type": "text/plain",
            },
        )


@app.function_name(name="cleanup_df_data")
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
    runtime_statuses = [
        OrchestrationRuntimeStatus.Canceled,
        OrchestrationRuntimeStatus.Completed,
        OrchestrationRuntimeStatus.Failed,
        OrchestrationRuntimeStatus.Terminated,
    ]

    logging.info(
        f"cleanup_durable_functions_data triggered, purging instances older than "
        f'{created_time_to.isoformat(timespec="seconds")}'
    )

    try:
        result = await client.purge_instance_history_by(
            created_time_from=created_time_from,
            created_time_to=created_time_to,
            runtime_status=runtime_statuses,
        )
        logging.info(f"Purge completed, deleted instances: {result.instances_deleted}")
    except Exception as ex:
        logging.error(f"Failed to purge Durable Functions data: {ex}")
