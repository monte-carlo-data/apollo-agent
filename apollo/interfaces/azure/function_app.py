import json
from typing import Dict

import azure.functions as func
import azure.durable_functions as df
from azure.durable_functions import (
    DurableOrchestrationContext,
    DurableOrchestrationClient,
    OrchestrationRuntimeStatus,
)
from azure.functions import WsgiMiddleware

from apollo.interfaces.generic import main

wsgi_middleware = WsgiMiddleware(main.app.wsgi_app)

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="async/api/v1/agent/execute/{connection_type}/{operation_name}")
@app.durable_client_input(client_name="client")
async def execute_async_operation(
    req: func.HttpRequest, client: DurableOrchestrationClient
):
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
    agent_response = main.execute_agent_operation(
        connection_type=body["connection_type"],
        operation_name=body["operation_name"],
        json_request=body["payload"],
    )
    return agent_response.result


@app.http_type(http_type="wsgi")
@app.route(route="/api/{*route}")
def agent_api(req: func.HttpRequest, context: func.Context):
    return wsgi_middleware.handle(req, context)
