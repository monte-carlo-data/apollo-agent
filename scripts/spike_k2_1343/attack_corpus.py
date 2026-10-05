"""
K2-1343 spike: scripts that try to get a disallowed AWS call past the pre-run
check. Shared by the local checker tests and the live run (which shows what the
server's ``api_calls`` reports for the ones that execute).

{name: (code, expect_checker_accepts)}
"""

LOG_GROUP = "/k2-1343-spike/app"

ALLOWED = [("logs", "FilterLogEvents")]

_OK_CALL = (
    'await call_boto3(service_name="logs", operation_name="FilterLogEvents", '
    f'params={{"logGroupName": "{LOG_GROUP}", "limit": 5}})'
)

CORPUS = {
    "baseline_allowed": (
        f"r = {_OK_CALL}\nresult = {{'n': len(r.get('events', []))}}\nresult",
        True,
    ),
    "computed_operation": (
        'op = "Get" + "SecretValue"\n'
        'result = await call_boto3(service_name="secretsmanager", operation_name=op, params={"SecretId": "x"})\nresult',
        False,
    ),
    "fstring_operation": (
        'v = "Secret"\nresult = await call_boto3(service_name="secretsmanager", '
        'operation_name=f"Get{v}Value", params={"SecretId": "x"})\nresult',
        False,
    ),
    "alias_assignment": (
        'f = call_boto3\nresult = await f(service_name="sts", operation_name="GetCallerIdentity")\nresult',
        False,
    ),
    "alias_via_list": (
        'result = await [call_boto3][0](service_name="sts", operation_name="GetCallerIdentity")\nresult',
        False,
    ),
    "alias_via_lambda": (
        'g = lambda **k: call_boto3(**k)\nresult = await g(service_name="sts", operation_name="GetCallerIdentity")\nresult',
        False,
    ),
    "kwargs_splat": (
        'k = {"service_name": "sts", "operation_name": "GetCallerIdentity"}\n'
        "result = await call_boto3(**k)\nresult",
        False,
    ),
    "globals_lookup": (
        'result = await globals()["call_boto3"](service_name="sts", operation_name="GetCallerIdentity")\nresult',
        False,
    ),
    "getattr_builtins": (
        'b = getattr(asyncio, "__builtins__")\nresult = str(type(b))\nresult',
        False,
    ),
    "dunder_class_walk": (
        "result = [c.__name__ for c in ().__class__.__base__.__subclasses__()][:5]\nresult",
        False,
    ),
    "aws_mcp_builtin_search": (
        'result = await aws_mcp.search_functions("read secrets manager secret")\nresult',
        False,
    ),
    "aws_mcp_help": ("import aws_mcp\nresult = str(help(aws_mcp))\nresult", False),
    "import_os": ("import os\nresult = dict(os.environ)\nresult", False),
    "snake_case_operation": (
        'result = await call_boto3(service_name="logs", operation_name="filter_log_events", '
        f'params={{"logGroupName": "{LOG_GROUP}", "limit": 1}})\nresult',
        False,
    ),
    "shadow_parameter": (
        "async def run(call_boto3):\n"
        '    return await call_boto3(service_name="sts", operation_name="GetCallerIdentity")\n'
        "result = 1\nresult",
        False,
    ),
    "positional_args": (
        'result = await call_boto3("sts", "GetCallerIdentity")\nresult',
        False,
    ),
    "string_exec": ('exec("x=1")\nresult = 1\nresult', False),
    "disallowed_literal": (
        'result = await call_boto3(service_name="sts", operation_name="GetCallerIdentity")\nresult',
        False,
    ),
}
