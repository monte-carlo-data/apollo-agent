"""
K2-1343 spike: provision (and ``--teardown``) the approved dev resources.

Accounts: dev 404798114945 (profile "dev"), dev-eu 378842099257 (profile "dev-eu").
Every resource is tagged k2-1343-spike=true, owner=ghynson, cleanup-after=2026-10-19.

Usage:
  python provision.py iam|data|lambdas|xacct|policy <variant>|all
  python provision.py --teardown
"""

import io
import json
import secrets
import sys
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import boto3
from botocore.exceptions import ClientError

ACCT = "404798114945"
XACCT = "378842099257"
REGION = "us-west-2"
REGION2 = "us-east-1"
XREGION = "eu-west-1"
SSO_ROLE_ARN = (
    f"arn:aws:iam::{ACCT}:role/aws-reserved/sso.amazonaws.com/"
    "AWSReservedSSO_dev-eng-dev_0cc27c55c4d00d4b"
)

EXEC_ROLE = "k2-1343-spike-agent-exec"
NARROW_ROLE = "k2-1343-spike-mcp-narrow"
DECOY_ROLE = "k2-1343-spike-decoy"
XACCT_ROLE = "k2-1343-spike-mcp-narrow-xacct"
BUCKET = f"k2-1343-spike-agent-store-{ACCT}"
BUCKET_KEY = "spike/dummy.txt"
SECRET = "k2-1343-spike/test-secret"
LOG_GROUP = "/k2-1343-spike/app"
HARNESS = "k2-1343-spike-mcp-harness"
ECHO = "k2-1343-spike-echo-mcp"
# dev agent mcd-agent-service-080291fd networking (read only, reused as-is)
SUBNETS = [
    "subnet-0e4aa3e786322ce3b",
    "subnet-079d501d2fbe1688d",
    "subnet-065f19aa51c816f55",
]
SECURITY_GROUPS = ["sg-0911b2fbbb317b05f"]

TAGS = {"k2-1343-spike": "true", "owner": "ghynson", "cleanup-after": "2026-10-19"}
MCD_TAGS = {**TAGS, "MonteCarloData": ""}
BUILD = Path(__file__).parent / "build"

dev = boto3.Session(profile_name="dev")
dev_eu = boto3.Session(profile_name="dev-eu")


def tag_list(tags: Dict[str, str]) -> List[Dict[str, str]]:
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def arn(name: str, acct: str = ACCT) -> str:
    return f"arn:aws:iam::{acct}:role/{name}"


def log_group_arn(region: str, acct: str = ACCT) -> str:
    return f"arn:aws:logs:{region}:{acct}:log-group:{LOG_GROUP}:*"


def put_role(
    iam: Any,
    name: str,
    trust: Dict,
    tags: Dict[str, str],
    inline: Optional[Dict[str, Dict]] = None,
    managed: Sequence[str] = (),
) -> None:
    try:
        iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=json.dumps(trust),
            Tags=tag_list(tags),
            MaxSessionDuration=3600,
            Description="K2-1343 spike (temporary)",
        )
        print("created role", name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
        iam.update_assume_role_policy(RoleName=name, PolicyDocument=json.dumps(trust))
        iam.tag_role(RoleName=name, Tags=tag_list(tags))
    for policy_name, doc in (inline or {}).items():
        iam.put_role_policy(
            RoleName=name, PolicyName=policy_name, PolicyDocument=json.dumps(doc)
        )
    for policy_arn in managed:
        iam.attach_role_policy(RoleName=name, PolicyArn=policy_arn)


def stmt(
    actions: List[str],
    resources: List[str],
    effect: str = "Allow",
    condition: Optional[Dict] = None,
    sid: Optional[str] = None,
) -> Dict[str, Any]:
    s: Dict[str, Any] = {"Effect": effect, "Action": actions, "Resource": resources}
    if condition:
        s["Condition"] = condition
    if sid:
        s["Sid"] = sid
    return s


def doc(*statements: Dict) -> Dict:
    return {"Version": "2012-10-17", "Statement": list(statements)}


def trust_principals(principal_arns: List[str], acct: str = ACCT) -> Dict:
    return doc(
        {
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{acct}:root"},
            "Action": "sts:AssumeRole",
            "Condition": {"ArnLike": {"aws:PrincipalArn": principal_arns}},
        }
    )


# ---- narrow role policy variants (applied step by step) ----------------------

LOGS_READ = stmt(
    ["logs:FilterLogEvents"],
    [log_group_arn(REGION), log_group_arn(REGION2)],
    sid="LogsRead",
)
ECS_METRICS = [
    stmt(["cloudwatch:GetMetricData"], ["*"], sid="MetricsRead"),
    stmt(
        ["ecs:DescribeServices"],
        [f"arn:aws:ecs:{REGION2}:{ACCT}:service/gene-mcd-agent/gene-mcd-agent"],
        sid="EcsRead",
    ),
]
INSIGHTS_SCOPED = stmt(
    ["logs:StartQuery", "logs:GetQueryResults"],
    [log_group_arn(REGION)],
    sid="InsightsScoped",
)
# Deny anything that arrives through the AWS MCP Server except the enabled reads.
MCP_DENY = stmt(
    ["*"],
    ["*"],
    effect="Deny",
    sid="DenyNonReadViaAwsMcp",
    condition={"Bool": {"aws:ViaAWSMCPService": "true"}},
)
MCP_DENY["NotAction"] = MCP_DENY.pop("Action")
MCP_DENY["NotAction"] = [
    "logs:FilterLogEvents",
    "cloudwatch:GetMetricData",
    "ecs:DescribeServices",
]

POLICY_VARIANTS = {
    "logs": [LOGS_READ],
    "logs+deny": [LOGS_READ, MCP_DENY],
    "logs+ecs+deny": [LOGS_READ, *ECS_METRICS, MCP_DENY],
    "logs+insights": [LOGS_READ, INSIGHTS_SCOPED],
    # identity policy allows Insights, the MCP Deny must still block it
    "logs+insights+deny": [LOGS_READ, INSIGHTS_SCOPED, MCP_DENY],
}


def set_narrow_policy(variant: str) -> None:
    iam = dev.client("iam")
    iam.put_role_policy(
        RoleName=NARROW_ROLE,
        PolicyName="spike-narrow",
        PolicyDocument=json.dumps(doc(*POLICY_VARIANTS[variant])),
    )
    print("narrow policy ->", variant)


def iam_roles():
    iam = dev.client("iam")
    put_role(
        iam,
        EXEC_ROLE,
        doc(
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ),
        TAGS,
        inline={
            # same statement as the dev agent's execution role
            "assume_role_policy": doc(
                stmt(
                    ["sts:AssumeRole"],
                    ["*"],
                    condition={"StringEquals": {"iam:ResourceTag/MonteCarloData": ""}},
                )
            ),
            "s3_policy": doc(
                stmt(
                    [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                    ],
                    [f"arn:aws:s3:::{BUCKET}", f"arn:aws:s3:::{BUCKET}/*"],
                )
            ),
            "secrets_policy": doc(
                stmt(
                    ["secretsmanager:GetSecretValue"],
                    [f"arn:aws:secretsmanager:{REGION}:{ACCT}:secret:{SECRET}-*"],
                )
            ),
        },
        managed=[
            "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
        ],
    )
    put_role(
        iam,
        NARROW_ROLE,
        trust_principals([arn(EXEC_ROLE), SSO_ROLE_ARN]),
        MCD_TAGS,
        inline={"spike-narrow": doc(LOGS_READ)},
    )
    put_role(
        iam,
        DECOY_ROLE,
        doc(
            {
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{ACCT}:root"},
                "Action": "sts:AssumeRole",
            }
        ),
        MCD_TAGS,
    )


def data():
    s3 = dev.client("s3", region_name=REGION)
    try:
        s3.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        print("created bucket", BUCKET)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise
    s3.put_public_access_block(
        Bucket=BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    s3.put_bucket_tagging(Bucket=BUCKET, Tagging={"TagSet": tag_list(TAGS)})
    s3.put_object(
        Bucket=BUCKET, Key=BUCKET_KEY, Body=b"k2-1343 dummy agent-store object\n"
    )

    sm = dev.client("secretsmanager", region_name=REGION)
    value = json.dumps(
        {"header_value": secrets.token_urlsafe(32), "note": "K2-1343 dummy"}
    )
    try:
        sm.create_secret(Name=SECRET, SecretString=value, Tags=tag_list(TAGS))
        print("created secret", SECRET)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceExistsException":
            raise

    for region in (REGION, REGION2):
        make_log_group(dev.client("logs", region_name=region), busy=(region == REGION))


def make_log_group(logs: Any, busy: bool) -> None:
    try:
        logs.create_log_group(logGroupName=LOG_GROUP, tags=TAGS)
        print("created log group", LOG_GROUP, logs.meta.region_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    logs.put_retention_policy(logGroupName=LOG_GROUP, retentionInDays=1)
    put_events(logs, "small", 50, error_every=5)
    if busy:
        put_events(logs, "busy", 100_000, error_every=50)


def put_events(logs: Any, stream: str, count: int, error_every: int) -> None:
    try:
        logs.create_log_stream(logGroupName=LOG_GROUP, logStreamName=stream)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
        return  # already seeded
    now = int(time.time() * 1000)
    start = now - 30 * 60 * 1000
    step = max(1, (30 * 60 * 1000) // count)
    batch, size = [], 0
    for i in range(count):
        level = "ERROR" if i % error_every == 0 else "INFO"
        msg = (
            f'{{"level": "{level}", "i": {i}, "job": "glue-orders-daily", '
            f'"msg": "{"Task failed: NullPointerException in stage 3" if level == "ERROR" else "processed batch"}", '
            f'"pad": "{"x" * 120}"}}'
        )
        event = {"timestamp": start + i * step, "message": msg}
        if len(batch) == 10_000 or size + len(msg) + 26 > 1_000_000:
            logs.put_log_events(
                logGroupName=LOG_GROUP, logStreamName=stream, logEvents=batch
            )
            batch, size = [], 0
        batch.append(event)
        size += len(msg) + 26
    if batch:
        logs.put_log_events(
            logGroupName=LOG_GROUP, logStreamName=stream, logEvents=batch
        )
    print(f"seeded {count} events -> {stream} ({logs.meta.region_name})")


def _zip_dir(path: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(path.rglob("*")):
            if f.is_file() and "__pycache__" not in f.parts:
                zf.write(f, f.relative_to(path))
    return buf.getvalue()


def put_function(
    lam: Any,
    name: str,
    code_dir: Path,
    handler: str,
    env: Dict[str, str],
    vpc: bool,
    memory: int = 512,
    timeout: int = 300,
) -> None:
    code = _zip_dir(code_dir)
    print(f"{name}: package {len(code) / 1e6:.1f} MB")
    kwargs: Dict[str, Any] = dict(
        Runtime="python3.13",
        Role=arn(EXEC_ROLE),
        Handler=handler,
        MemorySize=memory,
        Timeout=timeout,
        Architectures=["x86_64"],
        Environment={"Variables": env},
    )
    if vpc:
        kwargs["VpcConfig"] = {
            "SubnetIds": SUBNETS,
            "SecurityGroupIds": SECURITY_GROUPS,
        }
    try:
        lam.create_function(
            FunctionName=name, Code={"ZipFile": code}, Tags=TAGS, **kwargs
        )
        print("created function", name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceConflictException":
            raise
        lam.update_function_code(FunctionName=name, ZipFile=code)
        lam.get_waiter("function_updated_v2").wait(FunctionName=name)
        lam.update_function_configuration(FunctionName=name, **kwargs)
    lam.get_waiter("function_active_v2").wait(FunctionName=name)
    lam.get_waiter("function_updated_v2").wait(FunctionName=name)


def lambdas():
    lam = dev.client("lambda", region_name=REGION)
    put_function(lam, HARNESS, BUILD / "harness", "harness.handler", {}, vpc=True)
    api_key = json.loads(
        dev.client("secretsmanager", region_name=REGION).get_secret_value(
            SecretId=SECRET
        )["SecretString"]
    )["header_value"]
    put_function(
        lam,
        ECHO,
        BUILD / "echo",
        "echo_server.handler",
        {"ECHO_API_KEY": api_key},
        vpc=False,
        timeout=30,
    )
    try:
        url = lam.create_function_url_config(FunctionName=ECHO, AuthType="NONE")[
            "FunctionUrl"
        ]
        lam.add_permission(
            FunctionName=ECHO,
            StatementId="public-url",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceConflictException":
            raise
        url = lam.get_function_url_config(FunctionName=ECHO)["FunctionUrl"]
    print("echo url", url)


def xacct():
    iam = dev_eu.client("iam")
    put_role(
        iam,
        XACCT_ROLE,
        doc(
            {
                "Effect": "Allow",
                "Principal": {"AWS": arn(EXEC_ROLE)},
                "Action": "sts:AssumeRole",
            }
        ),
        MCD_TAGS,
        inline={
            "spike-narrow": doc(
                stmt(
                    ["logs:FilterLogEvents"],
                    [log_group_arn(XREGION, XACCT)],
                    sid="LogsRead",
                )
            )
        },
    )
    make_log_group(dev_eu.client("logs", region_name=XREGION), busy=False)


def teardown():
    def quiet(fn: Any, *a: Any, **k: Any) -> None:
        try:
            fn(*a, **k)
        except ClientError as exc:
            print("  skip:", exc.response["Error"]["Code"], getattr(fn, "__name__", ""))

    lam = dev.client("lambda", region_name=REGION)
    for fn in (HARNESS, ECHO):
        quiet(lam.delete_function, FunctionName=fn)
        quiet(
            dev.client("logs", region_name=REGION).delete_log_group,
            logGroupName=f"/aws/lambda/{fn}",
        )
    for session, region in ((dev, REGION), (dev, REGION2), (dev_eu, XREGION)):
        quiet(
            session.client("logs", region_name=region).delete_log_group,
            logGroupName=LOG_GROUP,
        )
    quiet(
        dev.client("secretsmanager", region_name=REGION).delete_secret,
        SecretId=SECRET,
        ForceDeleteWithoutRecovery=True,
    )
    s3: Any = dev.resource("s3", region_name=REGION).Bucket(  # type: ignore[attr-defined]
        BUCKET
    )
    try:
        s3.object_versions.delete()
        s3.objects.all().delete()
        s3.delete()
    except ClientError as exc:
        print("  skip bucket:", exc.response["Error"]["Code"])
    for session, name in (
        (dev, EXEC_ROLE),
        (dev, NARROW_ROLE),
        (dev, DECOY_ROLE),
        (dev_eu, XACCT_ROLE),
    ):
        iam = session.client("iam")
        try:
            for p in iam.list_role_policies(RoleName=name)["PolicyNames"]:
                iam.delete_role_policy(RoleName=name, PolicyName=p)
            for p in iam.list_attached_role_policies(RoleName=name)["AttachedPolicies"]:
                iam.detach_role_policy(RoleName=name, PolicyArn=p["PolicyArn"])
            iam.delete_role(RoleName=name)
            print("deleted role", name)
        except ClientError as exc:
            print("  skip role", name, exc.response["Error"]["Code"])


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "--teardown":
        teardown()
    elif cmd == "policy":
        set_narrow_policy(sys.argv[2])
    else:
        steps = {"iam": iam_roles, "data": data, "lambdas": lambdas, "xacct": xacct}
        for name in steps if cmd == "all" else [cmd]:
            steps[name]()
