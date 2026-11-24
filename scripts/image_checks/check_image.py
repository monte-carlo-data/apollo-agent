#!/usr/bin/env python3
"""Contract checks for a built apollo-agent image.

Usage:
    python3 scripts/image_checks/check_image.py --target lambda --image apollo-check:lambda \\
        [--expected-version 1.2.3,456] [--checks version,imports,ldd,health]

Runs on the CI host (stdlib only, Python >= 3.9) and drives ``docker``. Each check
runs in a fresh container with no network, using the image's own interpreter.
Every failure is printed as one line,
``IMAGE CHECK FAILED: <what> in the <target> image``, followed by the underlying
error indented below it, so a person or a bot reading the CI log sees the cause
first. See docs/image-contract-tests.md.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
PROBE_SOURCE = (HERE / "in_image.py").read_text()
RESULT_MARKER = "@@IMAGE-CHECK-RESULT@@"
PORT = 8080
DETAIL_LINES = 30

# Native drivers and compiled libraries the agent imports. Importing them with the
# image's interpreter catches missing system libraries and ABI mismatches.
NATIVE_MODULES: tuple[str, ...] = (
    "oracledb",
    "pyodbc",
    "ibm_db",
    "ibm_db_dbi",
    "teradatasql",
    "psycopg2",
    "snowflake.connector",
    "hdbcli.dbapi",
    "duckdb",
    "pymysql",
    "clickhouse_connect.dbapi",
    "databricks.sql",
    "impala.dbapi",
    "trino.dbapi",
    "prestodb",
    "salesforcecdpconnector.connection",
    "pyarrow",
    "pyarrow.flight",
    "pandas",
    "numpy",
    "cryptography.hazmat.bindings._rust",
    "lxml.etree",
    "orjson",
    "grpc",
    "google_crc32c",
    "lz4.frame",
    "zstandard",
    "Crypto.Cipher.AES",
    "bitarray",
    "pydantic_core",
    "git",
)
PROBES: tuple[str, ...] = ("oracledb thick mode", "ODBC Driver 17 for SQL Server")
# System libraries outside site-packages that drivers load.
NATIVE_LIB_DIRS: tuple[str, ...] = ("/opt/oracle/instantclient", "/opt/microsoft")

# Unresolved dependencies that are expected: (path substring, library). Each needs a reason.
LDD_IGNORED: tuple[tuple[str, str], ...] = (
    # Python's Tk bindings; the agent has no GUI and the slim/Lambda bases ship no Tcl/Tk.
    ("/_tkinter.", "libtcl8.6.so"),
    ("/_tkinter.", "libtk8.6.so"),
    # ibm_db's clidriver ships its optional XML support library without the XML4C runtime
    # it links; the DB2 CLI driver itself (libdb2.so) resolves fully.
    ("/clidriver/lib/libdb2clixml4c.so", "libDB2xml4c.so.58"),
    # The azure stage's apt purge (--auto-remove) takes libsqlite3 with it, so the stdlib
    # sqlite3 module can't load there. Nothing in the agent imports sqlite3; zeep, pandas and
    # filelock only import it lazily or optionally. Remove this once the azure stage keeps it.
    ("/lib-dynload/_sqlite3.", "libsqlite3.so.0"),
)

# A placeholder Application Insights key: function_app configures telemetry at import time.
AZURE_ENV = {
    "APPLICATIONINSIGHTS_CONNECTION_STRING": "InstrumentationKey=00000000-0000-0000-0000-000000000000"
}
# cloudrun.main builds a google.cloud.logging Client at import, which needs some
# credentials and a project. An authorized_user file with placeholder values
# satisfies google.auth without contacting Google (containers have no network).
GCP_DUMMY_CREDENTIALS = {
    "type": "authorized_user",
    "client_id": "image-check",
    "client_secret": "image-check",
    "refresh_token": "image-check",
}
GCP_CREDENTIALS_PATH = "/tmp/image-check-gcp-credentials.json"
GCP_ENV = {
    "GOOGLE_APPLICATION_CREDENTIALS": GCP_CREDENTIALS_PATH,
    "GOOGLE_CLOUD_PROJECT": "image-check",
}
# Azure Functions host: storage emulator and a fixed function key, for the health call only.
AZURITE = (
    "mcr.microsoft.com/azure-storage/azurite:3.35.0@sha256:"
    "647c63a91102a9d8e8000aab803436e1fc85fbb285e7ce830a82ee5d6661cf37",
    "azurite",
    "--blobHost",
    "0.0.0.0",
    "--queueHost",
    "0.0.0.0",
    "--tableHost",
    "0.0.0.0",
    "--skipApiVersionCheck",
    "--loose",
)
# Azurite's documented, public development account key.
AZURITE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
AZURE_FUNCTION_KEY = "image-check-function-key"
AZURE_HOST_KEYS = {
    "masterKey": {
        "name": "master",
        "value": "image-check-master-key",
        "encrypted": False,
    },
    "functionKeys": [
        {"name": "default", "value": AZURE_FUNCTION_KEY, "encrypted": False}
    ],
    "systemKeys": [],
    "hostName": None,
    "instanceId": "image-check",
    "source": "runtime",
    "decryptionKeyId": "",
}


def azurite_connection_string(host: str) -> str:
    account = "devstoreaccount1"
    return (
        f"DefaultEndpointsProtocol=http;AccountName={account};AccountKey={AZURITE_ACCOUNT_KEY};"
        f"BlobEndpoint=http://{host}:10000/{account};"
        f"QueueEndpoint=http://{host}:10001/{account};"
        f"TableEndpoint=http://{host}:10002/{account};"
    )


LAMBDA_ENV = {"AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "us-east-1"}


@dataclass(frozen=True)
class Server:
    """An HTTP entrypoint to start and probe. ``command`` None means the image's own CMD."""

    name: str
    command: tuple[str, ...] | None = None


@dataclass(frozen=True)
class Target:
    python: str
    workdir: str
    entry_modules: tuple[str, ...]
    env: dict[str, str] = field(default_factory=dict)
    servers: tuple[Server, ...] = ()
    lambda_handler: bool = False
    azure_host: bool = False


VENV_PYTHON = "/app/.venv/bin/python"

TARGETS: dict[str, Target] = {
    "generic": Target(
        python=VENV_PYTHON,
        workdir="/app",
        # aws_proxied is `generic` with another CMD, so its entry module is checked here.
        entry_modules=("apollo.interfaces.generic.main", "apollo.interfaces.aws.main"),
        servers=(
            Server("generic"),
            # Mirrors the aws_proxied stage's CMD in the Dockerfile.
            Server(
                "aws_proxied",
                (
                    "sh",
                    "-c",
                    ". $VENV_DIR/bin/activate && exec gunicorn --bind :$PORT"
                    " apollo.interfaces.aws.main:app",
                ),
            ),
        ),
    ),
    # Not built on branches (see "generic"); the merge queue and releases still publish it.
    "aws_proxied": Target(
        python=VENV_PYTHON,
        workdir="/app",
        entry_modules=("apollo.interfaces.aws.main",),
        servers=(Server("aws_proxied"),),
    ),
    "cloudrun": Target(
        python=VENV_PYTHON,
        workdir="/app",
        entry_modules=("apollo.interfaces.cloudrun.main", "google.cloud.logging"),
        env=GCP_ENV,
        servers=(Server("cloudrun"),),
    ),
    "lambda": Target(
        python="/var/lang/bin/python3",
        workdir="/var/task",
        entry_modules=("apollo.interfaces.lambda_function.handler",),
        env=LAMBDA_ENV,
        lambda_handler=True,
    ),
    "azure": Target(
        python="python",
        workdir="/home/site/wwwroot",
        entry_modules=("function_app",),
        env=AZURE_ENV,
        azure_host=True,
    ),
}
ALL_CHECKS = ("version", "imports", "ldd", "health")


@dataclass
class Failure:
    headline: str
    detail: str = ""


@dataclass
class Report:
    target: str
    failures: list[Failure] = field(default_factory=list)

    def fail(self, what: str, detail: str = "") -> None:
        failure = Failure(f"{what} in the {self.target} image", detail.strip())
        self.failures.append(failure)
        print(f"IMAGE CHECK FAILED: {failure.headline}", flush=True)
        if failure.detail:
            print(indent(failure.detail), flush=True)

    def ok(self, message: str) -> None:
        print(f"  ok: {message}", flush=True)


def indent(text: str, limit: int = DETAIL_LINES) -> str:
    lines = text.splitlines()
    if len(lines) > limit:
        lines = lines[:limit] + [f"... ({len(lines) - limit} more lines)"]
    return "\n".join(f"    {line}" for line in lines)


class DockerError(Exception):
    pass


def docker(
    *args: str, timeout: int = 600, check: bool = True
) -> subprocess.CompletedProcess:
    try:
        proc = subprocess.run(
            ["docker", *args], capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired as exc:
        raise DockerError(f"docker {args[0]} timed out after {timeout}s") from exc
    if check and proc.returncode != 0:
        raise DockerError(
            f"docker {args[0]} exited {proc.returncode}: {tail(proc.stderr)}"
        )
    return proc


def tail(text: str, lines: int = DETAIL_LINES) -> str:
    return "\n".join(text.strip().splitlines()[-lines:])


def env_args(env: dict[str, str]) -> list[str]:
    return [arg for key, value in env.items() for arg in ("-e", f"{key}={value}")]


def parse_result(stdout: str) -> dict[str, Any] | None:
    for line in reversed(stdout.splitlines()):
        if line.startswith(RESULT_MARKER):
            return json.loads(line[len(RESULT_MARKER) :])
    return None


class Checker:
    def __init__(self, target: str, image: str, expected_version: str | None) -> None:
        self.name = target
        self.target = TARGETS[target]
        self.image = image
        self.expected_version = expected_version
        self.report = Report(target)
        self._tmp = tempfile.TemporaryDirectory(prefix="image-check-")
        self._mounts: list[str] = []
        if self.target.env.get("GOOGLE_APPLICATION_CREDENTIALS"):
            creds = Path(self._tmp.name) / "gcp.json"
            creds.write_text(json.dumps(GCP_DUMMY_CREDENTIALS))
            creds.chmod(0o644)
            self._mounts = ["-v", f"{creds}:{GCP_CREDENTIALS_PATH}:ro"]

    def run_args(self, *, network: bool = False) -> list[str]:
        args = ["run", "--rm", "--platform", "linux/amd64", "-w", self.target.workdir]
        if not network:
            args += ["--network", "none"]
        return args + env_args(self.target.env) + self._mounts

    def probe(
        self, request: dict[str, Any], timeout: int = 600
    ) -> tuple[dict[str, Any] | None, str]:
        """Run in_image.py in a fresh container; returns (result, combined output)."""
        proc = docker(
            *self.run_args(),
            "--entrypoint",
            self.target.python,
            self.image,
            "-c",
            PROBE_SOURCE,
            json.dumps(request),
            timeout=timeout,
            check=False,
        )
        return parse_result(proc.stdout), (proc.stdout + proc.stderr)

    # -- checks ---------------------------------------------------------------------------

    def check_version(self) -> None:
        if not self.expected_version:
            print("  skip: version (no --expected-version given)")
            return
        proc = docker(
            *self.run_args(),
            "--entrypoint",
            self.target.python,
            self.image,
            "apollo/agent/settings.py",
            check=False,
        )
        found = proc.stdout.strip()
        if proc.returncode != 0 or found != self.expected_version:
            self.report.fail(
                "version file check failed",
                f"expected {self.expected_version!r}, found {found!r}\n{tail(proc.stderr, 10)}",
            )
        else:
            self.report.ok(f"version file is {found}")

    def check_imports(self) -> None:
        modules = list(NATIVE_MODULES) + list(self.target.entry_modules)
        result, output = self.probe(
            {"mode": "imports", "modules": modules, "probes": list(PROBES)}
        )
        if result is None or "fatal" in result:
            self.report.fail(
                f"import check could not run ({self.target.python})",
                (result or {}).get("fatal") or tail(output),
            )
            return
        for item in result["imports"]:
            if not item["ok"]:
                self.report.fail(f"{item['name']} import failed", item["error"])
        for item in result["probes"]:
            if not item["ok"]:
                self.report.fail(f"{item['name']} failed to load", item["error"])
        passed = sum(i["ok"] for i in result["imports"] + result["probes"])
        self.report.ok(
            f"{passed}/{len(result['imports']) + len(result['probes'])} imports and probes"
        )

    def check_ldd(self) -> None:
        result, output = self.probe(
            {"mode": "ldd", "extra_dirs": list(NATIVE_LIB_DIRS)}
        )
        if result is None or "fatal" in result:
            self.report.fail(
                "ldd check could not run", (result or {}).get("fatal") or tail(output)
            )
            return
        problems = 0
        for item in result["missing"]:
            libs = [
                lib
                for lib in item["libs"]
                if not any(
                    part in item["file"] and lib == ignored
                    for part, ignored in LDD_IGNORED
                )
            ]
            for lib in libs:
                problems += 1
                self.report.fail(f"{lib} not found for {item['file']} (ldd)")
        if not problems:
            self.report.ok(
                f"ldd: {result['scanned']} shared objects resolve ({result['tool']})"
            )

    def check_health(self) -> None:
        for server in self.target.servers:
            self._check_http_server(server)
        if self.target.lambda_handler:
            self._check_lambda_handler()
        if self.target.azure_host:
            self._check_azure_host()

    def _check_health_result(
        self, what: str, result: dict[str, Any] | None, logs: str
    ) -> None:
        if result is None or "fatal" in result or result.get("status") != 200:
            reason = (
                (result or {}).get("fatal")
                or (result or {}).get("error")
                or "no result"
            )
            self.report.fail(
                f"{what} health check failed", f"{reason}\n--- output ---\n{tail(logs)}"
            )
            return
        expected_version, _, expected_build = (self.expected_version or "").partition(
            ","
        )
        found = f"{result.get('version')},{result.get('build')}"
        if self.expected_version and found != f"{expected_version},{expected_build}":
            self.report.fail(
                f"{what} health reported the wrong version",
                f"expected {self.expected_version!r}, found {found!r}",
            )
            return
        self.report.ok(f"{what} health: HTTP 200, version {found}")

    def _serve_and_probe(
        self,
        what: str,
        run_args: list[str],
        command: tuple[str, ...] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 90,
    ) -> None:
        """Start the image's server detached, then poll health from inside the container."""
        run = ["run", "-d", "--platform", "linux/amd64", "-e", f"PORT={PORT}"]
        run += run_args + env_args(self.target.env) + self._mounts + [self.image]
        container = docker(*run, *(command or ())).stdout.strip()
        try:
            request = {
                "mode": "http-health",
                "url": f"http://127.0.0.1:{PORT}/api/v1/test/health",
                "headers": headers or {},
                "timeout": timeout,
            }
            proc = docker(
                "exec",
                container,
                self.target.python,
                "-c",
                PROBE_SOURCE,
                json.dumps(request),
                timeout=timeout + 90,
                check=False,
            )
            result = parse_result(proc.stdout)
            if result is None:
                # exec fails outright when the server died and took the container with it
                result = {
                    "error": tail(proc.stdout + proc.stderr, 10) or "container exited"
                }
            logs = docker("logs", container, check=False)
            self._check_health_result(what, result, logs.stdout + logs.stderr)
        finally:
            docker("rm", "-f", container, check=False)

    def _check_http_server(self, server: Server) -> None:
        self._serve_and_probe(
            f"{server.name} entrypoint", ["--network", "none"], command=server.command
        )

    def _check_lambda_handler(self) -> None:
        inspect = docker("image", "inspect", "-f", "{{json .Config.Cmd}}", self.image)
        cmd = json.loads(inspect.stdout) or []
        if len(cmd) != 1:
            self.report.fail(
                "lambda CMD is not a single handler path", f"CMD is {cmd!r}"
            )
            return
        result, output = self.probe({"mode": "lambda-health", "handler": cmd[0]})
        self._check_health_result(f"lambda handler {cmd[0]}", result, output)

    def _check_azure_host(self) -> None:
        """Start the Functions host with its real entrypoint and call health through it.

        A host that can't start, or a worker that can't load function_app, leaves a
        deployed agent answering 503 and unable to upgrade itself. The host needs a
        storage account (Durable Functions) and a function key (the route's auth level):
        an Azurite container on a private network, and a pre-seeded file-based key.
        Both networks allow outbound traffic: the host may fetch its extension bundle.
        """
        suffix = f"{os.getpid()}-{int(time.time())}"
        network, azurite = f"image-check-{suffix}", f"image-check-azurite-{suffix}"
        keys = Path(self._tmp.name) / "host.json"
        keys.write_text(json.dumps(AZURE_HOST_KEYS))
        keys.chmod(0o644)
        docker("network", "create", network)
        try:
            docker(
                "run", "-d", "--rm", "--name", azurite, "--network", network, *AZURITE
            )
            self._serve_and_probe(
                "Azure Functions host",
                [
                    "--network",
                    network,
                    "-e",
                    f"AzureWebJobsStorage={azurite_connection_string(azurite)}",
                    "-e",
                    "AzureWebJobsSecretStorageType=files",
                    "-v",
                    f"{keys}:/azure-functions-host/Secrets/host.json:ro",
                ],
                headers={"x-functions-key": AZURE_FUNCTION_KEY},
                timeout=180,
            )
        finally:
            docker("rm", "-f", azurite, check=False)
            docker("network", "rm", network, check=False)

    def run(self, checks: list[str]) -> Report:
        print(
            f"==> image checks for the {self.name} image ({self.image}): {', '.join(checks)}"
        )
        for check in checks:
            started = time.monotonic()
            print(f"--- {check}", flush=True)
            try:
                getattr(self, f"check_{check}")()
            except DockerError as exc:
                self.report.fail(f"{check} check could not run docker", str(exc))
            print(f"    ({time.monotonic() - started:.0f}s)", flush=True)
        self._tmp.cleanup()
        return self.report


def summarize(report: Report) -> int:
    if not report.failures:
        print(f"==> all image checks passed for the {report.target} image")
        return 0
    print(
        f"==> {len(report.failures)} image check(s) failed for the {report.target} image:"
    )
    for failure in report.failures:
        print(f"  - {failure.headline}")
    return 1


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--target", required=True, choices=sorted(TARGETS))
    parser.add_argument(
        "--image", help="image to check (default: apollo-check:<target>)"
    )
    parser.add_argument(
        "--expected-version", help="expected '<code_version>,<build_number>'"
    )
    parser.add_argument(
        "--checks",
        default=",".join(ALL_CHECKS),
        help=f"comma-separated subset of {','.join(ALL_CHECKS)}",
    )
    args = parser.parse_args(argv)
    args.checks = [c for c in args.checks.split(",") if c]
    unknown = sorted(set(args.checks) - set(ALL_CHECKS))
    if unknown:
        parser.error(f"unknown checks: {', '.join(unknown)}")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    image = args.image or f"apollo-check:{args.target}"
    report = Checker(args.target, image, args.expected_version).run(args.checks)
    return summarize(report)


if __name__ == "__main__":
    sys.exit(main())
