"""Probe that runs inside the image under test, with the image's own interpreter.

``check_image.py`` passes this file's source to ``python -c`` together with a JSON
request as the first argument, so nothing has to be mounted into the container.
Stdlib only: it must run in every image before anything else is known to work.

The result is printed as a single line prefixed with ``RESULT_MARKER``; anything
else on stdout (log lines from imported modules) is ignored by the caller.
"""

import concurrent.futures
import glob
import importlib
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
import types
import urllib.error
import urllib.request
from typing import Any, Callable

RESULT_MARKER = "@@IMAGE-CHECK-RESULT@@"


def _error_text(exc: BaseException) -> str:
    """The exception and the innermost frames, enough to see which file failed."""
    frames = [
        f
        for f in traceback.extract_tb(exc.__traceback__)
        if not f.filename.startswith("<frozen")
    ][-3:]
    where = "".join(traceback.format_list(frames)).rstrip()
    what = "".join(traceback.format_exception_only(type(exc), exc)).strip()
    return f"{what}\n{where}" if where else what


def _probe_oracle_thick() -> None:
    import oracledb

    # Loads libclntsh from the Instant Client the image installs (ldconfig path).
    oracledb.init_oracle_client()
    oracledb.clientversion()


def _probe_odbc_driver() -> None:
    import pyodbc

    drivers = pyodbc.drivers()
    if "ODBC Driver 17 for SQL Server" not in drivers:
        raise RuntimeError(
            f"'ODBC Driver 17 for SQL Server' not registered; found {drivers}"
        )


PROBES: dict[str, Callable[[], None]] = {
    "oracledb thick mode": _probe_oracle_thick,
    "ODBC Driver 17 for SQL Server": _probe_odbc_driver,
}


def run_imports(request: dict[str, Any]) -> dict[str, Any]:
    results = []
    for module in request["modules"]:
        try:
            importlib.import_module(module)
            results.append({"name": module, "ok": True})
        except BaseException as exc:  # SystemExit from an entry module counts too
            results.append({"name": module, "ok": False, "error": _error_text(exc)})
    probes = []
    for name in request.get("probes", []):
        try:
            PROBES[name]()
            probes.append({"name": name, "ok": True})
        except BaseException as exc:
            probes.append({"name": name, "ok": False, "error": _error_text(exc)})
    return {"imports": results, "probes": probes}


def _is_shared_object(name: str) -> bool:
    return name.endswith(".so") or ".so." in name


def _ldd_command() -> list[str]:
    ldd = shutil.which("ldd")
    if ldd:
        return [ldd]
    # Minimal images (the Lambda base) may not ship ldd; the dynamic loader's --list is what ldd runs.
    loaders = glob.glob("/lib64/ld-linux-*.so.*") + glob.glob("/lib/ld-linux-*.so.*")
    if not loaders:
        raise RuntimeError("neither ldd nor the dynamic loader was found in the image")
    return [loaders[0], "--list"]


def _missing_libs(command: list[str], path: str) -> list[str]:
    proc = subprocess.run(command + [path], capture_output=True, text=True, timeout=60)
    return sorted(
        {
            line.split("=>")[0].strip()
            for line in proc.stdout.splitlines()
            if "=> not found" in line
        }
    )


def run_ldd(request: dict[str, Any]) -> dict[str, Any]:
    # "" is the working directory: /var/task in the Lambda image, where its deps live.
    roots = [p or os.getcwd() for p in sys.path if os.path.isdir(p or ".")] + [
        d for d in request.get("extra_dirs", []) if os.path.isdir(d)
    ]
    files: set[str] = set()
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            # auditwheel's vendored `<pkg>.libs` resolve each other only through the
            # extension module's RUNPATH, so ldd them via that module (transitively).
            dirnames[:] = [d for d in dirnames if not d.endswith(".libs")]
            for name in filenames:
                if _is_shared_object(name):
                    path = os.path.realpath(os.path.join(dirpath, name))
                    if os.path.isfile(path):
                        files.add(path)
    command = _ldd_command()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        missing = dict(
            zip(
                sorted(files),
                pool.map(lambda f: _missing_libs(command, f), sorted(files)),
            )
        )
    return {
        "scanned": len(files),
        "tool": " ".join(command),
        "missing": [{"file": f, "libs": libs} for f, libs in missing.items() if libs],
    }


def _health_fields(body: str) -> dict[str, Any]:
    # Only version and build: the full health payload includes environment details.
    data = json.loads(body)
    return {"version": data.get("version"), "build": data.get("build")}


def run_http_health(request: dict[str, Any]) -> dict[str, Any]:
    deadline = time.monotonic() + request["timeout"]
    last_error = "no attempt made"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(request["url"], timeout=10) as resp:
                return {"status": resp.status, **_health_fields(resp.read().decode())}
        except urllib.error.HTTPError as exc:
            last_error = f"HTTP {exc.code}"
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(1)
    return {
        "status": None,
        "error": f"no healthy response within {request['timeout']}s ({last_error})",
    }


def run_lambda_health(request: dict[str, Any]) -> dict[str, Any]:
    module_name, _, attr = request["handler"].rpartition(".")
    handler = getattr(importlib.import_module(module_name), attr)
    event = {
        "httpMethod": "GET",
        "path": "/api/v1/test/health",
        "headers": {"Host": "localhost"},
        "multiValueHeaders": {"Host": ["localhost"]},
        "queryStringParameters": None,
        "multiValueQueryStringParameters": None,
        "requestContext": {},
        "body": None,
        "isBase64Encoded": False,
    }
    context = types.SimpleNamespace(
        function_name="image-check",
        aws_request_id="image-check",
        invoked_function_arn="",
    )
    response = handler(event, context)
    status = response.get("statusCode")
    if status != 200:
        return {"status": status, "error": f"handler returned status {status}"}
    return {"status": status, **_health_fields(response["body"])}


MODES: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "imports": run_imports,
    "ldd": run_ldd,
    "http-health": run_http_health,
    "lambda-health": run_lambda_health,
}


def main() -> None:
    request = json.loads(sys.argv[1])
    try:
        result = MODES[request["mode"]](request)
    except BaseException as exc:
        result = {"fatal": _error_text(exc)}
    sys.stdout.flush()
    print(RESULT_MARKER + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
