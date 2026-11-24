"""Tests for scripts/image_checks/check_image.py and in_image.py (no Docker needed)."""

import importlib.util
import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts" / "image_checks"
if not SCRIPTS.is_dir():
    # The `tests` Docker stages copy tests/ but not scripts/; run-linter runs these.
    pytest.skip("scripts/image_checks is not in this checkout", allow_module_level=True)


def _load(name):
    spec = importlib.util.spec_from_file_location(
        f"image_checks_{name}", SCRIPTS / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


check_image = _load("check_image")
in_image = _load("in_image")


def _probe(request):
    """Run in_image.py the way check_image does: source via -c, request as argv[1]."""
    proc = subprocess.run(
        [sys.executable, "-c", check_image.PROBE_SOURCE, json.dumps(request)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    return check_image.parse_result(proc.stdout)


# -- output format ---------------------------------------------------------------------


def test_failure_line_names_check_subject_and_image(capsys):
    report = check_image.Report("lambda")
    report.fail(
        "pyodbc import failed",
        "ImportError: libodbc.so.2: cannot open shared object file",
    )
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "IMAGE CHECK FAILED: pyodbc import failed in the lambda image"
    assert out[1] == "    ImportError: libodbc.so.2: cannot open shared object file"


def test_summary(capsys):
    report = check_image.Report("azure")
    assert check_image.summarize(report) == 0
    report.fail("function_app import failed")
    assert check_image.summarize(report) == 1
    assert (
        "  - function_app import failed in the azure image" in capsys.readouterr().out
    )


def test_indent_truncates_long_detail():
    text = "\n".join(str(i) for i in range(100))
    lines = check_image.indent(text, limit=5).splitlines()
    assert lines[:5] == ["    0", "    1", "    2", "    3", "    4"]
    assert lines[5] == "    ... (95 more lines)"


def test_parse_result_ignores_log_noise():
    stdout = (
        f"some log\n{check_image.RESULT_MARKER}" + json.dumps({"a": 1}) + "\nlate log\n"
    )
    assert check_image.parse_result(stdout) == {"a": 1}
    assert check_image.parse_result("nothing here") is None


def test_markers_match():
    assert check_image.RESULT_MARKER == in_image.RESULT_MARKER


# -- arguments -------------------------------------------------------------------------


def test_parse_args_defaults_to_all_checks():
    args = check_image.parse_args(["--target", "generic"])
    assert args.checks == list(check_image.ALL_CHECKS)


def test_parse_args_rejects_unknown_check_and_target():
    with pytest.raises(SystemExit):
        check_image.parse_args(["--target", "generic", "--checks", "imports,bogus"])
    with pytest.raises(SystemExit):
        check_image.parse_args(["--target", "windows"])


def test_branch_targets_have_profiles():
    for target in ("generic", "cloudrun", "lambda", "azure", "aws_proxied"):
        assert target in check_image.TARGETS
    # aws_proxied isn't built on branches, so the generic image covers its entry module
    assert "apollo.interfaces.aws.main" in check_image.TARGETS["generic"].entry_modules


# -- checks against a fake docker ------------------------------------------------------


class FakeChecker(check_image.Checker):
    def __init__(self, target, result, expected_version="1.2.3,45"):
        super().__init__(target, f"apollo-check:{target}", expected_version)
        self.result = result
        self.requests = []

    def probe(self, request, timeout=600):
        self.requests.append(request)
        return self.result, "raw container output"


def test_import_failures_are_reported_per_module(capsys):
    checker = FakeChecker(
        "lambda",
        {
            "imports": [
                {"name": "oracledb", "ok": True},
                {"name": "pyodbc", "ok": False, "error": "ImportError: libodbc.so.2"},
            ],
            "probes": [
                {"name": "oracledb thick mode", "ok": False, "error": "DPI-1047"}
            ],
        },
    )
    checker.check_imports()
    out = capsys.readouterr().out
    assert "IMAGE CHECK FAILED: pyodbc import failed in the lambda image" in out
    assert (
        "IMAGE CHECK FAILED: oracledb thick mode failed to load in the lambda image"
        in out
    )
    assert len(checker.report.failures) == 2
    modules = checker.requests[0]["modules"]
    assert (
        "pyodbc" in modules and "apollo.interfaces.lambda_function.handler" in modules
    )


def test_import_check_that_cannot_run_is_a_failure():
    checker = FakeChecker("azure", None)
    checker.check_imports()
    assert checker.report.failures[0].headline.startswith("import check could not run")
    assert "raw container output" in checker.report.failures[0].detail


def test_ldd_ignores_only_listed_pairs(capsys):
    checker = FakeChecker(
        "generic",
        {
            "scanned": 3,
            "tool": "/usr/bin/ldd",
            "missing": [
                {
                    "file": "/usr/local/lib/python3.13/lib-dynload/_tkinter.cpython-313.so",
                    "libs": ["libtcl8.6.so", "libtk8.6.so"],
                },
                {
                    "file": "/app/.venv/x/pyodbc.so",
                    "libs": ["libodbc.so.2", "libtcl8.6.so"],
                },
            ],
        },
    )
    checker.check_ldd()
    headlines = [f.headline for f in checker.report.failures]
    assert headlines == [
        "libodbc.so.2 not found for /app/.venv/x/pyodbc.so (ldd) in the generic image",
        "libtcl8.6.so not found for /app/.venv/x/pyodbc.so (ldd) in the generic image",
    ]


@pytest.mark.parametrize(
    "result,expected",
    [
        ({"status": 200, "version": "1.2.3", "build": "45"}, None),
        (
            {"status": 200, "version": "1.2.2", "build": "45"},
            "health reported the wrong version",
        ),
        ({"status": None, "error": "no healthy response"}, "health check failed"),
        ({"fatal": "ImportError: x"}, "health check failed"),
        (None, "health check failed"),
    ],
)
def test_health_result(result, expected):
    checker = FakeChecker("generic", None)
    checker._check_health_result("generic entrypoint", result, "logs")
    if expected is None:
        assert checker.report.failures == []
    else:
        assert expected in checker.report.failures[0].headline


def test_cloudrun_gets_placeholder_gcp_credentials():
    checker = check_image.Checker("cloudrun", "img", None)
    mount = checker._mounts[1]
    host_path = mount.split(":")[0]
    assert mount.endswith(f":{check_image.GCP_CREDENTIALS_PATH}:ro")
    assert json.loads(Path(host_path).read_text())["type"] == "authorized_user"
    assert "--network" in checker.run_args() and "none" in checker.run_args()


# -- in_image.py, run with this interpreter --------------------------------------------


def test_probe_imports_reports_errors():
    result = _probe({"mode": "imports", "modules": ["json", "no_such_module_xyz"]})
    assert result["imports"][0] == {"name": "json", "ok": True}
    assert result["imports"][1]["ok"] is False
    assert "ModuleNotFoundError" in result["imports"][1]["error"]


def test_probe_unknown_mode_is_fatal_not_a_crash():
    result = _probe({"mode": "nope"})
    assert "KeyError" in result["fatal"]


@pytest.mark.parametrize(
    "name,expected",
    [
        ("a.so", True),
        ("libx.so.2", True),
        ("libx.so.2.1", True),
        ("x.py", False),
        ("so.txt", False),
    ],
)
def test_is_shared_object(name, expected):
    assert in_image._is_shared_object(name) is expected


def test_http_health_returns_only_version_and_build():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps(
                {"version": "1.2.3", "build": "45", "env": {"SECRET": "x"}}
            ).encode()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/api/v1/test/health"
        result = in_image.run_http_health({"url": url, "timeout": 10})
    finally:
        server.shutdown()
    assert result == {"status": 200, "version": "1.2.3", "build": "45"}


def test_http_health_times_out_readably():
    result = in_image.run_http_health(
        {"url": "http://127.0.0.1:9/health", "timeout": 1}
    )
    assert result["status"] is None
    assert "no healthy response within 1s" in result["error"]
