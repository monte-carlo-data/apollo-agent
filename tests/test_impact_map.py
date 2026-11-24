"""Tests for scripts/impact_map.py against a throwaway git repo built from tests/impact_map/.

The Docker `tests` image ships only apollo/ and tests/, not scripts/, so these tests skip
there; CI runs them with a bare Python (see docs/impact-map.md).
"""

import importlib.util
import json
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "impact_map.py"
FIXTURES = Path(__file__).resolve().parent / "impact_map"
MANIFEST = FIXTURES / "manifest.toml"

if not SCRIPT.exists() or shutil.which("git") is None:
    pytest.skip("scripts/impact_map.py or git not available", allow_module_level=True)

_spec = importlib.util.spec_from_file_location("impact_map", SCRIPT)
assert _spec and _spec.loader
impact_map = importlib.util.module_from_spec(_spec)
sys.modules["impact_map"] = impact_map
_spec.loader.exec_module(impact_map)

ALL_TARGETS = ["generic", "cloudrun", "lambda", "azure"]
ALL_PLATFORMS = ["aws", "gcp", "azure"]
ALL_TYPES = ["db2", "glue", "snowflake", "sql-server"]


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


class FixtureRepo:
    """A git repo whose first commit (`base`) is tests/impact_map/repo."""

    def __init__(self, path: Path):
        self.path = path
        shutil.copytree(FIXTURES / "repo", path)
        _git(path, "init", "-q", "-b", "main")
        _git(path, "config", "user.email", "test@example.com")
        _git(path, "config", "user.name", "test")
        _git(path, "config", "commit.gpgsign", "false")
        self._commit("base")
        _git(path, "tag", "base")

    def _commit(self, message: str) -> None:
        _git(self.path, "add", "-A")
        _git(self.path, "commit", "-q", "--allow-empty", "-m", message)

    def edit(self, rel: str, change: Callable[[str], str]) -> None:
        file = self.path / rel
        before = file.read_text() if file.exists() else ""
        after = change(before)
        assert after != before, f"edit to {rel} changed nothing"
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(after)

    def replace(self, rel: str, old: str, new: str) -> None:
        self.edit(rel, lambda text: text.replace(old, new))

    def analyze(self) -> dict[str, Any]:
        self._commit("head")
        manifest = impact_map.Manifest.load(MANIFEST)
        git = impact_map.Git(self.path)
        return impact_map.ImpactMap(git, manifest, "base", "HEAD").analyze()


@pytest.fixture
def repo(tmp_path: Path) -> FixtureRepo:
    return FixtureRepo(tmp_path / "repo")


def _checks(report: dict[str, Any]) -> set[tuple[str, str | None]]:
    return {(c["area"], c["connection_type"]) for c in report["manual_checks"]}


def _assert_empty(report: dict[str, Any]) -> None:
    for key in (
        "changed_files",
        "changed_packages",
        "image_targets",
        "platforms",
        "connection_types",
        "manual_checks",
        "unmapped_packages",
    ):
        assert report[key] == [], key


# ------------------------------------------------------------------ analysis


def test_no_packaging_change(repo: FixtureRepo):
    repo.replace("apollo/integrations/db/sql_server_proxy_client.py", "\n", "\nx = 1\n")
    report = repo.analyze()
    assert report["schema_version"] == 1
    assert report["packaging_changed"] is False
    _assert_empty(report)
    assert impact_map.to_markdown(report) == ""


def test_transitive_bump_follows_via_to_top_level(repo: FixtureRepo):
    # cffi <- cryptography <- snowflake-connector-python
    repo.replace("requirements.txt", "cffi==1.17.1", "cffi==1.17.2")
    report = repo.analyze()
    assert report["packaging_changed"] is True
    assert report["changed_files"] == ["requirements.txt"]
    assert report["changed_packages"] == [
        {
            "name": "cffi",
            "from": "1.17.1",
            "to": "1.17.2",
            "requirements_files": ["requirements.txt"],
        }
    ]
    assert report["image_targets"] == ALL_TARGETS
    assert report["platforms"] == ALL_PLATFORMS
    assert report["connection_types"] == ["snowflake"]
    # cffi has no `areas`: no per-area checks for the drivers that pull it in
    assert report["manual_checks"] == []
    assert report["unmapped_packages"] == []


def test_transitive_bump_uses_import_scan(repo: FixtureRepo):
    # boto3 is only pulled in by snowflake-connector-python, but apollo imports it directly
    repo.replace("requirements.txt", "boto3==1.35.87", "boto3==1.35.99")
    report = repo.analyze()
    assert report["connection_types"] == ["glue", "snowflake"]
    [check] = report["manual_checks"]
    assert check["area"] == "other"
    assert check["connection_type"] is None
    assert "AWS Secrets Manager" in check["reason"]
    assert "`apollo/credentials/asm.py` imports `boto3`" in check["reason"]


def test_driver_bump_adds_per_area_checks(repo: FixtureRepo):
    repo.replace("requirements.in", "pyodbc==5.1.0", "pyodbc==5.2.0")
    repo.replace("requirements.txt", "pyodbc==5.1.0", "pyodbc==5.2.0")
    report = repo.analyze()
    assert report["changed_files"] == ["requirements.in", "requirements.txt"]
    assert report["connection_types"] == ["sql-server"]
    assert _checks(report) == {
        ("metadata_collection", "sql-server"),
        ("monitors", "sql-server"),
    }
    assert all("pyodbc 5.1.0 → 5.2.0" in c["reason"] for c in report["manual_checks"])
    markdown = impact_map.to_markdown(report)
    assert (
        "- [ ] **metadata_collection** · `sql-server`: pyodbc 5.1.0 → 5.2.0" in markdown
    )
    assert "Connection types: sql-server" in markdown


def test_marker_and_url_pins_parse(repo: FixtureRepo):
    repo.replace("requirements.txt", "ibm-db==3.2.8 ;", "ibm-db==3.2.9 ;")
    repo.replace("requirements.txt", "agent-base.git@v0.0.7", "agent-base.git@v0.0.8")
    report = repo.analyze()
    names = {p["name"]: (p["from"], p["to"]) for p in report["changed_packages"]}
    assert names["ibm-db"] == ("3.2.8", "3.2.9")
    assert names["agent-base"][1].endswith("@v0.0.8")
    assert report["connection_types"] == ["db2"]
    assert report["unmapped_packages"] == []


def test_bump_in_only_one_requirements_file(repo: FixtureRepo):
    repo.replace(
        "requirements-azure.in", "azure-functions==2.1.0", "azure-functions==2.2.0"
    )
    repo.replace(
        "requirements-azure.txt", "azure-functions==2.1.0", "azure-functions==2.2.0"
    )
    report = repo.analyze()
    assert report["image_targets"] == ["azure"]
    assert report["platforms"] == ["azure"]
    assert report["connection_types"] == []
    assert report["manual_checks"] == [
        {"area": "other", "connection_type": None, "reason": "Azure host start"}
    ]
    assert report["unmapped_packages"] == []


def test_dev_only_bump_targets_no_image(repo: FixtureRepo):
    repo.replace("requirements-dev.txt", "pytest==7.4.2", "pytest==8.0.0")
    report = repo.analyze()
    assert report["packaging_changed"] is True
    assert report["changed_packages"][0]["name"] == "pytest"
    assert report["image_targets"] == []
    assert report["platforms"] == []
    assert report["connection_types"] == []
    assert report["manual_checks"] == []
    assert impact_map.to_markdown(report) == ""


@pytest.mark.parametrize(
    "path", ["Dockerfile", "resources/lambda/openssl", "resources/new/file.txt"]
)
def test_dockerfile_or_resources_change_fails_open(repo: FixtureRepo, path: str):
    repo.edit(path, lambda text: text + "# changed\n")
    report = repo.analyze()
    assert report["packaging_changed"] is True
    assert report["changed_files"] == [path]
    assert report["image_targets"] == ALL_TARGETS
    assert report["platforms"] == ALL_PLATFORMS
    assert report["connection_types"] == ALL_TYPES
    assert report["unmapped_packages"] == []
    [check] = report["manual_checks"]
    assert check["area"] == "other" and f"`{path}` changed" in check["reason"]


def test_unmapped_top_level_package_fails_open(repo: FixtureRepo):
    repo.edit("requirements.in", lambda text: text + "leftpad==1.0\n")
    repo.edit(
        "requirements.txt",
        lambda text: text + "leftpad==1.0\n    # via -r requirements.in\n",
    )
    report = repo.analyze()
    assert report["unmapped_packages"] == ["leftpad"]
    assert report["image_targets"] == ALL_TARGETS
    assert report["platforms"] == ALL_PLATFORMS
    assert report["connection_types"] == ALL_TYPES
    assert any("leftpad" in c["reason"] for c in report["manual_checks"])
    assert "Unmapped packages: leftpad" in impact_map.to_markdown(report)


def test_transitive_package_with_unknown_parent_fails_open(repo: FixtureRepo):
    repo.edit(
        "requirements.txt", lambda text: text + "tinylib==2.0\n    # via mystery\n"
    )
    report = repo.analyze()
    assert report["unmapped_packages"] == ["tinylib"]
    assert report["image_targets"] == ALL_TARGETS


def test_in_file_changed_without_recompile_fails_open(repo: FixtureRepo):
    repo.replace("requirements.in", "pyodbc==5.1.0", "pyodbc==5.2.0")
    report = repo.analyze()
    assert report["changed_files"] == ["requirements.in"]
    assert report["unmapped_packages"] == ["pyodbc"]
    assert report["image_targets"] == ALL_TARGETS


def test_in_file_comment_only_change_is_not_an_impact(repo: FixtureRepo):
    repo.replace("requirements.in", "# CVE fix", "# CVE fix, see VULN-1")
    report = repo.analyze()
    assert report["packaging_changed"] is True
    assert report["changed_files"] == ["requirements.in"]
    assert report["image_targets"] == []
    assert report["unmapped_packages"] == []


def test_unknown_requirements_file_fails_open(repo: FixtureRepo):
    repo.edit("requirements-new.in", lambda _: "leftpad==1.0\n")
    repo.edit(
        "requirements-new.txt",
        lambda _: "leftpad==1.0\n    # via -r requirements-new.in\n",
    )
    report = repo.analyze()
    assert report["unmapped_packages"] == ["leftpad"]
    assert report["image_targets"] == ALL_TARGETS


def test_removed_package(repo: FixtureRepo):
    repo.replace("requirements.in", "pyodbc==5.1.0\n", "")
    repo.replace(
        "requirements.txt", "pyodbc==5.1.0\n    # via -r requirements.in\n", ""
    )
    report = repo.analyze()
    assert report["changed_packages"][0] == {
        "name": "pyodbc",
        "from": "5.1.0",
        "to": None,
        "requirements_files": ["requirements.txt"],
    }
    assert report["connection_types"] == ["sql-server"]


def test_same_pin_in_two_files_lists_both(repo: FixtureRepo):
    repo.replace("requirements.txt", "cryptography==50.0.0", "cryptography==50.0.1")
    repo.replace(
        "requirements-azure.txt", "cryptography==50.0.0", "cryptography==50.0.1"
    )
    report = repo.analyze()
    [package] = report["changed_packages"]
    assert package["requirements_files"] == [
        "requirements-azure.txt",
        "requirements.txt",
    ]
    assert report["image_targets"] == ALL_TARGETS


# ----------------------------------------------------------------- parsing


def test_parse_requirements_txt():
    text = (FIXTURES / "repo" / "requirements.txt").read_text()
    pins = impact_map.parse_requirements_txt(text)
    assert pins["cryptography"].version == "50.0.0"
    assert pins["cryptography"].top_level is True
    assert pins["cryptography"].parents == {"snowflake-connector-python"}
    assert pins["cffi"].top_level is False
    assert pins["cffi"].parents == {"cryptography"}
    assert pins["ibm-db"].version == "3.2.8"
    azure = impact_map.parse_requirements_txt(
        (FIXTURES / "repo" / "requirements-azure.txt").read_text()
    )
    # `-c requirements.txt` is a constraint, not a parent
    assert azure["cryptography"].parents == {"azure-functions"}
    assert azure["cryptography"].top_level is False


def test_parse_requirements_in_ignores_comments_and_options():
    specs = impact_map.parse_requirements_in(
        "-c requirements.txt\n# comment\nFoo_Bar>=1.0  # why\nbaz==2 ; python_version < '3.14'\n"
    )
    assert specs == {
        "foo-bar": "Foo_Bar>=1.0",
        "baz": "baz==2 ; python_version < '3.14'",
    }


@pytest.mark.parametrize(
    "line, expected",
    [
        ("import pyodbc", {"pyodbc"}),
        ("import os, ibm_db_dbi as db", {"os", "ibm_db_dbi"}),
        ("from google.cloud import run_v2", {"google.cloud", "google.cloud.run_v2"}),
        (
            "    from google.cloud.logging import Client, handlers  # noqa",
            {
                "google.cloud.logging",
                "google.cloud.logging.Client",
                "google.cloud.logging.handlers",
            },
        ),
        ("from snowflake.connector import (", {"snowflake.connector"}),
        ("from . import x", set()),
        ("x = 1", set()),
    ],
)
def test_imported_modules(line: str, expected: set[str]):
    assert impact_map.imported_modules(line) == expected


# --------------------------------------------------------------------- CLI


def _cli(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--repo",
            str(repo),
            "--manifest",
            str(MANIFEST),
            *args,
        ],
        capture_output=True,
        text=True,
    )


def test_cli_json_and_markdown(repo: FixtureRepo):
    repo.replace("requirements.txt", "pyodbc==5.1.0", "pyodbc==5.2.0")
    repo.analyze()  # commits head
    proc = _cli(repo.path, "--base", "base", "--head", "HEAD")
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["connection_types"] == ["sql-server"]
    proc = _cli(repo.path, "--base", "base", "--head", "HEAD", "--format", "markdown")
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.startswith("### Impact map: manual checks")


def test_cli_nothing_changed(repo: FixtureRepo):
    proc = _cli(repo.path, "--base", "base", "--head", "base", "--format", "markdown")
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == ""
    proc = _cli(repo.path, "--base", "base", "--head", "base")
    assert json.loads(proc.stdout)["packaging_changed"] is False


def test_cli_bad_ref_is_an_internal_error(repo: FixtureRepo):
    proc = _cli(repo.path, "--base", "no-such-ref", "--head", "HEAD")
    assert proc.returncode != 0
    assert proc.stdout == ""
    assert "no-such-ref" in proc.stderr


# ------------------------------------------------------------ review fixes


def test_removed_dependency_without_manifest_entry_does_not_fail_open(
    repo: FixtureRepo,
):
    # Removing a dependency also removes its manifest entry; that is not "unmapped".
    repo.replace("requirements.in", "oldlib==1.0  # not in the test manifest\n", "")
    repo.replace("requirements.txt", "oldlib==1.0\n    # via -r requirements.in\n", "")
    report = repo.analyze()
    assert report["changed_packages"][0]["name"] == "oldlib"
    assert report["unmapped_packages"] == []
    assert report["connection_types"] == []


def test_import_scan_reads_multiline_imports(repo: FixtureRepo):
    # cloudrun_updater.py does `from google.cloud import (\n    run_v2,\n)`
    repo.replace(
        "requirements.txt", "google-cloud-run==0.10.18", "google-cloud-run==0.10.19"
    )
    report = repo.analyze()
    [check] = report["manual_checks"]
    assert check["reason"].startswith("Cloud Run self-upgrade")


def test_show_distinguishes_missing_path_from_unreadable_ref(repo: FixtureRepo):
    git = impact_map.Git(repo.path)
    assert git.show("base", "does-not-exist.txt") is None
    assert "pyodbc" in (git.show("base", "requirements.txt") or "")
    with pytest.raises(impact_map.ImpactMapError):
        git.show("no-such-ref", "requirements.txt")


def test_source_imports():
    source = (
        "import a.b as c\nfrom x import (\n    y,\n    z as w,\n)\nfrom . import rel\n"
    )
    assert impact_map.source_imports(source) == {"a.b", "x", "x.y", "x.z"}
    # unparsable files fall back to the line scan
    assert impact_map.source_imports("import pyodbc\ndef (:\n") == {"pyodbc"}
