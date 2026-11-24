"""Checks scripts/impact_map_manifest.toml against the repo, so it can't silently drift.

- every package in a requirements*.in file has a [packages] entry (a new dependency can't go
  unmapped);
- every requirements*.txt maps to exactly the image targets the Dockerfile installs it into;
- every connection type, area, scope and module path the manifest uses is valid.

Skips in the Docker `tests` image, which has neither scripts/ nor the .in files.
"""

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "impact_map.py"
MANIFEST_PATH = ROOT / "scripts" / "impact_map_manifest.toml"

if not SCRIPT.exists() or not (ROOT / "requirements.in").exists():
    pytest.skip("repo packaging files not available", allow_module_level=True)

_spec = importlib.util.spec_from_file_location("impact_map", SCRIPT)
assert _spec and _spec.loader
impact_map = importlib.util.module_from_spec(_spec)
sys.modules["impact_map"] = impact_map
_spec.loader.exec_module(impact_map)

MANIFEST = impact_map.Manifest.load(MANIFEST_PATH)
SHIPPED_STAGES = impact_map.ALL_TARGETS
IN_FILES = sorted(ROOT.glob("requirements*.in"))


def _in_packages(path: Path) -> set[str]:
    return set(impact_map.parse_requirements_in(path.read_text()))


@pytest.mark.parametrize("in_file", IN_FILES, ids=lambda p: p.name)
def test_every_in_package_is_in_the_manifest(in_file: Path):
    missing = sorted(_in_packages(in_file) - MANIFEST.packages.keys())
    assert not missing, (
        f"{in_file.name} lists packages with no entry in scripts/impact_map_manifest.toml: "
        f"{missing}. Add them under [packages] (see docs/impact-map.md)."
    )


def test_dev_scope_matches_dev_requirements():
    dev = _in_packages(ROOT / "requirements-dev.in")
    shipped = set().union(
        *(_in_packages(p) for p in IN_FILES if p.name != "requirements-dev.in")
    )
    for name in dev - shipped:
        assert MANIFEST.packages[name]["scope"] == "dev", name
    for name in shipped:
        assert MANIFEST.packages[name]["scope"] != "dev", f"{name} ships in an image"


def test_manifest_has_no_stale_packages():
    listed = set().union(*(_in_packages(p) for p in IN_FILES))
    stale = sorted(MANIFEST.packages.keys() - listed)
    assert not stale, f"manifest packages no requirements*.in file lists: {stale}"


def test_package_entries_are_valid():
    allowed = {"scope", "connection_types", "imports", "areas", "manual"}
    for name, entry in MANIFEST.packages.items():
        assert set(entry) <= allowed, f"{name}: unknown keys {set(entry) - allowed}"
        assert entry.get("scope") in impact_map.SCOPES, f"{name}: bad scope"
        for connection_type in entry.get("connection_types", []):
            assert (
                connection_type in MANIFEST.connection_types
            ), f"{name}: {connection_type}"
        for area in entry.get("areas", []):
            assert area in impact_map.AREAS and area != "other", f"{name}: area {area}"
        for check in entry.get("manual", []):
            assert check["area"] in impact_map.AREAS, f"{name}: {check}"
            assert check["reason"], f"{name}: manual check needs a reason"
            assert check.get("connection_type") in (None, *MANIFEST.connection_types)


def test_connection_types_are_rest_v2_style():
    for connection_type, areas in MANIFEST.connection_types.items():
        assert re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", connection_type), connection_type
        assert set(areas) <= set(impact_map.AREAS) - {"other"}, connection_type


def test_module_entries_exist_and_are_valid():
    for key, entry in MANIFEST.modules.items():
        path = ROOT / key
        assert (
            path.is_dir() if key.endswith("/") else path.is_file()
        ), f"{key} does not exist"
        assert set(entry) <= {"connection_types", "manual"} and entry, key
        for connection_type in entry.get("connection_types", []):
            assert (
                connection_type in MANIFEST.connection_types
            ), f"{key}: {connection_type}"


def test_targets_and_platforms():
    assert set(MANIFEST.target_platforms) == set(impact_map.ALL_TARGETS)
    for target, platforms in MANIFEST.target_platforms.items():
        assert set(platforms) <= set(impact_map.ALL_PLATFORMS), target
    covered = {p for platforms in MANIFEST.target_platforms.values() for p in platforms}
    assert covered == set(impact_map.ALL_PLATFORMS)


def test_every_requirements_txt_is_mapped():
    on_disk = {p.name for p in ROOT.glob("requirements*.txt")}
    assert on_disk == set(MANIFEST.requirements_files)
    for in_file in IN_FILES:
        assert in_file.with_suffix(".txt").name in MANIFEST.requirements_files


def test_fail_open_paths_are_packaging_paths():
    for path in ("Dockerfile", "resources/lambda/openssl"):
        assert impact_map.ImpactMap._matches(path, MANIFEST.fail_open_paths), path
        assert impact_map.ImpactMap._matches(path, MANIFEST.packaging_paths), path


# ---------------------------------------------------------------- Dockerfile


def _dockerfile_requirements() -> dict[str, set[str]]:
    """Stage name -> requirements*.txt files it (or a stage it builds on) copies in."""
    own: dict[str, set[str]] = {}
    parents: dict[str, set[str]] = {}
    stage = ""
    for raw in (ROOT / "Dockerfile").read_text().splitlines():
        line = raw.strip()
        if match := re.match(
            r"FROM\s+(?:--\S+\s+)*(\S+)(?:\s+AS\s+(\S+))?", line, re.IGNORECASE
        ):
            image, stage = match.group(1), match.group(2) or match.group(1)
            own[stage] = set()
            parents[stage] = {image} if image in own else set()
        elif line.upper().startswith("COPY") and stage:
            if source := re.search(r"--from=(\S+)", line):
                parents[stage].add(source.group(1))
            else:
                words = [w for w in line.split()[1:] if not w.startswith("--")]
                own[stage].update(
                    Path(w).name
                    for w in words[:-1]
                    if re.fullmatch(r"/?requirements.*\.txt", w)
                )

    def collect(name: str) -> set[str]:
        files = set(own.get(name, set()))
        for parent in parents.get(name, set()):
            files |= collect(parent)
        return files

    return {name: collect(name) for name in own}


def test_requirements_files_match_dockerfile():
    stages = _dockerfile_requirements()
    for target in SHIPPED_STAGES:
        assert target in stages, f"Dockerfile has no `{target}` stage"
    # aws_proxied is generic with a different CMD: it must install exactly the same files
    assert stages.get("aws_proxied") == stages["generic"]
    from_dockerfile = {
        name: sorted(t for t in SHIPPED_STAGES if name in stages[t])
        for name in MANIFEST.requirements_files
    }
    from_manifest = {name: sorted(t) for name, t in MANIFEST.requirements_files.items()}
    assert from_manifest == from_dockerfile
