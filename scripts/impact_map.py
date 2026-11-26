#!/usr/bin/env python3
"""Impact map: which images, platforms and connection types a packaging change touches.

Compares two git refs. For every pin that changed in a ``requirements*.txt`` it walks the
``# via`` annotations pip-compile writes up to the packages that pulled it in, then maps
those packages to connection types through ``impact_map_manifest.toml`` and through the
``apollo/`` modules that import them. Requirements files map to the image targets that
install them. Anything it can't resolve fails open: every image target and platform.

Stdlib only, so it runs in a bare CI Python. See docs/impact-map.md.

    python scripts/impact_map.py --base origin/main --head HEAD [--format json|markdown]
"""

import argparse
import ast
import fnmatch
import json
import logging
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
ALL_TARGETS = ("generic", "cloudrun", "lambda", "azure")
ALL_PLATFORMS = ("aws", "gcp", "azure")
AREAS = ("metadata_collection", "query_logs", "monitors", "other")
SCOPES = ("direct", "transitive", "runtime", "dev")
DEFAULT_MANIFEST = Path(__file__).with_name("impact_map_manifest.toml")
SCANNED_SOURCE_DIR = "apollo"

AREA_LABELS = {
    "metadata_collection": "metadata collection",
    "query_logs": "query log collection",
    "monitors": "a monitor (custom SQL)",
    "other": "",
}

logger = logging.getLogger("impact_map")

_PIN_RE = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[[^\]]*\])?\s*"
    r"(?:==\s*(?P<version>[^\s;#]+)|@\s*(?P<url>[^\s;#]+))"
)
_IN_NAME_RE = re.compile(r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)")
_IMPORT_RE = re.compile(
    r"^\s*(?:from\s+(?P<from>[A-Za-z_][\w.]*)\s+import\s+(?P<names>.+)"
    r"|import\s+(?P<modules>.+))"
)


class ImpactMapError(Exception):
    """An internal error: the analysis could not run (bad ref, broken manifest, ...)."""


def normalize(name: str) -> str:
    """PEP 503 normalized distribution name."""
    return re.sub(r"[-_.]+", "-", name).lower()


# --------------------------------------------------------------------------- git


class Git:
    def __init__(self, repo: Path):
        self.repo = repo

    def _run(self, *args: str, ok_codes: tuple[int, ...] = (0,)) -> str:
        proc = subprocess.run(
            ["git", "-C", str(self.repo), *args],
            capture_output=True,
            text=True,
        )
        if proc.returncode not in ok_codes:
            raise ImpactMapError(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
        return proc.stdout

    def verify_ref(self, ref: str) -> None:
        self._run("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")

    def merge_base(self, base: str, head: str) -> str:
        return self._run("merge-base", base, head).strip()

    def changed_files(self, base: str, head: str) -> list[str]:
        out = self._run("diff", "--name-only", base, head)
        return sorted(line for line in out.splitlines() if line)

    def show(self, ref: str, path: str) -> str | None:
        """File content at ``ref``, or None when the file doesn't exist there.

        Any other failure (unreadable ref or object) raises, so it can't pass for "absent".
        """
        if not self._ls_tree(ref, path):
            return None
        return self._run("show", f"{ref}:{path}")

    def _ls_tree(self, ref: str, path: str, recursive: bool = False) -> list[str]:
        """Paths under ``path`` at ``ref``, relative to the repo root and unquoted."""
        args = ["ls-tree", "-z", "--full-tree", "--name-only"] + (
            ["-r"] if recursive else []
        )
        out = self._run(*args, ref, "--", path)
        return [name for name in out.split("\0") if name]

    def python_sources(self, ref: str, path: str) -> dict[str, str]:
        """file -> content for every ``*.py`` under ``path`` at ``ref`` (one git call)."""
        files = [
            f for f in self._ls_tree(ref, path, recursive=True) if f.endswith(".py")
        ]
        if not files:
            return {}
        proc = subprocess.run(
            ["git", "-C", str(self.repo), "cat-file", "--batch"],
            input="".join(f"{ref}:{f}\n" for f in files).encode(),
            capture_output=True,
        )
        if proc.returncode != 0:
            raise ImpactMapError(f"git cat-file failed: {proc.stderr.decode().strip()}")
        sources: dict[str, str] = {}
        out = proc.stdout
        pos = 0
        for f in files:
            header_end = out.index(b"\n", pos)
            header = out[pos:header_end].split()
            if len(header) != 3:
                raise ImpactMapError(f"cannot read {ref}:{f}: {out[pos:header_end]!r}")
            size = int(header[2])
            start = header_end + 1
            sources[f] = out[start : start + size].decode("utf-8", errors="replace")
            pos = start + size + 1
        return sources


# --------------------------------------------------------------------- parsing


@dataclass
class Pin:
    version: str
    parents: set[str] = field(default_factory=set)
    top_level: bool = False


def parse_requirements_txt(text: str | None) -> dict[str, Pin]:
    """Parse pip-compile output: pins plus their ``# via`` parents."""
    pins: dict[str, Pin] = {}
    if not text:
        return pins
    current: Pin | None = None
    in_via = False
    for line in text.splitlines():
        if not line.strip():
            continue
        if not line[0].isspace() and not line.startswith("#"):
            in_via = False
            match = _PIN_RE.match(line)
            if not match or line.startswith("-"):
                current = None
                continue
            version = match.group("version") or match.group("url") or ""
            current = Pin(version=version)
            pins[normalize(match.group("name"))] = current
            continue
        stripped = line.strip()
        if current is None or not stripped.startswith("#"):
            continue
        comment = stripped.lstrip("#").strip()
        if comment.startswith("via"):
            in_via = True
            comment = comment[len("via") :].strip()
            if not comment:
                continue
        elif not in_via:
            continue
        _add_parent(current, comment)
    return pins


def _add_parent(pin: Pin, token: str) -> None:
    if token.startswith("-r ") or token.startswith("--requirement"):
        pin.top_level = True
    elif token.startswith("-c ") or token.startswith("--constraint") or not token:
        return
    else:
        pin.parents.add(normalize(token.split()[0]))


def parse_requirements_in(text: str | None) -> dict[str, str]:
    """Package name -> requirement spec (comments and whitespace stripped)."""
    specs: dict[str, str] = {}
    if not text:
        return specs
    for line in text.splitlines():
        spec = re.split(r"(?:^|\s)#", line, maxsplit=1)[0].strip()
        if not spec or spec.startswith("-"):
            continue
        match = _IN_NAME_RE.match(spec)
        if match:
            specs[normalize(match.group("name"))] = " ".join(spec.split())
    return specs


def source_imports(source: str) -> set[str]:
    """Dotted module names a Python file imports (``from a import b`` -> a, a.b).

    Parses with ``ast`` so multi-line and nested imports count; falls back to a line scan
    for files that don't parse.
    """
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return set().union(*(imported_modules(line) for line in source.splitlines()))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            names.add(node.module)
            names.update(f"{node.module}.{a.name}" for a in node.names if a.name != "*")
    return names


def imported_modules(line: str) -> set[str]:
    """Dotted module names an import line can bind (``from a import b`` -> a, a.b)."""
    match = _IMPORT_RE.match(line.split("#", 1)[0])
    if not match:
        return set()
    names: set[str] = set()
    if match.group("from"):
        base = match.group("from")
        names.add(base)
        for item in match.group("names").strip("()\\ ").split(","):
            item = item.strip().strip("()").split(" as ")[0].strip()
            if item and item != "*" and re.fullmatch(r"[A-Za-z_]\w*", item):
                names.add(f"{base}.{item}")
    else:
        for item in match.group("modules").split(","):
            item = item.strip().split(" as ")[0].strip()
            if re.fullmatch(r"[A-Za-z_][\w.]*", item):
                names.add(item)
    return names


# -------------------------------------------------------------------- manifest


@dataclass
class Manifest:
    target_platforms: dict[str, list[str]]
    requirements_files: dict[str, list[str]]
    packaging_paths: list[str]
    fail_open_paths: list[str]
    connection_types: dict[str, list[str]]
    modules: dict[str, dict[str, Any]]
    packages: dict[str, dict[str, Any]]

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        try:
            data = tomllib.loads(path.read_text())
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise ImpactMapError(f"cannot read manifest {path}: {exc}") from exc
        try:
            return cls(
                target_platforms=data["targets"],
                requirements_files=data["requirements_files"],
                packaging_paths=data["packaging_paths"],
                fail_open_paths=data["fail_open_paths"],
                connection_types=data["connection_types"],
                modules=data.get("modules", {}),
                packages={normalize(k): v for k, v in data.get("packages", {}).items()},
            )
        except KeyError as exc:
            raise ImpactMapError(f"manifest {path} is missing {exc}") from exc

    def package_imports(self, name: str) -> list[str]:
        entry = self.packages.get(name)
        if entry is not None and "imports" in entry:
            return list(entry["imports"])
        return [name.replace("-", "_")]

    def module_entry(self, file_path: str) -> dict[str, Any] | None:
        """Longest manifest ``modules`` key that is the file or a directory containing it."""
        best: str | None = None
        for key in self.modules:
            if file_path == key or (key.endswith("/") and file_path.startswith(key)):
                if best is None or len(key) > len(best):
                    best = key
        return self.modules[best] if best is not None else None


# -------------------------------------------------------------------- analysis


@dataclass
class ChangedPackage:
    name: str
    from_version: str | None
    to_version: str | None
    requirements_files: set[str] = field(default_factory=set)

    def describe(self) -> str:
        return f"{self.name} {self.from_version or '(new)'} → {self.to_version or '(removed)'}"


@dataclass
class Result:
    changed_files: list[str] = field(default_factory=list)
    changed_packages: dict[tuple[str, str | None, str | None], ChangedPackage] = field(
        default_factory=dict
    )
    image_targets: set[str] = field(default_factory=set)
    connection_types: set[str] = field(default_factory=set)
    # (area, connection_type, text) -> details (what triggered the check)
    manual_checks: dict[tuple[str, str | None, str], list[str]] = field(
        default_factory=dict
    )
    unmapped_packages: set[str] = field(default_factory=set)
    fail_open_reasons: list[str] = field(default_factory=list)

    def add_check(
        self,
        area: str,
        connection_type: str | None,
        text: str,
        detail: str | None = None,
    ) -> None:
        details = self.manual_checks.setdefault((area, connection_type, text), [])
        if detail and detail not in details:
            details.append(detail)


class ImpactMap:
    def __init__(self, git: Git, manifest: Manifest, base: str, head: str):
        self.git = git
        self.manifest = manifest
        self.base = base
        self.head = head
        self._imports: dict[str, list[tuple[str, set[str]]]] = {}

    # ---------------------------------------------------------------- public

    def analyze(self) -> dict[str, Any]:
        result = Result()
        self.git.verify_ref(self.base)
        self.git.verify_ref(self.head)
        # Compare from the merge base, like a PR diff: both the changed-file list and the
        # "before" contents, so changes that landed on the base after the branch point
        # don't show up as reverted.
        self.base = self.git.merge_base(self.base, self.head)
        packaging = [
            f
            for f in self.git.changed_files(self.base, self.head)
            if self._is_packaging(f)
        ]
        result.changed_files = packaging
        if not packaging:
            logger.info("no packaging change between %s and %s", self.base, self.head)
            return self._render(result, packaging_changed=False)

        changed = set(packaging)
        for path in packaging:
            if self._matches(path, self.manifest.fail_open_paths):
                result.fail_open_reasons.append(f"`{path}` changed")
                result.add_check(
                    "other",
                    None,
                    f"`{path}` changed: it affects every image. Run a metadata collection "
                    "and a monitor on each platform's agent.",
                )
            elif path.endswith(".txt"):
                self._analyze_txt(path, result)
            elif path.endswith(".in"):
                self._analyze_in(path, changed, result)
        return self._render(result, packaging_changed=True)

    # -------------------------------------------------------------- helpers

    @staticmethod
    def _matches(path: str, patterns: list[str]) -> bool:
        return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)

    def _is_packaging(self, path: str) -> bool:
        return self._matches(path, self.manifest.packaging_paths)

    def _fail_open_unmapped(self, result: Result, package: str, why: str) -> None:
        result.unmapped_packages.add(package)
        result.fail_open_reasons.append(why)
        result.add_check(
            "other",
            None,
            f"{why}. Add it to `scripts/impact_map_manifest.toml` (see docs/impact-map.md); "
            "until then every image and platform is checked.",
        )

    def _analyze_in(self, path: str, changed: set[str], result: Result) -> None:
        txt = path[: -len(".in")] + ".txt"
        base_specs = parse_requirements_in(self.git.show(self.base, path))
        head_specs = parse_requirements_in(self.git.show(self.head, path))
        differing = sorted(
            name
            for name in base_specs.keys() | head_specs.keys()
            if base_specs.get(name) != head_specs.get(name)
        )
        if txt not in self.manifest.requirements_files:
            reason = f"`{path}` has no compiled file the manifest maps to an image"
            for name in differing:
                self._fail_open_unmapped(result, name, reason)
            if not differing:
                result.fail_open_reasons.append(reason)
                result.add_check(
                    "other", None, f"{reason}: add it to `[requirements_files]`."
                )
            return
        if differing and txt not in changed:
            for name in differing:
                self._fail_open_unmapped(
                    result,
                    name,
                    f"`{path}` changed `{name}` but `{txt}` was not recompiled",
                )

    def _analyze_txt(self, path: str, result: Result) -> None:
        targets = self.manifest.requirements_files.get(path)
        base_pins = parse_requirements_txt(self.git.show(self.base, path))
        head_pins = parse_requirements_txt(self.git.show(self.head, path))
        for name in sorted(base_pins.keys() | head_pins.keys()):
            old = base_pins[name].version if name in base_pins else None
            new = head_pins[name].version if name in head_pins else None
            if old == new:
                continue
            key = (name, old, new)
            package = result.changed_packages.setdefault(
                key, ChangedPackage(name, old, new)
            )
            package.requirements_files.add(path)
            if targets is None:
                self._fail_open_unmapped(
                    result, name, f"`{path}` is not mapped to any image in the manifest"
                )
                continue
            result.image_targets.update(targets)
            if targets:
                self._map_package(package, [base_pins, head_pins], result)

    def _ancestors(self, name: str, graphs: list[dict[str, Pin]]) -> set[str]:
        seen = {name}
        stack = [name]
        while stack:
            current = stack.pop()
            for graph in graphs:
                pin = graph.get(current)
                if pin is None:
                    continue
                for parent in pin.parents - seen:
                    seen.add(parent)
                    stack.append(parent)
        return seen

    def _map_package(
        self, package: ChangedPackage, graphs: list[dict[str, Pin]], result: Result
    ) -> None:
        chain = self._ancestors(package.name, graphs)
        head_graph = graphs[-1]
        unresolved = []
        for name in sorted(chain):
            # Only what ships at head must be mapped: a removed dependency's entry is
            # (rightly) gone from the manifest too.
            if name not in head_graph and any(name in g for g in graphs):
                continue
            pins = [g[name] for g in graphs if name in g]
            is_top = any(p.top_level for p in pins)
            has_parents = any(p.parents for p in pins)
            if name not in self.manifest.packages and (is_top or not has_parents):
                unresolved.append(name)
        if unresolved:
            via = (
                ""
                if unresolved == [package.name]
                else f" (via {', '.join(unresolved)})"
            )
            self._fail_open_unmapped(
                result,
                package.name,
                f"`{package.name}`{via} has no entry in the impact-map manifest",
            )

        own_types: set[str] = set()
        for name in sorted(chain):
            types = self._map_one(name, result)
            result.connection_types.update(types)
            if name == package.name:
                own_types = types

        # Per-area checks only for the changed package's own connection types: a driver
        # bump needs its collection paths checked, the drivers that merely pull it in don't.
        own = self.manifest.packages.get(package.name, {})
        for connection_type in sorted(own_types):
            for area in own.get("areas", []):
                if area in self.manifest.connection_types.get(connection_type, []):
                    result.add_check(
                        area,
                        connection_type,
                        "connection validation only tests the connection; run "
                        f"{AREA_LABELS[area]} on a {connection_type} connection",
                        package.describe(),
                    )

    def _map_one(self, name: str, result: Result) -> set[str]:
        """Connection types one package serves; records its manual checks on ``result``."""
        entry = self.manifest.packages.get(name, {})
        types = set(entry.get("connection_types", []))
        for check in entry.get("manual", []):
            result.add_check(
                check["area"], check.get("connection_type"), check["reason"]
            )
        for file_path in sorted(self._importers(name)):
            module = self.manifest.module_entry(file_path)
            if module is None:
                continue
            types.update(module.get("connection_types", []))
            if "manual" in module:
                result.add_check(
                    "other", None, module["manual"], f"`{file_path}` imports `{name}`"
                )
        return types

    def _importers(self, package: str) -> set[str]:
        """apollo/ files at the head ref that import any of the package's modules."""
        if self.head not in self._imports:
            sources = self.git.python_sources(self.head, SCANNED_SOURCE_DIR)
            self._imports[self.head] = sorted(
                (file_path, source_imports(text)) for file_path, text in sources.items()
            )
        prefixes = self.manifest.package_imports(package)
        hits = set()
        for file_path, modules in self._imports[self.head]:
            if any(m == p or m.startswith(p + ".") for m in modules for p in prefixes):
                hits.add(file_path)
        return hits

    def _render(self, result: Result, packaging_changed: bool) -> dict[str, Any]:
        fail_open = bool(result.fail_open_reasons)
        targets = set(ALL_TARGETS) if fail_open else result.image_targets
        platforms = {
            platform
            for target in targets
            for platform in self.manifest.target_platforms[target]
        }
        if fail_open:
            platforms = set(ALL_PLATFORMS)
            types = set(self.manifest.connection_types)
            logger.warning("failing open: %s", "; ".join(result.fail_open_reasons))
        else:
            types = result.connection_types
        return {
            "schema_version": SCHEMA_VERSION,
            "packaging_changed": packaging_changed,
            "changed_files": result.changed_files,
            "changed_packages": [
                {
                    "name": p.name,
                    "from": p.from_version,
                    "to": p.to_version,
                    "requirements_files": sorted(p.requirements_files),
                }
                for _, p in sorted(
                    result.changed_packages.items(), key=lambda kv: str(kv[0])
                )
            ],
            "image_targets": [t for t in ALL_TARGETS if t in targets],
            "platforms": [p for p in ALL_PLATFORMS if p in platforms],
            "connection_types": sorted(types),
            "manual_checks": [
                {
                    "area": area,
                    "connection_type": connection_type,
                    "reason": _reason(connection_type, text, details),
                }
                for (area, connection_type, text), details in sorted(
                    result.manual_checks.items(),
                    key=lambda kv: (AREAS.index(kv[0][0]), kv[0][1] or "", kv[0][2]),
                )
            ],
            "unmapped_packages": sorted(result.unmapped_packages),
        }


# ---------------------------------------------------------------------- output


def _reason(connection_type: str | None, text: str, details: list[str]) -> str:
    if not details:
        return text
    if connection_type:
        return f"{'; '.join(details)}: {text}."
    return f"{text} ({'; '.join(details)})."


def to_markdown(report: dict[str, Any]) -> str:
    """The manual checklist for a PR comment; empty when there is nothing to check."""
    checks = report["manual_checks"]
    if not checks:
        return ""
    lines = [
        "### Impact map: manual checks",
        "",
        "These paths only run during collection, query logs or monitors, so CI can't cover "
        "them. This list does not fail the build.",
        "",
        f"- Image targets: {', '.join(report['image_targets']) or 'none'}",
        f"- Platforms: {', '.join(report['platforms']) or 'none'}",
        f"- Connection types: {', '.join(report['connection_types']) or 'none'}",
    ]
    if report["unmapped_packages"]:
        lines.append(f"- Unmapped packages: {', '.join(report['unmapped_packages'])}")
    lines.append("")
    for check in checks:
        where = f" · `{check['connection_type']}`" if check["connection_type"] else ""
        lines.append(f"- [ ] **{check['area']}**{where}: {check['reason']}")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "--base", required=True, help="git ref to compare from (e.g. origin/main)"
    )
    parser.add_argument(
        "--head", required=True, help="git ref to compare to (e.g. HEAD)"
    )
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--repo", type=Path, default=Path("."), help="git checkout to read"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, stream=sys.stderr, format="impact_map: %(message)s"
    )

    try:
        manifest = Manifest.load(args.manifest)
        report = ImpactMap(Git(args.repo), manifest, args.base, args.head).analyze()
    except ImpactMapError as exc:
        logger.error("%s", exc)
        return 2

    if args.format == "json":
        print(json.dumps(report, indent=2))
    else:
        sys.stdout.write(to_markdown(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
