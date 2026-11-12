"""Swap a newer MessagePack into the Azure Functions extension bundles baked into the base image.

Usage: upgrade_bundle_messagepack.py <bundles_root> <version> <messagepack.nupkg> <annotations.nupkg>

Replaces MessagePack.dll / MessagePack.Annotations.dll in every bundle bin dir that ships them
and rewrites the matching function.deps.json and extensions.csproj entries, so the version on disk and the version
scanners read agree. Only swaps within the same major.minor line, whose strong-name assembly version
(<major>.<minor>.0.0) stays binary compatible. Exits non-zero if the bundle layout or MessagePack line is not what
this script expects.
"""

import base64
import hashlib
import json
import re
import sys
import zipfile
from pathlib import Path

PACKAGES = {
    "MessagePack": "lib/net6.0/MessagePack.dll",
    "MessagePack.Annotations": "lib/netstandard2.0/MessagePack.Annotations.dll",
}


def _version_tuple(version: str) -> tuple:
    return tuple(int(part) for part in version.split("."))


def _assembly_version(version: str) -> str:
    major, minor = _version_tuple(version)[:2]
    return f"{major}.{minor}.0.0"


def _check_compatible(deps: dict, bin_dir: Path, old: str, new: str) -> None:
    if _version_tuple(old)[:2] != _version_tuple(new)[:2]:
        _fail(f"{bin_dir}: MessagePack {old} is not on the {new} minor line")
    expected = _assembly_version(new)
    for name in PACKAGES:
        runtimes = [
            runtime
            for target in deps["targets"].values()
            for runtime in target.get(f"{name}/{old}", {}).get("runtime", {}).values()
        ]
        if not runtimes:
            _fail(f"{bin_dir}: no runtime entry for {name}/{old}")
        for runtime in runtimes:
            if runtime.get("assemblyVersion") != expected:
                _fail(
                    f"{bin_dir}: {name}/{old} assemblyVersion "
                    f"{runtime.get('assemblyVersion')} != {expected}"
                )


def _file_version(dll: bytes) -> str:
    marker = "FileVersion\0".encode("utf-16-le")
    start = dll.find(marker)
    if start < 0:
        _fail("no FileVersion in the new MessagePack.dll")
    start += len(marker)
    while dll[start : start + 2] == b"\0\0":
        start += 2
    end = dll.find(b"\0\0", start)
    end += (end - start) % 2
    return dll[start:end].decode("utf-16-le")


def _fail(message: str) -> None:
    print(f"upgrade_bundle_messagepack: {message}", file=sys.stderr)
    sys.exit(1)


def _rewrite_deps(
    deps: dict, old: str, new: str, sha512: dict, file_version: str
) -> None:
    for target in deps["targets"].values():
        for name in PACKAGES:
            entry = target.pop(f"{name}/{old}", None)
            if entry is None:
                continue
            for runtime in entry.get("runtime", {}).values():
                if "fileVersion" in runtime:
                    runtime["fileVersion"] = file_version
            target[f"{name}/{new}"] = entry
        for entry in target.values():
            dependencies = entry.get("dependencies", {})
            for name in PACKAGES:
                if dependencies.get(name) == old:
                    dependencies[name] = new
    for name in PACKAGES:
        library = deps["libraries"].pop(f"{name}/{old}", None)
        if library is None:
            _fail(f"no libraries entry for {name}/{old}")
        lower = name.lower()
        library["sha512"] = sha512[name]
        library["path"] = f"{lower}/{new}"
        library["hashPath"] = f"{lower}.{new}.nupkg.sha512"
        deps["libraries"][f"{name}/{new}"] = library


def main() -> None:
    root, new, mp_nupkg, ann_nupkg = sys.argv[1:5]
    nupkgs = {"MessagePack": Path(mp_nupkg), "MessagePack.Annotations": Path(ann_nupkg)}
    sha512 = {
        name: "sha512-"
        + base64.b64encode(hashlib.sha512(path.read_bytes()).digest()).decode()
        for name, path in nupkgs.items()
    }

    with zipfile.ZipFile(nupkgs["MessagePack"]) as archive:
        file_version = _file_version(archive.read(PACKAGES["MessagePack"]))

    deps_files = sorted(Path(root).glob("*/*/bin*/**/function.deps.json"))
    if not deps_files:
        _fail(f"no function.deps.json found under {root}")

    upgraded = 0
    for deps_file in deps_files:
        bin_dir = deps_file.parent
        deps = json.loads(deps_file.read_text())
        old_keys = [
            key
            for target in deps["targets"].values()
            for key in target
            if key.startswith("MessagePack/")
        ]
        if not old_keys:
            if (bin_dir / "MessagePack.dll").exists():
                _fail(f"{bin_dir} has MessagePack.dll but no deps.json entry")
            continue
        old = old_keys[0].split("/", 1)[1]
        if _version_tuple(old) >= _version_tuple(new):
            print(f"{bin_dir}: MessagePack {old} already >= {new}, leaving as is")
            continue
        _check_compatible(deps, bin_dir, old, new)
        for name, member in PACKAGES.items():
            dll = bin_dir / f"{name}.dll"
            if not dll.exists():
                _fail(f"{dll} missing")
            with zipfile.ZipFile(nupkgs[name]) as archive:
                dll.write_bytes(archive.read(member))
        _rewrite_deps(deps, old, new, sha512, file_version)
        deps_file.write_text(json.dumps(deps, indent=2))
        print(f"{bin_dir}: MessagePack {old} -> {new}")
        upgraded += 1

    for csproj in Path(root).glob("*/*/extensions.csproj"):
        text = csproj.read_text()
        match = re.search(r'Include="MessagePack" Version="([0-9.]+)"', text)
        if not match or _version_tuple(match.group(1)) >= _version_tuple(new):
            continue
        if _version_tuple(match.group(1))[:2] != _version_tuple(new)[:2]:
            _fail(
                f"{csproj}: MessagePack {match.group(1)} is not on the {new} minor line"
            )
        csproj.write_text(
            text.replace(match.group(0), f'Include="MessagePack" Version="{new}"')
        )

    print(f"upgraded MessagePack in {upgraded} bundle bin dir(s)")


if __name__ == "__main__":
    main()
