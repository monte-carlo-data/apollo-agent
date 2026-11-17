"""Swap a newer Microsoft.IdentityModel 8.x family into the Azure Functions host baked into the base image.

Usage: upgrade_host_identitymodel.py <host_dir> <version> <package.nupkg>...

The IdentityModel packages ship in lockstep and share internals, so every package passed in is swapped
together: the DLL in <host_dir> is replaced and the host's *.deps.json target, library and dependent
entries are rewritten so the version on disk and the version scanners read agree. Only upgrades within
the same major line. Does nothing for a package the host already ships at >= <version>. Exits non-zero
if the host layout is not what this script expects.
"""

import base64
import hashlib
import json
import re
import struct
import sys
import zipfile
from pathlib import Path
from typing import NoReturn


def _fail(message: str) -> NoReturn:
    print(f"upgrade_host_identitymodel: {message}", file=sys.stderr)
    sys.exit(1)


def _version_tuple(version: str) -> tuple:
    return tuple(int(part) for part in version.split("."))


def _file_version(dll: bytes) -> str:
    # VS_FIXEDFILEINFO: signature, struct version, then dwFileVersionMS / dwFileVersionLS.
    offset = dll.find(struct.pack("<I", 0xFEEF04BD))
    if offset < 0:
        _fail("no VS_FIXEDFILEINFO in the new assembly")
    ms, ls = struct.unpack_from("<II", dll, offset + 8)
    return f"{ms >> 16}.{ms & 0xFFFF}.{ls >> 16}.{ls & 0xFFFF}"


def _nuspec(path: Path) -> tuple:
    with zipfile.ZipFile(path) as nupkg:
        nuspec = next(
            n for n in nupkg.namelist() if n.endswith(".nuspec") and "/" not in n
        )
        text = nupkg.read(nuspec).decode("utf-8-sig")
    package_id = re.search(r"<id>([^<]+)</id>", text)
    version = re.search(r"<version>([^<]+)</version>", text)
    if not package_id or not version:
        _fail(f"{path.name} has no package id/version in its nuspec")
    return package_id.group(1), version.group(1)


def _read_dll(path: Path, package_id: str, tfm: str) -> bytes:
    member = f"lib/{tfm}/{package_id}.dll"
    with zipfile.ZipFile(path) as nupkg:
        if member not in nupkg.namelist():
            _fail(f"{path.name} has no {member}")
        return nupkg.read(member)


def _installed(deps: dict, package_id: str) -> tuple:
    keys = {key for key in deps["libraries"] if key.split("/")[0] == package_id}
    if len(keys) != 1:
        _fail(f"expected one {package_id} library entry, found {sorted(keys)}")
    key = keys.pop()
    runtimes = [
        (path, info)
        for target in deps["targets"].values()
        for path, info in target.get(key, {}).get("runtime", {}).items()
    ]
    if not runtimes:
        _fail(f"no runtime entry for {key}")
    return key.split("/")[1], runtimes[0][0]


def _rename(mapping: dict, old_key: str, new_key: str, value: dict) -> dict:
    return {
        new_key if key == old_key else key: value if key == old_key else item
        for key, item in mapping.items()
    }


def main(host_dir: Path, version: str, nupkgs: list) -> None:
    deps_files = list(host_dir.glob("*.deps.json"))
    if len(deps_files) != 1:
        _fail(f"expected one *.deps.json in {host_dir}, found {len(deps_files)}")
    deps_path = deps_files[0]
    raw = deps_path.read_bytes()
    bom = raw.startswith(b"\xef\xbb\xbf")
    newline = "\r\n" if b"\r\n" in raw else "\n"
    deps = json.loads(raw.decode("utf-8-sig"))

    upgrades = []
    for nupkg in nupkgs:
        package_id, nupkg_version = _nuspec(nupkg)
        if nupkg_version != version:
            _fail(f"{nupkg.name} is {package_id} {nupkg_version}, expected {version}")
        old, runtime_path = _installed(deps, package_id)
        if _version_tuple(old) >= _version_tuple(version):
            print(f"{package_id} {old} already >= {version}, leaving it")
            continue
        if _version_tuple(old)[0] != _version_tuple(version)[0]:
            _fail(f"{package_id} {old} is not on the {version} major line")
        dll = _read_dll(nupkg, package_id, runtime_path.split("/")[1])
        sha512 = base64.b64encode(hashlib.sha512(nupkg.read_bytes()).digest()).decode()
        target_dll = host_dir / Path(runtime_path).name
        if not target_dll.is_file():
            _fail(f"{target_dll} is missing")
        upgrades.append((package_id, old, runtime_path, target_dll, dll, sha512))

    if not upgrades:
        return
    olds = {old for _, old, *_ in upgrades}
    if len(olds) != 1:
        _fail(
            f"the host ships a mixed IdentityModel family {sorted(olds)}; refusing a partial swap"
        )

    for package_id, old, runtime_path, target_dll, dll, sha512 in upgrades:
        old_key, new_key = f"{package_id}/{old}", f"{package_id}/{version}"
        for name, target in deps["targets"].items():
            for entry in target.values():
                if entry.get("dependencies", {}).get(package_id) == old:
                    entry["dependencies"][package_id] = version
            if old_key in target:
                entry = target[old_key]
                entry["runtime"] = {
                    runtime_path: {
                        "assemblyVersion": f"{version}.0",
                        "fileVersion": _file_version(dll),
                    }
                }
                deps["targets"][name] = _rename(target, old_key, new_key, entry)
        library = deps["libraries"][old_key]
        library["sha512"] = f"sha512-{sha512}"
        library["path"] = f"{package_id.lower()}/{version}"
        library["hashPath"] = f"{package_id.lower()}.{version}.nupkg.sha512"
        deps["libraries"] = _rename(deps["libraries"], old_key, new_key, library)

        target_dll.write_bytes(dll)
        # The ReadyToRun perf map describes the replaced (precompiled) image, not the IL-only one.
        target_dll.with_suffix(".ni.r2rmap").unlink(missing_ok=True)
        print(f"{package_id}: {old} -> {version}")

    text = json.dumps(deps, indent=2, ensure_ascii=False).replace("\n", newline)
    deps_path.write_bytes((b"\xef\xbb\xbf" if bom else b"") + text.encode("utf-8"))


if __name__ == "__main__":
    if len(sys.argv) < 4:
        _fail(
            "usage: upgrade_host_identitymodel.py <host_dir> <version> <package.nupkg>..."
        )
    main(Path(sys.argv[1]), sys.argv[2], [Path(arg) for arg in sys.argv[3:]])
