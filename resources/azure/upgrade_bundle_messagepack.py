"""Upgrade MessagePack 2.5.x in the base image's pre-installed Functions extension bundles."""

import base64
import hashlib
import io
import json
import re
import sys
import urllib.request
import zipfile
from pathlib import Path

BUNDLES_ROOT = Path("/FuncExtensionBundles")

VERSION = "2.5.307"
FILE_VERSION = "2.5.307.27389"
ASSEMBLY_VERSION = "2.5.0.0"
PACKAGES = {
    "MessagePack": "5bf3258af48e475f940df5a53fdc735d40a0e45530ff546b1b2c8ea17cd20909",
    "MessagePack.Annotations": "39537748152d3ff440cb3979c0da2565e6ba7304915ad1a7ae4dca678f3128dc",
}
NUGET_URL = "https://api.nuget.org/v3-flatcontainer/{id}/{version}/{id}.{version}.nupkg"


def _version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def _download(package: str) -> bytes:
    url = NUGET_URL.format(id=package.lower(), version=VERSION)
    with urllib.request.urlopen(url, timeout=120) as response:
        data = response.read()
    digest = hashlib.sha256(data).hexdigest()
    if digest != PACKAGES[package]:
        raise RuntimeError(f"sha256 mismatch for {url}: {digest}")
    return data


def _needs_upgrade(installed: str) -> bool:
    parts = _version_tuple(installed)
    return parts[:2] == _version_tuple(VERSION)[:2] and parts < _version_tuple(VERSION)


def _patch_deps(deps_path: Path, nupkgs: dict[str, bytes]) -> bool:
    raw = deps_path.read_bytes()
    deps = json.loads(raw.decode("utf-8-sig"))
    libraries = deps.get("libraries", {})
    installed = next(
        (key.split("/", 1)[1] for key in libraries if key.startswith("MessagePack/")),
        None,
    )
    if installed is None:
        return False
    if not _needs_upgrade(installed):
        print(f"{deps_path}: MessagePack {installed}, nothing to do")
        return False

    renames = {f"{pkg}/{installed}": f"{pkg}/{VERSION}" for pkg in PACKAGES}
    dll_paths: dict[str, str] = {}

    for target in deps.get("targets", {}).values():
        for key in list(target):
            entry = target[key]
            for dep in PACKAGES:
                if entry.get("dependencies", {}).get(dep) == installed:
                    entry["dependencies"][dep] = VERSION
            if key in renames:
                for runtime_path, info in entry.get("runtime", {}).items():
                    if info.get("assemblyVersion") != ASSEMBLY_VERSION:
                        raise RuntimeError(
                            f"{deps_path}: unexpected assemblyVersion for {runtime_path}"
                        )
                    info["fileVersion"] = FILE_VERSION
                    dll_paths[runtime_path.rsplit("/", 1)[1]] = runtime_path
        new_target = {renames.get(key, key): value for key, value in target.items()}
        target.clear()
        target.update(new_target)

    new_libraries = {}
    for key, value in libraries.items():
        if key in renames:
            pkg = key.split("/", 1)[0]
            lower = pkg.lower()
            sha512 = hashlib.sha512(nupkgs[pkg]).digest()
            value["sha512"] = "sha512-" + base64.b64encode(sha512).decode()
            value["path"] = f"{lower}/{VERSION}"
            value["hashPath"] = f"{lower}.{VERSION}.nupkg.sha512"
            key = renames[key]
        new_libraries[key] = value
    deps["libraries"] = new_libraries

    bin_dir = deps_path.parent
    for pkg, data in nupkgs.items():
        dll_name = f"{pkg}.dll"
        if dll_name not in dll_paths:
            raise RuntimeError(f"{deps_path}: no runtime entry for {dll_name}")
        if not (bin_dir / dll_name).is_file():
            raise RuntimeError(f"{bin_dir / dll_name} not found")
        with zipfile.ZipFile(io.BytesIO(data)) as nupkg:
            (bin_dir / dll_name).write_bytes(nupkg.read(dll_paths[dll_name]))

    text = json.dumps(deps, indent=2)
    if b"\r\n" in raw:
        text = text.replace("\n", "\r\n")
    deps_path.write_bytes(text.encode("utf-8"))
    print(f"{deps_path}: MessagePack {installed} -> {VERSION}")
    return True


def _patch_csproj(csproj_path: Path) -> None:
    with open(csproj_path, encoding="utf-8", newline="") as f:
        text = f.read()
    pattern = re.compile(r'(Include="MessagePack" Version=")([0-9.]+)(")')
    match = pattern.search(text)
    if match and _needs_upgrade(match.group(2)):
        with open(csproj_path, "w", encoding="utf-8", newline="") as f:
            f.write(pattern.sub(rf"\g<1>{VERSION}\g<3>", text))
        print(f"{csproj_path}: MessagePack {match.group(2)} -> {VERSION}")


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else BUNDLES_ROOT
    if not root.is_dir():
        print(f"{root} not found, nothing to do")
        return 0
    nupkgs = {pkg: _download(pkg) for pkg in PACKAGES}
    for deps_path in sorted(root.rglob("*.deps.json")):
        _patch_deps(deps_path, nupkgs)
    for csproj_path in sorted(root.rglob("extensions.csproj")):
        _patch_csproj(csproj_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
