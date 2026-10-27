"""Upgrade the MSAL.NET assemblies the azure base image ships in the Functions host and extension bundles."""

import base64
import hashlib
import io
import json
import sys
import urllib.request
import zipfile
from pathlib import Path

ROOTS = (Path("/azure-functions-host"), Path("/FuncExtensionBundles"))

VERSION = "4.81.0"
ASSEMBLY_VERSION = "4.81.0.0"
FILE_VERSION = "4.81.0.0"
PACKAGES = {
    "Microsoft.Identity.Client": "eab2879b183870a6392ab8426a4cbdf07e1f896490598a4b4d22821a2f759dc3",
    "Microsoft.Identity.Client.Broker": "e806208707642dc38cfe3d1212f9e658164f6bb51baefe30fdb35590b19ad754",
    "Microsoft.Identity.Client.Extensions.Msal": "8acb49a4e10425361163e18b12782ed7034c687ce2e6302321f4f39370490203",
}
NUGET_URL = "https://api.nuget.org/v3-flatcontainer/{id}/{version}/{id}.{version}.nupkg"

_nupkgs: dict[str, bytes] = {}


def _version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def _needs_upgrade(installed: str) -> bool:
    parts = _version_tuple(installed)
    return parts[0] == _version_tuple(VERSION)[0] and parts < _version_tuple(VERSION)


def _nupkg(package: str) -> bytes:
    if package not in _nupkgs:
        url = NUGET_URL.format(id=package.lower(), version=VERSION)
        with urllib.request.urlopen(url, timeout=120) as response:
            data = response.read()
        digest = hashlib.sha256(data).hexdigest()
        if digest != PACKAGES[package]:
            raise RuntimeError(f"sha256 mismatch for {url}: {digest}")
        _nupkgs[package] = data
    return _nupkgs[package]


def _patch_deps(deps_path: Path) -> list[str]:
    raw = deps_path.read_bytes()
    text = raw.decode("utf-8-sig")
    newline = "\r\n" if "\r\n" in text else "\n"
    deps = json.loads(text)
    libraries = deps.get("libraries", {})

    installed: dict[str, str] = {}
    for key in libraries:
        name, _, version = key.partition("/")
        if name in PACKAGES and _needs_upgrade(version):
            installed[name] = version
    if not installed:
        return []

    renames = {f"{pkg}/{old}": f"{pkg}/{VERSION}" for pkg, old in installed.items()}
    dll_paths: dict[str, str] = {}

    for target in deps.get("targets", {}).values():
        for key, entry in target.items():
            dependencies = entry.get("dependencies", {})
            for pkg, old in installed.items():
                if dependencies.get(pkg) == old:
                    dependencies[pkg] = VERSION
            if key in renames:
                pkg = key.split("/", 1)[0]
                for runtime_path, info in entry.get("runtime", {}).items():
                    if not info.get("assemblyVersion", "").startswith("4."):
                        raise RuntimeError(
                            f"{deps_path}: unexpected assemblyVersion for {runtime_path}"
                        )
                    info["assemblyVersion"] = ASSEMBLY_VERSION
                    info["fileVersion"] = FILE_VERSION
                    dll_paths[pkg] = runtime_path
        new_target = {renames.get(key, key): value for key, value in target.items()}
        target.clear()
        target.update(new_target)

    new_libraries = {}
    for key, value in libraries.items():
        if key in renames:
            pkg = key.split("/", 1)[0]
            lower = pkg.lower()
            sha512 = hashlib.sha512(_nupkg(pkg)).digest()
            value["sha512"] = "sha512-" + base64.b64encode(sha512).decode()
            value["path"] = f"{lower}/{VERSION}"
            value["hashPath"] = f"{lower}.{VERSION}.nupkg.sha512"
            key = renames[key]
        new_libraries[key] = value
    deps["libraries"] = new_libraries

    bin_dir = deps_path.parent
    for pkg in installed:
        if pkg not in dll_paths:
            raise RuntimeError(f"{deps_path}: no runtime entry for {pkg}")
        dll = bin_dir / f"{pkg}.dll"
        if not dll.is_file():
            raise RuntimeError(f"{dll} not found")
        with zipfile.ZipFile(io.BytesIO(_nupkg(pkg))) as nupkg:
            dll.write_bytes(nupkg.read(dll_paths[pkg]))
        (bin_dir / f"{pkg}.ni.r2rmap").unlink(missing_ok=True)

    out = json.dumps(deps, indent=2, ensure_ascii=False).replace("\n", newline)
    prefix = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    deps_path.write_bytes(prefix + out.encode("utf-8"))
    return [f"{pkg} {old} -> {VERSION}" for pkg, old in sorted(installed.items())]


def main(roots: list[Path]) -> int:
    for root in roots:
        if not root.is_dir():
            print(f"{root} not found", file=sys.stderr)
            return 1
        deps_files = sorted(root.rglob("*.deps.json"))
        if not deps_files:
            print(f"no *.deps.json found under {root}", file=sys.stderr)
            return 1
        for deps_path in deps_files:
            changes = _patch_deps(deps_path)
            if changes:
                print(f"{deps_path}: {', '.join(changes)}")
    return 0


if __name__ == "__main__":
    sys.exit(main([Path(arg) for arg in sys.argv[1:]] or list(ROOTS)))
