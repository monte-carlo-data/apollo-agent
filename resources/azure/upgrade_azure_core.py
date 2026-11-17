"""Upgrade Azure.Core (and the assemblies it requires) in the azure base image's .NET trees."""

import base64
import hashlib
import io
import json
import sys
import urllib.request
import zipfile
from pathlib import Path

ROOTS = (Path("/azure-functions-host"), Path("/FuncExtensionBundles"))

# name -> (version, assemblyVersion, fileVersion, nupkg sha256)
PACKAGES = {
    "Azure.Core": (
        "1.59.0",
        "1.59.0.0",
        "1.5900.26.30904",
        "032c5abe6e2c5265e34086ef0201fdf8de42e8a77adc9bb73a908b6b5dc0af98",
    ),
    "System.ClientModel": (
        "1.14.0",
        "1.14.0.0",
        "1.1400.26.30307",
        "af4bd515754893e75489c58186c5410d4b2f9eb2d929b4561ee1f86e9adc9d2f",
    ),
    "Microsoft.Identity.Client": (
        "4.84.2",
        "4.84.2.0",
        "4.84.2.0",
        "a5830133086b59ffee21535fa2fdd1a66100165911f9fa6c91458c7fa1e55353",
    ),
    "Microsoft.Identity.Client.Extensions.Msal": (
        "4.84.2",
        "4.84.2.0",
        "4.84.2.0",
        "b9611017286cb42bbd5f3c6e55ddf6eca648fba8426d795f4030d6e10adf3c27",
    ),
    "System.Memory.Data": (
        "10.0.3",
        "10.0.0.3",
        "10.0.326.7603",
        "1de7cac82b8d27b6ce34e56a1a2d9649c1ba5c8a05c9f192c1340cf4e974ebe5",
    ),
}
NUGET_URL = "https://api.nuget.org/v3-flatcontainer/{id}/{version}/{id}.{version}.nupkg"


def _version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("-", 1)[0].split("."))


def _download(package: str) -> bytes:
    version, _, _, sha256 = PACKAGES[package]
    url = NUGET_URL.format(id=package.lower(), version=version)
    with urllib.request.urlopen(url, timeout=120) as response:
        data = response.read()
    digest = hashlib.sha256(data).hexdigest()
    if digest != sha256:
        raise RuntimeError(f"sha256 mismatch for {url}: {digest}")
    return data


def _installed_versions(deps: dict) -> dict[str, str]:
    installed = {}
    for key in deps.get("libraries", {}):
        name, _, version = key.partition("/")
        if name in PACKAGES:
            installed[name] = version
    return installed


def _patch_deps(deps_path: Path, nupkgs: dict[str, bytes]) -> bool:
    raw = deps_path.read_bytes()
    deps = json.loads(raw.decode("utf-8-sig"))
    installed = _installed_versions(deps)
    if "Azure.Core" not in installed:
        return False

    upgrades = {
        name: old
        for name, old in installed.items()
        if _version_tuple(old) < _version_tuple(PACKAGES[name][0])
    }
    if not upgrades:
        print(f"{deps_path}: Azure.Core {installed['Azure.Core']}, nothing to do")
        return True

    renames = {
        f"{name}/{old}": f"{name}/{PACKAGES[name][0]}" for name, old in upgrades.items()
    }
    dll_paths: dict[str, str] = {}

    for target in deps.get("targets", {}).values():
        for key, entry in target.items():
            dependencies = entry.get("dependencies", {})
            for name, old in upgrades.items():
                if dependencies.get(name) == old:
                    dependencies[name] = PACKAGES[name][0]
            if key in renames:
                name = key.split("/", 1)[0]
                _, assembly_version, file_version, _ = PACKAGES[name]
                for runtime_path, info in entry.get("runtime", {}).items():
                    info["assemblyVersion"] = assembly_version
                    info["fileVersion"] = file_version
                    dll_paths[name] = runtime_path
        new_target = {renames.get(key, key): value for key, value in target.items()}
        target.clear()
        target.update(new_target)

    new_libraries = {}
    for key, value in deps["libraries"].items():
        if key in renames:
            name = key.split("/", 1)[0]
            version = PACKAGES[name][0]
            lower = name.lower()
            sha512 = hashlib.sha512(nupkgs[name]).digest()
            value["sha512"] = "sha512-" + base64.b64encode(sha512).decode()
            value["path"] = f"{lower}/{version}"
            value["hashPath"] = f"{lower}.{version}.nupkg.sha512"
            key = renames[key]
        new_libraries[key] = value
    deps["libraries"] = new_libraries

    bin_dir = deps_path.parent
    if "Azure.Core" in upgrades and not (bin_dir / "Azure.Core.dll").is_file():
        raise RuntimeError(f"{bin_dir / 'Azure.Core.dll'} not found")
    replacements: dict[Path, bytes] = {}
    for name in upgrades:
        if name not in dll_paths:
            raise RuntimeError(f"{deps_path}: no runtime entry for {name}")
        dll = bin_dir / f"{name}.dll"
        if not dll.is_file():
            # bundles omit some assemblies and resolve them from the host instead
            print(
                f"{deps_path}: {name} {upgrades[name]} -> {PACKAGES[name][0]} (metadata only)"
            )
            continue
        with zipfile.ZipFile(io.BytesIO(nupkgs[name])) as nupkg:
            replacements[dll] = nupkg.read(dll_paths[name])

    for dll, data in replacements.items():
        dll.write_bytes(data)
        dll.with_name(f"{dll.stem}.ni.r2rmap").unlink(missing_ok=True)
        name = dll.stem
        print(f"{dll}: {upgrades[name]} -> {PACKAGES[name][0]}")

    text = json.dumps(deps, indent=2)
    if b"\r\n" in raw:
        text = text.replace("\n", "\r\n")
    prefix = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    deps_path.write_bytes(prefix + text.encode("utf-8"))
    return True


def main() -> int:
    roots = [Path(arg) for arg in sys.argv[1:]] or list(ROOTS)
    for root in roots:
        if not root.is_dir():
            print(f"{root} not found", file=sys.stderr)
            return 1
    nupkgs = {name: _download(name) for name in PACKAGES}
    for root in roots:
        found = [
            _patch_deps(path, nupkgs) for path in sorted(root.rglob("*.deps.json"))
        ]
        if not any(found):
            print(
                f"no *.deps.json referencing Azure.Core under {root}", file=sys.stderr
            )
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
