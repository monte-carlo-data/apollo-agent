#!/usr/bin/env python3
"""Remove the unused SignalR extension from the Azure Functions extension bundle.

Why
---
The Azure Functions base image bakes in the full extension bundle, whose SignalR
Service extension is the *sole* consumer of MessagePack (verified against the
bundle's ``function.deps.json`` reverse-dependency graph). MessagePack 2.5.192
carries CVE-2026-48109 (HIGH). apollo's Azure Function app only uses HTTP and
Durable Functions triggers — never SignalR — so the entire SignalR assembly
cluster is dead weight, and MessagePack is reachable code we don't need.

How (and why deletion, not a version bump)
------------------------------------------
Docker Scout reports a .NET package only when BOTH its ``deps.json`` entry and
its DLL exist on disk. Empirically, *overwriting* MessagePack.dll with a patched
build does NOT clear the finding (Scout reads the version from the manifest,
not the binary), but *deleting* the DLL does. So we delete the SignalR-cluster
DLLs to clear the CVE, and de-register the SignalR extension from every
``extensions.json`` so the Functions host doesn't try to load the now-missing
startup type at boot.

Safety
------
Every DLL removed below is referenced exclusively within the SignalR cluster —
nothing we keep (notably WebPubSub, which uses ``Azure.Messaging.WebPubSub`` /
``Microsoft.Azure.WebPubSub.Common``) shares any of them. See the PR for the
reverse-dependency analysis.

Fail-loud
---------
Exits non-zero if the bundle / ``extensions.json`` can't be found, if
MessagePack wasn't present to remove, or if no SignalR registration was removed.
This turns a future bundle restructure into a build failure rather than a silent
no-op that would let the CVE slip back in. The bundle version directory is
globbed (not hardcoded) so this survives base-image bundle bumps.
"""

import glob
import json
import os
import sys

BUNDLE_ROOT = "/FuncExtensionBundles/Microsoft.Azure.Functions.ExtensionBundle"

# The SignalR cluster: the SignalR WebJobs extension, its Microsoft.Azure.SignalR.*
# dependencies, and MessagePack(.Annotations). All SignalR-exclusive.
SIGNALR_CLUSTER_DLLS = (
    "MessagePack.dll",
    "MessagePack.Annotations.dll",
    "Microsoft.Azure.WebJobs.Extensions.SignalRService.dll",
    "Microsoft.Azure.SignalR.dll",
    "Microsoft.Azure.SignalR.Common.dll",
    "Microsoft.Azure.SignalR.Management.dll",
    "Microsoft.Azure.SignalR.Protocols.dll",
    "Microsoft.Azure.SignalR.Serverless.Protocols.dll",
)

# The vulnerable assembly whose removal is the whole point — its absence is the
# post-condition we assert before declaring success.
VULNERABLE_DLL = "MessagePack.dll"

# Matches the SignalR registration in extensions.json. We key off the startup
# type's assembly (stable) rather than the friendly "name" field.
SIGNALR_TYPE_MARKER = "SignalRService"

# The extension we actually depend on; must survive the prune.
REQUIRED_EXTENSION_NAME = "DurableTask"


def _fatal(msg: str) -> None:
    sys.exit(f"prune_signalr_extension: FATAL: {msg}")


def main() -> None:
    # Each bundle "bin" directory (bin/ and bin_v3/<rid>/) carries its own
    # extensions.json next to the assemblies, so the extensions.json locations
    # double as the set of directories to prune.
    ext_json_paths = sorted(
        glob.glob(f"{BUNDLE_ROOT}/*/**/extensions.json", recursive=True)
    )
    if not ext_json_paths:
        _fatal(
            f"no extensions.json found under {BUNDLE_ROOT} "
            "(extension bundle layout changed?)"
        )

    deleted_dlls = 0
    removed_vulnerable = False
    deregistered = 0

    for ext_json in ext_json_paths:
        bin_dir = os.path.dirname(ext_json)

        # 1. Delete the SignalR-cluster DLLs present in this bin directory.
        for name in SIGNALR_CLUSTER_DLLS:
            dll = os.path.join(bin_dir, name)
            if os.path.exists(dll):
                os.remove(dll)
                deleted_dlls += 1
                if name == VULNERABLE_DLL:
                    removed_vulnerable = True

        # 2. De-register SignalR so the host won't load the missing startup type.
        with open(ext_json) as f:
            data = json.load(f)
        before = len(data["extensions"])
        data["extensions"] = [
            e
            for e in data["extensions"]
            if SIGNALR_TYPE_MARKER not in e.get("typeName", "")
        ]
        removed_here = before - len(data["extensions"])
        if removed_here:
            with open(ext_json, "w") as f:
                json.dump(data, f, separators=(",", ":"))
            deregistered += removed_here

        # Sanity: the extension we depend on must still be registered here.
        if not any(
            e.get("name") == REQUIRED_EXTENSION_NAME for e in data["extensions"]
        ):
            _fatal(
                f"{REQUIRED_EXTENSION_NAME} extension missing from {ext_json} "
                "after prune — refusing to ship a broken Durable Functions image"
            )

    if not removed_vulnerable:
        _fatal(
            f"{VULNERABLE_DLL} was not found to remove — the SignalR/MessagePack "
            "layout changed; refusing to ship a build that may still contain "
            "CVE-2026-48109"
        )
    if deregistered == 0:
        _fatal("no SignalR registration found in any extensions.json to remove")

    # Post-condition: no MessagePack assembly survives anywhere in the bundle.
    leftover = glob.glob(f"{BUNDLE_ROOT}/**/MessagePack*.dll", recursive=True)
    if leftover:
        _fatal(f"MessagePack DLLs still present after prune: {leftover}")

    print(
        f"prune_signalr_extension: deleted {deleted_dlls} SignalR-cluster DLL(s) "
        f"and de-registered SignalR from {deregistered} extensions.json file(s) "
        f"across {len(ext_json_paths)} bundle bin dir(s)"
    )


if __name__ == "__main__":
    main()
