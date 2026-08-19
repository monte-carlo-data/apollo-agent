"""Validate every Azure Functions binding in ``function_app`` against the worker.

Run as a subprocess by ``test_azure_worker_bindings.py`` — importing ``function_app``
has process-wide side effects (it strips the root logger's handlers, configures the
Azure Monitor exporter, installs a log context), so it must not happen inside the
pytest process.

Prints a JSON report to stdout and exits:

* 0 — every binding's parameter annotation is one the worker accepts
* 1 — at least one binding would be rejected (the app would fail to start)
* 3 — the bundled worker isn't present, so there is nothing to check
"""

import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Where the Functions host keeps the Python worker it will run our app with, laid
# out as <root>/<python version>/<os>/<arch>/azure_functions_runtime.
_WORKER_ROOT = Path("/azure-functions-host/workers/python")

EXIT_OK = 0
EXIT_REJECTED = 1
EXIT_NO_WORKER = 3


def find_worker_runtime(root: Path = _WORKER_ROOT) -> Optional[Path]:
    """Return the worker directory matching the running interpreter, if present.

    The version directory is resolved from ``sys.version_info`` rather than
    hardcoded, so this keeps working across Python upgrades. Any other version
    present is accepted as a fallback: a worker built for a different interpreter
    still type-checks bindings the same way, and checking it beats skipping.
    """
    if not root.is_dir():
        return None

    running = root / f"{sys.version_info.major}.{sys.version_info.minor}"
    candidates = [running] + sorted(p for p in root.iterdir() if p != running)
    for base in candidates:
        if not base.is_dir():
            continue
        for hit in sorted(base.glob("*/*/azure_functions_runtime")):
            if hit.is_dir():
                return hit.parent
    return None


def check_bindings(worker_runtime: Path) -> List[Dict[str, str]]:
    """Return a report entry per binding, mirroring the worker's own validation.

    ``azure_functions_runtime.functions.validate_function_params`` reads the
    annotation off the indexed function and runs it through
    ``check_input_type_annotation`` for that binding type; a False verdict is what
    raises FunctionLoadError and fails app startup. Only ``meta`` is imported —
    the worker's ``protos`` package ships protobuf gencode newer than the runtime
    our own requirements pin, so importing it would raise a version error here
    (the real worker runs with its own bundled protobuf).
    """
    sys.path.append(str(worker_runtime))
    meta = importlib.import_module("azure_functions_runtime.bindings.meta")
    meta.load_binding_registry()

    # The app root is wherever the Functions host loads the app from, so take it
    # from the image's own setting instead of assuming the caller's PYTHONPATH
    # already covers it.
    script_root = os.getenv("AzureWebJobsScriptRoot")
    if script_root and script_root not in sys.path:
        sys.path.append(script_root)

    # Imported only now: the binding registry has to be loaded from the app's own
    # azure-functions install, and importing the app is the side-effecting step.
    function_app = importlib.import_module("function_app")

    report: List[Dict[str, str]] = []
    for function in function_app.app.get_functions():
        annotations = getattr(function.get_user_function(), "__annotations__", {})
        for binding in function.get_bindings():
            annotation: Any = annotations.get(binding.name)
            if not isinstance(annotation, type):
                # No annotation (or a non-type one): the worker skips the check too.
                continue
            accepted = meta.check_input_type_annotation(binding.type, annotation, False)
            report.append(
                {
                    "function": function.get_function_name(),
                    "parameter": binding.name,
                    "binding": binding.type,
                    "annotation": annotation.__name__,
                    "accepted": accepted,
                }
            )
    return report


def main() -> int:
    worker_runtime = find_worker_runtime()
    if worker_runtime is None:
        print(json.dumps({"worker": None, "bindings": []}))
        return EXIT_NO_WORKER

    # configure_azure_monitor() runs at import and rejects an empty connection
    # string, so supply a syntactically valid placeholder when the deployment one
    # isn't set. Nothing is exported: no telemetry is emitted by an import alone.
    os.environ.setdefault(
        "APPLICATIONINSIGHTS_CONNECTION_STRING",
        "InstrumentationKey=00000000-0000-0000-0000-000000000000",
    )

    report = check_bindings(worker_runtime)
    print(json.dumps({"worker": str(worker_runtime), "bindings": report}))
    rejected = [entry for entry in report if not entry["accepted"]]
    return EXIT_REJECTED if rejected else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
