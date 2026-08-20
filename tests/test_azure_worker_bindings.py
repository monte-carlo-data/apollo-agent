"""Guard the Azure Functions app against bindings the worker would reject.

A binding whose parameter annotation the worker doesn't accept is not a runtime
error in one endpoint — it aborts function indexing, so the whole app fails to
start with FunctionLoadError and every operation is unavailable. Nothing else in
the suite covers that: the app module is never imported, and the mismatch depends
on the interpreter and on the worker bundled in the image rather than on our code.

This runs the check against the real worker from the azure image, and is skipped
elsewhere (the other stages have no Functions host). See
``azure_worker_binding_probe.py`` for why it runs in a subprocess.
"""

import json
import subprocess
import sys
from pathlib import Path
from unittest import TestCase, skipIf

from tests import azure_worker_binding_probe as probe

_PROBE = Path(probe.__file__)
_WORKER_MISSING = probe.find_worker_runtime() is None


@skipIf(_WORKER_MISSING, "no bundled Functions worker: not the azure image")
class TestAzureWorkerBindings(TestCase):
    def test_every_binding_annotation_is_accepted_by_the_worker(self):
        result = subprocess.run(
            [sys.executable, str(_PROBE)],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(
            probe.EXIT_NO_WORKER,
            result.returncode,
            f"probe found no worker despite one being present\n{result.stderr}",
        )
        # stdout carries the JSON report; anything else means the probe itself broke
        # (an import error in function_app, say), which is worth failing loudly on.
        try:
            report = json.loads(result.stdout.splitlines()[-1])
        except (ValueError, IndexError):
            self.fail(
                "probe emitted no JSON report "
                f"(exit {result.returncode})\nstdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

        bindings = report["bindings"]
        self.assertTrue(bindings, "no bindings were checked")

        rejected = [
            f"{entry['function']}({entry['parameter']}: {entry['annotation']}) "
            f"vs binding {entry['binding']}"
            for entry in bindings
            if not entry["accepted"]
        ]
        self.assertEqual(
            [],
            rejected,
            "the Functions worker would reject these bindings, so the app would "
            "fail to start:\n  " + "\n  ".join(rejected),
        )
        self.assertEqual(probe.EXIT_OK, result.returncode)
