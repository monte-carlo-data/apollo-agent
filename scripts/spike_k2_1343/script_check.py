"""
K2-1343 spike: reference pre-run check for aws___run_script code.

A guard, not a sandbox (IAM on the narrow role is the boundary). Meant as the
executable spec for monolith's check in K2-1346. The script is accepted only if
every AWS call is a direct ``call_boto3(...)`` with literal, allowlisted
``service_name`` / ``operation_name`` (and, if given, an allowed literal
``region_name``), and nothing reaches the interpreter's dynamic features.
"""

import ast
from typing import FrozenSet, Iterable, List, Optional, Set, Tuple

_FORBIDDEN_NAMES = frozenset(
    {
        "aws_mcp",
        "exec",
        "eval",
        "compile",
        "getattr",
        "setattr",
        "delattr",
        "hasattr",
        "globals",
        "locals",
        "vars",
        "dir",
        "__import__",
        "__builtins__",
        "breakpoint",
        "open",
        "input",
        "help",
        "type",
        "object",
        "super",
        "memoryview",
        "importlib",
        "sys",
        "os",
        "builtins",
        "inspect",
        "functools",
        "operator",
    }
)
_CALL_BOTO3 = "call_boto3"
_ALLOWED_KWARGS = frozenset({"service_name", "operation_name", "region_name", "params"})


class ScriptRejected(Exception):
    pass


def check_script(
    code: str,
    allowed_api_calls: Iterable[Tuple[str, str]],
    allowed_regions: Optional[Iterable[str]] = None,
) -> List[Tuple[str, str]]:
    """Return the (service, operation) pairs the script calls; raise ScriptRejected otherwise."""
    allowed: Set[Tuple[str, str]] = {(s, o) for s, o in allowed_api_calls}
    regions: Optional[FrozenSet[str]] = (
        frozenset(allowed_regions) if allowed_regions is not None else None
    )
    try:
        tree = ast.parse(code, mode="exec")
    except SyntaxError as exc:
        raise ScriptRejected(f"syntax error: {exc.msg}") from exc

    direct_calls: Set[int] = set()
    found: List[Tuple[str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _is_name(node.func, _CALL_BOTO3):
            direct_calls.add(id(node.func))
            found.append(_check_call(node, allowed, regions))

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise ScriptRejected("import statements are not allowed")
        if isinstance(node, (ast.Global, ast.Nonlocal)):
            raise ScriptRejected("global/nonlocal are not allowed")
        if isinstance(node, ast.Name):
            if node.id in _FORBIDDEN_NAMES or node.id.startswith("__"):
                raise ScriptRejected(f"forbidden name: {node.id}")
            if node.id == _CALL_BOTO3 and id(node) not in direct_calls:
                raise ScriptRejected("call_boto3 may only be called directly")
            if node.id == _CALL_BOTO3 and not isinstance(node.ctx, ast.Load):
                raise ScriptRejected("call_boto3 may not be rebound")
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            raise ScriptRejected(f"forbidden attribute: {node.attr}")
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            args = node.args
            names = [a.arg for a in args.args + args.kwonlyargs + args.posonlyargs]
            if _CALL_BOTO3 in names or (
                not isinstance(node, ast.Lambda) and node.name == _CALL_BOTO3
            ):
                raise ScriptRejected("call_boto3 may not be shadowed")
        if isinstance(node, ast.arg) and node.arg == _CALL_BOTO3:
            raise ScriptRejected("call_boto3 may not be shadowed")
        if isinstance(node, ast.ClassDef):
            raise ScriptRejected("class definitions are not allowed")
    if not found:
        raise ScriptRejected("script makes no call_boto3 call")
    return found


def _is_name(node: ast.AST, name: str) -> bool:
    return isinstance(node, ast.Name) and node.id == name


def _literal_str(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _check_call(
    node: ast.Call,
    allowed: Set[Tuple[str, str]],
    regions: Optional[FrozenSet[str]],
) -> Tuple[str, str]:
    if node.args:
        raise ScriptRejected("call_boto3 must use keyword arguments only")
    values = {}
    for kw in node.keywords:
        if kw.arg is None:
            raise ScriptRejected("call_boto3 does not accept **kwargs")
        if kw.arg not in _ALLOWED_KWARGS:
            raise ScriptRejected(f"unexpected call_boto3 argument: {kw.arg}")
        values[kw.arg] = kw.value
    service = _literal_str(values.get("service_name", ast.Constant(None)))
    operation = _literal_str(values.get("operation_name", ast.Constant(None)))
    if service is None or operation is None:
        raise ScriptRejected("service_name and operation_name must be string literals")
    if (service, operation) not in allowed:
        raise ScriptRejected(f"API call not allowed: {service}.{operation}")
    if "region_name" in values and regions is not None:
        region = _literal_str(values["region_name"])
        if region is None or region not in regions:
            raise ScriptRejected("region_name must be an allowed string literal")
    return service, operation
