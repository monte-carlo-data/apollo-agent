import os
import socket
import urllib.request
from contextlib import contextmanager
from typing import Dict, Iterator
from unittest.mock import patch


def addrinfo(ip: str, port: int = 443) -> tuple:
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    return (family, socket.SOCK_STREAM, 6, "", (ip, port))


@contextmanager
def isolated_env(env: Dict[str, str]) -> Iterator[None]:
    # Replaces os.environ entirely with `env` (clear=True) and restricts httpx
    # proxy discovery to env vars, so the machine's (e.g. macOS system) proxy
    # settings can't leak into the test
    with patch.dict(os.environ, env, clear=True), patch(
        "httpx._utils.getproxies", urllib.request.getproxies_environment
    ):
        yield
