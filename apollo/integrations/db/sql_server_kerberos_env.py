"""Scope the MIT Kerberos process environment to one connection attempt.

libkrb5 takes its configuration from the process environment, not from connection-string
arguments, and msodbcsql exposes no per-connection Kerberos settings. That makes the
settings process-global in a worker that runs 8 gunicorn threads, so they are set here --
around the connect call that reads them -- rather than in the CTP transform that
materializes the files.

The transform (``ctp/transforms/prepare_kerberos.py``) writes the krb5.conf, the keytab
and the empty credential cache, and passes their paths through ``connect_args`` under the
``kerberos`` key. This module owns the mutable process state: it sets the variables, runs
kinit for the password form, and restores what was there before.

Two properties the transform could not provide:

- Restoration. Hive and Impala with ``auth_mechanism=GSSAPI`` read the same variables. A
  SQL Server connection that left ``KRB5_CONFIG`` pointing at its own single-realm file --
  deleted on client close -- would break an unrelated integration on the same agent.
- Serialization across the connect. The variables are shared, so acquiring a ticket into
  one connection's cache and then connecting has to be atomic with respect to other
  Kerberos connections. Holding the lock only over the kinit, as an earlier version did,
  left two connections under different principals able to read each other's values.

The collector has a twin of this module (``kerberos_environment.py`` in data-collector).
The two are deliberately separate implementations; see that module's docstring.
"""

import logging
import os
import subprocess
import threading
from contextlib import contextmanager
from typing import (
    Any,
    Generator,
    Optional,
)

logger = logging.getLogger(__name__)

_KRB5_CONFIG = "KRB5_CONFIG"
_KRB5_CLIENT_KTNAME = "KRB5_CLIENT_KTNAME"
_KRB5CCNAME = "KRB5CCNAME"
_MANAGED_ENV_VARS = (_KRB5_CONFIG, _KRB5_CLIENT_KTNAME, _KRB5CCNAME)

# Bounded so an unreachable KDC surfaces as a clear error instead of holding the lock --
# and therefore every other Kerberos connection -- for the operation's whole timeout.
_KINIT_TIMEOUT_SECONDS = 30

# Held for the entire scope, not just the kinit. See the module docstring.
_KERBEROS_ENV_LOCK = threading.Lock()

# Keys the transform passes through connect_args.
ATTR_KERBEROS = "kerberos"
_ATTR_KRB5_CONFIG_PATH = "krb5_config_path"
_ATTR_CCACHE = "ccache"
_ATTR_CLIENT_KEYTAB_PATH = "client_keytab_path"
_ATTR_PRINCIPAL = "principal"
_ATTR_PASSWORD = "password"


class KerberosEnvironmentError(Exception):
    """Raised when the Kerberos runtime cannot be established for a connection."""


@contextmanager
def kerberos_environment(params: dict[str, Any]) -> Generator[None, None, None]:
    """Point libkrb5 at this connection's artefacts, then restore the previous values.

    Serialized against other Kerberos connections in this process for the whole scope,
    so the caller should do the connect inside and nothing else.
    """
    krb5_config_path = params.get(_ATTR_KRB5_CONFIG_PATH)
    ccache = params.get(_ATTR_CCACHE)
    client_keytab_path = params.get(_ATTR_CLIENT_KEYTAB_PATH)
    principal = params.get(_ATTR_PRINCIPAL)
    password = params.get(_ATTR_PASSWORD)

    if not krb5_config_path or not ccache:
        raise KerberosEnvironmentError(
            "kerberos connection parameters are incomplete: "
            f"'{_ATTR_KRB5_CONFIG_PATH}' and '{_ATTR_CCACHE}' are both required"
        )

    with _KERBEROS_ENV_LOCK:
        saved = {name: os.environ.get(name) for name in _MANAGED_ENV_VARS}
        try:
            os.environ[_KRB5_CONFIG] = krb5_config_path
            os.environ[_KRB5CCNAME] = ccache
            if client_keytab_path:
                # Pointing the *client* keytab at the file makes GSSAPI acquire and
                # refresh the TGT by itself whenever the cache is empty -- no kinit and no
                # ticket-lifecycle code here.
                os.environ[_KRB5_CLIENT_KTNAME] = client_keytab_path
            else:
                # Clear any inherited keytab so another connection's cannot silently
                # satisfy this one under a different principal.
                os.environ.pop(_KRB5_CLIENT_KTNAME, None)
                _acquire_ticket_with_password(str(principal), str(password))
            yield
        finally:
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def _acquire_ticket_with_password(principal: str, password: str) -> None:
    """Acquire a TGT by running kinit against the cache KRB5CCNAME now names.

    Only the password form needs this: there is no library auto-acquire for a stored
    password, so acquisition happens in a separate process. That is also why the password
    form cannot use a MEMORY cache -- kinit would populate its own and take it away when
    it exits.

    Unconditional, with no probe for an existing ticket. The cache was created empty by
    the transform and is private to this connection, so a probe could only ever hit
    another connection's cache and reuse its ticket under the wrong principal.
    """
    try:
        result = subprocess.run(
            # "--" so a principal beginning with a dash cannot be parsed as an option.
            # The transform also rejects that shape; both, because self-hosted credentials
            # reach this path without passing through the transform's validation.
            ["kinit", "--", principal],
            # stdin, never argv -- argv is readable via /proc by any local process, so a
            # password there leaks outside this one.
            input=password,
            text=True,
            capture_output=True,
            timeout=_KINIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise KerberosEnvironmentError(
            f"timed out acquiring a Kerberos ticket after {_KINIT_TIMEOUT_SECONDS}s; "
            "the KDC may be unreachable (check network access to port 88 and that the "
            "realm's kdc hostname resolves)"
        ) from None
    except FileNotFoundError:
        raise KerberosEnvironmentError(
            "kinit not found: the krb5 client tools are required for Windows "
            "authentication (install krb5-user)"
        ) from None

    if result.returncode != 0:
        # kinit reports the reason on stderr and never echoes the password, so this is
        # safe to surface -- and it is the difference between a bad password, an unknown
        # principal and a clock-skew failure.
        detail = (result.stderr or result.stdout or "").strip()
        raise KerberosEnvironmentError(f"could not acquire a Kerberos ticket: {detail}")


def pop_kerberos_params(connect_args: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Remove and return the kerberos block, which is not an ODBC parameter.

    Popping is required, not tidiness: ``odbc_string_from_dict`` stringifies every value
    it is given, so a nested dict left here would serialize the keytab path -- and on the
    password form the password itself -- into the connection string.
    """
    params = connect_args.pop(ATTR_KERBEROS, None)
    if params is None:
        return None
    if not isinstance(params, dict):
        raise KerberosEnvironmentError(
            f"'{ATTR_KERBEROS}' in connect_args must be a dict, got "
            f"{type(params).__name__}"
        )
    return params
