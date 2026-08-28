import base64
import binascii
import os
import re
import stat
import tempfile
import uuid

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry
from apollo.integrations.db.sql_server_kerberos_env import KerberosConnectionParams

# Credential cache type depends on the credential form, and getting this wrong fails at
# connect time with a message that does not name the cause.
#
# keytab   -> MEMORY: GSSAPI acquires the TGT in-process from the client keytab, so the
#             cache never needs to be visible to another process and the ticket never
#             touches disk. This is what SDD §5.3 chose.
# password -> MEMORY: CANNOT work. There is no library auto-acquire for a stored
#             password, so acquisition happens by running kinit -- a separate process,
#             which populates its own per-process memory cache and takes it away when it
#             exits. The connecting process then finds "No Kerberos credentials
#             available (default cache: MEMORY:)". Verified against a live AWS Managed AD
#             + RDS SQL Server on 2026-08-21: keytab passed, password failed exactly so.
#
# The password form therefore needs a file cache the two processes can share. Prefer a
# tmpfs directory so the TGT still stays off durable disk.
#
# The MEMORY residual is per-connection: a bare "MEMORY:" resolves to the same in-process
# cache, so a later connection would find the previous TGT and the client-keytab
# auto-acquire -- which only fires on an empty cache -- would never run.
# Kerberos identifiers are interpolated into a krb5.conf that libkrb5 reads, and the
# principal becomes a kinit argv entry. A newline in realm/kdc would append arbitrary
# directives (e.g. redirecting default_ccache_name); a leading dash in a principal would be
# parsed by kinit as an option. Mirrors _KERBEROS_FIELD_PATTERNS in the monolith's
# credentials.py -- duplicated deliberately, because self-hosted credentials never pass
# through the monolith: they go from the customer's secret store straight to the agent.
_IDENTIFIER_PATTERNS = {
    "realm": re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]*\Z"),
    "kdc": re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]*(?::[0-9]{1,5})?\Z"),
    "principal": re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._/@-]*\Z"),
}

_CCACHE_MEMORY_PREFIX = "MEMORY:"
_TMPFS_DIRS = ("/dev/shm", "/run/shm")


# dns_lookup_kdc=false with an explicit kdc means no SRV lookups; rdns=false means no
# PTR lookups. Together they reduce what the agent needs from DNS to plain forward
# A-record resolution of the SQL Server FQDN -- worth doing because the ODBC driver
# derives the SPN from that name and cannot be told otherwise.
_KRB5_CONF_TEMPLATE = """\
[libdefaults]
    default_realm = {realm}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    rdns = false

[realms]
    {realm} = {{
        kdc = {kdc}
        admin_server = {kdc}
    }}

[domain_realm]
    .{domain} = {realm}
    {domain} = {realm}
"""


class PrepareKerberosTransform(Transform):
    """Assemble the Kerberos runtime for a Windows-authentication connection.

    Writes a krb5.conf, materialises the keytab when one is supplied, creates the
    credential cache the password form needs, and emits their locations for the proxy
    client. The connection-string change (``Trusted_Connection`` on, ``UID``/``PWD``
    dropped) comes from the step's own ``field_map`` rather than a derived value -- see
    ``_KERBEROS_STEP`` in ``ctp/defaults/sql_server.py``.

    One transform rather than a composition of ``write_ini_file`` + ``tmp_file_write``
    for two reasons: krb5.conf is not the flat ``[section]`` shape ``write_ini_file``
    emits, needing multiple sections and the nested ``REALM = { kdc = ... }`` form; and
    the three artefacts have to agree with each other, so validating them in one place
    is the only way to give a useful error.

    Conditionality is the step's job, not this transform's -- the config guards it with
    ``when``, so reaching ``_execute`` already means Windows authentication was asked
    for and the required fields are genuinely required.

    This transform sets no environment variables. ``KRB5_CONFIG`` / ``KRB5CCNAME`` /
    ``KRB5_CLIENT_KTNAME`` are process-global and shared with every other Kerberos
    consumer in the worker, so they are set and restored around the connect call itself
    by ``integrations/db/sql_server_kerberos_env.py``. The nested-dict passthrough
    follows Oracle's ``ssl_options``: a structured block the proxy client pops and
    resolves, because the resolution depends on process state the CTP cannot own.

    Input keys:
        realm:         Kerberos realm (required)
        kdc:           KDC hostname (required)
        principal:     client principal (required)
        keytab_base64: base64 keytab; mutually exclusive with password
        password:      service-account password; mutually exclusive with keytab_base64

    Output keys:
        kerberos: derived key for the dict of paths and cache name the proxy client
                  consumes (``krb5_config_path``, ``ccache``, and then either
                  ``client_keytab_path`` or ``principal`` + ``password``)
    """

    required_input_keys = ("realm", "kdc", "principal")
    optional_input_keys = ("keytab_base64", "password")
    required_output_keys = ("kerberos",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        realm = self._render(step, state, "realm")
        kdc = self._render(step, state, "kdc")
        principal = self._render(step, state, "principal")
        keytab_base64 = self._render(step, state, "keytab_base64")
        password = self._render(step, state, "password")

        for name, value in (("realm", realm), ("kdc", kdc), ("principal", principal)):
            if not value:
                raise CtpPipelineError(
                    stage="transform_execute",
                    step_name=step.type,
                    message=(
                        f"'{name}' is required when auth_type is kerberos "
                        "(Windows authentication)"
                    ),
                )

        for name, value in (("realm", realm), ("kdc", kdc), ("principal", principal)):
            if not _IDENTIFIER_PATTERNS[name].match(str(value)):
                # Names the field but never echoes the value: CTP errors reach the DC and
                # are forwarded to Sentry.
                raise CtpPipelineError(
                    stage="transform_execute",
                    step_name=step.type,
                    message=f"'{name}' contains characters that are not valid in a Kerberos identifier",
                )

        # Both forms authenticate as the same principal, so accepting both leaves it
        # ambiguous which one actually did -- and silently ignoring one would hide a
        # misconfigured credential. Reject instead.
        if keytab_base64 and password:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=(
                    "kerberos credentials must supply either a keytab or a password, "
                    "not both"
                ),
            )
        if not keytab_base64 and not password:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=(
                    "kerberos credentials must supply either a keytab or a password"
                ),
            )

        krb5_conf_path = self._write_krb5_conf(str(realm), str(kdc))
        # Register for cleanup: BaseProxyClient.close() deletes everything in
        # state.temp_files. Matters more here than for the other file-writing
        # transforms -- a keytab is a long-lived AD credential, and leaving one on disk
        # after the connection closes is exactly the exposure the MEMORY: ccache choice
        # was made to avoid.
        state.temp_files.append(krb5_conf_path)

        params: KerberosConnectionParams = {"krb5_config_path": krb5_conf_path}
        if keytab_base64:
            # GSSAPI acquires from the client keytab in-process, so an in-memory cache is
            # sufficient and the ticket never touches disk.
            params["ccache"] = f"{_CCACHE_MEMORY_PREFIX}{uuid.uuid4().hex}"
            keytab_path = self._write_keytab(str(keytab_base64), step.type)
            state.temp_files.append(keytab_path)
            params["client_keytab_path"] = keytab_path
        else:
            # kinit runs in a separate process and cannot share an in-memory cache, so
            # the password form needs a file the two can both see.
            ccache_path = self._create_ccache_file()
            state.temp_files.append(ccache_path)
            params["ccache"] = f"FILE:{ccache_path}"
            params["principal"] = str(principal)
            params["password"] = str(password)

        state.derived[step.output["kerberos"]] = params

    @staticmethod
    def _render(step: TransformStep, state: PipelineState, key: str):
        template = step.input.get(key)
        if template is None:
            return None
        rendered = (
            TemplateEngine.render(template, state)
            if isinstance(template, str)
            else template
        )
        # Jinja renders an absent optional field to the empty string in some templates
        # and to None in others; normalise so callers only check falsiness.
        return None if rendered in (None, "", "None") else rendered

    @classmethod
    def _write_secure_temp_file(cls, contents: bytes | str, suffix: str) -> str:
        return cls._write_secure_temp_file_in(contents, suffix, None)

    @staticmethod
    def _write_secure_temp_file_in(
        contents: bytes | str, suffix: str, directory: str | None
    ) -> str:
        is_bytes = isinstance(contents, bytes)
        with tempfile.NamedTemporaryFile(
            mode="wb" if is_bytes else "w", suffix=suffix, delete=False, dir=directory
        ) as handle:
            handle.write(contents)
            path = handle.name

        try:
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        except Exception:
            os.unlink(path)
            raise
        return path

    @classmethod
    def _create_ccache_file(cls) -> str:
        """Create an empty 0600 credential cache for the password path.

        Placed on tmpfs when available so the TGT stays out of durable storage; falls back
        to the default temp dir rather than failing, since a working connection matters
        more than the storage medium.
        """
        for directory in _TMPFS_DIRS:
            if os.path.isdir(directory):
                return cls._write_secure_temp_file_in(b"", ".ccache", directory)
        return cls._write_secure_temp_file(b"", ".ccache")

    @classmethod
    def _write_krb5_conf(cls, realm: str, kdc: str) -> str:
        # The domain_realm mapping keys off the lowercased realm, which is the AD
        # convention (realm is the uppercased domain).
        conf = _KRB5_CONF_TEMPLATE.format(realm=realm, kdc=kdc, domain=realm.lower())
        return cls._write_secure_temp_file(conf, ".conf")

    @classmethod
    def _write_keytab(cls, keytab_base64: str, step_name: str) -> str:
        try:
            # validate=True so malformed input fails here with a clear message rather
            # than producing a truncated keytab and an opaque GSSAPI error at connect.
            raw = base64.b64decode(keytab_base64, validate=True)
        except (binascii.Error, ValueError) as error:
            # Deliberately does not echo the value -- CTP errors reach the DC and are
            # forwarded to Sentry, and this one is a credential.
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step_name,
                message=f"keytab is not valid base64: {type(error).__name__}",
            ) from None

        if not raw:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step_name,
                message="keytab decoded to zero bytes",
            )
        # tmpfs first, like the ccache: the keytab outlives the ticket, so putting the
        # ticket in memory while the keytab lands on the writable layer is backwards.
        for directory in _TMPFS_DIRS:
            if os.path.isdir(directory):
                return cls._write_secure_temp_file_in(raw, ".keytab", directory)
        return cls._write_secure_temp_file(raw, ".keytab")


TransformRegistry.register("prepare_kerberos", PrepareKerberosTransform)
