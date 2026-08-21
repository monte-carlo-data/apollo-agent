import base64
import binascii
import os
import stat
import tempfile

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry

# libkrb5 is configured by environment, not by connection-string arguments, so this
# transform's real output is process state rather than a derived value.
_KRB5_CONFIG = "KRB5_CONFIG"
_KRB5_CLIENT_KTNAME = "KRB5_CLIENT_KTNAME"
_KRB5CCNAME = "KRB5CCNAME"

# MEMORY: keeps the TGT out of any file and scopes it to this process, which is what
# SDD §5.3 chose: DB operations run in long-lived workers, so a per-process cache is
# reused across a whole collection run without ever touching disk.
_CCACHE = "MEMORY:"

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

    Writes a krb5.conf, materialises the keytab when one is supplied, and exports the
    MIT environment variables that point libkrb5 at both. Sets ``derived[<enabled>]``
    so the mapper can emit ``Trusted_Connection`` and drop ``UID``/``PWD``.

    One transform rather than a composition of ``write_ini_file`` + ``tmp_file_write``
    for two reasons: krb5.conf is not the flat ``[section]`` shape ``write_ini_file``
    emits, needing multiple sections and the nested ``REALM = { kdc = ... }`` form; and
    the three artefacts have to agree with each other, so validating them in one place
    is the only way to give a useful error.

    Conditionality is the step's job, not this transform's -- the config guards it with
    ``when``, so reaching ``_execute`` already means Windows authentication was asked
    for and the required fields are genuinely required.

    Input keys:
        realm:         Kerberos realm (required)
        kdc:           KDC hostname (required)
        principal:     client principal (required)
        keytab_base64: base64 keytab; mutually exclusive with password
        password:      service-account password; mutually exclusive with keytab_base64

    Output keys:
        none — the effect is process environment plus the step's own field_map
    """

    required_input_keys = ("realm", "kdc", "principal")
    optional_input_keys = ("keytab_base64", "password")
    required_output_keys = ()
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
        os.environ[_KRB5_CONFIG] = krb5_conf_path
        os.environ[_KRB5CCNAME] = _CCACHE

        if keytab_base64:
            # Pointing the *client* keytab at the file makes GSSAPI acquire and refresh
            # the TGT by itself whenever the cache is empty -- no kinit, no renewal
            # daemon, and no ticket-lifecycle code in the agent (SDD §5.3).
            keytab_path = self._write_keytab(str(keytab_base64), step.type)
            state.temp_files.append(keytab_path)
            os.environ[_KRB5_CLIENT_KTNAME] = keytab_path
        else:
            # No library-managed equivalent exists for a stored password, so the caller
            # is responsible for kinit. Clear any inherited value so a stale keytab from
            # another connection cannot silently satisfy this one.
            os.environ.pop(_KRB5_CLIENT_KTNAME, None)

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

    @staticmethod
    def _write_secure_temp_file(contents: bytes | str, suffix: str) -> str:
        is_bytes = isinstance(contents, bytes)
        with tempfile.NamedTemporaryFile(
            mode="wb" if is_bytes else "w", suffix=suffix, delete=False
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
        return cls._write_secure_temp_file(raw, ".keytab")


TransformRegistry.register("prepare_kerberos", PrepareKerberosTransform)
