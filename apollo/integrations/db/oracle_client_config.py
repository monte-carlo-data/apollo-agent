"""Oracle TLS/mTLS configuration for the Oracle proxy client.

This module owns everything about securing an Oracle connection so the proxy
client can stay focused on the DB proxy responsibilities. Two very different
mechanisms live here because thin and thick mode handle TLS differently:

* Thin mode (pure Python driver) takes a standard ``ssl.SSLContext`` — see
  ``create_oracle_ssl_context``.
* Thick mode (Oracle Instant Client) does NOT accept an ``ssl.SSLContext``. It
  validates the server certificate against an Oracle *wallet* whose location is
  configured via ``WALLET_LOCATION`` in ``sqlnet.ora``. Established the hard way
  against real RDS Oracle:
  - The wallet must be a ``cwallet.sso`` produced by Oracle's ``orapki`` tool; a
    Python-built PKCS#12 is opened but never honored as a trust anchor.
  - The ``connect()`` ``wallet_location`` parameter does NOT drive server-cert
    trust in thick mode — only ``WALLET_LOCATION`` in ``sqlnet.ora`` does.
  - The wallet must already exist at ``WALLET_LOCATION`` when
    ``init_oracle_client`` runs, or the wallet subsystem starts with no trust
    store and every TLS connection fails with ORA-29024.
  - Oracle **caches the trust store after the first successful connection** for
    the process lifetime — it does NOT re-read the wallet per connect. So the
    wallet is built ONCE, from the first connection's ``ssl_options``, before
    init. A thick agent therefore supports a single Oracle TLS trust config,
    which fits its dedicated, process-global design (thick mode is already a
    one-way, process-global switch that can't coexist with thin connections).

``configure_thick_connection`` encapsulates that one-time setup (build wallet -> write
``sqlnet.ora`` -> ``init_oracle_client``), serialized by a process lock because
``sqlnet.ora`` and ``init_oracle_client`` are process-global.
"""

import hashlib
import logging
import os
import secrets
import shutil
import ssl
import subprocess
import tempfile
import threading
from typing import Optional

import oracledb
from cryptography import x509
from cryptography.hazmat.primitives.serialization import (
    BestAvailableEncryption,
    Encoding,
    load_pem_private_key,
    pkcs12,
)

from apollo.integrations.db.ssl_options import SslOptions

# Agent-level thick-mode switch. Thick mode (Oracle Instant Client) is a
# process-global, one-way setting that cannot coexist with thin connections in
# the same process, so it is configured per-agent via this env var rather than
# per-connection. Enable it only on an agent dedicated to thick-mode Oracle.
_ENV_VAR_THICK_MODE = "MCD_ORACLE_THICK_MODE"

# Optional override for the thick-mode TLS cipher suites (comma-separated Oracle
# cipher names). The default below intentionally includes RSA key-exchange
# suites: Oracle Client omits them from its defaults, but some servers (e.g. AWS
# RDS Oracle, which offers AES256-GCM-SHA384 / SSL_RSA_WITH_AES_256_GCM_SHA384)
# require them — the thin path handles this via `@SECLEVEL=1`, but thick has no
# such knob and needs an explicit SSL_CIPHER_SUITES list. Modern ECDHE suites
# are listed first so they are preferred where the server supports them.
_ENV_VAR_SSL_CIPHER_SUITES = "MCD_ORACLE_SSL_CIPHER_SUITES"
_DEFAULT_SSL_CIPHER_SUITES = ",".join(
    [
        "SSL_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "SSL_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "SSL_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "SSL_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "SSL_RSA_WITH_AES_256_GCM_SHA384",
        "SSL_RSA_WITH_AES_128_GCM_SHA256",
        "SSL_RSA_WITH_AES_256_CBC_SHA256",
        "SSL_RSA_WITH_AES_128_CBC_SHA256",
        "SSL_RSA_WITH_AES_256_CBC_SHA",
        "SSL_RSA_WITH_AES_128_CBC_SHA",
    ]
)

# Location of the bundled minimal JRE + orapki jars used to build Oracle wallets.
# Thick-mode TLS trust requires a real Oracle wallet (cwallet.sso) produced by
# Oracle's orapki tool; a Python-built PKCS#12 is not honored as a trust store.
# orapki is a Java tool, so the Docker image ships a stripped JRE plus the
# oraclepki jars under this directory (jre/ and lib/). Overridable for tests.
_ENV_VAR_ORACLE_PKI_HOME = "MCD_ORACLE_PKI_HOME"
_DEFAULT_ORACLE_PKI_HOME = "/opt/oracle-pki"
_ORAPKI_MAIN_CLASS = "oracle.security.pki.textui.OraclePKITextUI"

logger = logging.getLogger(__name__)

# Process-wide thick-mode state, established once on the first Oracle connection
# and reused for the process lifetime (Oracle caches the trust store after the
# first successful connection). Guarded by _thick_lock.
_thick_lock = threading.Lock()
_thick_config_dir: Optional[str] = None  # holds sqlnet.ora (passed to init)
_thick_wallet_dir: Optional[str] = None  # the single TLS wallet, or None if no TLS
# sha256 of the established TLS material (CA + client identity), so a later
# connection needing a different CA *or* client certificate is detected.
_thick_tls_fingerprint: Optional[str] = None


def thick_mode_enabled() -> bool:
    return os.getenv(_ENV_VAR_THICK_MODE, "false").strip().lower() == "true"


def _reset_for_testing() -> None:
    """Reset the process-wide thick-mode state so tests start fresh.

    Test-only — production never resets this (thick init is process-global and
    one-way). Gives tests a single seam instead of poking each private global.
    """
    global _thick_config_dir, _thick_wallet_dir, _thick_tls_fingerprint
    _thick_config_dir = None
    _thick_wallet_dir = None
    _thick_tls_fingerprint = None


def _oracle_pki_home() -> str:
    return os.getenv(_ENV_VAR_ORACLE_PKI_HOME) or _DEFAULT_ORACLE_PKI_HOME


def _run_orapki(*args: str) -> None:
    """Run an ``orapki`` wallet command via the bundled minimal JRE.

    ``args`` are the orapki arguments (e.g. ``"wallet", "create", ...``). Raises
    ``RuntimeError`` with orapki's output on failure. The command line itself is
    kept out of the message, and any password values we passed (``-pwd`` /
    ``-pkcs12pwd``) are scrubbed from orapki's stdout/stderr before they reach the
    exception — which propagates to logs AND the agent's API error response, where
    no keyword/entropy redaction would catch a bare CLI-flag value.

    Accepted risk: orapki only takes the wallet password as a CLI argument, so it
    is briefly visible via ``/proc/<pid>/cmdline`` for the subprocess lifetime. In
    the agent's single-tenant-per-container deployment this is not a meaningful
    exposure; there is no stdin/password-file alternative for orapki.
    """
    home = _oracle_pki_home()
    java_bin = os.path.join(home, "jre", "bin", "java")
    classpath = os.path.join(home, "lib", "*")
    result = subprocess.run(
        [java_bin, "-cp", classpath, _ORAPKI_MAIN_CLASS, *args],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        output = f"{result.stdout.strip()} {result.stderr.strip()}"
        # orapki can echo its own argv (including the password flags) back in
        # usage/error output; redact the secret values we passed.
        secrets_to_scrub = [
            args[i + 1]
            for i, arg in enumerate(args)
            if arg in ("-pwd", "-pkcs12pwd") and i + 1 < len(args)
        ]
        for secret in secrets_to_scrub:
            if secret:
                output = output.replace(secret, "__redacted__")
        raise RuntimeError(
            f"orapki wallet command failed (exit {result.returncode}): {output}"
        )


def _new_wallet_password() -> str:
    # orapki requires a wallet password of at least 8 chars containing letters and
    # digits; the suffix guarantees both classes regardless of token_urlsafe output.
    return secrets.token_urlsafe(24) + "aA1"


def _import_client_identity(
    wallet_dir: str, wallet_password: str, ssl_options: SslOptions
) -> None:
    """Import the client key + cert into the wallet as its user credential (mTLS).

    orapki imports a client identity from a PKCS#12 file, so the PEM key/cert are
    packed into a temporary, password-protected ``client.p12`` and imported.

    NOTE: not yet validated end-to-end — no reachable Oracle server currently
    requests client authentication (AWS RDS Oracle can't). Implemented so mTLS
    works where a server requires it; the one-way TLS path is proven.
    """
    if not (ssl_options.cert_data and ssl_options.key_data):
        return

    client_cert = x509.load_pem_x509_certificate(ssl_options.cert_data.encode())
    client_key = load_pem_private_key(
        ssl_options.key_data.encode(),
        password=(
            ssl_options.key_password.encode() if ssl_options.key_password else None
        ),
    )
    p12_password = _new_wallet_password()
    p12 = pkcs12.serialize_key_and_certificates(
        b"mcd-oracle-client",
        # load_pem_private_key's union includes key types PKCS12 rejects (e.g. DH);
        # a TLS client key is always RSA/EC, so this is safe.
        client_key,  # type: ignore[arg-type]
        client_cert,
        None,
        BestAvailableEncryption(p12_password.encode()),
    )
    p12_path = os.path.join(wallet_dir, "client.p12")
    try:
        with open(p12_path, "wb") as p12_file:
            p12_file.write(p12)
        _run_orapki(
            "wallet",
            "import_pkcs12",
            "-wallet",
            wallet_dir,
            "-pwd",
            wallet_password,
            "-pkcs12file",
            p12_path,
            "-pkcs12pwd",
            p12_password,
        )
    finally:
        if os.path.exists(p12_path):
            os.unlink(p12_path)


def create_oracle_thick_wallet(ssl_options: SslOptions) -> Optional[str]:
    """Build an auto-login Oracle wallet (``cwallet.sso``) for thick-mode TLS.

    Adds the CA cert(s) from ``ca_data`` as trusted certs via orapki, and — when
    ``cert_data``/``key_data`` are present — imports the client identity for mTLS.
    Returns the wallet directory, or ``None`` when SSL is disabled / no CA data is
    provided. On failure the partially-built directory is removed.
    """
    if ssl_options.disabled or not ssl_options.ca_data:
        return None

    wallet_dir = tempfile.mkdtemp(prefix="mcd_oracle_wallet_")
    try:
        wallet_password = _new_wallet_password()
        _run_orapki(
            "wallet",
            "create",
            "-wallet",
            wallet_dir,
            "-pwd",
            wallet_password,
            "-auto_login",
        )

        # Add each CA in ca_data as a trusted certificate. orapki's -cert takes a
        # single certificate, so a bundle is split into one temp file per cert.
        ca_certs = x509.load_pem_x509_certificates(ssl_options.ca_data.encode())
        for index, ca_cert in enumerate(ca_certs):
            ca_path = os.path.join(wallet_dir, f"ca_{index}.pem")
            try:
                with open(ca_path, "wb") as ca_file:
                    ca_file.write(ca_cert.public_bytes(Encoding.PEM))
                _run_orapki(
                    "wallet",
                    "add",
                    "-wallet",
                    wallet_dir,
                    "-trusted_cert",
                    "-cert",
                    ca_path,
                    "-pwd",
                    wallet_password,
                )
            finally:
                if os.path.exists(ca_path):
                    os.unlink(ca_path)

        _import_client_identity(wallet_dir, wallet_password, ssl_options)
        return wallet_dir
    except BaseException:
        shutil.rmtree(wallet_dir, ignore_errors=True)
        raise


def _write_thick_sqlnet(
    config_dir: str,
    wallet_dir: Optional[str] = None,
    verify_identity: bool = True,
) -> None:
    """Write the thick-mode ``sqlnet.ora`` into ``config_dir``.

    Always sets ``SSL_CIPHER_SUITES`` (thick has no ``@SECLEVEL`` knob, so the
    suites older servers require must be listed explicitly or the handshake fails
    with ORA-28860). When ``wallet_dir`` is given it also points
    ``WALLET_LOCATION`` at the wallet (the only trust source thick honors) and
    sets ``SSL_SERVER_DN_MATCH`` from ``verify_identity`` (default on). Note
    thick's DN match is Oracle-DN based, not SAN/hostname based like thin's
    ``check_hostname``.
    """
    suites = os.getenv(_ENV_VAR_SSL_CIPHER_SUITES) or _DEFAULT_SSL_CIPHER_SUITES
    lines = [f"SSL_CIPHER_SUITES = ({suites})"]
    if wallet_dir:
        lines.append(
            "WALLET_LOCATION = (SOURCE = (METHOD = FILE) "
            f"(METHOD_DATA = (DIRECTORY = {wallet_dir})))"
        )
        lines.append(f"SSL_SERVER_DN_MATCH = {'TRUE' if verify_identity else 'FALSE'}")
    with open(os.path.join(config_dir, "sqlnet.ora"), "w") as sqlnet:
        sqlnet.write("\n".join(lines) + "\n")


def _ensure_thick_config_dir() -> str:
    """Create (once) the config dir for ``init_oracle_client(config_dir=)`` and
    return its path. Reused for the process lifetime."""
    global _thick_config_dir
    if _thick_config_dir is None:
        _thick_config_dir = tempfile.mkdtemp(prefix="mcd_oracle_tns_")
    return _thick_config_dir


def _tls_fingerprint(ssl_options: SslOptions) -> Optional[str]:
    """Fingerprint the full TLS material — CA trust plus client identity.

    Thick-mode trust is process-global and frozen at the first connection, so the
    fingerprint must cover not just ``ca_data`` but the mTLS client identity
    (``cert_data``/``key_data``/``key_password``) too; otherwise a later
    same-CA-but-different-client-cert connection would silently reuse the first
    connection's wallet identity instead of being rejected.
    """
    if ssl_options.disabled or not ssl_options.ca_data:
        return None
    material = "\0".join(
        part or ""
        for part in (
            ssl_options.ca_data,
            ssl_options.cert_data,
            ssl_options.key_data,
            ssl_options.key_password,
        )
    )
    return hashlib.sha256(material.encode()).hexdigest()


def configure_thick_connection(ssl_options: SslOptions) -> None:
    """Configure thick-mode TLS trust for this process (idempotent, thread-safe).

    On the FIRST Oracle connection this builds the wallet from ``ssl_options`` and
    writes ``sqlnet.ora`` BEFORE ``init_oracle_client`` — the ordering Oracle
    requires for the wallet subsystem to start with the trust store present.
    Because Oracle's trust store is process-global and frozen at the first
    ``init_oracle_client``, a later connection that needs TLS trust the process
    can't apply (initialized without a wallet, or with a different CA) is rejected
    with a clear error rather than failing later with a confusing ORA-29024.

    The caller runs ``oracledb.connect`` after this returns.
    """
    global _thick_wallet_dir, _thick_tls_fingerprint
    with _thick_lock:
        config_dir = _ensure_thick_config_dir()
        if oracledb.is_thin_mode():
            # First Oracle connection in this process. Build the wallet + write
            # WALLET_LOCATION before init so trust is present when the wallet
            # subsystem starts; is_thin_mode() flips to False after init.
            wallet_dir = create_oracle_thick_wallet(ssl_options)
            try:
                _write_thick_sqlnet(config_dir, wallet_dir, ssl_options.verify_identity)
                oracledb.init_oracle_client(config_dir=config_dir)
            except BaseException:
                # sqlnet write or init failed after the wallet was built. Since
                # is_thin_mode() stays True, the next attempt would rebuild and
                # orphan this dir (holding the CA and, for mTLS, the client key);
                # drop it now and leave the globals unset so a retry starts clean.
                if wallet_dir:
                    shutil.rmtree(wallet_dir, ignore_errors=True)
                raise
            # Commit the process-wide state only once init has succeeded.
            _thick_wallet_dir = wallet_dir
            _thick_tls_fingerprint = _tls_fingerprint(ssl_options)
            logger.info(
                "oracle: thick mode initialized"
                + ("; TLS wallet configured" if _thick_wallet_dir else "")
            )
        else:
            _require_compatible_established_tls(ssl_options)


def _require_compatible_established_tls(ssl_options: SslOptions) -> None:
    """Reject a connection whose TLS trust the process can't apply.

    Thick-mode trust is process-global and fixed at the first
    ``init_oracle_client``. If a TLS connection arrives after the process was
    initialized without a wallet, or with a different CA or client certificate,
    its trust/identity cannot be applied — fail loudly with actionable guidance
    instead of letting Oracle fail later with ORA-29024 (certificate validation
    failure) or silently authenticating with the wrong client identity.
    """
    wants_tls = bool(not ssl_options.disabled and ssl_options.ca_data)
    if not wants_tls:
        return
    if _thick_wallet_dir is None:
        raise RuntimeError(
            "Oracle thick mode was already initialized in this agent process "
            "without an SSL trust store, so this SSL connection cannot be "
            "established — thick-mode TLS trust is process-global and fixed at "
            "first use. Restart the agent so its first Oracle connection uses "
            "this SSL configuration, or use a dedicated agent for this SSL "
            "integration."
        )
    if _tls_fingerprint(ssl_options) != _thick_tls_fingerprint:
        raise RuntimeError(
            "Oracle thick mode was already initialized in this agent process "
            "with a different SSL configuration (CA certificate or client "
            "certificate). A thick-mode agent supports a single Oracle TLS "
            "configuration (process-global). Restart the agent so it initializes "
            "with this configuration, or use a dedicated agent for this integration."
        )


def create_oracle_ssl_context(ssl_options: SslOptions) -> ssl.SSLContext | None:
    """
    Create an SSL context for Oracle connections.

    Creates an SSLContext with relaxed cipher requirements to support older cipher suites
    used by some databases (e.g., AWS RDS Oracle uses AES256-GCM-SHA384).

    Args:
        ssl_options: SslOptions object containing CA data and optionally client cert/key

    Returns:
        Configured ssl.SSLContext for use with oracledb connections, or None if SSL is disabled
        or no CA data is provided.

    Note: this is the thin-mode path. ssl_context is thin-only; thick mode uses
    create_oracle_thick_wallet() instead.
    """
    if ssl_options.disabled or not ssl_options.ca_data:
        return None

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    # Respect SslOptions verification settings
    # - skip_cert_verification: skips ALL validation (cert + hostname)
    # - verify_cert: whether to validate the certificate chain
    # - verify_identity: whether to check hostname matches certificate
    ssl_context.check_hostname = (
        not ssl_options.skip_cert_verification and ssl_options.verify_identity
    )
    ssl_context.verify_mode = (
        ssl.CERT_NONE
        if ssl_options.skip_cert_verification
        else (ssl.CERT_REQUIRED if ssl_options.verify_cert else ssl.CERT_NONE)
    )

    # @SECLEVEL=1 allows older ciphers like AES256-GCM-SHA384 (plain RSA, no forward secrecy)
    ssl_context.set_ciphers("DEFAULT:@SECLEVEL=1")

    # Load CA certificate for server verification (if not skipping verification)
    if ssl_options.ca_data and not ssl_options.skip_cert_verification:
        ssl_context.load_verify_locations(cadata=ssl_options.ca_data)

    # Load client certificate if provided (for mTLS)
    # Note: load_cert_chain() only accepts file paths, not string data,
    # so we must use temp files (unlike load_verify_locations which accepts cadata)
    if ssl_options.cert_data and ssl_options.key_data:
        cert_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
        cert_file.write(ssl_options.cert_data)
        cert_file.close()

        key_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
        key_file.write(ssl_options.key_data)
        key_file.close()

        try:
            ssl_context.load_cert_chain(
                certfile=cert_file.name,
                keyfile=key_file.name,
                password=ssl_options.key_password,
            )
        finally:
            # Clean up temp files after loading into SSL context
            os.unlink(cert_file.name)
            os.unlink(key_file.name)

    # Log SSL context creation with options
    has_client_cert = bool(ssl_options.cert_data and ssl_options.key_data)
    logger.info(
        "Oracle SSL context created",
        extra={
            "ssl_options": {
                "has_ca_data": bool(ssl_options.ca_data),
                "has_client_cert": has_client_cert,
                "skip_cert_verification": ssl_options.skip_cert_verification,
                "verify_cert": ssl_options.verify_cert,
                "verify_identity": ssl_options.verify_identity,
                "check_hostname": ssl_context.check_hostname,
                "verify_mode": ssl_context.verify_mode,
            }
        },
    )

    return ssl_context
