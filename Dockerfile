# Oracle Instant Client (Basic) — native libraries oracledb needs for "thick"
# mode, which some Oracle configurations require (e.g. servers that only expose
# the legacy password verifier that thin mode can't authenticate with).
ARG ORACLE_IC_ZIP=instantclient-basic-linux.x64-23.8.0.25.04.zip
ARG ORACLE_IC_URL=https://download.oracle.com/otn_software/linux/instantclient/2380000

# Oracle wallet tooling (orapki) for thick-mode TLS. Thick mode validates the
# server certificate against an Oracle wallet (cwallet.sso), and only Oracle's
# orapki tool can produce one that is trusted — a Python-built PKCS#12 is opened
# but never honored as a trust anchor. orapki is a Java tool, so build a stripped,
# minimal JRE (java.base + java.naming + jdk.crypto.ec) and fetch the oraclepki
# jars once here, then copy the ~55MB result into each runtime stage. Built on
# Amazon Linux 2023 (glibc 2.34 — the oldest of our runtime bases) so the runtime
# is forward-compatible with the newer-glibc Debian stages. osdt_core/osdt_cert
# are not published at the Instant Client version, so they are pinned separately.
ARG ORACLE_PKI_VERSION=23.8.0.25.04
ARG ORACLE_OSDT_VERSION=21.11.0.0
FROM amazoncorretto:21-al2023 AS oracle-pki-builder
ARG ORACLE_PKI_VERSION
ARG ORACLE_OSDT_VERSION
RUN dnf install -y binutils \
    && jlink --add-modules java.base,java.naming,jdk.crypto.ec \
         --strip-debug --no-header-files --no-man-pages --output /opt/oracle-pki/jre \
    && mkdir -p /opt/oracle-pki/lib \
    && M=https://repo1.maven.org/maven2/com/oracle/database/security \
    && curl -fsSLo /opt/oracle-pki/lib/oraclepki.jar $M/oraclepki/${ORACLE_PKI_VERSION}/oraclepki-${ORACLE_PKI_VERSION}.jar \
    && curl -fsSLo /opt/oracle-pki/lib/osdt_core.jar $M/osdt_core/${ORACLE_OSDT_VERSION}/osdt_core-${ORACLE_OSDT_VERSION}.jar \
    && curl -fsSLo /opt/oracle-pki/lib/osdt_cert.jar $M/osdt_cert/${ORACLE_OSDT_VERSION}/osdt_cert-${ORACLE_OSDT_VERSION}.jar

# system-base — system-level dependencies only (apt packages, no venv).
# Published as `<version>-system-base` so downstream consumers (e.g. hermes-agent)
# can build their own venv against the same native libs without inheriting
# apollo's pip-installed dependencies.
FROM python:3.13.14-slim AS system-base

ENV APP_HOME=/app
WORKDIR $APP_HOME

# Refresh apt index and upgrade base-image packages so OS-level security fixes
# (glibc, openssh, nghttp2, etc.) land on every rebuild rather than waiting for
# the upstream python:3.13.14-slim tag to be republished.
RUN apt-get update && apt-get upgrade -y
# install git as we need it for the direct oscrypto dependency
# this is a temporary workaround and it should be removed once we update oscrypto to 1.3.1+
# see: https://community.snowflake.com/s/article/Python-Connector-fails-to-connect-with-LibraryNotFoundError-Error-detecting-the-version-of-libcrypto
RUN apt-get install -y --no-install-recommends git
# install libcrypt1 for IBM DB2 ibm-db package compatibility (provides libcrypt.so.1)
RUN apt-get install -y --no-install-recommends libcrypt1
# openssh-client required by git client
RUN apt-get install -y openssh-client

# Azure database clients uses pyodbc which requires unixODBC and 'ODBC Driver 17 for SQL Server'
# ODBC Driver 17's latest release was April, 2024. To patch vulnerabilities raised since then,
# we have to apt-get those specific versions:
RUN apt-get install -y --no-install-recommends gnupg gnupg2 gnupg1 curl apt-transport-https
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg
RUN chmod a+r /etc/apt/keyrings/microsoft.gpg
RUN echo "deb [arch=amd64,arm64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc unixodbc-dev

# krb5-user provides kinit, which SQL Server Windows Authentication needs on the password
# credential form -- there is no library auto-acquire for a stored password. The keytab
# form does not need it (GSSAPI acquires in-process via KRB5_CLIENT_KTNAME), and the
# GSSAPI libraries themselves already arrive as an msodbcsql17 dependency.
RUN apt-get install -y --no-install-recommends krb5-user

# Oracle Instant Client for thick mode. libaio is a runtime dependency; on Debian
# trixie the time_t transition renamed libaio1 -> libaio1t64 (shipping
# libaio.so.1t64), so add the libaio.so.1 symlink the Oracle libs link against.
ARG ORACLE_IC_ZIP
ARG ORACLE_IC_URL
RUN apt-get install -y --no-install-recommends unzip \
    && (apt-get install -y --no-install-recommends libaio1t64 \
        || apt-get install -y --no-install-recommends libaio1) \
    && if [ ! -e /usr/lib/x86_64-linux-gnu/libaio.so.1 ]; then \
         ln -s libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1; fi \
    && mkdir -p /opt/oracle \
    && curl -fsSLo /tmp/${ORACLE_IC_ZIP} ${ORACLE_IC_URL}/${ORACLE_IC_ZIP} \
    && unzip -q /tmp/${ORACLE_IC_ZIP} -d /opt/oracle \
    && rm /tmp/${ORACLE_IC_ZIP} \
    && ln -s /opt/oracle/instantclient_* /opt/oracle/instantclient \
    && echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

# orapki + minimal JRE for building thick-mode TLS wallets (see oracle-pki-builder).
COPY --from=oracle-pki-builder /opt/oracle-pki /opt/oracle-pki

# VULN-1654: jq <= 1.7.1 has a heap-buffer-overflow; Debian bookworm only ships
# 1.6.x with no patched backport, so apt-get upgrade cannot reach 1.8.0. Remove
# it — no runtime stage uses jq (only README examples and CircleCI CI scripts do).
RUN apt-get purge -y jq || true

# clean up all unused libraries
RUN apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/*

# base — apollo runtime: venv + apollo's Python deps + apollo source.
# All apollo target stages (aws_proxied, cloudrun, generic, tests) extend this.
FROM system-base AS base

# Web server env var configuration
ENV GUNICORN_WORKERS=5
ENV GUNICORN_THREADS=8
ENV GUNICORN_TIMEOUT=0

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED=True

ENV VENV_DIR=.venv

# Create the non-root user up front and own the app dir, so every file created
# below (venv, pip-installed packages, copied source) is owned by mcdagent
# from the start. This avoids a final `chown -R` that would otherwise duplicate
# the entire venv into a new layer just to flip ownership metadata.
# Not added in system-base because hermes-agent extends that stage and creates
# its own mcdagent user; duplicating it here would conflict.
RUN groupadd --gid 1000 mcdagent \
    && useradd --uid 1000 --gid mcdagent --no-create-home --home-dir $APP_HOME --shell /usr/sbin/nologin mcdagent \
    && chown mcdagent:mcdagent $APP_HOME

USER mcdagent

COPY --chown=mcdagent:mcdagent requirements.txt ./

RUN python -m venv $VENV_DIR
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements.txt
# VULN-423
RUN . $VENV_DIR/bin/activate && pip install -U pip setuptools

# Docker Scout reads pip's vendored SBOM (_vendor/bom.cdx.json) and flags its
# bundled deps (setuptools, msgpack) as image CVEs, though they're pip-internal
# and never imported at runtime. Drop it, like the _manifest removals below.
RUN rm -f $VENV_DIR/lib/python*/site-packages/pip/_vendor/bom.cdx.json

# copy sources in the last step so we don't install python libraries due to a change in source code
COPY --chown=mcdagent:mcdagent apollo/ ./apollo

ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > ./apollo/agent/version

FROM base AS tests

COPY --chown=mcdagent:mcdagent requirements-dev.txt ./
COPY --chown=mcdagent:mcdagent requirements-cloudrun.txt ./
COPY --chown=mcdagent:mcdagent requirements-azure.txt ./
RUN . $VENV_DIR/bin/activate \
    && pip install --no-cache-dir \
    -r requirements-dev.txt \
    -r requirements-cloudrun.txt \
    -r requirements-azure.txt

COPY --chown=mcdagent:mcdagent tests ./tests
ARG CACHEBUST=1
RUN . $VENV_DIR/bin/activate && \
    PYTHONPATH=. pytest tests

FROM base AS generic

CMD . $VENV_DIR/bin/activate \
    && gunicorn --bind :$PORT --workers $GUNICORN_WORKERS --threads $GUNICORN_THREADS --timeout $GUNICORN_TIMEOUT apollo.interfaces.generic.main:app

FROM base AS aws_proxied

CMD . $VENV_DIR/bin/activate \
    && gunicorn --bind :$PORT --workers $GUNICORN_WORKERS --threads $GUNICORN_THREADS --timeout $GUNICORN_TIMEOUT apollo.interfaces.aws.main:app

FROM base AS cloudrun

COPY --chown=mcdagent:mcdagent requirements-cloudrun.txt ./
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements-cloudrun.txt

CMD . $VENV_DIR/bin/activate && \
    gunicorn --timeout 930 --bind :$PORT apollo.interfaces.cloudrun.main:app

FROM public.ecr.aws/lambda/python:3.13 AS lambda-builder

RUN dnf update -y
# install git as we need it for the direct oscrypto dependency
RUN dnf install git -y
# install libxcrypt-compat for IBM DB2 ibm-db package compatibility (requires libcrypt.so.1)
RUN dnf install -y libxcrypt-compat

COPY requirements.txt ./
COPY requirements-lambda.txt ./
RUN pip install --no-cache-dir --target "${LAMBDA_TASK_ROOT}" \
    -r requirements.txt \
    -r requirements-lambda.txt

FROM public.ecr.aws/lambda/python:3.13 AS lambda

# Create non-root user up front so the cross-stage COPY below can use --chown
# and so the final container runs as mcdagent. The Amazon Linux 2023 minimal
# rootfs that backs the Lambda base image doesn't ship shadow-utils
# (no `useradd`/`groupadd`); editing /etc/passwd and /etc/group directly
# avoids installing an extra package for one-time user registration.
RUN echo "mcdagent:x:1000:1000:mcdagent:${LAMBDA_TASK_ROOT}:/sbin/nologin" >> /etc/passwd \
    && echo "mcdagent:x:1000:" >> /etc/group

# VULN-369: Base ECR image includes urllib3-1.26.18 which is vulnerable (CVE-2024-37891).
# Note that this is the system install, not our app.
# Added setuptools as distutils is required by the git module we use for Looker
RUN pip install --no-cache-dir -U urllib3 setuptools

COPY --from=lambda-builder --chown=mcdagent:mcdagent "${LAMBDA_TASK_ROOT}" "${LAMBDA_TASK_ROOT}"

# install unixodbc and 'ODBC Driver 17 for SQL Server', needed for Azure Dedicated SQL Pools
# install git needed for looker views collection
RUN dnf -y update
RUN dnf -y install unixODBC git
RUN curl https://packages.microsoft.com/config/rhel/7/prod.repo \
    | tee /etc/yum.repos.d/mssql-release.repo
RUN ACCEPT_EULA=Y dnf install -y msodbcsql17

# kinit, for SQL Server Windows Authentication on the password credential form. This stage
# has its own base (AL2023, dnf) rather than system-base, so it needs its own install and
# the package is krb5-workstation, not krb5-user.
RUN dnf -y install krb5-workstation

# Oracle Instant Client for thick mode.
ARG ORACLE_IC_ZIP
ARG ORACLE_IC_URL
RUN dnf -y install libaio unzip \
    && mkdir -p /opt/oracle \
    && curl -fsSLo /tmp/${ORACLE_IC_ZIP} ${ORACLE_IC_URL}/${ORACLE_IC_ZIP} \
    && unzip -q /tmp/${ORACLE_IC_ZIP} -d /opt/oracle \
    && rm /tmp/${ORACLE_IC_ZIP} \
    && ln -s /opt/oracle/instantclient_* /opt/oracle/instantclient \
    && echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && /sbin/ldconfig

# orapki + minimal JRE for building thick-mode TLS wallets (see oracle-pki-builder).
COPY --from=oracle-pki-builder /opt/oracle-pki /opt/oracle-pki

# VULN-464
RUN rm -rf /var/lib/rpm/rpmdb.sqlite*

# Same pip vendored-SBOM noise as in the `base` stage, for the Lambda interpreter.
RUN rm -f /var/lang/lib/python*/site-packages/pip/_vendor/bom.cdx.json

# The Runtime Interface Emulator is only for local `docker run` testing —
# /lambda-entrypoint.sh execs it when AWS_LAMBDA_RUNTIME_API is unset, which
# Lambda always sets. It's a Go binary carrying the stdlib CVEs of whatever Go
# release AWS last built it with, re-imported on every refresh of this unpinned
# base image.
RUN rm -f /usr/local/bin/aws-lambda-rie

# VULN-1654: remove jq if present in the AL2023 base image — no runtime code uses it.
RUN dnf remove -y jq || true

RUN dnf clean all && rm -rf /var/cache/yum

COPY --chown=mcdagent:mcdagent apollo "${LAMBDA_TASK_ROOT}/apollo"
COPY --chown=mcdagent:mcdagent resources/lambda/openssl ${LAMBDA_TASK_ROOT}
ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > ./apollo/agent/version

USER mcdagent

CMD [ "apollo.interfaces.lambda_function.handler.lambda_handler" ]

# Python 3.14, not 3.13, because of the base OS: every `4-python3.13` tag is still
# Debian 12 (bookworm), which left regular Debian security support on 2026-07-11
# (YET-2254 / VULN-1399). Microsoft ships no trixie variant and has no plans to —
# `4-python3.14` is Ubuntu 24.04 LTS instead, and Azure Functions supports Python
# 3.14 (GA, end-of-support October 2030). This is the only apollo stage not on
# python:3.13-slim; the deps that needed cp314 wheels for it are floored in
# requirements.in.
FROM mcr.microsoft.com/azure-functions/python:4-python3.14 AS azure

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

# Register mcdagent early so COPY --chown below lands files mcdagent-owned
# without a later `chown -R` layer.
#
# /home/data and /home/LogFiles must be mcdagent-owned too. With
# WEBSITES_ENABLE_APP_SERVICE_STORAGE=false (our deployments) /home is the
# root-owned local container layer, so a non-root host process gets EACCES
# writing the two sentinel files it maintains there: the secrets sentinel
# under /home/data/Functions/secrets (fails host startup) and the debug
# sentinel under /home/LogFiles/Application/Functions/Host (logged error).
# Pre-create and chown both so the host can write them.
#
# Ubuntu 24.04 ships a stock `ubuntu` user at uid/gid 1000 (the Debian 12 base did
# not), so drop it first: mcdagent keeps uid/gid 1000 here, matching every other
# apollo stage and the hand-written lambda /etc/passwd entry.
RUN if getent passwd ubuntu >/dev/null; then userdel ubuntu && rm -rf /home/ubuntu; fi \
    && if getent group ubuntu >/dev/null; then groupdel ubuntu; fi \
    && groupadd --gid 1000 mcdagent \
    && useradd --uid 1000 --gid mcdagent --no-create-home --home-dir /home/site/wwwroot --shell /usr/sbin/nologin mcdagent \
    && mkdir -p /home/data /home/LogFiles \
    && chown mcdagent:mcdagent /home/site/wwwroot /home/data /home/LogFiles

RUN apt-get update
RUN apt-get install -y --no-install-recommends git
# install libcrypt1 for IBM DB2 ibm-db package compatibility (provides libcrypt.so.1)
RUN apt-get install -y --no-install-recommends libcrypt1

# Azure database clients and sql-server use pyodbc, which requires unixODBC and
# 'ODBC Driver 17 for SQL Server'. The base image ships msodbcsql18; on Ubuntu
# 24.04 msodbcsql17 (17.11.1.1-1, from the packages.microsoft.com noble feed the
# base image already has configured) installs alongside it with no downgrades or
# removals. That's why there are no version pins or `apt-mark hold` here: on the
# old Debian 12 base the ODBC stack had to be held at 2.3.11-2+deb12u1 to keep
# msodbcsql17 installable, which also blocked security updates for those packages.
RUN ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 unixodbc unixodbc-dev

# kinit, for SQL Server Windows Authentication on the password credential form. Needed
# here as well as in system-base and lambda: this stage has its own base image, so nothing
# is inherited. Three bases, three installs; see integrations/db/CLAUDE.md.
RUN apt-get install -y --no-install-recommends krb5-user

# Upgrade everything else to pick up OS security fixes (glibc, dpkg, xorg-server,
# etc.) on every rebuild rather than waiting for the base image to be republished.
RUN apt-get upgrade -y

# openssh-client required by git client
RUN apt-get install -y openssh-client

# Oracle Instant Client for thick mode. libaio is a runtime dependency; both Ubuntu
# 24.04 (this stage) and Debian trixie (the other stages) renamed libaio1 ->
# libaio1t64 in the time_t transition (shipping libaio.so.1t64), so add the
# libaio.so.1 symlink the Oracle libs link against.
ARG ORACLE_IC_ZIP
ARG ORACLE_IC_URL
RUN apt-get install -y --no-install-recommends unzip curl \
    && (apt-get install -y --no-install-recommends libaio1t64 \
        || apt-get install -y --no-install-recommends libaio1) \
    && if [ ! -e /usr/lib/x86_64-linux-gnu/libaio.so.1 ]; then \
         ln -s libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1; fi \
    && mkdir -p /opt/oracle \
    && curl -fsSLo /tmp/${ORACLE_IC_ZIP} ${ORACLE_IC_URL}/${ORACLE_IC_ZIP} \
    && unzip -q /tmp/${ORACLE_IC_ZIP} -d /opt/oracle \
    && rm /tmp/${ORACLE_IC_ZIP} \
    && ln -s /opt/oracle/instantclient_* /opt/oracle/instantclient \
    && echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

# orapki + minimal JRE for building thick-mode TLS wallets (see oracle-pki-builder).
COPY --from=oracle-pki-builder /opt/oracle-pki /opt/oracle-pki

# Purge the X11/GTK + cups/tiff/nss surface the MS azure-functions base drags in
# but the data-collector agent never uses. These packages carry a large block of
# unfixed ("wont-fix") distro HIGH/CRITICAL CVEs (libcups2t64, libnss3, libtiff6,
# and the X11/GTK stack); `apt-get upgrade` can't clear wont-fix CVEs, so
# the only lever is to remove the vulnerable surface from the image entirely. Runs
# AFTER the ODBC/oracle installs and the security `apt-get upgrade` above so it
# can't strip anything they need.
#
# NOTE: the core perl stack (perl / perl-modules-5.38 / libperl5.38) is deliberately
# NOT purged — `git`, installed above for the agent-base git+https dependency and
# Looker view collection, hard-depends on `perl`, so purging it would remove git and
# break the pip install. Only libx11-protocol-perl (an unused LWP/X11 perl module) is
# dropped from the perl side.
#
# No --allow-remove-essential: if apt would have to remove an Essential package to
# satisfy the purge, it aborts and the build fails loudly rather than shipping a
# broken image. --auto-remove sweeps the now-orphaned dependency cascade in one pass.
#
# VULN-1654: jq <= 1.7.1 has a heap-buffer-overflow; Ubuntu 24.04 may ship a
# vulnerable version. Remove if present — no runtime code uses jq.
# clean up all unused libraries
RUN apt-get purge -y jq || true
RUN apt-get purge -y --auto-remove \
        xvfb xserver-common x11-common x11-utils x11-xkb-utils x11-xserver-utils \
        libxext6 libxft2 libxi6 libxinerama1 libxpm4 libxrender1 libxrender-dev \
        libx11-6 libx11-data libx11-dev libx11-xcb1 libx11-protocol-perl x11proto-dev xauth \
        libcups2t64 libtiff6 libnss3 \
    && apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/*

# delete this file that includes an old golang version (including vulns) and is not used
RUN rm -rf /opt/startupcmdgen/

COPY requirements.txt /
COPY requirements-azure.txt /
# Azure Functions host puts BOTH wwwroot (app code) and .python_packages/lib/site-packages (deps) on
# sys.path — these two roots must stay separate; consolidating them would silently break imports.
# Install deps into the Functions app-package dir (not system site-packages) so the
# host's dependency isolation keeps them off the worker's sys.path. Otherwise our
# protobuf and the worker's bundled protobuf co-load and SIGSEGV the worker on py3.13+.
# No setuptools/pkg_resources needed here (opentelemetry moved off it; remaining users
# guard the import) — except for distutils, installed separately below.
RUN pip install --no-cache-dir \
    --target=/home/site/wwwroot/.python_packages/lib/site-packages \
    -r /requirements.txt -r /requirements-azure.txt \
    && chown -R mcdagent:mcdagent /home/site/wwwroot/.python_packages \
    # Drop the SBOM the azure-functions-durable wheel bundles at the site-packages
    # root: it lists stale versions that aren't installed, tripping scout false positives.
    && rm -rf /home/site/wwwroot/.python_packages/lib/site-packages/_manifest

# distutils for lambda-git, whose `git` module (imported by GitCloneClient for Looker
# view collection) does `from distutils.spawn import find_executable`. distutils left
# the stdlib in 3.12; setuptools carries the shim, so:
#   - it goes in a real site dir, not the --target dir above (.pth files are only
#     processed for site dirs), and
#   - the shim needs an explicit opt-in on 3.12+, hence SETUPTOOLS_USE_DISTUTILS.
# tests/test_git_client.py imports it, so tests-azure catches a regression here.
ENV SETUPTOOLS_USE_DISTUTILS=local
RUN pip install --no-cache-dir setuptools

# Same pip vendored-SBOM noise as in the `base` and `lambda` stages, for the
# interpreter the MS base image ships (the base tag is unpinned, so a refresh
# brings back whatever pip it currently bundles).
RUN rm -f /opt/python/*/lib/python*/site-packages/pip/_vendor/bom.cdx.json

COPY --chown=mcdagent:mcdagent apollo /home/site/wwwroot/apollo

# the files under apollo/interfaces/azure like function_app.py must be in the root folder of the app
COPY --chown=mcdagent:mcdagent apollo/interfaces/azure /home/site/wwwroot

ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > /home/site/wwwroot/apollo/agent/version \
    && chown mcdagent:mcdagent /home/site/wwwroot/apollo/agent/version

# required for the verify-version-in-docker-image step in circle-ci
WORKDIR /home/site/wwwroot

# Bind Kestrel on 8080 (non-root can't bind <1024). EXPOSE 8080 lets
# App Service auto-discover the port when WEBSITES_PORT is not set; if
# a customer pinned WEBSITES_PORT=80 explicitly, they need to unset it
# or update to 8080.
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER mcdagent

# tests-azure — run the unit suite against the azure image's Python 3.14 runtime.
# The `tests` stage above covers every other stage (all on Python 3.13); azure is
# the only stage on 3.14 (see its FROM comment), so without this lane nothing would
# exercise the interpreter Azure Functions customers actually run. Test deps are
# installed into the same --target dir as the runtime deps, so the suite imports
# exactly the versions the azure image ships. Root, because the azure stage ends as
# mcdagent and pip needs to write to .python_packages.
FROM azure AS tests-azure

USER root

COPY requirements-dev.txt /
COPY requirements-cloudrun.txt /
# Separate --target dir, NOT the runtime one: `pip install --target` does not merge
# into an existing namespace-package directory, so installing google-cloud-logging
# next to the already-installed google-cloud-* packages silently leaves
# google/cloud/logging out. Two sys.path entries each holding part of the
# google.cloud PEP 420 namespace do merge correctly, so keep them apart.
RUN pip install --no-cache-dir \
    --target=/test-packages \
    -r /requirements-dev.txt -r /requirements-cloudrun.txt

COPY tests /home/site/wwwroot/tests
# pip --target installs no console scripts on PATH, so invoke pytest as a module.
# `python`, not `python3`: the base image symlinks /usr/bin/python and
# /usr/bin/python3.14 to its 3.14 install, and the X11 purge in the azure stage
# takes Ubuntu's own python3 (3.12) with it, leaving no `python3` on PATH.
ARG CACHEBUST=1
RUN PYTHONPATH=/home/site/wwwroot:/home/site/wwwroot/.python_packages/lib/site-packages:/test-packages \
    python -m pytest tests
