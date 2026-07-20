# Oracle Instant Client (Basic) — native libraries oracledb needs for "thick"
# mode, which some Oracle configurations require (e.g. servers that only expose
# the legacy password verifier that thin mode can't authenticate with).
ARG ORACLE_IC_ZIP=instantclient-basic-linux.x64-23.8.0.25.04.zip
ARG ORACLE_IC_URL=https://download.oracle.com/otn_software/linux/instantclient/2380000

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

# VULN-464
RUN rm -rf /var/lib/rpm/rpmdb.sqlite*

RUN dnf clean all && rm -rf /var/cache/yum

COPY --chown=mcdagent:mcdagent apollo "${LAMBDA_TASK_ROOT}/apollo"
COPY --chown=mcdagent:mcdagent resources/lambda/openssl ${LAMBDA_TASK_ROOT}
ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > ./apollo/agent/version

USER mcdagent

CMD [ "apollo.interfaces.lambda_function.handler.lambda_handler" ]

FROM mcr.microsoft.com/azure-functions/python:4-python3.13 AS azure

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
RUN groupadd --gid 1000 mcdagent \
    && useradd --uid 1000 --gid mcdagent --no-create-home --home-dir /home/site/wwwroot --shell /usr/sbin/nologin mcdagent \
    && mkdir -p /home/data /home/LogFiles \
    && chown mcdagent:mcdagent /home/site/wwwroot /home/data /home/LogFiles

RUN apt-get update
RUN apt-get install -y --no-install-recommends git
# install libcrypt1 for IBM DB2 ibm-db package compatibility (provides libcrypt.so.1)
RUN apt-get install -y --no-install-recommends libcrypt1

# Azure database clients and sql-server uses pyodbc which requires unixODBC and 'ODBC Driver 17
# for SQL Server' Microsoft's python 3.13 base image comes with msodbcsql18 but we are expecting to
# use the msodbcsql17 driver so need to install specific versions of some libraries and allow Docker
# to downgrade some pre-installed packages.
RUN apt-get update
RUN apt-get install -y --no-install-recommends gnupg gnupg2 gnupg1 curl apt-transport-https
RUN ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 odbcinst=2.3.11-2+deb12u1 odbcinst1debian2=2.3.11-2+deb12u1 unixodbc-dev=2.3.11-2+deb12u1 unixodbc=2.3.11-2+deb12u1

# Hold the ODBC stack at the pinned versions above (those pins force downgrades
# so msodbcsql17 — rather than the msodbcsql18 expected by the MS base image —
# stays installable), then upgrade everything else to pick up Debian security
# fixes (glibc, dpkg, xorg-server, etc.).
RUN apt-mark hold msodbcsql17 odbcinst odbcinst1debian2 unixodbc unixodbc-dev \
    && apt-get upgrade -y

# openssh-client required by git client
RUN apt-get install -y openssh-client

# Oracle Instant Client for thick mode. libaio is a runtime dependency; on Debian
# trixie the time_t transition renamed libaio1 -> libaio1t64 (shipping
# libaio.so.1t64), so add the libaio.so.1 symlink the Oracle libs link against.
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

# Purge the X11/GTK + cups/tiff/nss surface the MS azure-functions base drags in
# but the data-collector agent never uses. These packages carry a large block of
# unfixed ("wont-fix") Debian HIGH/CRITICAL CVEs (libcups2, libnss3, libtiff6, and
# the X11/GTK stack); `apt-get upgrade` can't clear wont-fix CVEs, so
# the only lever is to remove the vulnerable surface from the image entirely. Runs
# AFTER the ODBC/oracle installs and the security `apt-get upgrade` above so it
# can't strip anything they need.
#
# NOTE: the core perl stack (perl / perl-modules-5.36 / libperl5.36) is deliberately
# NOT purged — `git`, installed above for the agent-base git+https dependency and
# Looker view collection, hard-depends on `perl`, so purging it would remove git and
# break the pip install. Only libx11-protocol-perl (an unused LWP/X11 perl module) is
# dropped from the perl side.
#
# No --allow-remove-essential: if apt would have to remove an Essential package to
# satisfy the purge, it aborts and the build fails loudly rather than shipping a
# broken image. --auto-remove sweeps the now-orphaned dependency cascade in one pass.
#
# clean up all unused libraries
RUN apt-get purge -y --auto-remove \
        xvfb xserver-common x11-common x11-utils x11-xkb-utils x11-xserver-utils \
        libxext6 libxft2 libxi6 libxinerama1 libxpm4 libxrender1 libxrender-dev \
        libx11-6 libx11-data libx11-dev libx11-xcb1 libx11-protocol-perl x11proto-dev xauth \
        libcups2 libtiff6 libnss3 \
    && apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/*

# delete this file that includes an old golang version (including vulns) and is not used
RUN rm -rf /opt/startupcmdgen/

COPY requirements.txt /
COPY requirements-azure.txt /
# Azure Functions host puts BOTH wwwroot (app code) and .python_packages/lib/site-packages (deps) on
# sys.path — these two roots must stay separate; consolidating them would silently break imports.
# Install deps into the Functions app-package dir (not system site-packages) so the
# host's dependency isolation keeps them off the worker's sys.path. Otherwise our
# protobuf and the worker's bundled protobuf co-load and SIGSEGV the worker on py3.13.
# No setuptools/pkg_resources needed (opentelemetry moved off it; remaining users guard the import).
RUN pip install --no-cache-dir \
    --target=/home/site/wwwroot/.python_packages/lib/site-packages \
    -r /requirements.txt -r /requirements-azure.txt \
    && chown -R mcdagent:mcdagent /home/site/wwwroot/.python_packages \
    && rm -rf /opt/python/3/_manifest \
    # Drop the SBOM the azure-functions-durable wheel bundles at the site-packages
    # root: it lists stale versions that aren't installed, tripping scout false positives.
    && rm -rf /home/site/wwwroot/.python_packages/lib/site-packages/_manifest

COPY --chown=mcdagent:mcdagent apollo /home/site/wwwroot/apollo

# the files under apollo/interfaces/azure like function_app.py must be in the root folder of the app
COPY --chown=mcdagent:mcdagent apollo/interfaces/azure /home/site/wwwroot

ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > /home/site/wwwroot/apollo/agent/version \
    && chown mcdagent:mcdagent /home/site/wwwroot/apollo/agent/version

# delete MS provided SBOM as it's outdated after the packages we installed
# docker scout will find vulnerabilities anyway by scanning the image
RUN rm -rf /usr/local/_manifest

# required for the verify-version-in-docker-image step in circle-ci
WORKDIR /home/site/wwwroot

# Bind Kestrel on 8080 (non-root can't bind <1024). EXPOSE 8080 lets
# App Service auto-discover the port when WEBSITES_PORT is not set; if
# a customer pinned WEBSITES_PORT=80 explicitly, they need to unset it
# or update to 8080.
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER mcdagent
