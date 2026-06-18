# system-base — system-level dependencies only (apt packages, no venv).
# Published as `<version>-system-base` so downstream consumers (e.g. hermes-agent)
# can build their own venv against the same native libs without inheriting
# apollo's pip-installed dependencies.
FROM python:3.12-slim AS system-base

ENV APP_HOME=/app
WORKDIR $APP_HOME

# Refresh apt index and upgrade base-image packages so OS-level security fixes
# (glibc, openssh, nghttp2, etc.) land on every rebuild rather than waiting for
# the upstream python:3.12-slim tag to be republished.
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

FROM public.ecr.aws/lambda/python:3.12 AS lambda-builder

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

FROM public.ecr.aws/lambda/python:3.12 AS lambda

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

# NOTE: the azure target lives in ./Dockerfile.azure — unlike the stages above
# it builds FROM Microsoft's Functions base image and shares nothing with the
# python:3.12-slim `base` stage, so it was extracted into its own file.
