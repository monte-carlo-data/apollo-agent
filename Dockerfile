FROM python:3.12.9-slim AS base

# Web server env var configuration
ENV GUNICORN_WORKERS=5
ENV GUNICORN_THREADS=8
ENV GUNICORN_TIMEOUT=0

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
ENV VENV_DIR .venv
WORKDIR $APP_HOME
COPY requirements.txt ./

RUN apt-get update
# install git as we need it for the direct oscrypto dependency
# this is a temporary workaround and it should be removed once we update oscrypto to 1.3.1+
# see: https://community.snowflake.com/s/article/Python-Connector-fails-to-connect-with-LibraryNotFoundError-Error-detecting-the-version-of-libcrypto
RUN apt-get install -y git \Add commentMore actions
    && apt-get install -y libcap2=1:2.66-4+deb12u1  # Fix CVE-2025-1390

# Upgrade pip globally to fix the vulnerability - VULN-510
RUN pip install --no-cache-dir -U pip==25.0.0

RUN python -m venv $VENV_DIR
# Upgrade pip inside the virtual environment - VULN-510
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -U pip==25.0.0
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements.txt

# VULN-423: setuptools 68.0.0 contains (CVE-2024-6345)
RUN . $VENV_DIR/bin/activate && pip install setuptools==75.1.0

# Azure database clients uses pyodbc which requires unixODBC and 'ODBC Driver 17 for SQL Server'
# [VULN-602] update passwd to 1:4.13+dfsg1-1+deb12u1
# [VULN-606] update krb5 (kerberos) to 1.20.1-2+deb12u3
# [VULN-607] update libcap2 to 1:2.66-4+deb12u1
RUN apt-get update \
    && apt-get install -y gnupg gnupg2 gnupg1 curl apt-transport-https \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list \
    > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc unixodbc-dev \
    && apt-get install -y passwd=1:4.13+dfsg1-1+deb12u1 \
    && apt-get install -y libgssapi-krb5-2=1.20.1-2+deb12u3 libkrb5-3=1.20.1-2+deb12u3 libkrb5support0=1.20.1-2+deb12u3 \
    && apt-get install -y libcap2=1:2.66-4+deb12u1  # Fix CVE-2025-1390

# copy sources in the last step so we don't install python libraries due to a change in source code
COPY apollo/ ./apollo

ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > ./apollo/agent/version

FROM base AS tests

COPY requirements-dev.txt ./
COPY requirements-cloudrun.txt ./
COPY requirements-azure.txt ./
RUN . $VENV_DIR/bin/activate \
    && pip install --no-cache-dir \
    -r requirements-dev.txt \
    -r requirements-cloudrun.txt \
    -r requirements-azure.txt

COPY tests ./tests
ARG CACHEBUST=1
RUN . $VENV_DIR/bin/activate && \
    PYTHONPATH=. pytest tests

FROM base AS generic

CMD . $VENV_DIR/bin/activate \
    && gunicorn --bind :$PORT --workers $GUNICORN_WORKERS --threads $GUNICORN_THREADS --timeout $GUNICORN_TIMEOUT apollo.interfaces.generic.main:app

FROM base AS aws_generic

CMD . $VENV_DIR/bin/activate \
    && gunicorn --bind :$PORT --workers $GUNICORN_WORKERS --threads $GUNICORN_THREADS --timeout $GUNICORN_TIMEOUT apollo.interfaces.aws.main:app

FROM base AS cloudrun

COPY requirements-cloudrun.txt ./
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -U pip==25.0.0  # VULN-510
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements-cloudrun.txt

RUN apt update
RUN apt install git -y
RUN apt install libcap2=1:2.66-4+deb12u1 -y  # Fix CVE-2025-1390

CMD . $VENV_DIR/bin/activate && \
    gunicorn --timeout 930 --bind :$PORT apollo.interfaces.cloudrun.main:app

FROM public.ecr.aws/lambda/python:3.12.2025.04.28.11 AS lambda-builder

RUN dnf update -y
# install git as we need it for the direct oscrypto dependency
RUN dnf install git -y

COPY requirements.txt ./
COPY requirements-lambda.txt ./
RUN pip install --no-cache-dir -U pip==25.0.0  # VULN-510
RUN pip install --no-cache-dir --target "${LAMBDA_TASK_ROOT}" \
    -r requirements.txt \
    -r requirements-lambda.txt

FROM public.ecr.aws/lambda/python:3.12.2025.04.28.11 AS lambda

# VULN-423: setuptools 68.0.0 contains (CVE-2024-6345)
RUN pip install --no-cache-dir setuptools==75.1.0
# VULN-369: Base ECR image includes urllib3-1.26.18 which is vulnerable (CVE-2024-37891)
RUN pip install --no-cache-dir --upgrade urllib3==1.26.19
RUN rm -rf /var/lang/lib/python3.12/site-packages/urllib3-1.26.19.dist-info

# VULN-230 CWE-77 VULN-510
RUN pip install --no-cache-dir -U pip==25.0.0

COPY --from=lambda-builder "${LAMBDA_TASK_ROOT}" "${LAMBDA_TASK_ROOT}"

# install unixodbc and 'ODBC Driver 17 for SQL Server', needed for Azure Dedicated SQL Pools
# install git needed for looker views collection
RUN dnf -y update \
    && dnf -y install unixODBC \
    git \
    && dnf clean all \
    && rm -rf /var/cache/yum
RUN curl https://packages.microsoft.com/config/rhel/7/prod.repo \
    | tee /etc/yum.repos.d/mssql-release.repo
RUN ACCEPT_EULA=Y dnf install -y msodbcsql17

# VULN-464: Upgrade package libarchive
RUN rm -rf /var/lib/rpm/rpmdb.sqlite*

COPY apollo "${LAMBDA_TASK_ROOT}/apollo"
COPY resources/lambda/openssl ${LAMBDA_TASK_ROOT}
ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > ./apollo/agent/version

CMD [ "apollo.interfaces.lambda_function.handler.lambda_handler" ]

FROM mcr.microsoft.com/azure-functions/python:4-python3.12 AS azure

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y git wget  # VULN-543 upgrade wget
RUN apt-get install -y libcap2=1:2.66-4+deb12u1  # Fix CVE-2025-1390

# Azure database clients and sql-server uses pyodbc which requires unixODBC and 'ODBC Driver 17
# for SQL Server' Microsoft's python 3.12 base image comes with msodbcsql18 but we are expecting to
# use the msodbcsql17 driver so need to install specific versions of some libraries and allow Docker
# to downgrade some pre-installed packages.
# Updating libgnutls30 to resolve CVE-2024-28835 and CVE-2024-28834.
# Updating libglib to resolve CVE-2024-52533.
# Updating OpenSSL to resolve CVE-2024-13176
RUN apt-get update \
    && apt-get install -y gnupg gnupg2 gnupg1 curl apt-transport-https libgnutls30 \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 odbcinst=2.3.11-2+deb12u1 odbcinst1debian2=2.3.11-2+deb12u1 unixodbc-dev=2.3.11-2+deb12u1 unixodbc=2.3.11-2+deb12u1 \
    && apt-get install -y sqlite3=3.40.1-2+deb12u1 openssl=3.0.16-1~deb12u1 libglib2.0-0

# delete this file that includes an old golang version (including vulns) and is not used
RUN rm -rf /opt/startupcmdgen/

COPY requirements.txt /
COPY requirements-azure.txt /
RUN pip install --no-cache-dir setuptools==75.1.0
RUN pip install --no-cache-dir -U pip==25.0.0  # VULN-510
RUN pip install --no-cache-dir -r /requirements.txt -r /requirements-azure.txt

COPY apollo /home/site/wwwroot/apollo

# the files under apollo/interfaces/azure like function_app.py must be in the root folder of the app
COPY apollo/interfaces/azure /home/site/wwwroot

ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > /home/site/wwwroot/apollo/agent/version

# delete MS provided SBOM as it's outdated after the packages we installed
# docker scout will find vulnerabilities anyway by scanning the image
RUN rm -rf /usr/local/_manifest

# required for the verify-version-in-docker-image step in circle-ci
WORKDIR /home/site/wwwroot
