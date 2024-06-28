FROM python:3.11-slim AS base

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
ENV VENV_DIR .venv
WORKDIR $APP_HOME
COPY requirements.txt ./

RUN apt update
# install git as we need it for the direct oscrypto dependency
# this is a temporary workaround and it should be removed once we update oscrypto to 1.3.1+
# see: https://community.snowflake.com/s/article/Python-Connector-fails-to-connect-with-LibraryNotFoundError-Error-detecting-the-version-of-libcrypto
RUN apt install git -y

RUN python -m venv $VENV_DIR
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements.txt

# CVE-2022-40897
RUN . $VENV_DIR/bin/activate && pip install setuptools==65.5.1

# Azure database clients uses pyodbc which requires unixODBC and 'ODBC Driver 17 for SQL Server'
RUN apt-get update \
    && apt-get install -y gnupg gnupg2 gnupg1 curl apt-transport-https \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc unixodbc-dev

# copy sources in the last step so we don't install python libraries due to a change in source code
COPY apollo/ ./apollo

ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > ./apollo/agent/version

FROM base AS tests

COPY requirements-dev.txt ./
COPY requirements-cloudrun.txt ./
COPY requirements-azure.txt ./
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements-dev.txt -r requirements-cloudrun.txt -r requirements-azure.txt

COPY tests ./tests
ARG CACHEBUST=1
RUN . $VENV_DIR/bin/activate && PYTHONPATH=. pytest tests

FROM base AS generic

CMD . $VENV_DIR/bin/activate && gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 apollo.interfaces.generic.main:app

FROM base AS cloudrun

COPY requirements-cloudrun.txt ./
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements-cloudrun.txt

RUN apt update
# install git as we need it for the git clone client
RUN apt install git -y

CMD . $VENV_DIR/bin/activate && gunicorn --timeout 930 --bind :$PORT apollo.interfaces.cloudrun.main:app

FROM public.ecr.aws/lambda/python:3.11 AS lambda-builder

RUN yum update -y
# install git as we need it for the direct oscrypto dependency
# this is a temporary workaround and it should be removed once we update oscrypto to 1.3.1+
# see: https://community.snowflake.com/s/article/Python-Connector-fails-to-connect-with-LibraryNotFoundError-Error-detecting-the-version-of-libcrypto
# please note we don't need git for the git connector as lambda-git takes care of installing it if
# not present in the lambda environment
# we don't use this image for the final lambda as installing git this way breaks the looker-git connector, we need
# to use in runtime the git version installed by lambda-git package
RUN yum install git -y

COPY requirements.txt ./
COPY requirements-lambda.txt ./
RUN pip install --no-cache-dir --target "${LAMBDA_TASK_ROOT}" -r requirements.txt -r requirements-lambda.txt

FROM public.ecr.aws/lambda/python:3.11 AS lambda

# VULN-29: Base ECR image includes setuptools-56.0.0 which is vulnerable (CVE-2022-40897)
RUN pip install --no-cache-dir setuptools==68.0.0
# VULN-369: Base ECR image includes urllib3-1.26.18 which is vulnerable (CVE-2024-37891)
RUN pip install --no-cache-dir --upgrade urllib3==1.26.19
RUN rm -rf /var/lang/lib/python3.11/site-packages/urllib3-1.26.18.dist-info

# VULN-230 CWE-77
RUN pip install --no-cache-dir --upgrade pip

COPY --from=lambda-builder "${LAMBDA_TASK_ROOT}" "${LAMBDA_TASK_ROOT}"

# install unixodbc and 'ODBC Driver 17 for SQL Server', needed for Azure Dedicated SQL Pools
RUN yum -y update \
    && yum -y install \
    unixODBC \
    && yum clean all \
    && rm -rf /var/cache/yum
RUN curl https://packages.microsoft.com/config/rhel/7/prod.repo | tee /etc/yum.repos.d/mssql-release.repo
RUN ACCEPT_EULA=Y yum install -y msodbcsql17

COPY apollo "${LAMBDA_TASK_ROOT}/apollo"
ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > ./apollo/agent/version

CMD [ "apollo.interfaces.lambda_function.handler.lambda_handler" ]

FROM mcr.microsoft.com/azure-functions/python:4-python3.11 AS azure

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

RUN apt update
# install git as we need it for the direct oscrypto dependency
# this is a temporary workaround and it should be removed once we update oscrypto to 1.3.1+
# see: https://community.snowflake.com/s/article/Python-Connector-fails-to-connect-with-LibraryNotFoundError-Error-detecting-the-version-of-libcrypto
RUN apt install git -y

# Azure database clients and sql-server uses pyodbc which requires unixODBC and 'ODBC Driver 17 for SQL Server'
# Microsoft's python 3.11 base image comes with msodbcsql18 but we are expecting to use the msodbcsql17 driver so need
# to install specific versions of some libraries and allow Docker to downgrade some pre-installed packages.
RUN apt-get update \
   && apt-get install -y gnupg gnupg2 gnupg1 curl apt-transport-https \
   && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
   && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
   && apt-get update \
   && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc=2.3.11-1 \
   unixodbc-dev=2.3.11-1 odbcinst1debian2=2.3.11-1 odbcinst=2.3.11-1 --allow-downgrades

COPY requirements.txt /
COPY requirements-azure.txt /
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