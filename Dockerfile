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

# copy sources in the last step so we don't install python libraries due to a change in source code
COPY apollo/ ./apollo

ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > ./apollo/agent/version

FROM base AS tests

COPY requirements-dev.txt ./
COPY requirements-cloudrun.txt ./
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements-dev.txt -r requirements-cloudrun.txt

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

# Azure Dedicated SQL Pools uses pyodbc which requires unixODBC and 'ODBC Driver 17 for SQL Server'
RUN yum -y install unixODBC
RUN curl https://packages.microsoft.com/config/rhel/7/prod.repo | tee /etc/yum.repos.d/mssql-release.repo
RUN ACCEPT_EULA=Y yum install -y msodbcsql17

COPY requirements.txt ./
COPY requirements-lambda.txt ./
RUN pip install --no-cache-dir --target "${LAMBDA_TASK_ROOT}" -r requirements.txt -r requirements-lambda.txt

FROM public.ecr.aws/lambda/python:3.11 AS lambda

# VULN-29: Base ECR image has setuptools-56.0.0 which is vulnerable (CVE-2022-40897)
RUN pip install --no-cache-dir setuptools==68.0.0

COPY --from=lambda-builder "${LAMBDA_TASK_ROOT}" "${LAMBDA_TASK_ROOT}"

COPY apollo "${LAMBDA_TASK_ROOT}/apollo"
ARG code_version="local"
ARG build_number="0"
RUN echo $code_version,$build_number > ./apollo/agent/version

CMD [ "apollo.interfaces.lambda_function.handler.lambda_handler" ]
