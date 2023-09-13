FROM python:3.11-slim AS base

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
ENV VENV_DIR .venv
WORKDIR $APP_HOME
COPY apollo/ ./apollo
COPY requirements.txt ./

#CVE-2022-40897, upgrade setuptools in the global packages
RUN pip install setuptools==65.5.1

RUN python -m venv $VENV_DIR
RUN . $VENV_DIR/bin/activate
RUN pip install --no-cache-dir -r requirements.txt

FROM base AS tests

RUN . $VENV_DIR/bin/activate
COPY requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY tests ./tests
ARG CACHEBUST=1
RUN python -m pytest tests/*

FROM base AS generic

RUN . $VENV_DIR/bin/activate
CMD gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 apollo.interfaces.generic.main:app

FROM base AS cloudrun

RUN . $VENV_DIR/bin/activate
COPY requirements-cloudrun.txt ./
RUN pip install --no-cache-dir -r requirements-cloudrun.txt

CMD gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 apollo.interfaces.cloudrun.main:app