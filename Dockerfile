FROM python:3.11-slim AS base

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
ENV VENV_DIR .venv
WORKDIR $APP_HOME
COPY apollo/ ./apollo
COPY requirements.txt ./

RUN python -m venv $VENV_DIR
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements.txt

# CVE-2022-40897
RUN . $VENV_DIR/bin/activate && pip install setuptools==65.5.1

FROM base AS tests

COPY requirements-dev.txt ./
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements-dev.txt

COPY tests ./tests
ARG CACHEBUST=1
RUN . $VENV_DIR/bin/activate && python -m pytest tests/*

FROM base AS generic

CMD . $VENV_DIR/bin/activate && gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 apollo.interfaces.generic.main:app

FROM base AS cloudrun

COPY requirements-cloudrun.txt ./
RUN . $VENV_DIR/bin/activate && pip install --no-cache-dir -r requirements-cloudrun.txt

CMD . $VENV_DIR/bin/activate && gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 apollo.interfaces.cloudrun.main:app