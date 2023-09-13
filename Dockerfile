FROM python:3.11-slim AS base

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
ENV PYTHON_PACKAGES_DIR /packages
ENV PYTHONPATH ${PYTHON_PACKAGES_DIR}:${APP_HOME}

WORKDIR $APP_HOME
COPY apollo/ ./apollo
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt --target ${PYTHON_PACKAGES_DIR}

FROM base AS tests

COPY requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt --target ${PYTHON_PACKAGES_DIR}

COPY tests ./tests
RUN python -m pytest tests/*

FROM base AS generic

CMD python -m gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 apollo.interfaces.generic.main:app

FROM base AS cloudrun

COPY requirements-cloudrun.txt ./
RUN pip install --no-cache-dir -r requirements-cloudrun.txt --target ${PYTHON_PACKAGES_DIR}

CMD python -m gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 apollo.interfaces.cloudrun.main:app