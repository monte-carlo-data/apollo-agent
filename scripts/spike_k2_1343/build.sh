#!/usr/bin/env bash
# K2-1343 spike: build Lambda zips (linux x86_64, py3.13) with the agent's pins.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf build && mkdir -p build/harness build/echo
PIP=(uv pip install -q --python-platform x86_64-manylinux2014 --python-version 3.13 --only-binary :all:)
"${PIP[@]}" --target build/harness "mcp==1.30.0" "typing-extensions==4.12.2" "boto3==1.35.87" "botocore==1.35.99"
cp ../../apollo/integrations/mcp/core.py build/harness/mcp_core.py
cp harness.py build/harness/
"${PIP[@]}" --target build/echo "mcp==1.30.0" "typing-extensions==4.12.2" "mangum==0.19.0"
cp echo_server.py build/echo/
du -sh build/*
