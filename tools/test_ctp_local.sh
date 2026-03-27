#!/usr/bin/env bash
# tools/test_ctp_local.sh — CTP local integration test runner
#
# Tests both the flat-credential (CTP pipeline) path and the DC pre-shaped
# (connect_args passthrough) path against real external systems.
#
#   CTP pipeline path: flat credentials sent directly to the agent REST API
#   DC passthrough path: full DC worker run via worker_local_execution.py
#
# Prerequisites:
#   - op (1Password CLI) authenticated
#   - Local apollo-agent NOT already running on port 8081 (this script starts one)
#   - DC repo checked out at ../data-collector (or set DC_REPO below)
#   - apollo-agent venv: ~/.venv/apollo-agent  dc-worker venv: ~/.venv/data-collector
#
# Usage:
#   source ~/.venv/apollo-agent/bin/activate
#   ./tools/test_ctp_local.sh [connector]
#
# Supported connectors: starburst-galaxy (default), redshift
#
# Local dev tool — do not commit.

set -euo pipefail

CONNECTOR="${1:-starburst-galaxy}"
AGENT_PORT=8081
AGENT_PID=""
DC_REPO="${DC_REPO:-$HOME/monte-carlo/data-collector}"
APOLLO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TMPDIR_LOCAL="$(mktemp -d)"
trap 'cleanup' EXIT

cleanup() {
    if [[ -n "$AGENT_PID" ]] && kill -0 "$AGENT_PID" 2>/dev/null; then
        echo "[cleanup] stopping local agent (pid $AGENT_PID)"
        kill "$AGENT_PID"
        wait "$AGENT_PID" 2>/dev/null || true
    fi
    rm -rf "$TMPDIR_LOCAL"
}

log()  { echo "==> $*"; }
ok()   { echo "    [PASS] $*"; }
fail() { echo "    [FAIL] $*" >&2; exit 1; }

# ── agent management ────────────────────────────────────────────────────────

start_agent() {
    log "Starting local apollo-agent on port $AGENT_PORT..."
    cd "$APOLLO_ROOT"
    source ~/.venv/apollo-agent/bin/activate
    flask --app apollo.interfaces.generic.main:app run --port "$AGENT_PORT" \
        --no-debugger --no-reload >"$TMPDIR_LOCAL/agent.log" 2>&1 &
    AGENT_PID=$!
    local attempts=0
    until curl -sf "http://localhost:$AGENT_PORT/api/v1/test/health" >/dev/null 2>&1; do
        sleep 0.5
        attempts=$((attempts+1))
        if [[ $attempts -ge 20 ]]; then
            echo "Agent log:" >&2
            cat "$TMPDIR_LOCAL/agent.log" >&2
            fail "Agent did not start within 10s"
        fi
    done
    ok "Agent running (pid $AGENT_PID)"
}

# ── test: direct agent API call (CTP pipeline path) ─────────────────────────
# Sends flat credentials directly to the agent REST endpoint, bypassing DC.
# The agent runs CtpRegistry.resolve() → CTP pipeline → proxy client.

test_agent_direct() {
    local label="$1"
    local connection_type="$2"
    local credentials_json="$3"
    local operation_json="$4"

    log "Running: $label"
    local response http_code
    response="$(curl -s -w '\n__HTTP_CODE__:%{http_code}' -X POST \
        "http://localhost:$AGENT_PORT/api/v1/agent/execute/$connection_type/run_query" \
        -H 'Content-Type: application/json' \
        -d "{\"credentials\": $credentials_json, \"operation\": $operation_json}" \
        2>&1)"
    http_code="$(echo "$response" | grep '__HTTP_CODE__:' | sed 's/.*__HTTP_CODE__://')"
    response="$(echo "$response" | grep -v '__HTTP_CODE__:')"
    if [[ "$http_code" != "200" ]]; then
        echo "    HTTP $http_code: $response" >&2
        fail "$label"
    fi
    # Check for error in response
    local error
    error="$(echo "$response" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('__mcd_error__',''))" 2>/dev/null || echo "")"
    if [[ -n "$error" ]]; then
        echo "    Agent error: $error" >&2
        fail "$label"
    fi
    ok "$label"
    echo "    response preview: $(echo "$response" | python3 -c "import json,sys; d=json.load(sys.stdin); print(str(d)[:200])" 2>/dev/null)"
}

# ── test: DC worker run (passthrough path) ──────────────────────────────────
# Runs the DC worker which pre-shapes credentials into connect_args before
# calling the agent. Agent uses the connect_args passthrough path.

test_dc_worker() {
    local label="$1"
    local input_file="$2"
    log "Running: $label"
    log "  input: $input_file"
    cd "$DC_REPO/lambdas"
    source ~/.venv/data-collector/bin/activate
    python worker_local_execution.py metadata --input_file "$input_file" \
        >"$TMPDIR_LOCAL/dc_output.log" 2>&1
    local exit_code=$?
    source ~/.venv/apollo-agent/bin/activate
    if [[ $exit_code -ne 0 ]]; then
        tail -30 "$TMPDIR_LOCAL/dc_output.log" >&2
        fail "$label (exit code $exit_code)"
    fi
    # Fatal Python import / top-level errors
    if grep -q "^Traceback\|^ModuleNotFoundError" "$TMPDIR_LOCAL/dc_output.log"; then
        tail -30 "$TMPDIR_LOCAL/dc_output.log" >&2
        fail "$label (fatal Python error — see above)"
    fi
    # DC JSON log levels: ERROR or CRITICAL indicate real failures
    local errors
    errors="$(python3 -c "
import json, sys
errs = []
for line in open('$TMPDIR_LOCAL/dc_output.log'):
    line = line.strip()
    if not line.startswith('{'):
        continue
    try:
        rec = json.loads(line)
        if rec.get('level') in ('ERROR', 'CRITICAL') and 'redis' not in rec.get('msg','').lower():
            errs.append(rec.get('msg', '')[:120])
    except Exception:
        pass
print('\n'.join(errs[:3]))
" 2>/dev/null)"
    if [[ -n "$errors" ]]; then
        echo "    DC errors:" >&2
        echo "$errors" >&2
        fail "$label (DC reported errors)"
    fi
    ok "$label"
}

# ── connector: starburst-galaxy ──────────────────────────────────────────────

test_starburst_galaxy() {
    log "Fetching Starburst Galaxy Dev credentials from 1Password..."
    local item
    item="$(op item get "Starburst Galaxy Dev" --vault Employee --format json)"
    local SG_HOST SG_USER SG_PASS
    SG_HOST="$(echo "$item" | python3 -c "import json,sys; d=json.load(sys.stdin); [print(f['value']) for f in d['fields'] if f.get('label')=='url']")"
    SG_USER="$(echo "$item" | python3 -c "import json,sys; d=json.load(sys.stdin); [print(f['value']) for f in d['fields'] if f.get('label')=='email']")"
    SG_PASS="$(op item get "Starburst Galaxy Dev" --vault Employee --fields password --reveal)"
    local SG_PORT=443
    log "  host=$SG_HOST  user=$SG_USER  port=$SG_PORT"

    # ── 1. CTP pipeline path: flat creds → agent directly ──
    # Sends flat credentials without connect_args — agent runs CTP pipeline.
    # The CTP pipeline maps: host/port/user/password → connect_args for trino.
    local flat_creds
    flat_creds="$(python3 -c "
import json
print(json.dumps({
    'host': '$SG_HOST',
    'port': '$SG_PORT',
    'user': '$SG_USER',
    'password': '$SG_PASS',
}))")"

    local list_catalogs_op
    list_catalogs_op='{"trace_id": "ctp-test-flat", "commands": [{"method": "cursor", "store": "cursor"}, {"target": "cursor", "method": "execute", "args": ["SHOW CATALOGS"]}, {"target": "cursor", "method": "fetchall"}]}'

    test_agent_direct \
        "starburst-galaxy: flat credentials (CTP pipeline path)" \
        "starburst-galaxy" \
        "$flat_creds" \
        "$list_catalogs_op"

    # ── 2. DC passthrough path: connect_args → agent via DC worker ──
    # DC always pre-shapes credentials into connect_args before calling the agent.
    # Agent sees connect_args and returns them unchanged (passthrough).
    local dc_input="$TMPDIR_LOCAL/sg_dc.json"
    python3 -c "
import json
print(json.dumps({
  'call_type': 'initial',
  'params': {
    'job_configuration': {
      'job_id': 'Job_id_test-ctp-shaped',
      'job_type': 'metadata_job',
      'connection': {
        'type': 'starburst-galaxy',
        'credentials': {
          'db_type': 'starburst-galaxy',
          'host': '$SG_HOST',
          'port': '$SG_PORT',
          'user': '$SG_USER',
          'password': '$SG_PASS',
          'agent_details': {
            'agent_id': '12345',
            'endpoint': 'http://localhost:${AGENT_PORT}/',
            'platform': 'AWS_GENERIC',
            'agent_type': 'REMOTE_AGENT',
            'auth_type': 'AWS_ASSUMABLE_ROLE',
            'storage_type': 'S3',
            'image_version': 2100,
            'image_build': 'test',
            'credentials': {}
          }
        }
      },
      'output_stream': {'type': 'console', 'stream_id': 'local_dev'},
      'account_id': 'temp_account_id',
      'warehouse_id': 'test_warehouse',
      'execution_id': 'test-ctp-shaped',
      'dc_schedule_uuid': '1234',
      'job_execution_uuid': '5678'
    }
  }
}, indent=2))" > "$dc_input"

    test_dc_worker \
        "starburst-galaxy: DC pre-shaped credentials (passthrough path)" \
        "$dc_input"
}

# ── connector: redshift ──────────────────────────────────────────────────────

test_redshift() {
    log "Fetching Redshift Dev credentials from 1Password..."
    local item
    item="$(op item get "RedShift redshift-cluster-1 admin password" --vault Engineering --format json)"
    # Hostname is not stored in 1Password; use the known dev cluster endpoint
    local RS_HOST="redshift-cluster-1.cm1ymn9mbz8x.us-east-2.redshift.amazonaws.com"
    local RS_USER RS_PASS RS_DB
    RS_USER="$(echo "$item" | python3 -c "import json,sys; d=json.load(sys.stdin); [print(f['value']) for f in d['fields'] if f.get('label') in ('username','user')]" | head -1)"
    RS_PASS="$(op item get "RedShift redshift-cluster-1 admin password" --vault Engineering --fields password --reveal)"
    RS_DB="dev"
    local RS_PORT=5439
    log "  host=$RS_HOST  user=$RS_USER  port=$RS_PORT  db=$RS_DB"

    # ── 1. CTP pipeline path: flat creds → agent directly ──
    local flat_creds
    flat_creds="$(python3 -c "
import json
print(json.dumps({
    'host': '$RS_HOST',
    'port': '$RS_PORT',
    'db_name': '$RS_DB',
    'user': '$RS_USER',
    'password': '$RS_PASS',
}))")"

    local select_one_op
    select_one_op='{"trace_id": "ctp-test-flat", "commands": [{"method": "cursor", "store": "cursor"}, {"target": "cursor", "method": "execute", "args": ["SELECT 1"]}, {"target": "cursor", "method": "fetchall"}]}'

    test_agent_direct \
        "redshift: flat credentials (CTP pipeline path)" \
        "redshift" \
        "$flat_creds" \
        "$select_one_op"

    # ── 2. DC passthrough path: connect_args + autocommit → agent via DC worker ──
    # DC sends connect_args pre-shaped with autocommit at the top level.
    # Use the flat credentials shape that DC's RedshiftClientWrapper expects.
    # Redshift is a direct (non-plugin) connector — no db_type needed.
    local dc_input="$TMPDIR_LOCAL/rs_dc.json"
    python3 -c "
import json
print(json.dumps({
  'call_type': 'initial',
  'params': {
    'job_configuration': {
      'job_id': 'Job_id_test-ctp-shaped',
      'job_type': 'metadata_job',
      'connection': {
        'type': 'redshift',
        'credentials': {
          'user': '$RS_USER',
          'password': '$RS_PASS',
          'db_name': '$RS_DB',
          'host': '$RS_HOST',
          'agent_details': {
            'agent_id': '12345',
            'endpoint': 'http://localhost:${AGENT_PORT}/',
            'platform': 'AWS_GENERIC',
            'agent_type': 'REMOTE_AGENT',
            'auth_type': 'AWS_ASSUMABLE_ROLE',
            'storage_type': 'S3',
            'image_version': 2100,
            'image_build': 'test',
            'credentials': {}
          }
        }
      },
      'output_stream': {'type': 'console', 'stream_id': 'local_dev'},
      'account_id': 'temp_account_id',
      'warehouse_id': 'test_warehouse',
      'execution_id': 'test-ctp-shaped',
      'dc_schedule_uuid': '1234',
      'job_execution_uuid': '5678'
    }
  }
}, indent=2))" > "$dc_input"

    test_dc_worker \
        "redshift: DC pre-shaped credentials (passthrough path)" \
        "$dc_input"
}

# ── connector: sap-hana ──────────────────────────────────────────────────────

test_sap_hana() {
    log "Fetching SAP HANA Dev credentials from 1Password..."
    local SH_HOST SH_USER SH_PASS SH_DB SH_PORT
    local item
    # Host/port/db come from "SAP HANA dev"; use SYSTEM user for broader access
    item="$(op item get "SAP HANA dev" --vault "3rd Party Creds" --format json)"
    SH_HOST="$(echo "$item" | python3 -c "import json,sys; d=json.load(sys.stdin); [print(u['href']) for u in d.get('urls',[]) if u.get('label')=='HOST']")"
    SH_PORT="$(echo "$item" | python3 -c "import json,sys; d=json.load(sys.stdin); [print(u['href']) for u in d.get('urls',[]) if u.get('label')=='PORT']")"
    SH_DB="$(echo "$item" | python3 -c "import json,sys; d=json.load(sys.stdin); [print(u['href']) for u in d.get('urls',[]) if u.get('label')=='Database']")"
    SH_USER="$(op item get "SAP HANA Dev System User" --vault "3rd Party Creds" --fields username)"
    SH_PASS="$(op item get "SAP HANA Dev System User" --vault "3rd Party Creds" --fields password --reveal)"
    log "  host=$SH_HOST  user=$SH_USER  port=$SH_PORT  db=$SH_DB"

    # ── 1. CTP pipeline path: flat creds → agent directly ──
    # Sends flat credentials (host/port/user/password/db_name) — agent runs CTP pipeline.
    # CTP maps: host→address, db_name→databaseName, seconds→milliseconds for timeouts.
    local flat_creds
    flat_creds="$(python3 -c "
import json
print(json.dumps({
    'host': '$SH_HOST',
    'port': int('$SH_PORT'),
    'user': '$SH_USER',
    'password': '$SH_PASS',
    'db_name': '$SH_DB',
}))")"

    local select_one_op
    select_one_op='{"trace_id": "ctp-test-flat", "commands": [{"method": "cursor", "store": "c"}, {"target": "c", "method": "execute", "args": ["SELECT 1 FROM DUMMY"]}, {"target": "c", "method": "fetchall", "store": "tmp_1"}, {"target": "c", "method": "description", "store": "tmp_2"}, {"target": "c", "method": "rowcount", "store": "tmp_3"}, {"target": "__utils", "method": "build_dict", "kwargs": {"all_results": {"__reference__": "tmp_1"}, "description": {"__reference__": "tmp_2"}, "rowcount": {"__reference__": "tmp_3"}}}]}'

    test_agent_direct \
        "sap-hana: flat credentials (CTP pipeline path)" \
        "sap-hana" \
        "$flat_creds" \
        "$select_one_op"

    # ── 2. DC passthrough path: connect_args → agent directly ──
    # DC pre-shapes credentials into connect_args using driver-native names before calling
    # the agent. SAP HANA metadata jobs do not go through worker_local_execution.py
    # (sap-hana is not a registered transformer type), so we test the DC-shaped credential
    # shape directly against the agent REST API.
    local dc_shaped_creds
    dc_shaped_creds="$(python3 -c "
import json
print(json.dumps({
    'connect_args': {
        'address': '$SH_HOST',
        'port': int('$SH_PORT'),
        'user': '$SH_USER',
        'password': '$SH_PASS',
        'databaseName': '$SH_DB',
        'connectTimeout': 30000,
        'communicationTimeout': 60000,
    }
}))")"

    test_agent_direct \
        "sap-hana: DC pre-shaped credentials (passthrough path)" \
        "sap-hana" \
        "$dc_shaped_creds" \
        "$select_one_op"
}

# ── main ─────────────────────────────────────────────────────────────────────

log "CTP local integration test: connector=$CONNECTOR"
log "Apollo root: $APOLLO_ROOT"
log "DC repo: $DC_REPO"
echo ""

start_agent

case "$CONNECTOR" in
    starburst-galaxy)
        test_starburst_galaxy
        ;;
    redshift)
        test_redshift
        ;;
    sap-hana)
        test_sap_hana
        ;;
    *)
        fail "Unknown connector: $CONNECTOR. Supported: starburst-galaxy, redshift, sap-hana"
        ;;
esac

echo ""
log "All tests passed."
