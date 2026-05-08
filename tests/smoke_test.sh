#!/usr/bin/env bash
# Copyright 2026 University of Denver
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Smoke test for digitaldu-backend-curation-service.
#
# Hits every endpoint in the merged service and reports pass/fail. By default
# only runs read-only / safe endpoints. State-mutating endpoints require
# --full and a test fixture (--test-folder, --test-uuid).
#
# Required tools: curl, jq
#
# Exit codes:
#   0 — all tests passed
#   1 — one or more tests failed
#   2 — config error (missing API_KEY, can't reach service, etc.)
#
# Usage:
#   API_KEY=xxx ./smoke_test.sh
#   API_KEY=xxx ./smoke_test.sh --url https://curation-api.internal
#   API_KEY=xxx ./smoke_test.sh --full --test-folder my_test_batch
#   API_KEY=xxx ./smoke_test.sh --legacy-auth        # test ?api_key= path too
#
set -u

# --- Config -----------------------------------------------------------------
URL="${CURATION_URL:-http://127.0.0.1:8185}"
API_KEY="${API_KEY:-}"
TEST_FOLDER=""
TEST_UUID=""
TEST_PACKAGE=""
RUN_FULL=0
RUN_LEGACY_AUTH=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --url)            URL="$2"; shift 2 ;;
        --full)           RUN_FULL=1; shift ;;
        --legacy-auth)    RUN_LEGACY_AUTH=1; shift ;;
        --test-folder)    TEST_FOLDER="$2"; shift 2 ;;
        --test-uuid)      TEST_UUID="$2"; shift 2 ;;
        --test-package)   TEST_PACKAGE="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,/^set -u$/p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$API_KEY" ]]; then
    echo "ERROR: API_KEY env var is required" >&2
    exit 2
fi

for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 || { echo "ERROR: $tool not found in PATH" >&2; exit 2; }
done

# --- Output helpers ---------------------------------------------------------
RED=$'\033[0;31m'
GREEN=$'\033[0;32m'
YELLOW=$'\033[0;33m'
GREY=$'\033[0;90m'
RESET=$'\033[0m'

PASSED=0
FAILED=0
SKIPPED=0
FAILED_NAMES=()

pass() { echo "  ${GREEN}✓${RESET} $1"; PASSED=$((PASSED+1)); }
fail() { echo "  ${RED}✗${RESET} $1 — $2"; FAILED=$((FAILED+1)); FAILED_NAMES+=("$1"); }
skip() { echo "  ${YELLOW}∙${RESET} $1 ${GREY}(skipped: $2)${RESET}"; SKIPPED=$((SKIPPED+1)); }
section() { echo; echo "${YELLOW}=== $1 ===${RESET}"; }

# --- Test runner ------------------------------------------------------------
# test_endpoint NAME METHOD PATH EXPECTED_STATUS [JQ_EXPRESSION_FOR_BODY_CHECK]
#
# JQ_EXPRESSION should evaluate truthy for the body to count as passing.
# Pass empty string ("") to skip body checking.
test_endpoint() {
    local name="$1" method="$2" path="$3" expected_status="$4" jq_expr="${5:-}"
    local response status_code body

    response=$(curl -sS -o /tmp/curation_smoke_body.$$ -w "%{http_code}" \
        -X "$method" \
        -H "X-API-Key: $API_KEY" \
        "$URL$path" 2>/tmp/curation_smoke_err.$$)
    status_code="$response"
    body=$(cat /tmp/curation_smoke_body.$$)
    rm -f /tmp/curation_smoke_body.$$ /tmp/curation_smoke_err.$$

    if [[ "$status_code" != "$expected_status" ]]; then
        fail "$name" "expected status $expected_status, got $status_code (body: ${body:0:120})"
        return
    fi

    if [[ -n "$jq_expr" ]]; then
        if ! echo "$body" | jq -e "$jq_expr" >/dev/null 2>&1; then
            fail "$name" "body check failed ($jq_expr) (body: ${body:0:120})"
            return
        fi
    fi

    pass "$name"
}

# Same as test_endpoint but uses ?api_key= legacy auth instead of header.
test_endpoint_legacy_auth() {
    local name="$1" method="$2" path="$3" expected_status="$4"
    local sep="?"
    [[ "$path" == *"?"* ]] && sep="&"
    local response status_code

    response=$(curl -sS -o /dev/null -w "%{http_code}" \
        -X "$method" \
        "$URL$path${sep}api_key=$API_KEY" 2>/dev/null)
    status_code="$response"

    if [[ "$status_code" != "$expected_status" ]]; then
        fail "$name (legacy auth)" "expected $expected_status, got $status_code"
    else
        pass "$name (legacy auth)"
    fi
}

# --- Connectivity -----------------------------------------------------------
section "Connectivity"

if ! curl -sS -o /dev/null --max-time 5 "$URL/health" 2>/dev/null; then
    echo "${RED}✗ Cannot reach $URL${RESET}" >&2
    echo "  Is the service running? Try: systemctl status curation-api" >&2
    exit 2
fi
pass "Service reachable at $URL"

# --- Health and index -------------------------------------------------------
section "Health and index"
test_endpoint "GET /"        GET "/"        200 ""
test_endpoint "GET /health"  GET "/health"  200 '.status == "ok"'

# --- Authentication ---------------------------------------------------------
section "Authentication"

# Header-based auth (preferred)
test_endpoint "QA endpoint with valid header"      GET "/api/v2/qa/list-ready-folders"   200
test_endpoint "ASTools endpoint with valid header" GET "/api/v1/astools/processed"       200 '.errors == []'

# Missing key
status=$(curl -sS -o /dev/null -w "%{http_code}" "$URL/api/v2/qa/list-ready-folders")
[[ "$status" == "403" ]] && pass "QA missing key returns 403" || fail "QA missing key" "got $status, want 403"

status=$(curl -sS -o /dev/null -w "%{http_code}" "$URL/api/v1/astools/processed")
[[ "$status" == "401" ]] && pass "ASTools missing key returns 401" || fail "ASTools missing key" "got $status, want 401"

# Wrong key
status=$(curl -sS -o /dev/null -w "%{http_code}" -H "X-API-Key: wrong" "$URL/api/v2/qa/list-ready-folders")
[[ "$status" == "403" ]] && pass "QA wrong key returns 403" || fail "QA wrong key" "got $status, want 403"

status=$(curl -sS -o /dev/null -w "%{http_code}" -H "X-API-Key: wrong" "$URL/api/v1/astools/processed")
[[ "$status" == "401" ]] && pass "ASTools wrong key returns 401" || fail "ASTools wrong key" "got $status, want 401"

# Legacy ?api_key= still works
if [[ "$RUN_LEGACY_AUTH" == "1" ]]; then
    test_endpoint_legacy_auth "QA legacy auth"      GET "/api/v2/qa/list-ready-folders" 200
    test_endpoint_legacy_auth "ASTools legacy auth" GET "/api/v1/astools/processed"     200
fi

# --- QA service: read-only endpoints ----------------------------------------
section "QA service (read-only)"

test_endpoint "list-ready-folders"      GET "/api/v2/qa/list-ready-folders"        200

# Bad request (missing params) — expect 400
test_endpoint "check-collection-folder no folder" GET "/api/v2/qa/check-collection-folder"  400
test_endpoint "package-names no folder"           GET "/api/v2/qa/package-names"            400
test_endpoint "check-package-names no folder"     GET "/api/v2/qa/check-package-names"      400
test_endpoint "check-file-names no folder"        GET "/api/v2/qa/check-file-names"         400
test_endpoint "check-uri-txt no folder"           GET "/api/v2/qa/check-uri-txt"            400
test_endpoint "get-uri-txt no folder"             GET "/api/v2/qa/get-uri-txt"              400
test_endpoint "get-total-batch-size no folder"    GET "/api/v2/qa/get-total-batch-size"     400
test_endpoint "move-to-ingest no params"          GET "/api/v2/qa/move-to-ingest"           400
test_endpoint "move-to-sftp no uuid"              GET "/api/v2/qa/move-to-sftp"             400
test_endpoint "upload-status no uuid"             GET "/api/v2/qa/upload-status"            400
test_endpoint "reset_permissions no folder"       GET "/api/v2/qa/reset_permissions"        400

# Endpoints that take a folder param — exercise the success path if a fixture is provided
if [[ -n "$TEST_FOLDER" ]]; then
    fold_q="?folder=$TEST_FOLDER"
    test_endpoint "check-collection-folder"   GET "/api/v2/qa/check-collection-folder$fold_q" 200
    test_endpoint "package-names"             GET "/api/v2/qa/package-names$fold_q"           200 '.packages | type == "array"'
    test_endpoint "check-package-names"       GET "/api/v2/qa/check-package-names$fold_q"     200
    test_endpoint "check-file-names"          GET "/api/v2/qa/check-file-names$fold_q"        200
    test_endpoint "check-uri-txt"             GET "/api/v2/qa/check-uri-txt$fold_q"           200
    test_endpoint "get-total-batch-size"      GET "/api/v2/qa/get-total-batch-size$fold_q"    200
    if [[ -n "$TEST_PACKAGE" ]]; then
        pkg_q="${fold_q}&package=$TEST_PACKAGE"
        test_endpoint "get-uri-txt"           GET "/api/v2/qa/get-uri-txt$pkg_q"              200
        test_endpoint "package-file-count"    GET "/api/v2/qa/package-file-count$pkg_q"       200
    else
        skip "get-uri-txt"        "no --test-package"
        skip "package-file-count" "no --test-package"
    fi
else
    skip "QA folder-driven endpoints" "no --test-folder"
fi

# --- ASTools service: read-only endpoints -----------------------------------
section "ASTools service (read-only)"

test_endpoint "workspace"  GET "/api/v1/astools/workspace"  200 '.errors | type == "array"'
test_endpoint "processed"  GET "/api/v1/astools/processed"  200 '.errors | type == "array"'

test_endpoint "workspace/packages no batch"        GET "/api/v1/astools/workspace/packages"        400
test_endpoint "workspace/packages/files no batch"  GET "/api/v1/astools/workspace/packages/files"  400
test_endpoint "workspace/uri no params"            GET "/api/v1/astools/workspace/uri"             400
test_endpoint "check-uri-txt no folder"            GET "/api/v1/astools/check-uri-txt"             400
test_endpoint "move-to-ready no folder"            GET "/api/v1/astools/move-to-ready"             400

if [[ -n "$TEST_FOLDER" ]]; then
    test_endpoint "workspace/packages"        GET "/api/v1/astools/workspace/packages?batch=$TEST_FOLDER"        200 '.errors | type == "array"'
    test_endpoint "workspace/packages/files"  GET "/api/v1/astools/workspace/packages/files?batch=$TEST_FOLDER"  200 '.errors | type == "array"'
    test_endpoint "check-uri-txt"             GET "/api/v1/astools/check-uri-txt?folder=$TEST_FOLDER"            200
    if [[ -n "$TEST_PACKAGE" ]]; then
        test_endpoint "workspace/uri"         GET "/api/v1/astools/workspace/uri?folder=$TEST_FOLDER&package=$TEST_PACKAGE" 200
    else
        skip "workspace/uri" "no --test-package"
    fi
else
    skip "ASTools folder-driven endpoints" "no --test-folder"
fi

# --- State-mutating endpoints (only with --full) ----------------------------
section "State-mutating (require --full + fixture)"

if [[ "$RUN_FULL" == "1" ]]; then
    if [[ -z "$TEST_FOLDER" ]]; then
        echo "${RED}--full requires --test-folder${RESET}" >&2
        exit 2
    fi

    # set-collection-folder is the safest mutator — writes a single text file.
    test_endpoint "set-collection-folder"  GET "/api/v2/qa/set-collection-folder?folder=$TEST_FOLDER"  200 '.is_set != null'

    # move-to-ready (astools) — moves a folder; only run if the test_folder is intentionally disposable
    test_endpoint "move-to-ready"  GET "/api/v1/astools/move-to-ready?folder=$TEST_FOLDER"  200

    # The following are skipped even with --full because they touch SFTP / Archivematica /
    # ArchivesSpace and need careful coordination. Run them by hand if needed.
    skip "move-to-ingest"        "needs production fixture (touches SFTP)"
    skip "move-to-sftp"          "needs production fixture (touches Archivematica)"
    skip "move-to-ingested"      "needs production fixture (touches Wasabi S3)"
    skip "reset_permissions"     "filesystem permission change — manual only"
    skip "cleanup_sftp"          "destructive (rm on remote) — manual only"
    skip "make-digital-objects"  "long-running ArchivesSpace write — manual only"
else
    skip "State-mutating tests" "use --full to enable"
fi

# --- Summary ----------------------------------------------------------------
section "Summary"
TOTAL=$((PASSED + FAILED))
echo "  ${GREEN}Passed: $PASSED${RESET}"
echo "  ${RED}Failed: $FAILED${RESET}"
echo "  ${YELLOW}Skipped: $SKIPPED${RESET}"
echo "  Total run: $TOTAL"

if [[ "$FAILED" -gt 0 ]]; then
    echo
    echo "${RED}Failed tests:${RESET}"
    for name in "${FAILED_NAMES[@]}"; do echo "  - $name"; done
    exit 1
fi

exit 0
