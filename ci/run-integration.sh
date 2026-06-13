#!/usr/bin/env bash
# Copyright 2026 Query Farm LLC - https://query.farm
#
# Run the vgi integration sqllogictest suite against the Java example worker,
# using a prebuilt standalone `haybarn-unittest` and the signed community vgi
# extension (no C++ build from source). See ci/README.md for the approach.
#
# Required environment:
#   VGI_SRC          path to a Query-farm/vgi checkout (contains test/sql/integration)
#   HAYBARN_UNITTEST path to the haybarn-unittest binary
#   VGI_WORKER_BIN   path to the built example worker launcher
#                    (vgi-example-worker/build/install/.../bin/vgi-example-worker)
# Optional:
#   STAGE            scratch dir for the preprocessed test tree (default: mktemp)
set -euo pipefail

: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"
: "${VGI_WORKER_BIN:?path to the example worker launcher}"

HERE="$(cd "$(dirname "$0")" && pwd)"
STAGE="${STAGE:-$(mktemp -d)}"
INTEGRATION="$VGI_SRC/test/sql/integration"
[ -d "$INTEGRATION" ] || { echo "::error::no test/sql/integration under VGI_SRC=$VGI_SRC"; exit 1; }

echo "Staging preprocessed tests into $STAGE ..."
mkdir -p "$STAGE/test/sql/integration"
( cd "$INTEGRATION"
  # writable/simple_writable are out of scope (read-only port);
  # nested_type_combinations segfaults the upstream runner (documented in CLAUDE.md);
  # bool_in_union characterizes a pre-existing, platform-dependent union-bool bug
  # (the worker reads uninitialized memory for bool variants after row 1, so the
  # result is undefined — its pinned expected output matches arm64 but not amd64).
  find . -name '*.test' \
       -not -path '*/writable/*' -not -path '*/simple_writable/*' \
       -not -name 'nested_type_combinations.test' \
       -not -name 'bool_in_union.test' | while read -r f; do
    mkdir -p "$STAGE/test/sql/integration/$(dirname "$f")"
    awk -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/integration/$f"
  done )

# Transport selection (TRANSPORT=launch|http; default launch):
#   launch — flock-coordinated AF_UNIX worker pool, amortising JVM cold-start
#            across the run. Set VGI_RPC_SHM_SIZE_BYTES to also exercise the
#            shared-memory side channel (the `shm` lane).
#   http   — boot the worker as an HTTP server and attach over http:// (mirrors
#            vgi's `make test_http`).
export VGI_WORKER_BIN
TRANSPORT="${TRANSPORT:-launch}"
# An empty VGI_RPC_SHM_SIZE_BYTES must not reach the C++ client (it would try to
# attach a zero-size segment); only a real value enables the shm side channel.
[ -n "${VGI_RPC_SHM_SIZE_BYTES:-}" ] || unset VGI_RPC_SHM_SIZE_BYTES

# Background worker processes (http servers) are tracked here and killed on exit.
BG_PIDS=()
cleanup() { for p in "${BG_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

# boot_http_worker <executable> — start it as an HTTP server on an ephemeral
# port and echo the port it reports (`PORT:<n>`, the same readiness contract as
# vgi-python's vgi-fixture-http). The executable inherits $VGI_WORKER_BIN (the
# catalog wrappers exec it).
boot_http_worker() {
  local exe="$1" log pid port=""
  log="$(mktemp)"
  VGI_WORKER_BIN="$VGI_WORKER_BIN" "$exe" --http --port 0 >"$log" 2>&1 &
  pid=$!
  BG_PIDS+=("$pid")
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || { echo "::error::http worker '$exe' exited" >&2; cat "$log" >&2; return 1; }
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$port" ] && break
    sleep 0.5
  done
  [ -n "$port" ] || { echo "::error::http worker '$exe' never reported a port" >&2; cat "$log" >&2; return 1; }
  echo "$port"
}

case "$TRANSPORT" in
  launch)
    # VGI_TEST_DEDICATED_WORKER is a plain (non-pooled) worker for the crash/
    # pool-recovery tests; the three wrappers route the one binary into the
    # versioned / versioned_tables / attach_options catalogs.
    export VGI_TEST_WORKER="launch:${VGI_WORKER_BIN}"
    export VGI_TEST_DEDICATED_WORKER="${VGI_WORKER_BIN}"
    export VGI_VERSIONED_WORKER="launch:${HERE}/wrappers/vgi-worker-versioned"
    export VGI_VERSIONED_TABLES_WORKER="launch:${HERE}/wrappers/vgi-worker-versioned-tables"
    export VGI_ATTACH_OPTIONS_WORKER="launch:${HERE}/wrappers/vgi-worker-attach-options"
    # We are the launcher transport, so opt into the launcher-only tests
    # (launcher/options_smoke.test) — matches vgi's `make test_launcher`.
    export VGI_REQUIRE_LAUNCHER_TRANSPORT=1
    # Also serve the versioned_tables catalog over HTTP: the four
    # attach/versioned_tables_*_http tests attach an http:// worker regardless of
    # the main transport, and pass against the Java worker. (The other http-only
    # tests need worker HTTP features the Java port doesn't yet implement —
    # versioning_http needs sticky-session cookies, bearer_token needs bearer
    # auth, gzip_fallback needs zstd-disable negotiation — so their env vars stay
    # unset and they skip.)
    vth_port="$(boot_http_worker "${HERE}/wrappers/vgi-worker-versioned-tables")"
    export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${vth_port}"
    echo "versioned_tables http worker on ${VGI_VERSIONED_TABLES_HTTP_WORKER}"
    ;;
  http)
    # Whole-suite-over-HTTP. For LOCAL use — NOT yet a CI lane: the Java worker's
    # HTTP transport has a known gap (function-backed table-function streaming,
    # e.g. `example.sequence(...)`, errors over http where vgi-python succeeds;
    # the haybarn runner's built-in `skip_error_messages HTTP` policy masks those
    # as skips, so the lane looks green while silently dropping those tests).
    # See ci/README.md.
    port="$(boot_http_worker "$VGI_WORKER_BIN")"
    echo "HTTP worker on port $port"
    export VGI_TEST_WORKER="http://localhost:${port}"
    ;;
  *)
    echo "::error::unknown TRANSPORT=$TRANSPORT (expected launch|http)"; exit 1 ;;
esac

cd "$STAGE"

echo "Warming the extension cache (vgi from community, deps from core) ..."
mkdir -p "$STAGE/test"
cat > "$STAGE/test/_warm.test" <<'EOF'
# name: test/_warm.test
# group: [warm]
statement ok
INSTALL vgi FROM community;

statement ok
INSTALL httpfs FROM core;

statement ok
INSTALL json FROM core;

statement ok
INSTALL parquet FROM core;

statement ok
INSTALL spatial FROM core;
EOF
"$HAYBARN_UNITTEST" "test/_warm.test" >/dev/null 2>&1 || echo "::warning::extension warm step did not fully succeed"
rm -f "$STAGE/test/_warm.test"

# Run the whole suite in ONE unittest invocation (as `make test_launcher`
# does), streaming the runner's native sqllogictest report: a `[i/N] (..%):
# test/...` progress line per file and the final
# `All tests passed (.. N assertions in M test cases)` summary (a failure
# prints the offending query + a `M test cases | K failed` summary). This keeps
# the CI log showing that the tests actually ran — and how many assertions —
# rather than a rolled-up count. Out-of-scope tests were already dropped at
# staging, so the glob never matches them; any failed assertion exits non-zero
# and fails the job (via `set -e`).
echo "Running suite (single invocation — native sqllogictest report) ..."
"$HAYBARN_UNITTEST" "test/sql/integration/*"
