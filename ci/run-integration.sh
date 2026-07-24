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

# Transport selection (TRANSPORT=launch|http; default launch) — resolved before
# staging because the http lane needs httpfs injected into every test and drops
# a couple of files the prebuilt binary can't serve over http (see below).
TRANSPORT="${TRANSPORT:-launch}"

# Per-transport staging knobs.
#   AWK_HTTP=1   — the http lane: inject `LOAD httpfs` before each worker ATTACH
#                  (the prebuilt haybarn-unittest doesn't statically link httpfs).
#   HTTP_SKIP    — extra files dropped on the http lane only:
#     * projection_pushdown_repro.test — chunk=2 means one POST round-trip per two
#       rows; transport-agnostic and fully covered by the launch lane (upstream's
#       make test_http drops it for the same reason).
#     * dynamic_filter.test — Top-N + dynamic-filter continuation terminates after
#       the first batch over http in the *prebuilt* haybarn-unittest binary. This
#       is a property of that C++ build, not the worker: vgi-python's worker fails
#       the identical assertion against the same binary, while upstream's locally
#       built unittest passes it. Out of scope for this prebuilt-binary lane.
AWK_HTTP=0
HTTP_SKIP=()
if [ "$TRANSPORT" = "http" ]; then
  AWK_HTTP=1
  HTTP_SKIP=(-not -name 'projection_pushdown_repro.test' -not -name 'dynamic_filter.test')
fi

echo "Staging preprocessed tests into $STAGE (transport=$TRANSPORT) ..."
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
       -not -name 'bool_in_union.test' \
       ${HTTP_SKIP[@]+"${HTTP_SKIP[@]}"} | while read -r f; do
    mkdir -p "$STAGE/test/sql/integration/$(dirname "$f")"
    awk -v http="$AWK_HTTP" -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/integration/$f"
  done )

# Transport recap (resolved above, before staging):
#   launch — flock-coordinated AF_UNIX worker pool, amortising JVM cold-start
#            across the run. Set VGI_RPC_SHM_SIZE_BYTES to also exercise the
#            shared-memory side channel (the `shm` lane).
#   http   — boot the worker as an HTTP server and attach over http:// (mirrors
#            vgi's `make test_http`).
export VGI_WORKER_BIN
# Scratch dir for the native-branch / required-field-filter fixtures. The tests
# COPY their parquet/csv here and the worker's scan branches read the same path
# back, so both sides must name the SAME directory (upstream gates those 6 tests
# behind `require-env VGI_TEST_BRANCH_DIR`; unset, they silently skip).
export VGI_TEST_BRANCH_DIR="${VGI_TEST_BRANCH_DIR:-$(mktemp -d)}"
echo "branch scratch dir: $VGI_TEST_BRANCH_DIR"
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
  # Start the worker with its cwd set to $STAGE — the directory the unittest runs
  # from — so DuckDB's per-test temp dir (__TEST_DIR__ → duckdb_unittest_tempdir/
  # <pid>) and the worker resolve the SAME relative path. Without this the http
  # worker (a separate process started from the repo root) cannot create the
  # COPY ... TO destination the test hands it as a relative path.
  ( cd "$STAGE" && VGI_WORKER_BIN="$VGI_WORKER_BIN" exec "$exe" --http --port 0 ) >"$log" 2>&1 &
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
    # bad-enum fixture worker: serves the example catalog but advertises an
    # unrecognized null_handling enum for `double`, driving the C++ parser's
    # strict-enum rejection (bad_enum.test). Skipped over HTTP (like
    # bad-protocol), so wired on the launch lane only.
    export VGI_BAD_ENUM_WORKER="launch:${HERE}/wrappers/vgi-worker-bad-enum"
    # We are the launcher transport, so opt into the launcher-only tests
    # (launcher/options_smoke.test) — matches vgi's `make test_launcher`.
    export VGI_REQUIRE_LAUNCHER_TRANSPORT=1
    # Serve the versioned_tables + versioned catalogs over HTTP too: the
    # attach/versioned_tables_*_http and attach/versioning_http tests attach an
    # http:// worker regardless of the main transport. versioning_http exercises
    # the sticky-cookie round-trip (vgi_sticky). (bearer_token / gzip_fallback
    # run as a separate step below since they need VGI_TEST_WORKER itself to be a
    # specially-configured http worker.)
    vth_port="$(boot_http_worker "${HERE}/wrappers/vgi-worker-versioned-tables")"
    export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${vth_port}"
    echo "versioned_tables http worker on ${VGI_VERSIONED_TABLES_HTTP_WORKER}"
    vh_port="$(boot_http_worker "${HERE}/wrappers/vgi-worker-versioned")"
    export VGI_VERSIONED_HTTP_WORKER="http://localhost:${vh_port}"
    echo "versioned http worker on ${VGI_VERSIONED_HTTP_WORKER}"
    ;;
  http)
    # Whole-suite-over-HTTP (mirrors vgi's `make test_http`). Every ATTACH goes
    # over http://, so the staging step injected `LOAD httpfs` into each test
    # (AWK_HTTP=1) and dropped the two files the prebuilt binary can't serve over
    # http (HTTP_SKIP). The example worker plus the versioned / versioned_tables
    # catalogs are each booted as their own http server.
    #
    # NB: VGI_REQUIRE_LAUNCHER_TRANSPORT is deliberately NOT set — the
    # launcher-only tests (launcher/options_smoke.test) must skip on this lane.
    port="$(boot_http_worker "$VGI_WORKER_BIN")"
    echo "example http worker on port $port"
    export VGI_TEST_WORKER="http://localhost:${port}"
    vth_port="$(boot_http_worker "${HERE}/wrappers/vgi-worker-versioned-tables")"
    export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${vth_port}"
    export VGI_VERSIONED_TABLES_WORKER="http://localhost:${vth_port}"
    echo "versioned_tables http worker on ${VGI_VERSIONED_TABLES_HTTP_WORKER}"
    vh_port="$(boot_http_worker "${HERE}/wrappers/vgi-worker-versioned")"
    export VGI_VERSIONED_HTTP_WORKER="http://localhost:${vh_port}"
    export VGI_VERSIONED_WORKER="http://localhost:${vh_port}"
    echo "versioned http worker on ${VGI_VERSIONED_HTTP_WORKER}"
    # attach_options catalog over http so attach/attach_options_echo.test runs on
    # this lane too — it's a plain per-attach catalog round-trip (the test even
    # has an explicit "Pool / HTTP safety" section), so http serves it fine.
    ao_port="$(boot_http_worker "${HERE}/wrappers/vgi-worker-attach-options")"
    export VGI_ATTACH_OPTIONS_WORKER="http://localhost:${ao_port}"
    echo "attach_options http worker on ${VGI_ATTACH_OPTIONS_WORKER}"
    # NB: VGI_TEST_DEDICATED_WORKER stays UNSET here. The buffering crash /
    # pool-recovery tests SIGKILL their worker mid-process, which is only safe on
    # a subprocess (bare-path) transport — over http it would tear down the single
    # shared server for the whole suite. Those tests are designed to skip on any
    # shared-worker transport (see table_buffering_worker_crash.test's header).
    ;;
  *)
    echo "::error::unknown TRANSPORT=$TRANSPORT (expected launch|http)"; exit 1 ;;
esac

# ---------------------------------------------------------------------------
# Skip contract + executed-case floor.
#
# haybarn-unittest exits 0 whether one test skipped or every test did: a failed
# `require` / `require-env` is a SKIP, not an error. So "All tests passed" is not
# evidence anything ran — a dead shared worker, an empty stage, or a mis-wired
# env var all read as green while the suite quietly tested nothing. Two guards
# close that gap (summarize_run, below):
#
#   * every skip reason must be named in EXPECTED_SKIP_REASONS — an unlisted
#     reason (e.g. a whole-suite `require httpfs`) fails the lane;
#   * the count of executed cases must stay above MIN_EXECUTED — the signature
#     of a silent collapse is this number falling off a cliff.
#
# The strings are the exact reasons the runner prints under "Skipped tests for
# the following reasons:". These are fixtures / lanes this port deliberately does
# not stand up; each is a legitimate, load-bearing skip.
EXPECTED_SKIP_REASONS=(
  'require-env VGI_BAD_PROTOCOL_WORKER'          # incompatible-protocol fixture, not wired here
  'require-env VGI_RULES_WORKER'                 # vgi-rust multibatch-repro fixture
  'require-env VGI_SCHEMA_RECONCILE_DB'          # schema-reconcile sqlite fixture, not set here
  'require-env VGI_WORKER_SUPPORTS_DYNAMIC_CODE' # dynamic-code registration, not implemented
  'require-env VGI_HTTP_TRANSPORT'               # http-identity tests; this port never sets the flag
  'require-env VGI_HTTP_DISABLE_ZSTD'            # gzip-fallback; skips in the main run, runs in its own step
  'require-env VGI_HTTP_NO_COMPRESSION'          # no-compression fixture server (Python-side)
  'require-env VGI_TEST_BEARER_TOKEN'            # bearer-auth fixture server
  'require-env VGI_TEST_COMPANION_TARGET'        # companion-catalog fixture (Python-side)
  'require-env VGI_TEST_ICEBERG'                 # iceberg fixture, not stood up here
  'require-env VGI_DOCKER_IMAGE'                 # containerised worker lane
  'require-env VGI_DOCKER_TCP_IMAGE'             # containerised worker over TCP
  'require-env VGI_GITHUB_NETWORK_TESTS'         # hits github.com; opt-in only
)
# Lane-specific additions — a skip expected on one lane is a red flag on another.
# The launch lane wires the bad-enum / dedicated-crash / launcher workers, so
# those tests RUN there; over http they gate off (shared server / no launcher).
if [ "$TRANSPORT" = "http" ]; then
  EXPECTED_SKIP_REASONS+=(
    'require-env VGI_BAD_ENUM_WORKER'
    'require-env VGI_REQUIRE_LAUNCHER_TRANSPORT'
    'require-env VGI_TEST_DEDICATED_WORKER'
  )
fi

# Floor on executed cases in the main suite invocation. Deliberately a floor, not
# an equality — the upstream suite grows. Measured 2026-07-24 against
# Query-farm/vgi main: the launch (and shm, which runs TRANSPORT=launch) lane
# executes 271, the http lane 265. The floors sit ~16 below, leaving headroom for
# churn while staying far above the handful a silent collapse would leave.
if [ "$TRANSPORT" = "http" ]; then
  MIN_EXECUTED="${MIN_EXECUTED:-248}"
else
  MIN_EXECUTED="${MIN_EXECUTED:-255}"
fi

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

# summarize_run <log> — parse the runner's console report and enforce the skip
# contract + record the executed-case count. Returns non-zero on an unexpected
# skip reason or a runner that matched no tests at all (the empty-stage
# signature); a suite-assertion failure is caught by the exit code (below).
#
#   total    = the N in the last "[i/N] (..%):" progress line (cases staged)
#   skipped  = the sum of the "Skipped tests for the following reasons:" block
#   executed = total - skipped
MAIN_EXECUTED=0
summarize_run() {
  local log="$1" total skipped executed rc=0 reason
  if grep -q 'No test cases matched\|No tests ran' "$log"; then
    echo "::error::the runner matched no test cases — the glob or the staging is wrong" \
         "(an empty stage still exits 0). transport=$TRANSPORT"
    return 1
  fi
  total="$(sed -n 's/^\[[0-9]*\/\([0-9]*\)\].*/\1/p' "$log" | tail -1)"
  [ -n "$total" ] || { echo "::error::could not parse a test-case total out of the report"; return 1; }
  skipped="$(awk '
    /^Skipped tests for the following reasons:/ { in_block = 1; next }
    in_block && /^[[:space:]]*$/               { in_block = 0; next }
    in_block && match($0, /: [0-9]+[[:space:]]*$/) { n += substr($0, RSTART + 2) }
    END { print n + 0 }' "$log")"
  executed=$(( total - skipped ))
  echo "  executed: $executed / $total   skipped: $skipped   (transport=$TRANSPORT)"
  # Every skip reason must be on the allowlist; an unlisted one fails the lane.
  if [ "$skipped" -gt 0 ]; then
    while IFS= read -r reason; do
      [ -n "$reason" ] || continue
      if printf '%s\n' "${EXPECTED_SKIP_REASONS[@]}" | grep -qxF "$reason"; then
        continue
      fi
      echo "::error::unexpected skip reason '$reason'. Either a gate regressed and a" \
           "whole file silently stopped running, or this skip is legitimate — if so add" \
           "it to EXPECTED_SKIP_REASONS in ci/run-integration.sh with the reason why."
      rc=1
    done < <(awk '
      /^Skipped tests for the following reasons:/ { in_block = 1; next }
      in_block && /^[[:space:]]*$/               { in_block = 0; next }
      in_block && match($0, /: [0-9]+[[:space:]]*$/) { print substr($0, 1, RSTART - 1) }' "$log")
  fi
  MAIN_EXECUTED="$executed"
  return "$rc"
}

# run_unittest <glob...> — run one haybarn-unittest invocation, streaming its
# report, then fail on anything its exit code cannot express: an unexpected skip,
# an empty stage, or a fatal-signal report a fork()ed child printed against the
# parent's counters (invisible to the exit code by construction).
run_unittest() {
  local log rc=0
  log="$(mktemp)"
  # `&& rc=0 || rc=${PIPESTATUS[0]}` keeps the suite's own exit code without
  # tripping errexit and without a trailing `|| true` (which, as a new simple
  # command, would overwrite PIPESTATUS with 0 — the accounting must still run
  # when the suite itself failed).
  "$HAYBARN_UNITTEST" "$@" 2>&1 | tee "$log" && rc=0 || rc="${PIPESTATUS[0]}"
  if grep -q 'due to a fatal error condition' "$log"; then
    echo "::error::a forked child ran the test harness's signal handler (see the" \
         "'fatal error condition' block above). The parent exited $rc and would" \
         "otherwise have passed. This is invisible to the exit code by construction."
    rc=1
  fi
  summarize_run "$log" || rc=1
  rm -f "$log"
  return "$rc"
}

# Run the whole suite in ONE invocation (as `make test_launcher` does), streaming
# the runner's native sqllogictest report: a `[i/N] (..%): test/...` progress
# line per file and the final `All tests passed (.. N assertions in M test
# cases)` summary. Out-of-scope tests were dropped at staging, so the glob never
# matches them; any failed assertion, unexpected skip, or executed-case collapse
# fails the job. run_unittest returns non-zero on failure → `set -e` aborts.
echo "Running suite (single invocation — native sqllogictest report) ..."
run_unittest "test/sql/integration/*"

# gzip_fallback.test needs VGI_TEST_WORKER itself to be a zstd-disabled HTTP
# worker (it attaches it directly and asserts the gzip codec fallback). That
# conflicts with the main suite's launch: worker, so run it as a dedicated
# single-test invocation against its own worker. Only on the launch lane (shm is
# the same coverage; the http lane is local-only). The subshell keeps the env
# overrides from leaking (a `VAR=v func` prefix persists in bash, unlike
# `VAR=v some_binary`) and isolates the summarize_run accounting — the floor
# below is checked against the MAIN suite, not this single-test run.
if [ "$TRANSPORT" = "launch" ]; then
  echo "Running http/gzip_fallback.test (zstd-disabled http worker) ..."
  gz_port="$(VGI_HTTP_DISABLE_ZSTD=1 boot_http_worker "$VGI_WORKER_BIN")"
  ( export VGI_TEST_WORKER="http://localhost:${gz_port}" VGI_HTTP_DISABLE_ZSTD=1
    run_unittest "test/sql/integration/http/gzip_fallback.test" )
fi

# Executed-case floor on the main suite — the collapse-detector. Reached only if
# the runs above passed (a failure already aborted via set -e); MAIN_EXECUTED
# holds the main invocation's count (the gzip subshell does not touch it).
if [ "$MAIN_EXECUTED" -lt "$MIN_EXECUTED" ]; then
  echo "::error::only $MAIN_EXECUTED test cases executed on the $TRANSPORT lane," \
       "floor is MIN_EXECUTED=$MIN_EXECUTED. This is the signature of a suite-wide" \
       "silent skip (a failed require is a SKIP, not an error). Do NOT lower the floor" \
       "to make this pass — find what stopped running."
  exit 1
fi
echo "Executed $MAIN_EXECUTED main-suite test cases (floor $MIN_EXECUTED) on the $TRANSPORT lane."
