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
  # nested_type_combinations segfaults the upstream runner (documented in CLAUDE.md).
  find . -name '*.test' \
       -not -path '*/writable/*' -not -path '*/simple_writable/*' \
       -not -name 'nested_type_combinations.test' | while read -r f; do
    mkdir -p "$STAGE/test/sql/integration/$(dirname "$f")"
    awk -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/integration/$f"
  done )

# launch: amortises the JVM cold-start across the whole run via a flock-coordinated
# AF_UNIX worker pool. VGI_TEST_DEDICATED_WORKER is a plain (non-pooled) worker for
# the crash/pool-recovery tests. The three wrappers route the one binary into the
# versioned / versioned_tables / attach_options catalogs.
export VGI_WORKER_BIN
export VGI_TEST_WORKER="launch:${VGI_WORKER_BIN}"
export VGI_TEST_DEDICATED_WORKER="${VGI_WORKER_BIN}"
export VGI_VERSIONED_WORKER="launch:${HERE}/wrappers/vgi-worker-versioned"
export VGI_VERSIONED_TABLES_WORKER="launch:${HERE}/wrappers/vgi-worker-versioned-tables"
export VGI_ATTACH_OPTIONS_WORKER="launch:${HERE}/wrappers/vgi-worker-attach-options"

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

echo "Running suite ..."
pass=0; skip=0; fail=0; failed=()
while IFS= read -r t; do
  rel="${t#"$STAGE"/}"
  if out=$("$HAYBARN_UNITTEST" "$rel" 2>&1); then :; fi
  if grep -q "All tests passed" <<<"$out"; then
    pass=$((pass + 1))
  elif grep -qE "All tests were skipped|No tests ran" <<<"$out"; then
    skip=$((skip + 1))
  else
    fail=$((fail + 1)); failed+=("$rel")
    echo "----- FAIL: $rel -----"
    printf '%s\n' "$out" | tail -40
  fi
done < <(find "$STAGE/test" -name '*.test' | sort)

echo "=================================================="
echo "PASS=$pass  SKIP=$skip  FAIL=$fail"
if [ "$fail" -gt 0 ]; then
  printf '  failed: %s\n' "${failed[@]}"
  exit 1
fi
