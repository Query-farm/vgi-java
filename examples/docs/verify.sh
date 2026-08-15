#!/usr/bin/env bash
# Copyright 2026 Query Farm LLC - https://query.farm
#
# Runs every documentation example against a real engine and checks its output.
# These examples are embedded verbatim in the vgi-java documentation, so "it
# compiled" is not the bar — each one has to produce the rows the docs claim.
#
#   HAYBARN=/path/to/haybarn ./verify.sh
#
# Haybarn is DuckDB plus the `vgi` extension. Stock DuckDB cannot INSTALL vgi,
# which is why this is a script rather than a Gradle test.

set -euo pipefail

HAYBARN="${HAYBARN:-haybarn}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
BIN="$DIR/build/install/vgi-java-examples/bin/vgi-java-examples"

if ! command -v "$HAYBARN" >/dev/null 2>&1 && [ ! -x "$HAYBARN" ]; then
  echo "haybarn not found. Set HAYBARN=/path/to/haybarn." >&2
  exit 1
fi

echo "building…"
(cd "$ROOT" && ./gradlew --quiet :examples:docs:installDist)

# A pooled `launch:` worker outlives a rebuild — it keeps answering with the
# previous build until it idles out. Kill any stragglers so this script tests
# what was just built rather than what was running.
pkill -f "farm.query.vgi.examples" 2>/dev/null || true
sleep 1

fail=0

# check <name> <sql> <expected-substring>
check() {
  local name="$1" sql="$2" want="$3" got
  got="$("$HAYBARN" -c "LOAD vgi;
ATTACH 'demo' (TYPE vgi, LOCATION 'launch:$BIN');
$sql" 2>&1 || true)"
  if grep -qF -- "$want" <<<"$got"; then
    echo "ok   $name"
  else
    echo "FAIL $name — expected to find: $want"
    sed 's/^/     /' <<<"$got"
    fail=1
  fi
}

check scalar \
  "SELECT demo.upper_case('hello') AS shout;" "HELLO"

# The named form only resolves when the build carried -parameters; without it
# the argument is called arg0 and this is the check that catches it.
check named-argument \
  "SELECT demo.upper_case(value := 'hello') AS shout;" "HELLO"

check argument-name \
  "SELECT arg_name FROM vgi_function_arguments() WHERE function_name = 'upper_case';" "value"

check table \
  "SELECT count(*) AS c, sum(n) AS t FROM demo.numbers(5);" "10"

# Four scan threads over one shared cursor: every row exactly once.
check parallel-scan \
  "SELECT count(*) AS rows, count(DISTINCT n) AS distinct_n FROM demo.numbers(1000000);" "1000000"

check table-in-out \
  "SELECT sum(n) AS s FROM demo.echo((SELECT * FROM demo.numbers(3)));" "3"

check aggregate \
  "SELECT demo.vgi_sum(v) AS t FROM (VALUES (1,10),(1,20),(2,5)) t(g,v);" "35"

check buffering \
  "SELECT sum(n) AS s FROM demo.collect((SELECT * FROM demo.numbers(4)));" "6"

check catalog-table \
  "SELECT count(*) AS c FROM demo.catalog.first_five;" "5"

check catalog-view \
  "SELECT count(*) AS c FROM demo.catalog.evens;" "3"

exit "$fail"
