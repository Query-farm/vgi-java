#!/usr/bin/env bash
# Build the combined example worker and print the SQL to attach it from Haybarn.
#
# Usage:
#   ./run.sh            # build + print the ATTACH line
#   ./run.sh --serve    # build + run the worker on a Unix socket in the foreground
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Building the example worker (:examples:docs:installDist)…"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
(cd "$ROOT" && ./gradlew --quiet :examples:docs:installDist)

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/build/install/vgi-java-examples/bin/vgi-java-examples"
echo "==> Worker launch script: $BIN"
echo

if [[ "${1:-}" == "--serve" ]]; then
  SOCK="${TMPDIR:-/tmp}/vgi-demo.sock"
  echo "==> Serving on $SOCK (Ctrl-C to stop). Attach from another shell with:"
  echo "    ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'launch:$BIN');"
  exec "$BIN" --unix "$SOCK" --idle-timeout 0
fi

cat <<SQL
Run these in a Haybarn shell (the vgi extension must be installed: INSTALL vgi FROM community; LOAD vgi;):

  ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'launch:$BIN');

  SELECT demo.upper_case('hello');                                  -- HELLO
  SELECT * FROM demo.numbers(5);                                    -- 0..4
  SELECT n FROM demo.echo((SELECT * FROM demo.numbers(3)));         -- 0,1,2
  SELECT g, demo.vgi_sum(v)
    FROM (VALUES (1,10),(1,20),(2,5)) t(g,v) GROUP BY g;            -- 1->30, 2->5
  SELECT n FROM demo.collect((SELECT * FROM demo.numbers(4)));      -- 0..3

  DETACH demo;

The 'launch:' prefix starts the JVM worker once and reuses it across queries
(via a flock-coordinated Unix socket) — essential, since a cold JVM start is
seconds. See https://query.farm/vgi/docs/java/tutorial/scalar/.
SQL
