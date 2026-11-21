#!/usr/bin/env bash
# Copyright 2026 Query Farm LLC - https://query.farm
#
# Run the vgi repo's SDK-neutral Orchard hosted-mode harness
# (test/hosted/run_hosted_conformance.py, see its README.md) against the example
# worker. The worker binds port 0 and prints PORT:<n>; JAVA_OPTS (for example
# -Dorg.slf4j.simpleLogger.defaultLogLevel=debug) reaches it through the
# application start script. Every non-HTTP transport is a --refuse-cmd.
#
# Usage: ci/run-hosted-conformance.sh <path/to/run_hosted_conformance.py> [harness args...]
# Needs uv on PATH and the example worker installed
# (./gradlew :vgi-example-worker:installDist).
set -euo pipefail

HARNESS="${1:?path to run_hosted_conformance.py}"
shift
HERE="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$HERE/vgi-example-worker/build/install/vgi-example-worker/bin/vgi-example-worker"
[ -x "$BIN" ] || { echo "::error::example worker not installed at $BIN"; exit 1; }
sock="${TMPDIR:-/tmp}/vgi-java-hosted-refuse-$$.sock"

exec uv run --script "$HARNESS" \
  --worker-cwd "$HERE" \
  --worker-cmd "'$BIN' --http --host 127.0.0.1 --port 0" \
  --refuse-cmd "'$BIN' </dev/null" \
  --refuse-cmd "'$BIN' --unix '$sock'" \
  --refuse-cmd "'$BIN' --tcp 127.0.0.1:{port}" \
  --refuse-cmd "'$BIN' --iroh-raw-upstream 127.0.0.1:{port} --iroh-issuer conformance" \
  "$@"
