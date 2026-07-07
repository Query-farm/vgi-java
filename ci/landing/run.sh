#!/usr/bin/env bash
# Copyright 2026 Query Farm LLC - https://query.farm
#
# Boot the built Java example HTTP worker and drive the cross-language landing
# conformance runner against it (schema validation + vgi-landing-asset marker +
# lazy column endpoints). See ~/Development/vgi/docs/http-landing-contract.md.
#
# Requires:
#   VGI_WORKER_BIN  path to the example worker launcher
#                   (build via `./gradlew :vgi-example-worker:installDist`).
#   python with the `jsonschema` package importable.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="${VGI_WORKER_BIN:?set VGI_WORKER_BIN to the example worker launcher}"
PY="${PYTHON:-python3}"

LOG="$(mktemp)"
ERR="$(mktemp)"
"$BIN" --http --host 127.0.0.1 --port 0 >"$LOG" 2>"$ERR" &
WPID=$!
cleanup() { kill "$WPID" 2>/dev/null || true; }
trap cleanup EXIT

PORT=""
for _ in $(seq 1 150); do
  PORT="$(grep -oE 'PORT:[0-9]+' "$LOG" 2>/dev/null | head -1 | cut -d: -f2 || true)"
  [ -n "$PORT" ] && break
  # Bail early if the worker already died.
  kill -0 "$WPID" 2>/dev/null || break
  sleep 0.2
done

if [ -z "$PORT" ]; then
  echo "ERROR: worker did not report a PORT line" >&2
  echo "--- worker stdout ---" >&2; cat "$LOG" >&2
  echo "--- worker stderr ---" >&2; cat "$ERR" >&2
  exit 1
fi

echo "worker listening on http://127.0.0.1:$PORT"
"$PY" "$HERE/run_landing_conformance.py" \
  --url "http://127.0.0.1:$PORT" \
  --schema "$HERE/describe.schema.json"
