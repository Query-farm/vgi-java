#!/usr/bin/env bash
# Copyright 2026 Query Farm LLC - https://query.farm
#
# Run vgi-rpc's hosted-protocols conformance group against the example worker on
# stdio, AF_UNIX and HTTP (with --identity). The group checks that the worker
# hosts `conformance.Secondary.v1` beside `vgi.v2` through Worker.hostedProtocols,
# the vgi-rpc error model, the traceback policy, and -- over HTTP -- the
# vgi_rpc.Identity.v1 conformance policy.
#
# Required environment:
#   VGI_WORKER_BIN   path to the built example worker launcher
# Optional:
#   HOSTED           command that runs the group (default: vgi-rpc-test-hosted)
set -euo pipefail

: "${VGI_WORKER_BIN:?path to the example worker launcher}"
read -r -a HOSTED <<<"${HOSTED:-vgi-rpc-test-hosted}"
EXPECT="vgi.v2,conformance.Secondary.v1"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

echo "=== stdio ==="
"${HOSTED[@]}" --cmd "$VGI_WORKER_BIN" --expect "$EXPECT" -- -q -rs

echo "=== unix ==="
# Short path: AF_UNIX paths are capped at ~104 bytes.
sock="/tmp/vgi-hosted-$$.sock"
rm -f "$sock"
"$VGI_WORKER_BIN" --unix "$sock" >/dev/null 2>&1 &
PIDS+=("$!")
for _ in $(seq 1 120); do [ -S "$sock" ] && break; sleep 0.5; done
[ -S "$sock" ] || { echo "::error::unix worker never bound $sock"; exit 1; }
"${HOSTED[@]}" --unix "$sock" --expect "$EXPECT" -- -q -rs

echo "=== http (--identity) ==="
log="$(mktemp)"
"$VGI_WORKER_BIN" --http --port 0 --identity >"$log" 2>&1 &
PIDS+=("$!")
port=""
for _ in $(seq 1 120); do
  port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
  [ -n "$port" ] && break
  sleep 0.5
done
[ -n "$port" ] || { echo "::error::http worker never reported a port"; cat "$log"; exit 1; }
# Over HTTP with --identity nothing should skip; -rs names any skip that does.
out="$(mktemp)"
"${HOSTED[@]}" --url "http://127.0.0.1:${port}" --expect "$EXPECT" --identity -- -q -rs | tee "$out"
if grep -q '^SKIPPED' "$out"; then
  echo "::error::the HTTP hosted-protocols run skipped tests; over HTTP with --identity none may skip"
  exit 1
fi
