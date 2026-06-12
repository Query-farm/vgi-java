# CI: the vgi integration suite

[`.github/workflows/integration.yml`](../.github/workflows/integration.yml)
runs the canonical [Query-farm/vgi](https://github.com/Query-farm/vgi)
integration sqllogictest suite against this repo's Java example worker on every
push / PR. The same `.test` files run against the Python and Go ports, so a
green run here is real wire-compatibility evidence.

## How it works (no C++ build)

Rather than building the vgi DuckDB extension from source, CI drives a
**prebuilt** standalone `haybarn-unittest` (the DuckDB/Haybarn sqllogictest
runner, published in Haybarn's releases) and installs the **signed** vgi
extension from the Haybarn community channel:

1. **Build the worker** — `./gradlew :vgi-example-worker:installDist`.
2. **Checkout the test suite** — `Query-farm/vgi` at a pinned commit; its
   `test/sql/integration/*.test` files are the suite.
3. **Download the runner** — `haybarn_unittest-linux-amd64.zip` from the pinned
   Haybarn release.
4. **Preprocess** — the standalone runner links none of the extensions the
   tests gate on, so [`preprocess-require.awk`](preprocess-require.awk) rewrites
   each `require <ext>` into an explicit signed `INSTALL <ext> FROM
   {community,core}; LOAD <ext>;`. Everything else (notably `require-env`) is
   left untouched.
5. **Run** — [`run-integration.sh`](run-integration.sh) stages the preprocessed
   tree, points the four `VGI_*_WORKER` env vars at the worker (via `launch:`,
   which amortises JVM cold-start across the run) and the three catalog
   [`wrappers/`](wrappers), warms the extension cache once, then runs each
   `.test` and tallies pass / skip / fail.

Out of scope and excluded: `writable/`, `simple_writable/` (the port is
read-only), `nested_type_combinations.test` (segfaults the upstream runner —
see the project `CLAUDE.md`), and `bool_in_union.test` (characterizes a
pre-existing, platform-dependent union-bool bug: the worker reads uninitialized
memory for boolean union variants after the first row, so its pinned expected
output matches arm64 but not amd64 — a real bug to fix separately, not a CI
artifact). The HTTP / bearer / dynamic-code /
`schema_reconcile` tests skip via their `require-env` gates (we don't set those
workers), exactly as in the reference harness.

## Run it locally

```bash
./gradlew :vgi-example-worker:installDist
VGI_SRC=~/path/to/vgi-checkout \
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
VGI_WORKER_BIN="$PWD/vgi-example-worker/build/install/vgi-example-worker/bin/vgi-example-worker" \
  ci/run-integration.sh
```

## Version pins (and their coupling)

Two pins live in the workflow's `env:` block:

| Pin | What | Why |
|-----|------|-----|
| `VGI_REF` | the `Query-farm/vgi` commit supplying the `.test` files | reproducibility — bump deliberately |
| `HAYBARN_RELEASE` | the Haybarn release supplying `haybarn-unittest` | must be ABI-compatible with the community vgi extension (both `v1.5.3`) |

**The coupling to know about:** the vgi extension is pulled live from the
community channel (`INSTALL vgi FROM community`), which always serves the
*currently published* build — it is not version-pinned here. So CI verifies the
Java worker against **what users can actually install today**. If `VGI_REF`
points at a commit whose tests exercise a protocol feature the published
extension doesn't yet ship (or vice-versa), that test can fail or skip. When
bumping `VGI_REF`, re-run the suite locally against the current community
extension and adjust the pin / wrappers together. The catalog
[`wrappers/`](wrappers) encode the canonical `versioned` /
`versioned_tables` / `attach_options` fixture version sets and must track those
fixtures when they move.
