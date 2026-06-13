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
   [`wrappers/`](wrappers), warms the extension cache once, then runs the whole
   suite in a **single `unittest` invocation** (as `make test_launcher` does).
   The CI log streams the runner's native report — a `[i/N] (..%): test/...`
   line per file and the final `All tests passed (.. N assertions in M test
   cases)` summary (thousands of assertions across the ~185 files) — so you can
   see the tests actually ran. Any failed assertion exits non-zero and fails
   the job.

## Transport lanes

The workflow runs a matrix over transports (`run-integration.sh` honours
`TRANSPORT=launch|http` and a `VGI_RPC_SHM_SIZE_BYTES` passthrough):

- **`launch`** — the AF_UNIX worker pool (default).
- **`shm`** — `launch` plus the POSIX shared-memory side channel
  (`VGI_RPC_SHM_SIZE_BYTES`); needs JDK 22+ (`--enable-native-access`).

Both lanes also boot a **versioned_tables catalog worker over HTTP** and set
`VGI_VERSIONED_TABLES_HTTP_WORKER`, so the four `attach/versioned_tables_*_http`
tests run (they attach an `http://` worker regardless of the main transport and
pass against the Java worker). The other http-only tests stay skipped because
they need Java worker HTTP features that aren't ported yet: `versioning_http`
(sticky-session `vgi_sticky` cookies), `bearer_token` (HTTP bearer auth),
`gzip_fallback` (zstd-disable negotiation).

**`http` is intentionally *not* a CI lane yet.** `TRANSPORT=http` works locally
(it boots the worker with `--http` and attaches over `http://`), but the Java
worker's HTTP transport has a known gap: **table-function streaming** (e.g.
`example.sequence(...)`) errors over http where the vgi-python worker succeeds.
The haybarn runner has a built-in `skip_error_messages HTTP` policy that turns
those errors into *skips*, so an http run looks green (0 fail) while silently
dropping the broken table tests. Promoting http to the matrix should wait until
that streaming path is fixed. (`subprocess` was skipped as low-value — same
logic as `launch` but with per-ATTACH JVM cold-start.)

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
