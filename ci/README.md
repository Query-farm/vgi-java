# CI: the vgi integration suite

CI uses the VGI source revision pinned by `VGI_REF` for both the test suite and a native runner linked with that VGI extension. `scripts/ci_fetch_native_runner.py` verifies the runner provenance and sets `RUNNER_LINKS_EXTENSIONS=1`; this lets wire-contract changes run before a separate public extension release. The local driver still defaults to the signed community extension when that flag is omitted. Existing test exclusions and coverage floors apply in both modes.

[`.github/workflows/integration.yml`](../.github/workflows/integration.yml)
runs the canonical [Query-farm/vgi](https://github.com/Query-farm/vgi)
integration sqllogictest suite against this repo's Java example worker on every
push / PR. The same `.test` files run against the Python and Go ports, so a
green run here is real wire-compatibility evidence.

## How it works (no C++ build)

CI downloads the native integration artifact built by the pinned VGI revision.
The SDK repository builds only its Java worker:

1. **Build the worker once** — `./gradlew :vgi-example-worker:installDist`
   creates the distribution shared by every transport lane.
2. **Checkout the test suite** — `Query-farm/vgi` at `VGI_REF` supplies the
   canonical sqllogictest files and artifact-fetch helper.
3. **Download and verify the runner** — the helper selects that exact commit's
   artifact, validates its manifest and checksums, and exports the runner path.
4. **Stage and run** — `RUNNER_LINKS_EXTENSIONS=1` preserves extension gates and
   runs the staged suite against the linked native build. The driver still
   supports preprocessing signed community installs for independent local
   runners. Existing skip checks and execution floors apply to both modes.

## Transport lanes

The workflow runs a matrix over transports (`run-integration.sh` honours
`TRANSPORT=launch|http` and a `VGI_RPC_SHM_SIZE_BYTES` passthrough):

- **`launch`** — the AF_UNIX worker pool (default).
- **`shm`** — `launch` plus the POSIX shared-memory side channel
  (`VGI_RPC_SHM_SIZE_BYTES`); needs JDK 22+ (`--enable-native-access`).

All lanes also boot a **versioned_tables catalog worker over HTTP** and set
`VGI_VERSIONED_TABLES_HTTP_WORKER`, so the four `attach/versioned_tables_*_http`
tests run. The **`http`** lane runs the main worker through its stateless HTTP
transport and is required alongside launch and shared memory. Optional fixture
capabilities remain controlled by the driver's `require-env` settings.

Out of scope and excluded: `writable/`, `simple_writable/` (the port is
read-only), `nested_type_combinations.test` (segfaults the upstream runner —
see the project `CLAUDE.md`), and `bool_in_union.test` (characterizes a
pre-existing, platform-dependent union-bool bug: the worker reads uninitialized
memory for boolean union variants after the first row, so its pinned expected
output matches arm64 but not amd64 — a real bug to fix separately, not a CI
artifact). The HTTP / bearer / dynamic-code /
`schema_reconcile` tests skip via their `require-env` gates (we don't set those
workers), exactly as in the reference harness.

## The skip contract + executed-case floor

`haybarn-unittest` exits 0 whether one test skipped or every test did — a failed
`require` / `require-env` is a **skip**, not an error. So "All tests passed" on its own is not evidence anything ran: a dead
shared worker, an empty stage, or a mis-wired env var all read as green while the
suite quietly tested nothing. `run-integration.sh` closes that gap with two
guards in `summarize_run`:

- **Skip allowlist.** Every skip reason the runner prints must appear in
  `EXPECTED_SKIP_REASONS` (a base list plus, on the http lane, the bad-enum /
  dedicated-crash / launcher reasons that gate off there but *run* on launch). An
  unlisted reason — the signature of a whole file silently gated off, like
  `require httpfs: 227` — fails the lane. A genuinely new, legitimate skip is
  added to the list *with the reason why*.
- **Executed-case floor.** The main suite's executed-case count must stay above a
  per-lane `MIN_EXECUTED` (launch/shm 255, http 248 — floors ~16 below the
  measured 271 / 265, leaving room for upstream growth). A collapse in this
  number is the tell of a suite-wide silent skip. It is a floor, not an equality:
  **do not lower it to make a run pass** — find what stopped running. Ported from
  vgi-typescript, whose harness had this from the start.

**No error is a skip.** Given no `--test-config`, the runner's default
`skip_error_messages` turns any error containing `HTTP` or `Unable to connect`
into a skip — and over the http transport *every* worker error contains `HTTP`,
so that lane used to report real failures as skips. Every unittest invocation
therefore passes `--test-config` with the vgi checkout's
`test/configs/no_error_skip.json` (override with `VGI_TEST_CONFIG`); a checkout
without that file fails the run instead of silently masking again.

`run_unittest` also scans for the `fatal error condition` block a fork()ed child
prints against the parent's counters — a failure invisible to the exit code by
construction.

## Run it locally

```bash
./gradlew :vgi-example-worker:installDist
VGI_SRC=~/path/to/vgi-checkout \
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
VGI_WORKER_BIN="$PWD/vgi-example-worker/build/install/vgi-example-worker/bin/vgi-example-worker" \
  ci/run-integration.sh
```

## Integration revisions

The workflow's `env:` block identifies these upstream inputs:

| Input | What | Policy |
|-------|------|--------|
| `VGI_REF` | the `Query-farm/vgi` revision supplying the suite and native runner | exact commit; both inputs must match |
| `VGI_RPC_JAVA_REF` | the RPC framework built from source | released tag, or exact source commit while the required package is unreleased |

Signed community publication is a separate release step. An independent local
runner can still exercise that channel through the driver's default mode. The
catalog [`wrappers/`](wrappers) must track the canonical fixture definitions.
