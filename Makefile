# vgi-java — top-level test driver
#
# Mirrors vgi-go's Makefile shape: build, test, test-single, test-http.
# Tests are run by the C++ extension's unittest binary at
# $(VGI_BUILD_DIR)/test/unittest. Set VGI_BUILD_DIR if your DuckDB checkout
# isn't at ~/Development/vgi.

VGI_BUILD_DIR ?= $(HOME)/Development/vgi/build/release
DUCKDB        ?= $(VGI_BUILD_DIR)/duckdb
VGI_EXT       ?= $(VGI_BUILD_DIR)/extension/vgi/vgi.duckdb_extension
UNITTEST      ?= $(VGI_BUILD_DIR)/test/unittest

EXAMPLE_WORKER := $(CURDIR)/vgi-example-worker/build/install/vgi-example-worker/bin/vgi-example-worker

# launch:<argv> location → C++ extension uses the AF_UNIX launcher protocol
# (see ~/Development/vgi/docs/launcher-protocol.md) instead of subprocess-fork
# per ATTACH. Amortises JVM cold-start across the whole test run.
LAUNCHER_PREFIX ?= launch:
EXAMPLE_LOCATION := $(LAUNCHER_PREFIX)$(EXAMPLE_WORKER)

# Auxiliary fixture workers.
#
# 44 of the shared suite's 326 files gate on `require-env`, and 11 of those
# gates name a fixture worker serving a *different* catalog than `example`.
# ci/wrappers/ already routes the one worker binary into each of them (via
# VGI_WORKER_CATALOG_NAME), and ci/run-integration.sh exports them — but this
# lane did not, so a developer ran 282 cases locally while CI ran 293. Eleven
# files' worth of behaviour was CI-only, which is the wrong way round: the
# expensive lane should be the one that finds LESS.
#
# The wrappers exec $VGI_WORKER_BIN, so they need it in the environment.
WRAPPERS := $(CURDIR)/ci/wrappers
VERSIONED_LOCATION        := $(LAUNCHER_PREFIX)$(WRAPPERS)/vgi-worker-versioned
VERSIONED_TABLES_LOCATION := $(LAUNCHER_PREFIX)$(WRAPPERS)/vgi-worker-versioned-tables
ATTACH_OPTIONS_LOCATION   := $(LAUNCHER_PREFIX)$(WRAPPERS)/vgi-worker-attach-options
BAD_ENUM_LOCATION         := $(LAUNCHER_PREFIX)$(WRAPPERS)/vgi-worker-bad-enum
BAD_PROTOCOL_LOCATION     := $(LAUNCHER_PREFIX)$(WRAPPERS)/vgi-worker-bad-protocol

# Every fixture-worker variable the shared suite reads, in one place so `test`
# and `test-single` cannot drift apart.
#
#   VGI_TEST_DEDICATED_WORKER is deliberately NOT a launch: location — the
#   crash/pool-recovery tests need a worker this process owns and can watch die,
#   which a shared launcher worker is not.
#
#   VGI_ATTACH_OPTIONS_REQUIRED_WORKER is the same wrapper as
#   VGI_ATTACH_OPTIONS_WORKER: that worker serves the `attach_options_required`
#   catalog too, and upstream split those assertions into their own file behind
#   their own env var.
#   VGI_TEST_BRANCH_DIR is the scratch dir the multi_branch_* fixtures and the
#   .test files that seed them (parquet / csv) must BOTH name. The vgi Makefile
#   exports it, so this lane inherited it under `make test_java` and not when run
#   standalone — 7 files skipped depending on how you invoked the same target.
VGI_TEST_BRANCH_DIR ?= $(shell python3 -c 'import tempfile;print(tempfile.gettempdir())')

FIXTURE_ENV := \
	VGI_WORKER_BIN=$(EXAMPLE_WORKER) \
	VGI_TEST_BRANCH_DIR=$(VGI_TEST_BRANCH_DIR) \
	VGI_TEST_WORKER=$(EXAMPLE_LOCATION) \
	VGI_TEST_DEDICATED_WORKER=$(EXAMPLE_WORKER) \
	VGI_VERSIONED_WORKER=$(VERSIONED_LOCATION) \
	VGI_VERSIONED_TABLES_WORKER=$(VERSIONED_TABLES_LOCATION) \
	VGI_ATTACH_OPTIONS_WORKER=$(ATTACH_OPTIONS_LOCATION) \
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER=$(ATTACH_OPTIONS_LOCATION) \
	VGI_BAD_ENUM_WORKER=$(BAD_ENUM_LOCATION) \
	VGI_BAD_PROTOCOL_WORKER=$(BAD_PROTOCOL_LOCATION) \
	VGI_REQUIRE_LAUNCHER_TRANSPORT=1

.PHONY: build smoke test test-single clean

## Build all worker dist images.
build:
	./gradlew :vgi-example-worker:installDist

## Smoke: ATTACH + add_values(1,2). Pre-built worker assumed.
smoke: build
	@if [ ! -x "$(DUCKDB)" ]; then \
	  echo "DuckDB binary missing at $(DUCKDB) — build the C++ extension first:"; \
	  echo "  (cd $(HOME)/Development/vgi && make release)"; \
	  exit 1; \
	fi
	@echo "Running smoke test against worker: $(EXAMPLE_LOCATION)"
	@$(DUCKDB) -unsigned -c "LOAD '$(VGI_EXT)'; \
	  ATTACH 'example' AS example (TYPE vgi, LOCATION '$(EXAMPLE_LOCATION)'); \
	  SELECT example.add_values(1, 2) AS result; \
	  DETACH example;"

## Run the in-scope integration tests.
##
## Exclusions (audited 2026-08-21 — each is anchored so it drops exactly the
## files named, nothing else):
##
##   simple_writable/ — writable VGI is genuinely unimplemented in this repo;
##     there is no writable worker to point these at. (Upstream has no
##     `writable/` directory today, so no glob is needed for one.)
##
##   attach/ddl_wire_contract.test — KNOWN FAILURE against this worker, not a
##     scope gap. vgi-rpc-java's RpcServer.validateParameterContract rejects the
##     C++ extension's catalog_schema_create request over two disagreements:
##     (a) it treats a dictionary-encoded utf8 (`on_conflict: Utf8[dictionary: 0]`)
##     as a different type from plain `Utf8`, and (b) it wants `tags` non-nullable
##     where the client sends it nullable. Reproduced 2026-08-21. Fix the
##     validator, then delete this line — do NOT re-broaden the glob.
##
## Deliberately NOT excluded any more:
##   * bearer_auth/ — the worker has wired bearer auth since 40af24a
##     (2026-06-13, Main.java buildHttpConfig); the test self-skips on
##     `require-env VGI_TEST_BEARER_TOKEN`, which this lane does not set.
##     Wiring a real bearer lane needs an HTTP worker booted on an ephemeral
##     port (see vgi-rust ci/run-integration.sh) — follow-up work.
##   * the rest of attach/ — 12 of the 13 files gate on fixture-worker env vars
##     (VGI_VERSIONED*_WORKER / VGI_ATTACH_OPTIONS*_WORKER) that ci/run-integration.sh
##     exports but this lane does not, so they skip visibly instead of silently
##     vanishing from the staged set.
##   * accumulate/attach_scope.test and catalog/multi_branch_two_writable.test —
##     both pass here; they were only ever collateral damage of the unanchored
##     `*attach*` / `*writable*` globs.
test: build
	@find $(HOME)/Development/vgi/test/sql/integration -name '*.test' \
	  -not -path '*/simple_writable/*' | sort > /tmp/intest.txt
	@$(FIXTURE_ENV) $(UNITTEST) -f /tmp/intest.txt

## Run a single sqllogictest by file name.
test-single: build
	@if [ -z "$(TEST)" ]; then echo "usage: make test-single TEST=test/sql/integration/scalar/add_values.test"; exit 1; fi
	@$(FIXTURE_ENV) $(UNITTEST) "$(TEST)"

clean:
	./gradlew clean
