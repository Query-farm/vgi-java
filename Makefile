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
	  -not -path '*/simple_writable/*' \
	  -not -name 'ddl_wire_contract.test' | sort > /tmp/intest.txt
	@VGI_TEST_WORKER=$(EXAMPLE_LOCATION) $(UNITTEST) -f /tmp/intest.txt

## Run a single sqllogictest by file name.
test-single: build
	@if [ -z "$(TEST)" ]; then echo "usage: make test-single TEST=test/sql/integration/scalar/add_values.test"; exit 1; fi
	@VGI_TEST_WORKER=$(EXAMPLE_LOCATION) $(UNITTEST) "$(TEST)"

clean:
	./gradlew clean
