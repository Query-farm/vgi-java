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

## Run the in-scope integration tests (excludes writable, simple_writable,
## attach, bearer_auth — those need additional worker binaries we haven't
## ported yet).
test: build
	@find $(HOME)/Development/vgi/test/sql/integration -name '*.test' \
	  -not -path '*writable*' -not -path '*simple_writable*' \
	  -not -path '*bearer_auth*' -not -path '*attach*' | sort > /tmp/intest.txt
	@VGI_TEST_WORKER=$(EXAMPLE_LOCATION) $(UNITTEST) -f /tmp/intest.txt

## Run a single sqllogictest by file name.
test-single: build
	@if [ -z "$(TEST)" ]; then echo "usage: make test-single TEST=test/sql/integration/scalar/add_values.test"; exit 1; fi
	@VGI_TEST_WORKER=$(EXAMPLE_LOCATION) $(UNITTEST) "$(TEST)"

clean:
	./gradlew clean
