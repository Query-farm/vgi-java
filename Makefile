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

.PHONY: build smoke clean

## Build all worker dist images.
build:
	./gradlew :vgi-example-worker:installDist

## Phase 1 smoke: ATTACH + add_values(1,2). Pre-built worker assumed.
smoke: build
	@if [ ! -x "$(DUCKDB)" ]; then \
	  echo "DuckDB binary missing at $(DUCKDB) — build the C++ extension first:"; \
	  echo "  (cd $(HOME)/Development/vgi && make release)"; \
	  exit 1; \
	fi
	@echo "Running smoke test against worker: $(EXAMPLE_WORKER)"
	@$(DUCKDB) -unsigned -c "LOAD '$(VGI_EXT)'; \
	  ATTACH 'example' AS example (TYPE vgi, LOCATION '$(EXAMPLE_WORKER)'); \
	  SELECT example.add_values(1, 2) AS result; \
	  DETACH example;"

clean:
	./gradlew clean
