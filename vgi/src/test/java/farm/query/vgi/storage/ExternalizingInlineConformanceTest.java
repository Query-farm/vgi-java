// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

/**
 * {@link FunctionStorageConformanceTest} over {@link ExternalizingStorage} with
 * no blob vendor: every value is an inline envelope, so the wrapper must be
 * invisible to the storage contract.
 */
class ExternalizingInlineConformanceTest extends FunctionStorageConformanceTest {
    @Override
    FunctionStorage createStore() {
        return new ExternalizingStorage(new SqliteFunctionStorage(":memory:"), null, 4096, null);
    }
}
