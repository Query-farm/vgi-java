// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

/** {@link FunctionStorageConformanceTest} over the in-process {@code :memory:} tier. */
class SqliteMemoryConformanceTest extends FunctionStorageConformanceTest {
    @Override
    FunctionStorage createStore() {
        return new SqliteFunctionStorage(":memory:");
    }
}
