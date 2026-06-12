// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

/** {@link BoundStorageConformanceTest} over the in-process {@code :memory:} tier. */
class SqliteMemoryBoundConformanceTest extends BoundStorageConformanceTest {
    @Override
    FunctionStorage createBackend() {
        return new SqliteFunctionStorage(":memory:");
    }
}
