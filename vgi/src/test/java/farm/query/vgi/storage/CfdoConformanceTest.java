// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

/**
 * {@link FunctionStorageConformanceTest} over the distributed tier, against the
 * in-process {@link MockDoServer} (page size 2 — the multi-row cases exercise
 * the client's continuation loops).
 */
class CfdoConformanceTest extends FunctionStorageConformanceTest {

    static final String SHARD = "att-0123456789abcdef0123456789abcdef";

    private MockDoServer mock;

    @Override
    FunctionStorage createStore() throws Exception {
        mock = new MockDoServer();
        return new CfdoStorage(mock.baseUrl(), null).forShard(SHARD);
    }

    @Override
    void afterStoreClosed() {
        if (mock != null) {
            mock.close();
        }
    }
}
