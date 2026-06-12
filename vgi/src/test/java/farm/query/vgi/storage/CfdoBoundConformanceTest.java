// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * {@link BoundStorageConformanceTest} over the distributed tier (against
 * {@link MockDoServer}), plus the shard-routing behaviors only this tier has:
 * per-attach isolation and the no-attach hard error.
 */
class CfdoBoundConformanceTest extends BoundStorageConformanceTest {

    private MockDoServer mock;

    @Override
    FunctionStorage createBackend() throws Exception {
        mock = new MockDoServer();
        return new CfdoStorage(mock.baseUrl(), null);
    }

    @Override
    void afterBackendClosed() {
        if (mock != null) {
            mock.close();
        }
    }

    @Test
    void shardIsolationViaAttachPlaintext() {
        byte[] exec = b("same-exec");
        BoundStorage attachA = new BoundStorage(backend, exec, attach((byte) 0x01));
        BoundStorage attachB = new BoundStorage(backend, exec, attach((byte) 0x02));
        BoundStorage attachA2 = new BoundStorage(backend, exec, attach((byte) 0x01));

        attachA.statePut(b("ns"), b("k"), b("v-a"));
        assertNull(attachB.stateGet(b("ns"), b("k")), "different attach → different shard");
        assertArrayEquals(b("v-a"), attachA2.stateGet(b("ns"), b("k")), "same attach → same shard");
    }

    @Test
    void cfdoWithoutAttachThrows() {
        assertThrows(IllegalStateException.class,
                () -> new BoundStorage(backend, b("exec"), null));
    }
}
