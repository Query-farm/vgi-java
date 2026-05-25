// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Exercises {@link CfdoStorage} against a REAL Cloudflare Worker + Durable
 * Object over HTTP (a local {@code wrangler dev} or deployed instance) — proves
 * the client speaks the actual protocol, not just the in-process mock. Runs
 * only when VGI_CF_DO_INTEGRATION_URL is set; VGI_CF_DO_TOKEN supplies the key.
 */
@EnabledIfEnvironmentVariable(named = "VGI_CF_DO_INTEGRATION_URL", matches = ".+")
class CfdoStorageLiveTest {

    private static CfdoStorage live() {
        byte[] u = new byte[16];
        new SecureRandom().nextBytes(u);
        String shard = "att-" + HexFormat.of().formatHex(u); // fresh shard isolates this run
        return new CfdoStorage(System.getenv("VGI_CF_DO_INTEGRATION_URL"), System.getenv("VGI_CF_DO_TOKEN"))
                .forShard(shard);
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void appendLogRoundTrips() {
        CfdoStorage s = live();
        byte[] exec = b("exec-log");
        byte[] ns = b("buf");
        byte[] key = b("k");
        long o1 = s.stateAppend(exec, ns, key, b("a"));
        long o2 = s.stateAppend(exec, ns, key, b("b"));
        assertTrue(o1 < o2);
        List<CfdoStorage.LogEntry> rows = s.stateLogScan(exec, ns, key, -1, 0);
        assertEquals(2, rows.size());
        assertArrayEquals(b("a"), rows.get(0).value());
        assertArrayEquals(b("b"), rows.get(1).value());
        assertEquals(1, s.stateLogScan(exec, ns, key, o1, 1).size());
    }

    @Test
    void transactionPutGetAndClear() {
        CfdoStorage s = live();
        byte[] txn = b("txn-live");
        byte[] ns = b("txn");
        byte[] key = b("watermark");
        assertNull(s.stateGet(txn, ns, key));
        s.statePut(txn, ns, key, b("42"));
        assertArrayEquals(b("42"), s.stateGet(txn, ns, key));
        s.executionClear(txn);
        assertNull(s.stateGet(txn, ns, key));
    }
}
