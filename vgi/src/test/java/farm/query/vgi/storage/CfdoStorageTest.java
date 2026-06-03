// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link CfdoStorage} against an in-process JDK HttpServer that
 * implements the DO's unified state_* protocol. The mock REQUIRES a valid
 * shard_key on every request and a 32-hex attempt_id on every destructive op,
 * so a passing run proves the client both speaks the protocol and shards.
 */
class CfdoStorageTest {

    private static final String SHARD = "att-0123456789abcdef0123456789abcdef";
    private static final Pattern SHARD_RE = Pattern.compile("^[A-Za-z0-9._-]{1,128}$");
    private static final Pattern ATTEMPT_RE = Pattern.compile("^[0-9a-f]{32}$");
    private static final Gson GSON = new Gson();

    private HttpServer server;
    private MockDo mock;
    private CfdoStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        mock = new MockDo();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        for (String ep : new String[] {
            "state_append", "state_log_scan", "execution_clear", "state_get_many", "state_put_many"
        }) {
            server.createContext("/" + ep, mock);
        }
        server.start();
        String url = "http://127.0.0.1:" + server.getAddress().getPort();
        storage = new CfdoStorage(url, null).forShard(SHARD);
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    @Test
    void appendLogRoundTripsAndShards() {
        byte[] exec = "exec1".getBytes(StandardCharsets.UTF_8);
        byte[] ns = "buf".getBytes(StandardCharsets.UTF_8);
        byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        long o1 = storage.stateAppend(exec, ns, key, "a".getBytes(StandardCharsets.UTF_8));
        long o2 = storage.stateAppend(exec, ns, key, "b".getBytes(StandardCharsets.UTF_8));
        assertTrue(o1 < o2, "ordinals are monotonic");

        List<CfdoStorage.LogEntry> rows = storage.stateLogScan(exec, ns, key, -1, 0);
        assertEquals(2, rows.size());
        assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), rows.get(0).value());
        assertArrayEquals("b".getBytes(StandardCharsets.UTF_8), rows.get(1).value());

        // after_id cursor + limit.
        List<CfdoStorage.LogEntry> tail = storage.stateLogScan(exec, ns, key, o1, 1);
        assertEquals(1, tail.size());
        assertArrayEquals("b".getBytes(StandardCharsets.UTF_8), tail.get(0).value());

        // Every request carried our shard key; destructive ops carried attempt_ids.
        assertEquals(Set.of(SHARD), mock.shardKeys);
        assertEquals(0, mock.missingShard);
        assertEquals(0, mock.missingAttempt);
        assertTrue(mock.attemptIds.size() >= 2);
    }

    @Test
    void transactionPutGetAndClear() {
        byte[] txn = "txn1".getBytes(StandardCharsets.UTF_8);
        byte[] ns = "txn".getBytes(StandardCharsets.UTF_8);
        byte[] key = "watermark".getBytes(StandardCharsets.UTF_8);
        assertNull(storage.stateGet(txn, ns, key));
        storage.statePut(txn, ns, key, "42".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals("42".getBytes(StandardCharsets.UTF_8), storage.stateGet(txn, ns, key));
        storage.executionClear(txn);
        assertNull(storage.stateGet(txn, ns, key));
        assertEquals(0, mock.missingShard);
        assertEquals(0, mock.missingAttempt);
    }

    @Test
    void emptyShardKeyRejectedByServerContract() {
        CfdoStorage unsharded = new CfdoStorage("http://127.0.0.1:" + server.getAddress().getPort(), null);
        assertThrows(CfdoStorage.CfdoException.class,
                () -> unsharded.statePut("e".getBytes(), "n".getBytes(), "k".getBytes(), "v".getBytes()));
        assertTrue(mock.missingShard > 0);
    }

    // ------------------------------------------------------------------
    // In-memory mock Durable Object Worker.
    // ------------------------------------------------------------------
    private static final class MockDo implements com.sun.net.httpserver.HttpHandler {
        final Map<String, byte[]> kv = new HashMap<>();
        final Map<String, List<long[]>> logIds = new HashMap<>(); // composite -> list of [id]
        final Map<String, List<byte[]>> logVals = new HashMap<>();
        final Set<String> shardKeys = new HashSet<>();
        final List<String> attemptIds = new ArrayList<>();
        int missingShard = 0;
        int missingAttempt = 0;
        long seq = 0;

        private static String s(JsonObject b, String k) {
            return b.has(k) && !b.get(k).isJsonNull() ? b.get(k).getAsString() : "";
        }

        private static String composite(JsonObject b, String key) {
            return s(b, "shard_key") + "" + s(b, "scope_id") + "" + s(b, "ns") + "" + key;
        }

        @Override
        public void handle(HttpExchange ex) throws IOException {
            String endpoint = ex.getRequestURI().getPath().substring(1);
            String reqBody = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            JsonObject b = JsonParser.parseString(reqBody).getAsJsonObject();

            boolean destructive = Set.of("state_append", "execution_clear", "state_put_many").contains(endpoint);
            String sk = s(b, "shard_key");
            if (!SHARD_RE.matcher(sk).matches()) {
                missingShard++;
                respond(ex, 400, "{\"error\":\"bad_request\"}");
                return;
            }
            shardKeys.add(sk);
            if (destructive) {
                String aid = s(b, "attempt_id");
                if (!ATTEMPT_RE.matcher(aid).matches()) {
                    missingAttempt++;
                    respond(ex, 400, "{\"error\":\"bad_request\"}");
                    return;
                }
                attemptIds.add(aid);
            }

            JsonObject out = new JsonObject();
            switch (endpoint) {
                case "state_put_many" -> {
                    JsonArray items = b.getAsJsonArray("items");
                    for (var it : items) {
                        JsonObject im = it.getAsJsonObject();
                        kv.put(composite(b, im.get("key").getAsString()),
                                Base64.getDecoder().decode(im.get("value").getAsString()));
                    }
                    out.addProperty("written", items.size());
                }
                case "state_get_many" -> {
                    JsonArray rows = new JsonArray();
                    for (var k : b.getAsJsonArray("keys")) {
                        byte[] v = kv.get(composite(b, k.getAsString()));
                        if (v == null) {
                            rows.add(com.google.gson.JsonNull.INSTANCE);
                        } else {
                            JsonObject r = new JsonObject();
                            r.addProperty("value", Base64.getEncoder().encodeToString(v));
                            rows.add(r);
                        }
                    }
                    out.add("rows", rows);
                }
                case "state_append" -> {
                    seq++;
                    String c = composite(b, s(b, "key"));
                    logIds.computeIfAbsent(c, x -> new ArrayList<>()).add(new long[] {seq});
                    logVals.computeIfAbsent(c, x -> new ArrayList<>())
                            .add(Base64.getDecoder().decode(s(b, "item")));
                    out.addProperty("ordinal", seq);
                }
                case "state_log_scan" -> {
                    String c = composite(b, s(b, "key"));
                    long after = b.has("after_id") ? b.get("after_id").getAsLong() : -1;
                    int limit = b.has("limit") ? b.get("limit").getAsInt() : 0;
                    List<long[]> ids = logIds.getOrDefault(c, List.of());
                    List<byte[]> vals = logVals.getOrDefault(c, List.of());
                    JsonArray rows = new JsonArray();
                    for (int i = 0; i < ids.size(); i++) {
                        long id = ids.get(i)[0];
                        if (id <= after) {
                            continue;
                        }
                        JsonObject r = new JsonObject();
                        r.addProperty("id", id);
                        r.addProperty("value", Base64.getEncoder().encodeToString(vals.get(i)));
                        rows.add(r);
                        if (limit > 0 && rows.size() >= limit) {
                            break;
                        }
                    }
                    out.add("rows", rows);
                }
                case "execution_clear" -> {
                    String prefix = s(b, "shard_key") + "" + s(b, "scope_id") + "";
                    int deleted = 0;
                    for (var k : new ArrayList<>(kv.keySet())) {
                        if (k.startsWith(prefix)) {
                            kv.remove(k);
                            deleted++;
                        }
                    }
                    logIds.keySet().removeIf(k -> k.startsWith(prefix));
                    logVals.keySet().removeIf(k -> k.startsWith(prefix));
                    out.addProperty("deleted", deleted);
                }
                default -> {
                    respond(ex, 404, "{\"error\":\"not_found\"}");
                    return;
                }
            }
            respond(ex, 200, GSON.toJson(out));
        }

        private static void respond(HttpExchange ex, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(status, bytes.length);
            ex.getResponseBody().write(bytes);
            ex.close();
        }
    }
}
