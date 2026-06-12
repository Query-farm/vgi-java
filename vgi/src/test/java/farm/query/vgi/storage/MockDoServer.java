// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * In-process mock of the Cloudflare Durable Object Worker, implementing the
 * full unified state / counter / queue protocol. It REQUIRES a valid shard_key on
 * every request and a 32-hex attempt_id on every destructive op, shards all
 * state by it, keeps key/value rows in unsigned-lexicographic order, and pages
 * state_scan / state_drain at {@link #PAGE} rows per response so client-side
 * continuation loops are genuinely exercised. A drain snapshots the namespace
 * under its attempt_id on the first page (tombstone semantics): later pages
 * must carry the same attempt_id, and a repeated (attempt_id, after_key)
 * replays identically.
 */
final class MockDoServer implements com.sun.net.httpserver.HttpHandler, AutoCloseable {

    static final Pattern SHARD_RE = Pattern.compile("^[A-Za-z0-9._-]{1,128}$");
    static final Pattern ATTEMPT_RE = Pattern.compile("^[0-9a-f]{32}$");
    static final int PAGE = 2;

    private static final Gson GSON = new Gson();
    private static final Set<String> DESTRUCTIVE = Set.of(
            "state_append", "execution_clear", "state_put_many", "state_delete", "state_drain",
            "state_counter_add", "state_counter_set", "state_counter_delete",
            "queue_push", "queue_pop", "queue_clear");

    private final HttpServer server;

    // Per-(shard|scope|ns) key/value rows in unsigned key order.
    final Map<String, TreeMap<byte[], byte[]>> state = new HashMap<>();
    // Per-(shard|scope|ns|key) int64 counters.
    final Map<String, Long> counters = new HashMap<>();
    // Append log: composite (shard|scope|ns|key) -> parallel id/value lists.
    final Map<String, List<Long>> logIds = new HashMap<>();
    final Map<String, List<byte[]>> logVals = new HashMap<>();
    // Per-(shard|exec) FIFO queues.
    final Map<String, ArrayDeque<byte[]>> queues = new HashMap<>();
    // Drain snapshots keyed by attempt_id (tombstone replay).
    final Map<String, List<byte[][]>> drainSnapshots = new HashMap<>();

    final Set<String> shardKeys = new HashSet<>();
    final List<String> attemptIds = new ArrayList<>();
    int missingShard = 0;
    int missingAttempt = 0;
    int badDrainContinuation = 0;
    private long seq = 0;

    MockDoServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this);
        server.start();
    }

    String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private static String s(JsonObject b, String k) {
        return b.has(k) && !b.get(k).isJsonNull() ? b.get(k).getAsString() : "";
    }

    private static byte[] optB64(JsonObject b, String k) {
        return b.has(k) && !b.get(k).isJsonNull()
                ? Base64.getDecoder().decode(b.get(k).getAsString()) : null;
    }

    private static String b64(byte[] v) {
        return Base64.getEncoder().encodeToString(v);
    }

    private static String nsKey(JsonObject b) {
        return s(b, "shard_key") + "|" + s(b, "scope_id") + "|" + s(b, "ns");
    }

    private TreeMap<byte[], byte[]> rows(JsonObject b) {
        return state.computeIfAbsent(nsKey(b), x -> new TreeMap<>(Arrays::compareUnsigned));
    }

    @Override
    public synchronized void handle(HttpExchange ex) throws IOException {
        String endpoint = ex.getRequestURI().getPath().substring(1);
        String reqBody = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        JsonObject b = JsonParser.parseString(reqBody).getAsJsonObject();

        String sk = s(b, "shard_key");
        if (!SHARD_RE.matcher(sk).matches()) {
            missingShard++;
            respond(ex, 400, "{\"error\":\"bad_request\"}");
            return;
        }
        shardKeys.add(sk);
        if (DESTRUCTIVE.contains(endpoint)) {
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
                TreeMap<byte[], byte[]> m = rows(b);
                JsonArray items = b.getAsJsonArray("items");
                for (var it : items) {
                    JsonObject im = it.getAsJsonObject();
                    m.put(Base64.getDecoder().decode(im.get("key").getAsString()),
                            Base64.getDecoder().decode(im.get("value").getAsString()));
                }
                out.addProperty("written", items.size());
            }
            case "state_get_many" -> {
                TreeMap<byte[], byte[]> m = rows(b);
                JsonArray rowsOut = new JsonArray();
                for (var k : b.getAsJsonArray("keys")) {
                    byte[] v = m.get(Base64.getDecoder().decode(k.getAsString()));
                    if (v == null) {
                        rowsOut.add(JsonNull.INSTANCE);
                    } else {
                        JsonObject r = new JsonObject();
                        r.addProperty("value", b64(v));
                        rowsOut.add(r);
                    }
                }
                out.add("rows", rowsOut);
            }
            case "state_scan" -> {
                byte[] start = optB64(b, "start");
                byte[] end = optB64(b, "end");
                boolean reverse = b.has("reverse") && b.get("reverse").getAsBoolean();
                long limit = b.has("limit") ? b.get("limit").getAsLong() : -1;
                byte[] afterKey = optB64(b, "after_key");

                List<byte[][]> ordered = new ArrayList<>();
                var m = rows(b);
                var view = (start == null && end == null) ? m
                        : start == null ? m.headMap(end, false)
                        : end == null ? m.tailMap(start, true)
                        : m.subMap(start, true, end, false);
                for (var e2 : (reverse ? new TreeMap<>(view).descendingMap() : view).entrySet()) {
                    ordered.add(new byte[][] {e2.getKey(), e2.getValue()});
                }
                int from = 0;
                if (afterKey != null) {
                    for (int i = 0; i < ordered.size(); i++) {
                        if (Arrays.equals(ordered.get(i)[0], afterKey)) {
                            from = i + 1;
                            break;
                        }
                    }
                }
                long cap = limit >= 0 ? Math.min(PAGE, limit) : PAGE;
                JsonArray rowsOut = new JsonArray();
                int i = from;
                for (; i < ordered.size() && rowsOut.size() < cap; i++) {
                    JsonObject r = new JsonObject();
                    r.addProperty("key", b64(ordered.get(i)[0]));
                    r.addProperty("value", b64(ordered.get(i)[1]));
                    rowsOut.add(r);
                }
                out.add("rows", rowsOut);
                if (i < ordered.size() && rowsOut.size() > 0) {
                    out.addProperty("next_after", b64(ordered.get(i - 1)[0]));
                } else {
                    out.add("next_after", JsonNull.INSTANCE);
                }
            }
            case "state_drain" -> {
                String aid = s(b, "attempt_id");
                byte[] afterKey = optB64(b, "after_key");
                List<byte[][]> snapshot = drainSnapshots.get(aid);
                if (snapshot == null) {
                    if (afterKey != null) {
                        badDrainContinuation++;
                        respond(ex, 400, "{\"error\":\"unknown_drain_attempt\"}");
                        return;
                    }
                    snapshot = new ArrayList<>();
                    var m = rows(b);
                    for (var e2 : m.entrySet()) {
                        snapshot.add(new byte[][] {e2.getKey(), e2.getValue()});
                    }
                    m.clear();
                    drainSnapshots.put(aid, snapshot);
                }
                int from = 0;
                if (afterKey != null) {
                    for (int i = 0; i < snapshot.size(); i++) {
                        if (Arrays.equals(snapshot.get(i)[0], afterKey)) {
                            from = i + 1;
                            break;
                        }
                    }
                }
                JsonArray rowsOut = new JsonArray();
                int i = from;
                for (; i < snapshot.size() && rowsOut.size() < PAGE; i++) {
                    JsonObject r = new JsonObject();
                    r.addProperty("key", b64(snapshot.get(i)[0]));
                    r.addProperty("value", b64(snapshot.get(i)[1]));
                    rowsOut.add(r);
                }
                out.add("rows", rowsOut);
                if (i < snapshot.size()) {
                    out.addProperty("next_after", b64(snapshot.get(i - 1)[0]));
                } else {
                    out.add("next_after", JsonNull.INSTANCE);
                }
            }
            case "state_delete" -> {
                var m = rows(b);
                int deleted = 0;
                if (b.has("keys")) {
                    for (var k : b.getAsJsonArray("keys")) {
                        if (m.remove(Base64.getDecoder().decode(k.getAsString())) != null) {
                            deleted++;
                        }
                    }
                } else {
                    byte[] start = optB64(b, "start");
                    byte[] end = optB64(b, "end");
                    var view = (start == null && end == null) ? m
                            : start == null ? m.headMap(end, false)
                            : end == null ? m.tailMap(start, true)
                            : m.subMap(start, true, end, false);
                    deleted = view.size();
                    view.clear();
                }
                out.addProperty("deleted", deleted);
            }
            case "state_append" -> {
                seq++;
                String c = nsKey(b) + "|" + s(b, "key");
                logIds.computeIfAbsent(c, x -> new ArrayList<>()).add(seq);
                logVals.computeIfAbsent(c, x -> new ArrayList<>())
                        .add(Base64.getDecoder().decode(s(b, "item")));
                out.addProperty("ordinal", seq);
            }
            case "state_log_scan" -> {
                String c = nsKey(b) + "|" + s(b, "key");
                long after = b.has("after_id") ? b.get("after_id").getAsLong() : -1;
                int limit = b.has("limit") ? b.get("limit").getAsInt() : 0;
                List<Long> ids = logIds.getOrDefault(c, List.of());
                List<byte[]> vals = logVals.getOrDefault(c, List.of());
                JsonArray rowsOut = new JsonArray();
                for (int i = 0; i < ids.size(); i++) {
                    if (ids.get(i) <= after) {
                        continue;
                    }
                    JsonObject r = new JsonObject();
                    r.addProperty("id", ids.get(i));
                    r.addProperty("value", b64(vals.get(i)));
                    rowsOut.add(r);
                    if (limit > 0 && rowsOut.size() >= limit) {
                        break;
                    }
                }
                out.add("rows", rowsOut);
            }
            case "state_counter_get" -> {
                String c = nsKey(b) + "|" + s(b, "key");
                out.addProperty("n", counters.getOrDefault(c, 0L));
            }
            case "state_counter_add" -> {
                String c = nsKey(b) + "|" + s(b, "key");
                long n = counters.getOrDefault(c, 0L) + b.get("delta").getAsLong();
                counters.put(c, n);
                out.addProperty("n", n);
            }
            case "state_counter_set" -> {
                String c = nsKey(b) + "|" + s(b, "key");
                counters.put(c, b.get("value").getAsLong());
            }
            case "state_counter_delete" -> {
                counters.remove(nsKey(b) + "|" + s(b, "key"));
            }
            case "queue_push" -> {
                String q = s(b, "shard_key") + "|" + s(b, "execution_id");
                var dq = queues.computeIfAbsent(q, x -> new ArrayDeque<>());
                JsonArray items = b.getAsJsonArray("items");
                for (var it : items) {
                    dq.addLast(Base64.getDecoder().decode(it.getAsString()));
                }
                out.addProperty("count", items.size());
            }
            case "queue_pop" -> {
                String q = s(b, "shard_key") + "|" + s(b, "execution_id");
                var dq = queues.get(q);
                byte[] item = dq == null ? null : dq.pollFirst();
                if (item == null) {
                    out.add("item", JsonNull.INSTANCE);
                } else {
                    out.addProperty("item", b64(item));
                }
            }
            case "queue_clear" -> {
                String q = s(b, "shard_key") + "|" + s(b, "execution_id");
                var dq = queues.remove(q);
                out.addProperty("cleared", dq == null ? 0 : dq.size());
            }
            case "execution_clear" -> {
                String prefix = s(b, "shard_key") + "|" + s(b, "scope_id") + "|";
                int deleted = 0;
                for (var k : new ArrayList<>(state.keySet())) {
                    if (k.startsWith(prefix)) {
                        deleted += state.remove(k).size();
                    }
                }
                for (var k : new ArrayList<>(logIds.keySet())) {
                    if (k.startsWith(prefix)) {
                        deleted += logIds.remove(k).size();
                        logVals.remove(k);
                    }
                }
                for (var k : new ArrayList<>(counters.keySet())) {
                    if (k.startsWith(prefix)) {
                        counters.remove(k);
                        deleted++;
                    }
                }
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
