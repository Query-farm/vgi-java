// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

/**
 * Cloudflare Durable Object storage client, speaking the Worker's unified
 * {@code state_*} JSON+base64 protocol
 * (vgi-cloudflare-durable-object-storage/src/index.ts). Every request carries
 * the per-attach {@code shard_key} (set via {@link #forShard}); destructive ops
 * carry a fresh 32-hex {@code attempt_id}. Mirrors vgi-python / vgi-go.
 *
 * <p>Implements the full unified surface — composite-key K/V with ranged
 * scan/drain/delete, the append-only log, atomic int64 counters, and the FIFO
 * work queue. {@code state_scan} / {@code state_drain} page under a server-side
 * byte budget via an opaque {@code after_key}/{@code next_after} continuation
 * cursor; a drain mints ONE attempt_id and reuses it on every page so the
 * server's snapshot-then-page semantics stay atomic and replay-safe. It is the
 * distributed tier of {@link FunctionStorage}.
 */
public final class CfdoStorage implements FunctionStorage {

    private static final Gson GSON = new Gson();
    private static final Base64.Encoder B64E = Base64.getEncoder();
    private static final Base64.Decoder B64D = Base64.getDecoder();
    private static final HexFormat HEX = HexFormat.of();
    private static final SecureRandom RNG = new SecureRandom();

    private final String baseUrl;
    private final String token; // nullable
    private final String shardKey; // "" until pinned via forShard
    private final HttpClient client;

    /** Thrown on a non-2xx response from the Worker, or a transport error. */
    public static final class CfdoException extends RuntimeException {
        /**
         * Creates the exception with the Worker's error body or the transport
         * failure message.
         *
         * @param message the failure detail
         */
        public CfdoException(String message) {
            super(message);
        }
    }

    /**
     * Creates an unpinned client; call {@link #forShard} before issuing requests.
     *
     * @param baseUrl the Worker base URL (trailing slashes are stripped)
     * @param token   the optional bearer token; {@code null} or empty to omit
     */
    public CfdoStorage(String baseUrl, String token) {
        this(stripSlash(baseUrl), token, "", HttpClient.newHttpClient());
    }

    CfdoStorage(String baseUrl, String token, String shardKey, HttpClient client) {
        this.baseUrl = baseUrl;
        this.token = token;
        this.shardKey = shardKey == null ? "" : shardKey;
        this.client = client;
    }

    /**
     * Builds from {@code VGI_CF_DO_URL} / {@code VGI_CF_DO_TOKEN}.
     *
     * @return an unpinned client targeting the configured Worker
     * @throws IllegalStateException if {@code VGI_CF_DO_URL} is missing or empty
     */
    public static CfdoStorage fromEnv() {
        String url = System.getenv("VGI_CF_DO_URL");
        if (url == null || url.isEmpty()) {
            throw new IllegalStateException(
                    "VGI_CF_DO_URL is required when VGI_WORKER_SHARED_STORAGE=cloudflare-do");
        }
        return new CfdoStorage(url, System.getenv("VGI_CF_DO_TOKEN"));
    }

    /**
     * Returns a view pinned to one shard key, routing per logical ATTACH.
     *
     * @param shard the shard key (e.g. {@code att-<hex uuid>} from {@link ShardKey})
     * @return a new client that splices {@code shard} into every request
     */
    @Override
    public CfdoStorage forShard(String shard) {
        return new CfdoStorage(baseUrl, token, shard, client);
    }

    /**
     * The Durable Object routes on shard_key ({@code idFromName}) — unsharded
     * use is refused, so {@link BoundStorage} demands an attach identity.
     *
     * @return always {@code true}
     */
    @Override
    public boolean requiresShardKey() {
        return true;
    }

    // --- Append-only log (table-buffering) ---

    @Override
    public long stateAppend(byte[] scopeId, byte[] ns, byte[] key, byte[] value) {
        JsonObject body = scoped(scopeId, ns);
        body.addProperty("key", b64(key));
        body.addProperty("item", b64(value));
        body.addProperty("attempt_id", newAttemptId());
        return post("state_append", body).get("ordinal").getAsLong();
    }

    @Override
    public List<LogEntry> stateLogScan(byte[] scopeId, byte[] ns, byte[] key, long afterId, int limit) {
        JsonObject body = scoped(scopeId, ns);
        body.addProperty("key", b64(key));
        body.addProperty("after_id", afterId);
        if (limit > 0) {
            body.addProperty("limit", limit);
        }
        JsonObject resp = post("state_log_scan", body);
        List<LogEntry> out = new ArrayList<>();
        JsonArray rows = resp.getAsJsonArray("rows");
        if (rows != null) {
            for (JsonElement e : rows) {
                JsonObject r = e.getAsJsonObject();
                out.add(new LogEntry(r.get("id").getAsLong(), unb64(r.get("value").getAsString())));
            }
        }
        return out;
    }

    /** Wipe all state + log rows for a scope across every namespace. */
    @Override
    public int executionClear(byte[] scopeId) {
        JsonObject body = new JsonObject();
        body.addProperty("scope_id", b64(scopeId));
        body.addProperty("attempt_id", newAttemptId());
        return post("execution_clear", body).get("deleted").getAsInt();
    }

    // --- Key/value (scope = transaction_opaque_data or execution_id) ---

    @Override
    public List<byte[]> stateGetMany(byte[] scopeId, byte[] ns, List<byte[]> keys) {
        List<byte[]> out = new ArrayList<>(keys.size());
        if (keys.isEmpty()) {
            return out;
        }
        JsonObject body = scoped(scopeId, ns);
        JsonArray keyArr = new JsonArray();
        for (byte[] k : keys) {
            keyArr.add(b64(k));
        }
        body.add("keys", keyArr);
        JsonArray rows = post("state_get_many", body).getAsJsonArray("rows");
        for (int i = 0; i < keys.size(); i++) {
            JsonElement row = (rows != null && i < rows.size()) ? rows.get(i) : null;
            if (row == null || row.isJsonNull()) {
                out.add(null);
            } else {
                out.add(unb64(row.getAsJsonObject().get("value").getAsString()));
            }
        }
        return out;
    }

    @Override
    public void statePutMany(byte[] scopeId, byte[] ns, List<KV> items) {
        if (items.isEmpty()) {
            return;
        }
        JsonObject body = scoped(scopeId, ns);
        JsonArray arr = new JsonArray();
        for (KV kv : items) {
            JsonObject item = new JsonObject();
            item.addProperty("key", b64(kv.key()));
            item.addProperty("value", b64(kv.value()));
            arr.add(item);
        }
        body.add("items", arr);
        body.addProperty("attempt_id", newAttemptId());
        post("state_put_many", body);
    }

    @Override
    public int stateDelete(byte[] scopeId, byte[] ns, List<byte[]> keys) {
        if (keys.isEmpty()) {
            return 0;
        }
        JsonObject body = scoped(scopeId, ns);
        JsonArray keyArr = new JsonArray();
        for (byte[] k : keys) {
            keyArr.add(b64(k));
        }
        body.add("keys", keyArr);
        body.addProperty("attempt_id", newAttemptId());
        return post("state_delete", body).get("deleted").getAsInt();
    }

    @Override
    public int stateDeleteRange(byte[] scopeId, byte[] ns, byte[] start, byte[] end) {
        JsonObject body = scoped(scopeId, ns);
        if (start != null) {
            body.addProperty("start", b64(start));
        }
        if (end != null) {
            body.addProperty("end", b64(end));
        }
        body.addProperty("attempt_id", newAttemptId());
        return post("state_delete", body).get("deleted").getAsInt();
    }

    @Override
    public List<KV> stateScan(byte[] scopeId, byte[] ns, byte[] start, byte[] end,
                              boolean reverse, int limit) {
        List<KV> out = new ArrayList<>();
        String afterKey = null;
        long remaining = limit > 0 ? limit : -1;
        while (true) {
            JsonObject body = scoped(scopeId, ns);
            if (start != null) {
                body.addProperty("start", b64(start));
            }
            if (end != null) {
                body.addProperty("end", b64(end));
            }
            if (reverse) {
                body.addProperty("reverse", true);
            }
            if (remaining >= 0) {
                body.addProperty("limit", remaining);
            }
            if (afterKey != null) {
                body.addProperty("after_key", afterKey);
            }
            JsonObject data = post("state_scan", body);
            JsonArray rows = data.getAsJsonArray("rows");
            if (rows != null) {
                for (JsonElement e : rows) {
                    JsonObject r = e.getAsJsonObject();
                    out.add(new KV(unb64(r.get("key").getAsString()), unb64(r.get("value").getAsString())));
                    if (remaining > 0) {
                        remaining--;
                    }
                }
            }
            afterKey = optString(data, "next_after");
            if (afterKey == null || afterKey.isEmpty() || remaining == 0) {
                break;
            }
        }
        return out;
    }

    @Override
    public List<KV> stateDrain(byte[] scopeId, byte[] ns) {
        // One attempt_id for every page: the first page tombstones the whole
        // namespace server-side, later pages read that snapshot, and a retried
        // (attempt_id, after_key) replays identically.
        String attemptId = newAttemptId();
        List<KV> out = new ArrayList<>();
        String afterKey = null;
        while (true) {
            JsonObject body = scoped(scopeId, ns);
            if (afterKey != null) {
                body.addProperty("after_key", afterKey);
            }
            body.addProperty("attempt_id", attemptId);
            JsonObject data = post("state_drain", body);
            JsonArray rows = data.getAsJsonArray("rows");
            if (rows != null) {
                for (JsonElement e : rows) {
                    JsonObject r = e.getAsJsonObject();
                    out.add(new KV(unb64(r.get("key").getAsString()), unb64(r.get("value").getAsString())));
                }
            }
            afterKey = optString(data, "next_after");
            if (afterKey == null || afterKey.isEmpty()) {
                break;
            }
        }
        return out;
    }

    // --- Atomic int64 counters (function_counter) ---

    @Override
    public long stateCounterGet(byte[] scopeId, byte[] ns, byte[] key) {
        JsonObject body = scoped(scopeId, ns);
        body.addProperty("key", b64(key));
        return post("state_counter_get", body).get("n").getAsLong();
    }

    @Override
    public long stateCounterAdd(byte[] scopeId, byte[] ns, byte[] key, long delta) {
        JsonObject body = scoped(scopeId, ns);
        body.addProperty("key", b64(key));
        body.addProperty("delta", delta);
        body.addProperty("attempt_id", newAttemptId());
        return post("state_counter_add", body).get("n").getAsLong();
    }

    @Override
    public void stateCounterSet(byte[] scopeId, byte[] ns, byte[] key, long value) {
        JsonObject body = scoped(scopeId, ns);
        body.addProperty("key", b64(key));
        body.addProperty("value", value);
        body.addProperty("attempt_id", newAttemptId());
        post("state_counter_set", body);
    }

    @Override
    public void stateCounterDelete(byte[] scopeId, byte[] ns, byte[] key) {
        JsonObject body = scoped(scopeId, ns);
        body.addProperty("key", b64(key));
        body.addProperty("attempt_id", newAttemptId());
        post("state_counter_delete", body);
    }

    // --- Work queue (FIFO, destructive pop) ---

    @Override
    public int queuePush(byte[] executionId, List<byte[]> items) {
        JsonObject body = new JsonObject();
        body.addProperty("execution_id", b64(executionId));
        JsonArray arr = new JsonArray();
        for (byte[] item : items) {
            arr.add(b64(item));
        }
        body.add("items", arr);
        body.addProperty("attempt_id", newAttemptId());
        return post("queue_push", body).get("count").getAsInt();
    }

    @Override
    public byte[] queuePop(byte[] executionId) {
        JsonObject body = new JsonObject();
        body.addProperty("execution_id", b64(executionId));
        body.addProperty("attempt_id", newAttemptId());
        JsonElement item = post("queue_pop", body).get("item");
        return (item == null || item.isJsonNull() || item.getAsString().isEmpty())
                ? null : unb64(item.getAsString());
    }

    @Override
    public int queueClear(byte[] executionId) {
        JsonObject body = new JsonObject();
        body.addProperty("execution_id", b64(executionId));
        body.addProperty("attempt_id", newAttemptId());
        return post("queue_clear", body).get("cleared").getAsInt();
    }

    // --- HTTP plumbing ---

    private JsonObject scoped(byte[] scopeId, byte[] ns) {
        JsonObject b = new JsonObject();
        b.addProperty("scope_id", b64(scopeId));
        b.addProperty("ns", b64(ns));
        return b;
    }

    private JsonObject post(String endpoint, JsonObject body) {
        // The Worker routes on shard_key (idFromName) and rejects requests
        // without one — always splice it in.
        body.addProperty("shard_key", shardKey);
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(baseUrl + "/" + endpoint))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(GSON.toJson(body)));
        if (token != null && !token.isEmpty()) {
            rb.header("Authorization", "Bearer " + token);
        }
        HttpResponse<String> resp;
        try {
            resp = client.send(rb.build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new CfdoException("cfdo: " + endpoint + " transport error: " + e.getMessage());
        }
        int sc = resp.statusCode();
        JsonObject data;
        try {
            data = JsonParser.parseString(resp.body()).getAsJsonObject();
        } catch (Exception parseErr) {
            if (sc >= 200 && sc < 300) {
                return new JsonObject();
            }
            throw new CfdoException("cfdo: " + endpoint + " error " + sc + ": <non-json body>");
        }
        if (sc < 200 || sc >= 300) {
            throw new CfdoException("cfdo: " + endpoint + " error " + sc + ": " + data);
        }
        return data;
    }

    private static String optString(JsonObject obj, String field) {
        JsonElement e = obj.get(field);
        return (e == null || e.isJsonNull()) ? null : e.getAsString();
    }

    /** Fresh 32-char lowercase-hex idempotency token (the DO's attempt_id shape). */
    static String newAttemptId() {
        byte[] b = new byte[16];
        RNG.nextBytes(b);
        return HEX.formatHex(b);
    }

    private static String b64(byte[] bytes) {
        return B64E.encodeToString(bytes);
    }

    private static byte[] unb64(String s) {
        return B64D.decode(s);
    }

    private static String stripSlash(String url) {
        int end = url.length();
        while (end > 0 && url.charAt(end - 1) == '/') {
            end--;
        }
        return url.substring(0, end);
    }
}
