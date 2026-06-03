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
 * <p>vgi-java's shared-state surface is the append-only log (table-buffering),
 * the transaction key/value store, and aggregate group state, so this client
 * maps those onto {@code state_append} / {@code state_log_scan} /
 * {@code execution_clear} and {@code state_get_many} / {@code state_put_many} /
 * {@code state_delete}. It is the distributed tier of {@link FunctionStorage}.
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
    public CfdoStorage forShard(String shard) {
        return new CfdoStorage(baseUrl, token, shard, client);
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
    public void stateDelete(byte[] scopeId, byte[] ns, List<byte[]> keys) {
        if (keys.isEmpty()) {
            return;
        }
        JsonObject body = scoped(scopeId, ns);
        JsonArray keyArr = new JsonArray();
        for (byte[] k : keys) {
            keyArr.add(b64(k));
        }
        body.add("keys", keyArr);
        body.addProperty("attempt_id", newAttemptId());
        post("state_delete", body);
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
