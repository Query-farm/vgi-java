// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Result-cache control metadata ({@code vgi.cache.*}) advertised by a table
 * function on the <em>first</em> data batch it emits.
 *
 * <p>The vocabulary mirrors HTTP caching (RFC 9111/9110): a freshness lifetime
 * ({@link Builder#ttl}/{@link Builder#expires}), a reuse {@link Builder#scope},
 * validators ({@link Builder#etag} / {@link Builder#lastModified}) for
 * conditional revalidation, and stale-serving grace windows. Presence of
 * {@code ttl} <em>or</em> {@code expires} is what makes a result cacheable;
 * {@link Builder#noStore} overrides any freshness key.
 *
 * <p>The key strings are the single source of truth shared with the C++
 * extension, which reads them by string off each batch's {@code custom_metadata}.
 * Render them with {@link #toMetadata()} and hand the map to
 * {@code OutputCollector.emit(root, metadata)}:
 *
 * <pre>{@code
 * BatchUtil.emit(schema, rows, out, CacheControl.ttl(300).toMetadata(), filler);
 * }</pre>
 *
 * <p>Booleans render as {@code "1"} and are omitted when false; unset optional
 * fields are omitted entirely. {@code scope} is always emitted. Mirrors
 * vgi-python's {@code vgi/cache_control.py}.
 */
public final class CacheControl {

    /** Freshness lifetime in whole seconds, relative to full-result receipt. */
    public static final String TTL_KEY = "vgi.cache.ttl";
    /** Absolute RFC 3339 UTC freshness deadline. */
    public static final String EXPIRES_KEY = "vgi.cache.expires";
    /** Explicit "never cache"; overrides any freshness key. */
    public static final String NO_STORE_KEY = "vgi.cache.no_store";
    /** Reuse scope — {@link #SCOPE_CATALOG} or {@link #SCOPE_TRANSACTION}. */
    public static final String SCOPE_KEY = "vgi.cache.scope";
    /** Strong validator (opaque quoted string) for conditional revalidation. */
    public static final String ETAG_KEY = "vgi.cache.etag";
    /** Weaker RFC 3339 UTC validator; fallback when no ETag. */
    public static final String LAST_MODIFIED_KEY = "vgi.cache.last_modified";
    /** The worker can check freshness cheaply without recomputing. */
    public static final String REVALIDATABLE_KEY = "vgi.cache.revalidatable";
    /** Grace window (seconds) to serve stale while revalidating in the background. */
    public static final String STALE_WHILE_REVALIDATE_KEY = "vgi.cache.stale_while_revalidate";
    /** Grace window (seconds) to serve stale if a revalidation RPC fails. */
    public static final String STALE_IF_ERROR_KEY = "vgi.cache.stale_if_error";
    /** 304-equivalent, set on a 0-row batch replying to a conditional request. */
    public static final String NOT_MODIFIED_KEY = "vgi.cache.not_modified";
    /**
     * Opt in to per-partition caching: the client ALSO stores the result split by
     * partition value. Only meaningful on a {@code SINGLE_VALUE_PARTITIONS} function.
     */
    public static final String PARTITION_SCOPE_KEY = "vgi.cache.partition_scope";
    /**
     * Opt in to per-<em>value</em> memoization: the client ALSO memoizes each
     * distinct input tuple's output. Only meaningful for an exchange-mode map
     * (a scalar, or a blended table-in-out called via correlated {@code LATERAL}).
     * Default OFF — see {@link Builder#perValue(boolean)} for why.
     */
    public static final String PER_VALUE_KEY = "vgi.cache.per_value";

    /**
     * Request-side key: the client's stored ETag, delivered on the first tick's
     * input {@code custom_metadata} when it asks the worker to confirm freshness.
     */
    public static final String IF_NONE_MATCH_KEY = "vgi.cache.if_none_match";
    /** Request-side key: the client's stored Last-Modified validator. */
    public static final String IF_MODIFIED_SINCE_KEY = "vgi.cache.if_modified_since";

    /** Reusable across transactions within the calling catalog identity (default). */
    public static final String SCOPE_CATALOG = "catalog";
    /** Reusable only within the transaction that produced it. */
    public static final String SCOPE_TRANSACTION = "transaction";

    private static final Set<String> VALID_SCOPES = Set.of(SCOPE_CATALOG, SCOPE_TRANSACTION);

    private final Integer ttl;
    private final String expires;
    private final String scope;
    private final boolean noStore;
    private final String etag;
    private final String lastModified;
    private final boolean revalidatable;
    private final Integer staleWhileRevalidate;
    private final Integer staleIfError;
    private final boolean notModified;
    private final boolean partitionScope;
    private final boolean perValue;

    private CacheControl(Builder b) {
        if (!VALID_SCOPES.contains(b.scope)) {
            throw new IllegalArgumentException(
                    "CacheControl.scope must be one of " + VALID_SCOPES + ", got " + b.scope);
        }
        requireNonNegative("ttl", b.ttl);
        requireNonNegative("stale_while_revalidate", b.staleWhileRevalidate);
        requireNonNegative("stale_if_error", b.staleIfError);
        this.ttl = b.ttl;
        this.expires = b.expires;
        this.scope = b.scope;
        this.noStore = b.noStore;
        this.etag = b.etag;
        this.lastModified = b.lastModified;
        this.revalidatable = b.revalidatable;
        this.staleWhileRevalidate = b.staleWhileRevalidate;
        this.staleIfError = b.staleIfError;
        this.notModified = b.notModified;
        this.partitionScope = b.partitionScope;
        this.perValue = b.perValue;
    }

    private static void requireNonNegative(String name, Integer value) {
        if (value != null && value < 0) {
            throw new IllegalArgumentException("CacheControl." + name + " must be >= 0, got " + value);
        }
    }

    /**
     * A fresh builder with no freshness keys and {@link #SCOPE_CATALOG}.
     *
     * @return a new builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Shorthand for the common case: a catalog-scoped result with a freshness
     * lifetime and nothing else.
     *
     * @param seconds freshness lifetime in whole seconds
     * @return a cache control advertising only {@code ttl}
     */
    public static CacheControl ttl(int seconds) { return builder().ttl(seconds).build(); }

    /**
     * Shorthand for a result the client must never store.
     *
     * @return a cache control advertising only {@code no_store}
     */
    public static CacheControl noStore() { return builder().noStore(true).build(); }

    /**
     * Render to the {@code vgi.cache.*} batch-metadata keys.
     *
     * @return a mutable, insertion-ordered map ready to pass to
     *     {@code OutputCollector.emit(root, metadata)}
     */
    public Map<String, String> toMetadata() {
        Map<String, String> md = new LinkedHashMap<>();
        if (ttl != null) md.put(TTL_KEY, Integer.toString(ttl));
        if (expires != null) md.put(EXPIRES_KEY, expires);
        if (noStore) md.put(NO_STORE_KEY, "1");
        md.put(SCOPE_KEY, scope);
        if (etag != null) md.put(ETAG_KEY, etag);
        if (lastModified != null) md.put(LAST_MODIFIED_KEY, lastModified);
        if (revalidatable) md.put(REVALIDATABLE_KEY, "1");
        if (staleWhileRevalidate != null) md.put(STALE_WHILE_REVALIDATE_KEY, Integer.toString(staleWhileRevalidate));
        if (staleIfError != null) md.put(STALE_IF_ERROR_KEY, Integer.toString(staleIfError));
        if (notModified) md.put(NOT_MODIFIED_KEY, "1");
        if (partitionScope) md.put(PARTITION_SCOPE_KEY, "1");
        if (perValue) md.put(PER_VALUE_KEY, "1");
        return md;
    }

    /**
     * Fold a cache control into an existing emit-metadata map. The rendered
     * cache keys win on collision (last write), matching vgi-python's
     * {@code _merge_cache_control}.
     *
     * @param metadata     the caller's metadata, or {@code null}
     * @param cacheControl the cache control to fold in, or {@code null} for a
     *     non-first batch (returns {@code metadata} unchanged)
     * @return the merged map, or {@code metadata} when {@code cacheControl} is null
     */
    public static Map<String, String> merge(Map<String, String> metadata, CacheControl cacheControl) {
        if (cacheControl == null) return metadata;
        Map<String, String> merged = metadata == null ? new LinkedHashMap<>() : new LinkedHashMap<>(metadata);
        merged.putAll(cacheControl.toMetadata());
        return merged;
    }

    /** Builder for {@link CacheControl}; see the class javadoc for field semantics. */
    public static final class Builder {
        private Integer ttl;
        private String expires;
        private String scope = SCOPE_CATALOG;
        private boolean noStore;
        private String etag;
        private String lastModified;
        private boolean revalidatable;
        private Integer staleWhileRevalidate;
        private Integer staleIfError;
        private boolean notModified;
        private boolean partitionScope;
        private boolean perValue;

        private Builder() {}

        /**
         * Freshness lifetime in whole seconds, relative to full-result receipt.
         *
         * @param seconds the lifetime; must be non-negative
         * @return this builder
         */
        public Builder ttl(int seconds) { this.ttl = seconds; return this; }

        /**
         * Absolute RFC 3339 UTC deadline; the lifetime is {@code expires - now}.
         *
         * @param rfc3339Utc the deadline
         * @return this builder
         */
        public Builder expires(String rfc3339Utc) { this.expires = rfc3339Utc; return this; }

        /**
         * Reuse scope.
         *
         * @param scope {@link #SCOPE_CATALOG} or {@link #SCOPE_TRANSACTION}
         * @return this builder
         */
        public Builder scope(String scope) { this.scope = scope; return this; }

        /**
         * Explicit "never cache"; overrides any freshness key.
         *
         * @param noStore whether the client must not store the result
         * @return this builder
         */
        public Builder noStore(boolean noStore) { this.noStore = noStore; return this; }

        /**
         * Strong validator for conditional revalidation.
         *
         * @param etag an opaque quoted string, e.g. {@code "\"v1\""}
         * @return this builder
         */
        public Builder etag(String etag) { this.etag = etag; return this; }

        /**
         * Weaker RFC 3339 UTC validator; fallback when no ETag is advertised.
         *
         * @param rfc3339Utc the last-modified instant
         * @return this builder
         */
        public Builder lastModified(String rfc3339Utc) { this.lastModified = rfc3339Utc; return this; }

        /**
         * Whether the worker can confirm freshness cheaply without recomputing;
         * gates whether the client ever sends a conditional request.
         *
         * @param revalidatable the flag
         * @return this builder
         */
        public Builder revalidatable(boolean revalidatable) { this.revalidatable = revalidatable; return this; }

        /**
         * Grace window to serve stale while revalidating in the background.
         *
         * @param seconds the window; must be non-negative
         * @return this builder
         */
        public Builder staleWhileRevalidate(int seconds) { this.staleWhileRevalidate = seconds; return this; }

        /**
         * Grace window to serve stale if a revalidation RPC fails.
         *
         * @param seconds the window; must be non-negative
         * @return this builder
         */
        public Builder staleIfError(int seconds) { this.staleIfError = seconds; return this; }

        /**
         * 304-equivalent: set on a 0-row batch replying to a conditional request
         * to assert the client's stored payload is still fresh.
         *
         * @param notModified the flag
         * @return this builder
         */
        public Builder notModified(boolean notModified) { this.notModified = notModified; return this; }

        /**
         * Opt in to per-partition caching: on a {@code SINGLE_VALUE_PARTITIONS}
         * function the client ADDITIONALLY stores the result split by partition
         * value, so a later {@code =}/{@code IN} scan on the partition column(s)
         * serves the requested partitions without calling the worker. Additive —
         * the whole-scan entry is still stored and served.
         *
         * @param partitionScope the flag
         * @return this builder
         */
        public Builder partitionScope(boolean partitionScope) {
            this.partitionScope = partitionScope; return this;
        }

        /**
         * Opt in to per-<em>value</em> memoization: the client ADDITIONALLY
         * memoizes each distinct worker-input tuple's output, keyed on that
         * tuple, so the same value is served without calling the worker on a
         * later chunk or a later query. Only meaningful for an exchange-mode
         * map — a scalar, or a blended table-in-out invoked through a
         * correlated {@code LATERAL}.
         *
         * <p><strong>Defaults off, and should stay off unless one call is
         * genuinely expensive.</strong> A per-value serve is not free: it costs
         * a key probe, a decode of the stored value and a row-assembly step,
         * for every distinct value. That only pays back when it is cheaper than
         * simply asking the worker. For a cheap arithmetic map it is a large net
         * loss — the probe-and-assemble path measures roughly 50x the cost of
         * the worker call it replaces. The engine cannot tell an expensive call
         * from a cheap one, which is why this is an explicit advertisement
         * rather than a heuristic: only the function author knows. Turn it on
         * for a model inference, a geocode, or a rate-limited remote fetch,
         * where one call dwarfs a cache probe.
         *
         * <p>Independent of whole-result cacheability: a function can be worth
         * caching by {@code ttl} without per-value memoization paying off, and
         * vice versa.
         *
         * @param perValue the flag
         * @return this builder
         */
        public Builder perValue(boolean perValue) {
            this.perValue = perValue; return this;
        }

        /**
         * Validate and freeze.
         *
         * @return the immutable cache control
         * @throws IllegalArgumentException on an unknown scope or a negative duration
         */
        public CacheControl build() { return new CacheControl(this); }
    }
}
