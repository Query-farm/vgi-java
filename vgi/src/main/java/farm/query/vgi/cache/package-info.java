// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Result-cache control metadata a table function advertises to the client.
 *
 * <p>The C++ extension caches the complete result of a table-function scan and
 * serves identical future scans without a worker round-trip. A worker opts a
 * result in by attaching {@code vgi.cache.*} keys to the {@code custom_metadata}
 * of the <em>first</em> batch it emits; see
 * {@link farm.query.vgi.cache.CacheControl}.
 *
 * <p>The reverse direction — the client asking the worker to confirm a stale
 * result is still fresh — arrives as {@link farm.query.vgi.cache.CacheControl#IF_NONE_MATCH_KEY}
 * on the first tick's input batch metadata.
 */
package farm.query.vgi.cache;
