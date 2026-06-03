// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Shared-state storage backends for cross-process and distributed worker state.
 *
 * <p>VGI workers persist append-only logs (table-buffering), transaction
 * key/value state, and aggregate group state outside any single request. This
 * package unifies those needs behind one interface with three deployment tiers
 * — in-process SQLite, file-backed cross-process SQLite, and a distributed
 * Cloudflare Durable Object — selected at startup from the environment.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.storage.FunctionStorage} — the unified shared-state
 *       surface ({@code state_*} log + key/value ops) implemented by every tier.</li>
 *   <li>{@link farm.query.vgi.storage.StorageResolver} — picks the tier at startup
 *       from {@code VGI_WORKER_SHARED_STORAGE}.</li>
 *   <li>{@link farm.query.vgi.storage.SqliteFunctionStorage} — the in-process and
 *       file-backed cross-process tiers.</li>
 *   <li>{@link farm.query.vgi.storage.CfdoStorage} — the distributed Cloudflare
 *       Durable Object client tier.</li>
 *   <li>{@link farm.query.vgi.storage.ShardKey} — derives the per-attach Durable
 *       Object routing key.</li>
 * </ul>
 */
package farm.query.vgi.storage;
