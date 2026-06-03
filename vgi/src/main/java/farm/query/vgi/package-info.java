// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Library entrypoint for the VGI (Vector Gateway Interface) protocol.
 *
 * <p>VGI is a DuckDB extension protocol in which an external Java worker serves
 * catalog data to DuckDB over Apache Arrow IPC. This package holds the worker
 * bootstrap, the RPC service contract, and the small registration records that
 * applications use to describe their catalog before serving requests.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.Worker} — builder + run-loop façade: register
 *       functions and catalog metadata, then serve over stdio / AF_UNIX / HTTP.</li>
 *   <li>{@link farm.query.vgi.VgiService} — the RPC surface a worker implements
 *       (bind/init, catalog enumeration, aggregate and buffering lifecycles).</li>
 *   <li>{@link farm.query.vgi.SettingSpec} — declares a custom DuckDB setting.</li>
 *   <li>{@link farm.query.vgi.AttachOptionSpec} — declares an ATTACH-time option.</li>
 *   <li>{@link farm.query.vgi.SecretTypeSpec} — declares a DuckDB secret type.</li>
 *   <li>{@link farm.query.vgi.CatalogDataVersionRelease} — one published
 *       data-version release in a catalog's timeline.</li>
 * </ul>
 */
package farm.query.vgi;
