// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

/**
 * The attach an {@link AttachHook} or a {@link TransactionHooks} call belongs to.
 *
 * @param catalogName the catalog name the {@code ATTACH} named
 * @param attachId    the plain attach id: the bytes functions see as {@code attachId()} and
 *                    the shard key of the worker's shared storage (never the sealed wire value)
 * @param options     the attach's option values, secret ones included
 */
public record AttachContext(String catalogName, byte[] attachId, AttachOptions options) {}
