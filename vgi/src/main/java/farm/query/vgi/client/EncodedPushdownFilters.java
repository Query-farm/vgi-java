// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import java.util.List;

/**
 * The two wire artefacts a pushdown-filter encode produces, which travel in
 * two different fields of the same {@code InitRequest}.
 *
 * <p>{@code pushdownFilters} is the filter batch itself (the JSON spec plus its
 * sibling constant columns) and belongs in {@code InitRequest.pushdown_filters}.
 * {@code joinKeys} holds one single-column batch per {@code join_keys}
 * predicate and belongs in {@code InitRequest.join_keys}; the worker matches
 * each batch back to its filter node <em>by column name</em>, so the two lists
 * must be sent together — a filter batch whose join-key batches were dropped
 * decodes to a filter the worker cannot resolve.
 *
 * @param pushdownFilters IPC bytes for {@code InitRequest.pushdown_filters}
 * @param joinKeys        IPC bytes for {@code InitRequest.join_keys}, one batch
 *                        per join-key predicate; empty when there are none
 */
public record EncodedPushdownFilters(byte[] pushdownFilters, List<byte[]> joinKeys) {}
