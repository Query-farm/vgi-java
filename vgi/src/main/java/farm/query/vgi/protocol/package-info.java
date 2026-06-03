// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Wire DTOs for the VGI protocol — Arrow-IPC record types for request and response messages.
 *
 * <p>Each record in this package maps one-for-one to a VGI wire message and is serialised
 * via {@link farm.query.vgirpc.schema.ArrowSerializableRecord} into the byte-compatible
 * Arrow layout shared with the Python (canonical) and Go reference implementations.
 * Record component names equal the wire field names verbatim (hence the {@code snake_case}
 * components); nested Arrow batches and schemas travel as opaque {@code byte[]} IPC blobs.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.protocol.CatalogAttachRequest} /
 *       {@link farm.query.vgi.protocol.CatalogAttachResult} — catalog attach handshake.</li>
 *   <li>{@link farm.query.vgi.protocol.BindRequest} /
 *       {@link farm.query.vgi.protocol.BindResponse} — table/function bind, including the
 *       two-phase secret-resolution exchange.</li>
 *   <li>{@link farm.query.vgi.protocol.CardinalityResponse} — optimizer cardinality hints.</li>
 *   <li>{@link farm.query.vgi.protocol.AggregateBindRequest} and the other
 *       {@code Aggregate*} records — the aggregate function lifecycle
 *       (bind / update / combine / finalize / destructor).</li>
 * </ul>
 */
package farm.query.vgi.protocol;
