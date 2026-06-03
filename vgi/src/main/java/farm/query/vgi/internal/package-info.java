// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Implementation details of the VGI worker — serializers, codecs, dispatch, and stores.
 *
 * <p>This package is <strong>not</strong> public API and is subject to change
 * without notice. It holds the machinery that turns registered fixtures into
 * the byte-compatible Arrow IPC wire shapes the DuckDB extension expects:
 * the {@code *InfoSerializer} / {@code *Serializer} family that hand-builds the
 * one-row metadata batches, the argument/schema decoders, and the per-execution
 * state stores that back stateful function lifecycles.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.internal.IpcStructBuilder} — shared helpers for
 *       building one-row IPC record batches (fields, value writers, dict registration).</li>
 *   <li>{@link farm.query.vgi.internal.FunctionInfoSerializer} — encodes a
 *       {@code FunctionInfo} into the dict-encoded wire batch the catalog enumeration reads.</li>
 *   <li>{@link farm.query.vgi.internal.ArgumentsParser} — decodes
 *       {@code BindRequest.arguments} IPC bytes into a typed {@code Arguments}.</li>
 *   <li>{@link farm.query.vgi.internal.AggregateRunner} — drives the
 *       aggregate bind/update/combine/finalize/destructor lifecycle.</li>
 *   <li>{@link farm.query.vgi.internal.CatalogRegistry} — per-attach catalog
 *       state and time-travel version resolution.</li>
 * </ul>
 */
package farm.query.vgi.internal;
