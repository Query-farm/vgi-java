// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Mirrors the C++ {@code FunctionInfoSchema}. Serialised as an item in
 * {@code catalog_schema_contents_functions}.
 *
 * <p>Field order, types, and nullability are part of the wire contract.
 *
 * @param comment                      optional function comment, or {@code null}.
 * @param tags                         arbitrary key/value metadata tags.
 * @param name                         function name.
 * @param schema_name                  owning schema name.
 * @param function_type                dictionary-encoded function kind (e.g. {@code "scalar"},
 *                                     {@code "table"}, {@code "table_in_out"}, {@code "table_buffering"}).
 * @param arguments                    IPC-encoded argument-spec batch.
 * @param output_schema                IPC-encoded output schema.
 * @param stability                    dictionary-encoded function stability, or {@code null}.
 * @param null_handling                dictionary-encoded null-handling policy, or {@code null}.
 * @param description                  human-readable description.
 * @param examples                     usage examples.
 * @param categories                   classification categories.
 * @param projection_pushdown          whether the function supports projection pushdown, or {@code null}.
 * @param filter_pushdown              whether the function supports filter pushdown, or {@code null}.
 * @param sampling_pushdown            whether the function supports sampling pushdown, or {@code null}.
 * @param late_materialization         whether the function supports late materialization, or {@code null}.
 * @param supported_expression_filters expression-filter kinds the function can accept.
 * @param order_preservation           dictionary-encoded order-preservation guarantee, or {@code null}.
 * @param max_workers                  maximum parallel worker count, or {@code null}
 *                                     when the function declares none. Boxed on purpose:
 *                                     the field's <em>schema</em> is non-nullable (that is
 *                                     the registered wire contract), but vgi-python writes
 *                                     a null row value into it for a function with no
 *                                     declared limit, and the C++ extension reads it as an
 *                                     {@code optional}. A primitive {@code int} here cannot
 *                                     represent what the reference implementation actually
 *                                     sends. Same schema-vs-row-null wart as
 *                                     {@code TableInfo.cardinality_estimate}.
 * @param supports_batch_index         whether per-batch index tagging is supported.
 * @param supports_splits              whether the scan divides into named,
 *        independently redeemable splits. A distributed engine reads this to
 *        decide whether it can retry a task: a split NAMES its work, so
 *        re-running one reads exactly the same rows.
 * @param filters_exactly_applied      whether the worker applies every pushed
 *        filter EXACTLY, letting the engine drop its own copy. Wrong answers if
 *        declared falsely.
 * @param supports_positions           whether the data has addressable
 *        positions, for incremental / streaming reads.
 * @param split_token_ttl_seconds      how long a split token stays redeemable.
 *        {@code null} means UNBOUNDED, not "expires immediately" — a client must
 *        not assume a TTL exists, or long-running streams are foreclosed.
 * @param partition_kind               dictionary-encoded partitioning behaviour.
 * @param order_dependent              dictionary-encoded order-dependence.
 * @param distinct_dependent           dictionary-encoded distinct-dependence.
 * @param supports_window              whether the function supports windowed execution.
 * @param streaming_partitioned        whether the function streams per partition.
 * @param has_finalize                 whether the function has a finalize phase.
 * @param source_order_dependent       whether the source (finalize) phase is order-dependent.
 * @param sink_order_dependent         whether the sink (process) phase is order-dependent.
 * @param requires_input_batch_index   whether the sink phase requires a monotone input batch index.
 * @param input_from_args              blended ("UNNEST-style") table-in-out: the positional args
 *                                     ARE the per-row input columns (real typed args, no TABLE
 *                                     placeholder), so one registration serves the literal /
 *                                     column / LATERAL call shapes. Set from
 *                                     {@link farm.query.vgi.tableinout.RowTransformFunction}.
 * @param required_settings            session settings that must be present.
 * @param required_secrets             secrets the function needs to resolve.
 */
public record FunctionInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        String schema_name,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String function_type,
        byte[] arguments,
        byte[] output_schema,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) @Nullable String stability,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) @Nullable String null_handling,
        String description,
        List<FunctionExample> examples,
        List<String> categories,
        @Nullable Boolean projection_pushdown,
        @Nullable Boolean filter_pushdown,
        @Nullable Boolean sampling_pushdown,
        @Nullable Boolean late_materialization,
        List<String> supported_expression_filters,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) @Nullable String order_preservation,
        @ArrowField(ArrowFieldType.INT32) @Nullable Integer max_workers,
        boolean supports_batch_index,
        boolean supports_splits,
        boolean filters_exactly_applied,
        boolean supports_positions,
        @Nullable Long split_token_ttl_seconds,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String partition_kind,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String order_dependent,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String distinct_dependent,
        boolean supports_window,
        boolean streaming_partitioned,
        boolean has_finalize,
        boolean source_order_dependent,
        boolean sink_order_dependent,
        boolean requires_input_batch_index,
        boolean input_from_args,
        List<String> required_settings,
        List<FunctionRequiredSecret> required_secrets) implements ArrowSerializableRecord {
}
