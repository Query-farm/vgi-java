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
 * @param max_workers                  maximum parallel worker count.
 * @param supports_batch_index         whether per-batch index tagging is supported.
 * @param partition_kind               dictionary-encoded partitioning behaviour.
 * @param order_dependent              dictionary-encoded order-dependence.
 * @param distinct_dependent           dictionary-encoded distinct-dependence.
 * @param supports_window              whether the function supports windowed execution.
 * @param streaming_partitioned        whether the function streams per partition.
 * @param has_finalize                 whether the function has a finalize phase.
 * @param source_order_dependent       whether the source (finalize) phase is order-dependent.
 * @param sink_order_dependent         whether the sink (process) phase is order-dependent.
 * @param requires_input_batch_index   whether the sink phase requires a monotone input batch index.
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
        @ArrowField(ArrowFieldType.INT32) int max_workers,
        boolean supports_batch_index,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String partition_kind,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String order_dependent,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String distinct_dependent,
        boolean supports_window,
        boolean streaming_partitioned,
        boolean has_finalize,
        boolean source_order_dependent,
        boolean sink_order_dependent,
        boolean requires_input_batch_index,
        List<String> required_settings,
        List<FunctionRequiredSecret> required_secrets) implements ArrowSerializableRecord {
}
