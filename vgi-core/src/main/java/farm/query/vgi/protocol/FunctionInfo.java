// Copyright 2025-2026 Query.Farm LLC

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
