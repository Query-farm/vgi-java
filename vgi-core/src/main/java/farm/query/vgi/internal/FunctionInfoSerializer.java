// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import farm.query.vgi.protocol.FunctionInfo;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

import static farm.query.vgi.internal.IpcStructBuilder.BINARY;
import static farm.query.vgi.internal.IpcStructBuilder.BOOL;
import static farm.query.vgi.internal.IpcStructBuilder.I32;
import static farm.query.vgi.internal.IpcStructBuilder.UTF8;
import static farm.query.vgi.internal.IpcStructBuilder.listOf;
import static farm.query.vgi.internal.IpcStructBuilder.listOfPrim;
import static farm.query.vgi.internal.IpcStructBuilder.mapUtf8Utf8;
import static farm.query.vgi.internal.IpcStructBuilder.nonNull;
import static farm.query.vgi.internal.IpcStructBuilder.nullable;
import static farm.query.vgi.internal.IpcStructBuilder.writeBool;
import static farm.query.vgi.internal.IpcStructBuilder.writeInt32;
import static farm.query.vgi.internal.IpcStructBuilder.writeMap;
import static farm.query.vgi.internal.IpcStructBuilder.writeNullableBool;
import static farm.query.vgi.internal.IpcStructBuilder.writeStringList;
import static farm.query.vgi.internal.IpcStructBuilder.writeVarBinarySafe;
import static farm.query.vgi.internal.IpcStructBuilder.writeVarChar;

/**
 * Serialiser for {@link FunctionInfo}. The C++ extension's
 * {@code FunctionInfoSchema} requires {@code dictionary<int16, utf8>} for the
 * enum-shaped fields ({@code function_type}, {@code stability},
 * {@code null_handling}, etc.); we hand-build the IPC batch to match.
 */
final class FunctionInfoSerializer {

    private FunctionInfoSerializer() {}

    private static final EnumDict FUNCTION_TYPE = new EnumDict("function_type",
            DictionaryIds.FUNCTION_TYPE, List.of("scalar", "table", "aggregate", "table_buffering"));
    private static final EnumDict STABILITY = new EnumDict("stability",
            DictionaryIds.STABILITY, List.of("CONSISTENT", "VOLATILE", "CONSISTENT_WITHIN_QUERY"));
    private static final EnumDict NULL_HANDLING = new EnumDict("null_handling",
            DictionaryIds.NULL_HANDLING, List.of("DEFAULT", "SPECIAL"));
    private static final EnumDict ORDER_PRESERVATION = new EnumDict("order_preservation",
            DictionaryIds.ORDER_PRESERVATION,
            List.of("NO_ORDER_PRESERVED", "INSERTION_ORDER", "FIXED_ORDER"));
    private static final EnumDict ORDER_DEPENDENT = new EnumDict("order_dependent",
            DictionaryIds.ORDER_DEPENDENT, List.of("NOT_ORDER_DEPENDENT", "ORDER_DEPENDENT"));
    private static final EnumDict DISTINCT_DEPENDENT = new EnumDict("distinct_dependent",
            DictionaryIds.DISTINCT_DEPENDENT, List.of("NOT_DISTINCT_DEPENDENT", "DISTINCT_DEPENDENT"));
    private static final EnumDict PARTITION_KIND = new EnumDict("partition_kind",
            DictionaryIds.PARTITION_KIND,
            List.of("NOT_PARTITIONED", "SINGLE_VALUE_PARTITIONS",
                    "OVERLAPPING_PARTITIONS", "DISJOINT_PARTITIONS"));

    private static final List<EnumDict> DICTS = List.of(
            FUNCTION_TYPE, STABILITY, NULL_HANDLING,
            ORDER_PRESERVATION, ORDER_DEPENDENT, DISTINCT_DEPENDENT, PARTITION_KIND);

    private static final Schema SCHEMA = new Schema(List.of(
            nullable("comment", UTF8),
            mapUtf8Utf8("tags"),
            nonNull("name", UTF8),
            nonNull("schema_name", UTF8),
            FUNCTION_TYPE.field(false),
            nonNull("arguments", BINARY),
            nonNull("output_schema", BINARY),
            STABILITY.field(true),
            NULL_HANDLING.field(true),
            nonNull("description", UTF8),
            listOf("examples", new Field("item",
                    new FieldType(true, new ArrowType.Struct(), null),
                    List.of(nonNull("sql", UTF8),
                            nonNull("description", UTF8),
                            nullable("expected_output", UTF8)))),
            listOfPrim("categories", UTF8),
            nullable("projection_pushdown", BOOL),
            nullable("filter_pushdown", BOOL),
            nullable("sampling_pushdown", BOOL),
            // late_materialization landed in the C++ FunctionInfoSchema between
            // sampling_pushdown and supported_expression_filters
            // (vgi_protocol_schemas.hpp). Nullable; only TableFunctions that
            // expose an is_row_id virtual column + filter/projection pushdown
            // opt in (Meta.late_materialization), everything else is null.
            nullable("late_materialization", BOOL),
            listOfPrim("supported_expression_filters", UTF8),
            ORDER_PRESERVATION.field(true),
            nonNull("max_workers", I32),
            // supports_batch_index / partition_kind landed in the C++
            // extension's FunctionInfoSchema between max_workers and
            // order_dependent. Older Java workers omitted them and HTTP-mode
            // catalog enumeration fails on field-count mismatch (stdio is
            // lenient and still parses). Safe defaults are false /
            // NOT_PARTITIONED — see vgi_catalog_api.cpp.
            nonNull("supports_batch_index", BOOL),
            PARTITION_KIND.field(false),
            ORDER_DEPENDENT.field(false),
            DISTINCT_DEPENDENT.field(false),
            nonNull("supports_window", BOOL),
            nonNull("streaming_partitioned", BOOL),
            nonNull("has_finalize", BOOL),
            // source_order_dependent / sink_order_dependent /
            // requires_input_batch_index landed in the C++ FunctionInfoSchema
            // between has_finalize and required_settings (vgi_catalog_api.cpp).
            // Only meaningful for TableBuffering functions; default false for
            // every other function_type — matches the C++ side's value_or.
            nonNull("source_order_dependent", BOOL),
            nonNull("sink_order_dependent", BOOL),
            nonNull("requires_input_batch_index", BOOL),
            listOfPrim("required_settings", UTF8),
            listOf("required_secrets", new Field("item",
                    new FieldType(true, new ArrowType.Struct(), null),
                    List.of(nonNull("secret_type", UTF8),
                            nullable("scope", UTF8),
                            nullable("secret_name", UTF8))))));

    static byte[] serialize(FunctionInfo info) {
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        for (EnumDict d : DICTS) d.register(provider);

        return IpcStructBuilder.build(SCHEMA, provider, v -> {
            writeVarChar(v.get("comment"), info.comment());
            writeMap(v.get("tags"), info.tags());
            writeVarChar(v.get("name"), info.name());
            writeVarChar(v.get("schema_name"), info.schema_name());
            FUNCTION_TYPE.write(v.get("function_type"), info.function_type());
            writeVarBinarySafe(v.get("arguments"), info.arguments());
            writeVarBinarySafe(v.get("output_schema"), info.output_schema());
            STABILITY.write(v.get("stability"), info.stability());
            NULL_HANDLING.write(v.get("null_handling"), info.null_handling());
            writeVarChar(v.get("description"), info.description());
            writeStringList(v.get("examples"), List.of());
            writeStringList(v.get("categories"), info.categories());
            writeNullableBool(v.get("projection_pushdown"), info.projection_pushdown());
            writeNullableBool(v.get("filter_pushdown"), info.filter_pushdown());
            writeNullableBool(v.get("sampling_pushdown"), info.sampling_pushdown());
            writeNullableBool(v.get("late_materialization"), info.late_materialization());
            writeStringList(v.get("supported_expression_filters"), info.supported_expression_filters());
            ORDER_PRESERVATION.write(v.get("order_preservation"), info.order_preservation());
            writeInt32(v.get("max_workers"), info.max_workers());
            writeBool(v.get("supports_batch_index"), info.supports_batch_index());
            PARTITION_KIND.write(v.get("partition_kind"), info.partition_kind());
            ORDER_DEPENDENT.write(v.get("order_dependent"), info.order_dependent());
            DISTINCT_DEPENDENT.write(v.get("distinct_dependent"), info.distinct_dependent());
            writeBool(v.get("supports_window"), info.supports_window());
            writeBool(v.get("streaming_partitioned"), info.streaming_partitioned());
            writeBool(v.get("has_finalize"), info.has_finalize());
            writeBool(v.get("source_order_dependent"), info.source_order_dependent());
            writeBool(v.get("sink_order_dependent"), info.sink_order_dependent());
            writeBool(v.get("requires_input_batch_index"), info.requires_input_batch_index());
            writeStringList(v.get("required_settings"), info.required_settings());
            writeStringList(v.get("required_secrets"), List.of());
        });
    }
}
