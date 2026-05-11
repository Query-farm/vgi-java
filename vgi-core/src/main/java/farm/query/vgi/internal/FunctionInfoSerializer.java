// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

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

    private static final EnumDict FUNCTION_TYPE = new EnumDict("function_type", 1,
            List.of("scalar", "table", "aggregate"));
    private static final EnumDict STABILITY = new EnumDict("stability", 2,
            List.of("CONSISTENT", "VOLATILE", "CONSISTENT_WITHIN_QUERY"));
    private static final EnumDict NULL_HANDLING = new EnumDict("null_handling", 3,
            List.of("DEFAULT", "SPECIAL"));
    private static final EnumDict ORDER_PRESERVATION = new EnumDict("order_preservation", 4,
            List.of("NO_ORDER_PRESERVED", "INSERTION_ORDER", "FIXED_ORDER"));
    private static final EnumDict ORDER_DEPENDENT = new EnumDict("order_dependent", 5,
            List.of("NOT_ORDER_DEPENDENT", "ORDER_DEPENDENT"));
    private static final EnumDict DISTINCT_DEPENDENT = new EnumDict("distinct_dependent", 6,
            List.of("NOT_DISTINCT_DEPENDENT", "DISTINCT_DEPENDENT"));

    private static final List<EnumDict> DICTS = List.of(
            FUNCTION_TYPE, STABILITY, NULL_HANDLING,
            ORDER_PRESERVATION, ORDER_DEPENDENT, DISTINCT_DEPENDENT);

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
            listOfPrim("supported_expression_filters", UTF8),
            ORDER_PRESERVATION.field(true),
            nonNull("max_workers", I32),
            ORDER_DEPENDENT.field(false),
            DISTINCT_DEPENDENT.field(false),
            nonNull("supports_window", BOOL),
            nonNull("streaming_partitioned", BOOL),
            nonNull("has_finalize", BOOL),
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
            writeStringList(v.get("supported_expression_filters"), info.supported_expression_filters());
            ORDER_PRESERVATION.write(v.get("order_preservation"), info.order_preservation());
            writeInt32(v.get("max_workers"), info.max_workers());
            ORDER_DEPENDENT.write(v.get("order_dependent"), info.order_dependent());
            DISTINCT_DEPENDENT.write(v.get("distinct_dependent"), info.distinct_dependent());
            writeBool(v.get("supports_window"), info.supports_window());
            writeBool(v.get("streaming_partitioned"), info.streaming_partitioned());
            writeBool(v.get("has_finalize"), info.has_finalize());
            writeStringList(v.get("required_settings"), info.required_settings());
            writeStringList(v.get("required_secrets"), List.of());
        });
    }
}
