// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgirpc.wire.Allocators;
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
import static farm.query.vgi.internal.IpcStructBuilder.dict;
import static farm.query.vgi.internal.IpcStructBuilder.listOf;
import static farm.query.vgi.internal.IpcStructBuilder.listOfPrim;
import static farm.query.vgi.internal.IpcStructBuilder.mapUtf8Utf8;
import static farm.query.vgi.internal.IpcStructBuilder.nonNull;
import static farm.query.vgi.internal.IpcStructBuilder.nullable;
import static farm.query.vgi.internal.IpcStructBuilder.registerDict;
import static farm.query.vgi.internal.IpcStructBuilder.writeBool;
import static farm.query.vgi.internal.IpcStructBuilder.writeDictIndex;
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

    private static final long DICT_FUNCTION_TYPE = 1;
    private static final long DICT_STABILITY = 2;
    private static final long DICT_NULL_HANDLING = 3;
    private static final long DICT_ORDER_PRESERVATION = 4;
    private static final long DICT_ORDER_DEPENDENT = 5;
    private static final long DICT_DISTINCT_DEPENDENT = 6;

    private static final List<String> FUNCTION_TYPE_VALUES = List.of("scalar", "table", "aggregate");
    private static final List<String> STABILITY_VALUES = List.of("CONSISTENT", "VOLATILE", "CONSISTENT_WITHIN_QUERY");
    private static final List<String> NULL_HANDLING_VALUES = List.of("DEFAULT", "SPECIAL");
    private static final List<String> ORDER_PRESERVATION_VALUES =
            List.of("NO_ORDER_PRESERVED", "INSERTION_ORDER", "FIXED_ORDER");
    private static final List<String> ORDER_DEPENDENT_VALUES = List.of("NOT_ORDER_DEPENDENT", "ORDER_DEPENDENT");
    private static final List<String> DISTINCT_DEPENDENT_VALUES =
            List.of("NOT_DISTINCT_DEPENDENT", "DISTINCT_DEPENDENT");

    private static final Schema SCHEMA = new Schema(List.of(
            nullable("comment", UTF8),
            mapUtf8Utf8("tags"),
            nonNull("name", UTF8),
            nonNull("schema_name", UTF8),
            dict("function_type", DICT_FUNCTION_TYPE, false),
            nonNull("arguments", BINARY),
            nonNull("output_schema", BINARY),
            dict("stability", DICT_STABILITY, true),
            dict("null_handling", DICT_NULL_HANDLING, true),
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
            dict("order_preservation", DICT_ORDER_PRESERVATION, true),
            nonNull("max_workers", I32),
            dict("order_dependent", DICT_ORDER_DEPENDENT, false),
            dict("distinct_dependent", DICT_DISTINCT_DEPENDENT, false),
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
        registerDict(provider, Allocators.root(), DICT_FUNCTION_TYPE, FUNCTION_TYPE_VALUES);
        registerDict(provider, Allocators.root(), DICT_STABILITY, STABILITY_VALUES);
        registerDict(provider, Allocators.root(), DICT_NULL_HANDLING, NULL_HANDLING_VALUES);
        registerDict(provider, Allocators.root(), DICT_ORDER_PRESERVATION, ORDER_PRESERVATION_VALUES);
        registerDict(provider, Allocators.root(), DICT_ORDER_DEPENDENT, ORDER_DEPENDENT_VALUES);
        registerDict(provider, Allocators.root(), DICT_DISTINCT_DEPENDENT, DISTINCT_DEPENDENT_VALUES);

        return IpcStructBuilder.build(SCHEMA, provider, v -> {
            writeVarChar(v.get("comment"), info.comment());
            writeMap(v.get("tags"), info.tags());
            writeVarChar(v.get("name"), info.name());
            writeVarChar(v.get("schema_name"), info.schema_name());
            writeDictIndex(v.get("function_type"), info.function_type(), FUNCTION_TYPE_VALUES, "function_type");
            writeVarBinarySafe(v.get("arguments"), info.arguments());
            writeVarBinarySafe(v.get("output_schema"), info.output_schema());
            writeDictIndex(v.get("stability"), info.stability(), STABILITY_VALUES, "stability");
            writeDictIndex(v.get("null_handling"), info.null_handling(), NULL_HANDLING_VALUES, "null_handling");
            writeVarChar(v.get("description"), info.description());
            writeStringList(v.get("examples"), List.of());
            writeStringList(v.get("categories"), info.categories());
            writeNullableBool(v.get("projection_pushdown"), info.projection_pushdown());
            writeNullableBool(v.get("filter_pushdown"), info.filter_pushdown());
            writeNullableBool(v.get("sampling_pushdown"), info.sampling_pushdown());
            writeStringList(v.get("supported_expression_filters"), info.supported_expression_filters());
            writeDictIndex(v.get("order_preservation"), info.order_preservation(), ORDER_PRESERVATION_VALUES, "order_preservation");
            writeInt32(v.get("max_workers"), info.max_workers());
            writeDictIndex(v.get("order_dependent"), info.order_dependent(), ORDER_DEPENDENT_VALUES, "order_dependent");
            writeDictIndex(v.get("distinct_dependent"), info.distinct_dependent(), DISTINCT_DEPENDENT_VALUES, "distinct_dependent");
            writeBool(v.get("supports_window"), info.supports_window());
            writeBool(v.get("streaming_partitioned"), info.streaming_partitioned());
            writeBool(v.get("has_finalize"), info.has_finalize());
            writeStringList(v.get("required_settings"), info.required_settings());
            writeStringList(v.get("required_secrets"), List.of());
        });
    }
}
