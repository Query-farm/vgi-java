// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import farm.query.vgi.protocol.OptionChanges;
import farm.query.vgi.protocol.CatalogObjectRef;
import farm.query.vgi.protocol.ObjectDependency;
import farm.query.vgi.protocol.DependencySnapshot;
import farm.query.vgi.protocol.CatalogMetadataChanges;
import farm.query.vgi.protocol.SqlExpression;
import farm.query.vgi.protocol.SortKey;
import farm.query.vgi.protocol.TableLayout;
import farm.query.vgi.protocol.TableLayoutChange;
import farm.query.vgi.protocol.TableCreateRequest;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.OptionDefinitions;
import farm.query.vgi.protocol.OptionState;
import farm.query.vgi.protocol.RequiredFilterPaths;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.ParameterBinder;
import farm.query.vgirpc.ServiceIntrospector;
import farm.query.vgi.VgiService;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CatalogContractsTest {
    @Test void attachDefaultSchemaPreservesNullAndRawNames() throws Exception {
        assertNull(roundTrip("attach_default_schema_none", farm.query.vgi.protocol.CatalogAttachResult.class).default_schema());
        assertEquals("main", roundTrip("attach_default_schema_main", farm.query.vgi.protocol.CatalogAttachResult.class).default_schema());
        assertEquals("sales.archive", roundTrip("attach_default_schema_dotted", farm.query.vgi.protocol.CatalogAttachResult.class).default_schema());
    }

    @Test void typedIndexesPreserveRawPathsContextAndOptionState() throws Exception {
        assertEquals(List.of("a.b"), roundTrip("index_key_column", farm.query.vgi.protocol.IndexKey.class).column_path());
        assertEquals(List.of("record", "a.b"), roundTrip("index_key_nested", farm.query.vgi.protocol.IndexKey.class).column_path());
        assertNotNull(roundTrip("index_key_expression", farm.query.vgi.protocol.IndexKey.class).expression());
        var definition = roundTrip("index_definition", farm.query.vgi.protocol.IndexDefinition.class);
        roundTrip("index_definition_primary", farm.query.vgi.protocol.IndexDefinition.class);
        assertArrayEquals(fixture("index_key_column"), definition.keys().get(0));
        assertArrayEquals(fixture("index_key_expression"), definition.keys().get(1));
        assertArrayEquals(fixture("values"), definition.options());
        for (var name : List.of("index_info", "index_create", "index_create_cross_schema")) {
            try (var allocator = new RootAllocator();
                 var reader = new ArrowStreamReader(new ByteArrayInputStream(fixture(name)), allocator)) {
                assertTrue(reader.loadNextBatch());
                var source = reader.getVectorSchemaRoot();
                assertEquals(farm.query.vgi.generated.VgiProtocolSchemas.byName().get(
                        name.equals("index_info") ? "IndexInfo" : "IndexCreateRequest"),
                        WireRecordSchemaConformanceTest.producedSchema(fixture(name)));
                var bytes = new ByteArrayOutputStream();
                var dictionaries = new org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider(
                        reader.getDictionaryVectors().values().toArray(org.apache.arrow.vector.dictionary.Dictionary[]::new));
                try (var writer = new ArrowStreamWriter(source, dictionaries, bytes)) {
                    writer.start(); writer.writeBatch(); writer.end();
                }
                try (var restored = new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator)) {
                    assertTrue(restored.loadNextBatch());
                    var row = restored.getVectorSchemaRoot();
                    assertEquals(source.getSchema(), row.getSchema());
                    assertEquals(1, row.getRowCount());
                    assertArrayEquals(fixture("index_definition"), ((org.apache.arrow.vector.VarBinaryVector)row.getVector("definition")).get(0));
                    if (name.equals("index_info")) assertArrayEquals(fixture("state"), ((org.apache.arrow.vector.VarBinaryVector)row.getVector("options")).get(0));
                }
            }
        }
    }

    @Test void columnExpressionMetadataRetainsExactIpcBytes() throws Exception {
        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(new ByteArrayInputStream(fixture("column_expression_metadata")), allocator)) {
            assertTrue(reader.loadNextBatch());
            var source = reader.getVectorSchemaRoot();
            var bytes = new ByteArrayOutputStream();
            try (var writer = new ArrowStreamWriter(source, null, bytes)) {
                writer.start(); writer.writeBatch(); writer.end();
            }
            try (var restored = new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator)) {
                assertTrue(restored.loadNextBatch());
                var fields = restored.getVectorSchemaRoot().getSchema().getFields();
                assertArrayEquals(fixture("column_default_expression"), Base64.getDecoder().decode(fields.get(0).getMetadata().get("vgi.column.default.v1")));
                assertArrayEquals(fixture("column_generated_expression"), Base64.getDecoder().decode(fields.get(1).getMetadata().get("vgi.column.generated.v1")));
                assertEquals("virtual", fields.get(1).getMetadata().get("vgi.column.generation.v1"));
            }
        }
    }

    @Test void canonicalConstraintRecordsPreserveNestedPayloadsAndRevisions() throws Exception {
        roundTrip("sql_statement", farm.query.vgi.protocol.SqlStatement.class);
        roundTrip("constraint_ref", farm.query.vgi.protocol.CatalogObjectRef.class);
        roundTrip("foreign_key_definition", farm.query.vgi.protocol.ForeignKeyDefinition.class);
        roundTrip("constraint_definition_not_null", farm.query.vgi.protocol.ConstraintDefinition.class);
        roundTrip("constraint_definition_unique", farm.query.vgi.protocol.ConstraintDefinition.class);
        roundTrip("constraint_definition_primary", farm.query.vgi.protocol.ConstraintDefinition.class);
        roundTrip("constraint_definition_check", farm.query.vgi.protocol.ConstraintDefinition.class);
        roundTrip("constraint_definition_foreign", farm.query.vgi.protocol.ConstraintDefinition.class);
        roundTrip("constraint_info", farm.query.vgi.protocol.ConstraintInfo.class);
        roundTrip("constraint_info_untrusted", farm.query.vgi.protocol.ConstraintInfo.class);
        roundTrip("constraint_change_add", farm.query.vgi.protocol.ConstraintChange.class);
        roundTrip("constraint_change_drop", farm.query.vgi.protocol.ConstraintChange.class);
        roundTrip("constraint_change_rename", farm.query.vgi.protocol.ConstraintChange.class);
        roundTrip("constraint_change_unname", farm.query.vgi.protocol.ConstraintChange.class);
        roundTrip("constraint_change_replace", farm.query.vgi.protocol.ConstraintChange.class);
        roundTrip("constraint_snapshot", farm.query.vgi.protocol.ConstraintSnapshot.class);
        roundTrip("constraint_snapshot_empty", farm.query.vgi.protocol.ConstraintSnapshot.class);
        roundTrip("constraints_apply", farm.query.vgi.protocol.ConstraintsApplyRequest.class);
        roundTrip("constraints_apply_autocommit", farm.query.vgi.protocol.ConstraintsApplyRequest.class);
        var statement = roundTrip("sql_statement", farm.query.vgi.protocol.SqlStatement.class);
        assertEquals("desc", statement.evaluation_context().default_order());
        assertEquals("nulls_first", statement.evaluation_context().default_null_order());
        assertFalse(statement.evaluation_context().preserve_insertion_order());
        var snapshot = roundTrip("constraint_snapshot", farm.query.vgi.protocol.ConstraintSnapshot.class);
        assertArrayEquals(new byte[] { 0, 114, 101, 118, 105, 115, 105, 111, 110, (byte)255, (byte)128 }, snapshot.definition_revision());
        assertEquals(1, snapshot.constraints().size());
        assertArrayEquals(fixture("constraint_info"), snapshot.constraints().getFirst());
        var info = roundTrip("constraint_info", farm.query.vgi.protocol.ConstraintInfo.class);
        assertArrayEquals(fixture("constraint_ref"), info.ref());
        assertArrayEquals(fixture("constraint_definition_foreign"), info.definition());
        var request = roundTrip("constraints_apply", farm.query.vgi.protocol.ConstraintsApplyRequest.class);
        assertArrayEquals(snapshot.definition_revision(), request.expected_revision());
        assertArrayEquals(fixture("constraint_change_add"), request.changes().get(0));
        assertArrayEquals(fixture("constraint_change_rename"), request.changes().get(1));
        assertNull(roundTrip("constraints_apply_autocommit", farm.query.vgi.protocol.ConstraintsApplyRequest.class).transaction_opaque_data());
        assertNull(roundTrip("constraint_change_unname", farm.query.vgi.protocol.ConstraintChange.class).declared_name());
    }

    @Test void macroFamiliesPreserveDeclarationsDefaultsAndLegacyAbsence() throws Exception {
        var parameter = roundTrip("macro_parameter", farm.query.vgi.protocol.MacroParameter.class);
        var source = RecordCodec.deserializeFromBytes(fixture("macro_parameter"), farm.query.vgi.protocol.MacroParameter.class);
        assertEquals("fallback", parameter.name());
        assertArrayEquals(source.declared_type(), parameter.declared_type());
        assertArrayEquals(source.default_value(), parameter.default_value());
        assertNull(parameter.default_expression());
        assertEquals("A typed NULL default", parameter.documentation());
        var expression = roundTrip("macro_parameter_expression", farm.query.vgi.protocol.MacroParameter.class);
        assertNull(expression.default_value());
        assertEquals("2 + 3", RecordCodec.deserializeFromBytes(expression.default_expression(), SqlExpression.class).sql());
        var untyped = roundTrip("macro_parameter_untyped", farm.query.vgi.protocol.MacroParameter.class);
        assertNull(untyped.declared_type());
        assertNull(untyped.default_value());
        var overload = roundTrip("macro_overload", farm.query.vgi.protocol.MacroOverload.class);
        assertEquals(List.of("one.two", "nested"), overload.schema_path());
        assertEquals(List.of("x", "fallback", "scale"), overload.parameters().stream().map(p -> p.name()).toList());
        var create = roundTrip("macro_create", farm.query.vgi.protocol.MacroCreateRequest.class);
        assertEquals(2, create.overloads().size());
        assertEquals("upper(x)", create.overloads().get(1).definition());
        assertTrue(create.parameters().isEmpty());
        assertEquals("", create.definition());
        assertNull(roundTrip("macro_create_legacy", farm.query.vgi.protocol.MacroCreateRequest.class).overloads());
        assertEquals(2, roundTrip("macro_info", farm.query.vgi.protocol.MacroInfo.class).overloads().size());
        assertNull(roundTrip("macro_info_legacy", farm.query.vgi.protocol.MacroInfo.class).overloads());
        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(new ByteArrayInputStream(parameter.default_value()), allocator)) {
            assertTrue(reader.loadNextBatch());
            assertTrue(reader.getVectorSchemaRoot().getVector(0).isNull(0));
            assertEquals("retained", reader.getVectorSchemaRoot().getSchema().getFields().getFirst().getMetadata().get("domain"));
        }
    }

    @Test void logicalCarriersPreserveRawTypeMetadataAndDynamicPayloadBytes() throws Exception {
        for (var name : List.of("logical_tuple", "logical_struct", "logical_variant_sql_null",
                "logical_variant_null", "logical_variant_absent", "logical_variant_null_member")) {
            try (var allocator = new RootAllocator();
                 var reader = new ArrowStreamReader(new ByteArrayInputStream(fixture(name)), allocator)) {
                assertTrue(reader.loadNextBatch());
                var original = reader.getVectorSchemaRoot();
                var bytes = new ByteArrayOutputStream();
                try (var writer = new ArrowStreamWriter(original, null, bytes)) {
                    writer.start(); writer.writeBatch(); writer.end();
                }
                try (var restoredReader = new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator)) {
                    assertTrue(restoredReader.loadNextBatch());
                    var restored = restoredReader.getVectorSchemaRoot();
                    assertEquals(original.getSchema(), restored.getSchema());
                    assertEquals(1, restored.getRowCount());
                    var metadata = restored.getSchema().getFields().getFirst().getMetadata();
                    var marker = metadata == null ? null : metadata.get("vgi.logical_type.v1");
                    if (name.equals("logical_tuple")) assertEquals("tuple", marker);
                    else assertNull(marker);
                    assertEquals(name.equals("logical_variant_sql_null"), restored.getVector(0).isNull(0));
                    if (name.startsWith("logical_variant_")) {
                        assertEquals("arrow.parquet.variant", metadata.get("ARROW:extension:name"));
                        assertTrue(metadata.containsKey("ARROW:extension:metadata"));
                        assertEquals("", metadata.get("ARROW:extension:metadata"));
                        var children = restored.getSchema().getFields().getFirst().getChildren();
                        assertEquals(List.of("metadata", "value"), children.stream().map(f -> f.getName()).toList());
                        assertFalse(children.get(0).isNullable());
                        assertTrue(children.get(1).isNullable());
                        if (!name.equals("logical_variant_sql_null")) {
                            var column = (org.apache.arrow.vector.complex.StructVector) restored.getVector(0);
                            byte[] expectedMetadata = name.equals("logical_variant_null_member")
                                    ? new byte[] {1, 1, 0, 7, 'p', 'r', 'e', 's', 'e', 'n', 't'} : new byte[] {1, 0, 0};
                            byte[] expectedValue = name.equals("logical_variant_null") ? new byte[] {0}
                                    : name.equals("logical_variant_absent") ? new byte[] {2, 0, 0} : new byte[] {2, 1, 0, 0, 1, 0};
                            assertArrayEquals(expectedMetadata, ((org.apache.arrow.vector.VarBinaryVector) column.getChild("metadata")).get(0));
                            assertArrayEquals(expectedValue, ((org.apache.arrow.vector.VarBinaryVector) column.getChild("value")).get(0));
                        }
                    } else assertEquals(original.getVector(0).getObject(0), restored.getVector(0).getObject(0));
                    assertFalse(restoredReader.loadNextBatch());
                }
                assertFalse(reader.loadNextBatch());
            }
        }
        assertFalse(java.util.Arrays.equals(fixture("logical_variant_null"), fixture("logical_variant_sql_null")));
        assertFalse(java.util.Arrays.equals(fixture("logical_variant_absent"), fixture("logical_variant_null_member")));
    }

    @Test void queryParameterBatchesPreserveMappingMetadataAndEmptyRows() throws Exception {
        for (var name : List.of("query_parameters", "query_empty_parameters")) {
            try (var allocator = new RootAllocator();
                 var reader = new ArrowStreamReader(new ByteArrayInputStream(fixture(name)), allocator)) {
                assertTrue(reader.loadNextBatch());
                var original = reader.getVectorSchemaRoot();
                var bytes = new ByteArrayOutputStream();
                try (var writer = new ArrowStreamWriter(original, null, bytes)) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                try (var restoredReader = new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator)) {
                    assertTrue(restoredReader.loadNextBatch());
                    var restored = restoredReader.getVectorSchemaRoot();
                    assertEquals(original.getSchema(), restored.getSchema());
                    assertEquals(1, restored.getRowCount());
                    assertEquals(name.equals("query_empty_parameters") ? "positional" : "named",
                            restored.getSchema().getCustomMetadata().get("vgi.query.parameters.v1"));
                    if (name.equals("query_empty_parameters")) assertTrue(restored.getFieldVectors().isEmpty());
                    else {
                        assertTrue(restored.getVector(0).isNull(0));
                        assertEquals("retained", restored.getSchema().getFields().get(1).getChildren().getFirst().getMetadata().get("nested"));
                        assertEquals(original.getVector(1).getObject(0), restored.getVector(1).getObject(0));
                    }
                    assertFalse(restoredReader.loadNextBatch());
                }
                assertFalse(reader.loadNextBatch());
            }
        }
    }

    @Test void queryRecordsPreserveNestedContracts() throws Exception {
        roundTrip("query_offer", farm.query.vgi.protocol.ClientQueryCapabilities.class);
        roundTrip("query_capabilities", farm.query.vgi.protocol.QueryCapabilities.class);
        roundTrip("query_context", farm.query.vgi.protocol.QueryEvaluationContext.class);
        roundTrip("query_object_reference", farm.query.vgi.protocol.QueryObjectReference.class);
        roundTrip("query_prepare", farm.query.vgi.protocol.CatalogQueryPrepareRequest.class);
        roundTrip("query_accepted", farm.query.vgi.protocol.CatalogQueryPrepareResult.class);
        roundTrip("query_declined", farm.query.vgi.protocol.CatalogQueryPrepareResult.class);
        var request = roundTrip("query_prepare", farm.query.vgi.protocol.CatalogQueryPrepareRequest.class);
        assertEquals("UTC", request.evaluation_context().timezone());
        assertEquals(List.of("one.two", "nested"), request.referenced_objects().get(0).object().schema_path());
        assertEquals(0L, request.catalog_version());
        assertNull(roundTrip("query_declined", farm.query.vgi.protocol.CatalogQueryPrepareResult.class).prepared_query_token());
    }

    @Test void viewSecuritySurvivesCodecRoundTrip() throws Exception {
        assertEquals("regular", roundTrip("view_info", farm.query.vgi.protocol.ViewInfo.class).security());
        assertEquals("barrier", roundTrip("view_info_barrier", farm.query.vgi.protocol.ViewInfo.class).security());
    }

    @Test void viewAliasesPreserveRawIdentifiers() throws Exception {
        var info = roundTrip("view_info", farm.query.vgi.protocol.ViewInfo.class);
        assertEquals(List.of("a.b", "with\"quote"), info.aliases());
        assertEquals(List.of(), roundTrip("view_info_implicit", farm.query.vgi.protocol.ViewInfo.class).aliases());
    }

    @Test void columnParametersBindRawComponentsThroughTheServiceSignature() throws Exception {
        var methods = ServiceIntrospector.describe(VgiService.class);
        for (var operation : List.of("column_add", "column_drop", "column_rename", "column_type_change",
                "column_default_set", "column_default_drop", "column_comment_set", "not_null_set", "not_null_drop")) {
            var method = methods.get("catalog_table_" + operation);
            for (var variant : List.of("literal", "nested")) {
                try (var allocator = new RootAllocator();
                     var reader = new ArrowStreamReader(new ByteArrayInputStream(fixture(operation + "_" + variant)), allocator)) {
                    assertTrue(reader.loadNextBatch());
                    var root = reader.getVectorSchemaRoot();
                    var row = Marshalling.decodeRow(root);
                    var args = ParameterBinder.bind(method.reflectMethod(), row, null);
                    var parameters = method.reflectMethod().getParameters();
                    for (int i = 0; i < method.paramsSchema().getFields().size(); i++) {
                        var name = parameters[i].getName();
                        if (row.get(name) instanceof byte[] bytes) assertArrayEquals(bytes, (byte[]) args[i]);
                        else assertEquals(row.get(name), args[i], name);
                    }
                    assertEquals(variant.equals("literal") ? List.of("a.b") : List.of("a", "b.\"quoted"), row.get("column_path"));
                    try (var encoded = Marshalling.encodeRow(method.paramsSchema(), row, allocator)) {
                        var restored = Marshalling.decodeRow(encoded);
                        assertEquals(row.get("column_path"), restored.get("column_path"));
                        assertEquals(row.get("ignore_not_found"), restored.get("ignore_not_found"));
                        if (operation.equals("column_default_set") || operation.equals("column_type_change")) {
                            if (operation.equals("column_type_change") && variant.equals("literal")) {
                                assertNull(restored.get("expression"));
                            } else {
                                var payload = (byte[]) row.get("expression");
                                assertArrayEquals(payload, (byte[]) restored.get("expression"));
                                var expression = RecordCodec.deserializeFromBytes(payload, SqlExpression.class);
                                assertEquals(row.get("schema_path"), expression.schema_path());
                                assertFalse(expression.profile().isEmpty());
                                assertNotNull(expression.evaluation_context());
                            }
                        }
                    }
                    assertFalse(reader.loadNextBatch());
                }
            }
        }
    }

    @Test
    void metadataPatchesPreserveCommentPresenceAndLiteralTags() throws Exception {
        var patch = roundTrip("metadata_patch", CatalogMetadataChanges.class);
        assertTrue(patch.set_comment());
        assertEquals("", patch.comment());
        assertEquals(java.util.Map.of("a.b", "", "__proto__", "literal", "", "empty-key"), patch.set_tags());
        assertEquals(List.of("old", "OLD"), patch.remove_tags());
        var clear = roundTrip("metadata_clear_comment", CatalogMetadataChanges.class);
        assertTrue(clear.set_comment());
        assertNull(clear.comment());
        assertTrue(clear.set_tags().isEmpty());
        var noop = roundTrip("metadata_noop", CatalogMetadataChanges.class);
        assertFalse(noop.set_comment());
        assertNull(noop.comment());
        assertTrue(noop.set_tags().isEmpty());
        assertTrue(noop.remove_tags().isEmpty());
    }

    @Test
    void dependencyRecordsPreserveIdentityAndNestedPayloads() throws Exception {
        var table = roundTrip("object_ref", CatalogObjectRef.class);
        assertEquals(List.of("one.two", "nested"), table.schema_path());
        assertNull(table.owner_table());
        var trigger = roundTrip("trigger_ref", CatalogObjectRef.class);
        assertEquals(table.name(), trigger.owner_table());
        var edge = roundTrip("object_dependency", ObjectDependency.class);
        assertEquals("owned", edge.kind());
        assertTrue(edge.blocks_alter());
        assertNull(edge.subject_column_paths());
        assertNull(edge.dependent_column_path());
        var membership = roundTrip("dependency_membership", ObjectDependency.class);
        assertFalse(membership.blocks_alter());
        assertNull(membership.subject_column_paths());
        assertNull(membership.dependent_column_path());
        var literal = roundTrip("dependency_column_literal", ObjectDependency.class);
        assertTrue(literal.blocks_alter());
        assertEquals(List.of(List.of("a.b")), literal.subject_column_paths());
        assertNull(literal.dependent_column_path());
        var nested = roundTrip("dependency_column_nested", ObjectDependency.class);
        assertEquals(List.of(List.of("record", "a.b")), nested.subject_column_paths());
        var generated = roundTrip("dependency_generated", ObjectDependency.class);
        assertArrayEquals(generated.dependent(), generated.subject());
        assertEquals(List.of("generated"), generated.dependent_column_path());
        assertEquals(List.of(List.of("a.b"), List.of("record", "a.b")), generated.subject_column_paths());
        var chain = roundTrip("dependency_generated_chain", ObjectDependency.class);
        assertEquals(List.of("second.generated"), chain.dependent_column_path());
        assertEquals(List.of(List.of("generated")), chain.subject_column_paths());
        var columns = roundTrip("dependency_column_snapshot", DependencySnapshot.class);
        assertEquals(3, columns.dependencies().size());
        assertArrayEquals(fixture("dependency_column_literal"), columns.dependencies().get(0));
        assertArrayEquals(fixture("dependency_generated"), columns.dependencies().get(1));
        assertArrayEquals(fixture("dependency_generated_chain"), columns.dependencies().get(2));

        assertArrayEquals(fixture("object_ref"), edge.subject());
        assertArrayEquals(fixture("trigger_ref"), edge.dependent());
        var graph = roundTrip("dependency_snapshot", DependencySnapshot.class);
        assertEquals(42L, graph.catalog_version());
        assertEquals(4, graph.objects().size());
        assertEquals(3, graph.dependencies().size());
        var empty = roundTrip("empty_dependency_snapshot", DependencySnapshot.class);
        assertTrue(empty.objects().isEmpty());
        assertTrue(empty.dependencies().isEmpty());
    }

    static byte[] fixture(String name) throws Exception {
        JsonObject corpus = JsonParser.parseString(Files.readString(
                Path.of("../conformance/catalog-contracts.json"))).getAsJsonObject();
        return Base64.getDecoder().decode(corpus.getAsJsonObject("records").get(name).getAsString());
    }

    private static <T extends ArrowSerializableRecord> T roundTrip(String name, Class<T> type) throws Exception {
        T value = RecordCodec.deserializeFromBytes(fixture(name), type);
        return RecordCodec.deserializeFromBytes(RecordCodec.serializeToBytes(value), type);
    }

    @Test void optionsPreservePythonPayloadsAndExplicitNulls() throws Exception {
        OptionChanges changes = roundTrip("changes", OptionChanges.class);
        assertArrayEquals(fixture("values"), changes.set_values());
        assertEquals(List.of("reset.me"), changes.reset_names());
        OptionChanges reset = roundTrip("reset_only", OptionChanges.class);
        assertNull(reset.set_values());
        assertEquals(List.of("reset.me"), reset.reset_names());
        OptionState state = roundTrip("state", OptionState.class);
        assertArrayEquals(fixture("values"), state.values());
        assertEquals(List.of("optional"), state.explicit_names());
        OptionState empty = roundTrip("empty_state", OptionState.class);
        assertArrayEquals(fixture("empty_values"), empty.values());
        assertEquals(List.of(), empty.explicit_names());
    }

    @Test void nestedListsPreserveLiteralNamesAndAlternatives() throws Exception {
        RequiredFilterPaths paths = roundTrip("paths", RequiredFilterPaths.class);
        assertEquals(List.of(List.of(List.of("a.b"), List.of("a", "b")),
                List.of(List.of("record", "literal.\"name"))), paths.groups());
        assertEquals(List.of(), roundTrip("empty_paths", RequiredFilterPaths.class).groups());
    }
    @Test void capabilityProfilesAndDefinitionsRoundTrip() throws Exception {
        CatalogCapabilities caps = roundTrip("capabilities", CatalogCapabilities.class);
        assertEquals(List.of("vgi.schema.options.read.v1"), caps.read());
        assertEquals(List.of("vgi.schema.options.create.v1"), caps.ddl());
        assertEquals(List.of(), roundTrip("empty_capabilities", CatalogCapabilities.class).read());
        OptionDefinitions definitions = roundTrip("definitions", OptionDefinitions.class);
        assertArrayEquals(fixture("values"), definitions.defaults());
        assertEquals(List.of("optional", "record"), definitions.create_names());
        assertEquals(List.of("optional"), definitions.alter_names());
    }

    @Test void layoutsPreserveOrdersAndClearVersusUnchanged() throws Exception {
        SqlExpression expression = roundTrip("expression", SqlExpression.class);
        assertEquals(List.of("one.two", "nested"), expression.schema_path());
        SortKey key = roundTrip("sort_key", SortKey.class);
        assertArrayEquals(fixture("expression"), key.expression());
        assertEquals("DESC", key.direction());
        assertEquals("FIRST", key.null_order());
        TableLayout layout = roundTrip("layout", TableLayout.class);
        assertArrayEquals(fixture("expression"), layout.partition_by().getFirst());
        assertArrayEquals(fixture("sort_key"), layout.sorted_by().getFirst());
        assertEquals("ALL_ROWS", layout.coverage());
        assertEquals(List.of(), roundTrip("empty_layout", TableLayout.class).sorted_by());
        TableLayoutChange change = roundTrip("layout_change", TableLayoutChange.class);
        assertEquals("REWRITE", change.mode());
        assertEquals(List.of(), change.partition_by());
        assertArrayEquals(fixture("sort_key"), change.sorted_by().getFirst());
        TableLayoutChange clear = roundTrip("layout_clear", TableLayoutChange.class);
        assertEquals("FUTURE_WRITES", clear.mode());
        assertNull(clear.partition_by());
        assertEquals(List.of(), clear.sorted_by());
        TableCreateRequest create = roundTrip("table_create", TableCreateRequest.class);
        assertEquals("kept", create.comment());
        assertArrayEquals(fixture("values"), create.options());
        assertArrayEquals(fixture("layout"), create.layout());
        TableCreateRequest old = roundTrip("table_create_legacy", TableCreateRequest.class);
        assertNull(old.comment());
        assertNull(old.tags());
        assertNull(old.options());
        assertNull(old.layout());
        assertEquals(1, create.constraint_definitions().size());
        assertTrue(old.constraint_definitions().isEmpty());
        var info = farm.query.vgi.client.TableInfoDecoder.decode(TableInfoSerializer.serialize(
                farm.query.vgi.client.TableInfoDecoder.decode(fixture("table_info"))));
        assertArrayEquals(new byte[] {114, 101, 118, 105, 115, 105, 111, 110, 0, (byte) 255}, info.definition_revision());
        assertEquals(1, info.constraints().size());
        var constraint = RecordCodec.deserializeFromBytes(info.constraints().getFirst(), farm.query.vgi.protocol.ConstraintInfo.class);
        assertArrayEquals(create.constraint_definitions().getFirst(), constraint.definition());
        assertEquals("fixture-stable-key", RecordCodec.deserializeFromBytes(constraint.ref(), CatalogObjectRef.class).name());
        assertEquals(List.of(List.of("record", "a.b")), RecordCodec.deserializeFromBytes(
                constraint.definition(), farm.query.vgi.protocol.ConstraintDefinition.class).column_paths());
    }
}
