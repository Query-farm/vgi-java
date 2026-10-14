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

    private static byte[] fixture(String name) throws Exception {
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
    }
}
