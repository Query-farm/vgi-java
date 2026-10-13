// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import farm.query.vgi.protocol.OptionChanges;
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
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CatalogContractsTest {
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
