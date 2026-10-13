// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import farm.query.vgi.protocol.OptionChanges;
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
}
