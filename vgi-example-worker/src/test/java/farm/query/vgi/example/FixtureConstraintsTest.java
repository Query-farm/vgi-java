package farm.query.vgi.example;

import farm.query.vgi.protocol.ConstraintDefinition;
import farm.query.vgi.protocol.ConstraintInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import static org.junit.jupiter.api.Assertions.*;
import farm.query.vgi.protocol.*;

class FixtureConstraintsTest {
    @Test void readsOnlySelectOfferedProfilesAndTheirPrerequisite() {
        String read = "vgi.constraint.records.read.v1";
        String scalar = "vgi.expression.duckdb.scalar.v1";
        for (var offered : List.of(List.<String>of(), List.of(scalar), List.of(read), List.of(read, scalar))) {
            var offer = new CatalogCapabilities(offered, List.of(), List.of("vgi.constraint.enforce.v1"), List.of());
            var client = new ClientCapabilities("fixture-test", List.of(), List.of(), true, List.of(), RecordCodec.serializeToBytes(offer));
            var request = new CatalogAttachRequest("example", null, null, null, RecordCodec.serializeToBytes(client));
            var selected = RecordCodec.deserializeFromBytes(FixtureConstraints.negotiate(request).selected(), CatalogCapabilities.class);
            assertEquals(offered.contains(read) ? offered : List.of(), selected.read());
            assertTrue(selected.write().isEmpty());
            assertTrue(selected.ddl().isEmpty());
            assertTrue(selected.transaction().isEmpty());
        }
        assertNull(FixtureConstraints.negotiate(CatalogAttachRequest.of("other", null, null, null)).selected());
    }

    @Test void invalidImmutableDataCannotBecomeTrustedFacts() {
        var missing = new HashMap<String, Object>();
        missing.put("id", null);
        var notNull = new ConstraintDefinition("NOT_NULL", List.of(List.of("id")), null, null, null);
        assertThrows(IllegalArgumentException.class, () -> FixtureConstraints.validate(List.of(missing), notNull, List.of()));
        Map<String, Object> row = Map.of("id", 1L, "budget", -1.0, "department_id", 7L);
        assertThrows(IllegalArgumentException.class, () -> FixtureConstraints.validate(List.of(row, row),
                new ConstraintDefinition("PRIMARY_KEY", List.of(List.of("id")), null, null, null), List.of()));
        assertThrows(IllegalArgumentException.class, () -> FixtureConstraints.validate(List.of(row),
                new ConstraintDefinition("FOREIGN_KEY", List.of(List.of("department_id")), null, null, null), List.of()));
        var info = RecordCodec.deserializeFromBytes(FixtureConstraints.check("departments", "check", "budget >= 0"), ConstraintInfo.class);
        var check = RecordCodec.deserializeFromBytes(info.definition(), ConstraintDefinition.class);
        assertThrows(IllegalArgumentException.class, () -> FixtureConstraints.validate(List.of(row), check, List.of()));
    }
}
