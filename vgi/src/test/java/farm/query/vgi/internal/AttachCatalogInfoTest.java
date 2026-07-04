// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.catalog.ScanBranch;
import farm.query.vgi.protocol.AttachCatalogInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip coverage for the companion-catalog (lakehouse federation) wire
 * additions: {@link AttachCatalogInfo} and the catalog-table {@link ScanBranch}
 * source fields.
 */
class AttachCatalogInfoTest {

    @Test
    void attachCatalogInfoRoundTrips() {
        AttachCatalogInfo info = new AttachCatalogInfo(
                "acme_lake",
                "ducklake:sqlite:/data/meta.sqlite",
                "",
                Map.of("DATA_PATH", "/data/"),
                true,
                true,
                "pg");
        byte[] bytes = RecordCodec.serializeToBytes(info);
        AttachCatalogInfo back = RecordCodec.deserializeFromBytes(bytes, AttachCatalogInfo.class);

        assertEquals("acme_lake", back.alias());
        assertEquals("ducklake:sqlite:/data/meta.sqlite", back.target());
        assertEquals("", back.db_type());
        assertEquals("/data/", back.options().get("DATA_PATH"));
        assertTrue(back.hidden());
        assertTrue(back.required());
        assertEquals("pg", back.secret_ref());
    }

    @Test
    void catalogTableScanBranchSerializes() {
        // function_name="" + sourceTable set: a catalog-table branch. The
        // constructor must accept it (empty function name is allowed here).
        ScanBranch b = ScanBranch.catalogTable("acme_lake", "main", "events", "id < 100");
        assertEquals("", b.functionName());
        assertEquals("events", b.sourceTable());
        assertFalse(b.writable());

        byte[] wire = ScanBranchesResultSerializer.serialize(List.of(b), List.of());
        assertTrue(wire.length > 0, "serialized branches should be non-empty");
    }
}
