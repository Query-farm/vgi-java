// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code duckdb.spatial/intersects_extent@1} must agree with DuckDB core's {@code &&} on every
 * case of the generated conformance corpus ({@code conformance/intersects_extent_cases.json}),
 * for all four little/big-endian encoding combinations. The predicate is decoded from a real v2
 * filter payload and evaluated through {@link FilterApplier}, the same path scans use per batch.
 */
final class IntersectsExtentConformanceTest {
    private static final FilterIdentity INTERSECTS_EXTENT =
            new FilterIdentity("duckdb.spatial", "intersects_extent", 1);
    private static final PushdownFiltersDecoder.Capabilities CAPABILITIES =
            new PushdownFiltersDecoder.Capabilities(Set.of(INTERSECTS_EXTENT), Set.of(), Map.of());
    private static final Map<String, String> WKB = Map.of(
            "ARROW:extension:name", "geoarrow.wkb", "ARROW:extension:metadata", "{}");
    private static final Field GEOM = new Field("geom",
            new FieldType(true, new ArrowType.Binary(), null, WKB), null);
    private static final Schema SCHEMA = new Schema(List.of(GEOM));
    private static final String[] ENCODINGS = {"wkb_le", "wkb_be"};

    @Test
    void matchesDuckDbCoreOnEveryCase() throws Exception {
        JsonNode doc;
        try (InputStream in = getClass().getResourceAsStream("/conformance/intersects_extent_cases.json")) {
            doc = new ObjectMapper().readTree(in);
        }
        Map<String, JsonNode> geometries = new LinkedHashMap<>();
        for (JsonNode g : doc.get("geometries")) geometries.put(g.get("name").asText(), g);
        List<String> names = new ArrayList<>(geometries.keySet());
        Map<String, Boolean> expected = new LinkedHashMap<>();
        for (JsonNode c : doc.get("cases")) {
            expected.put(c.get("left").asText() + "|" + c.get("right").asText(),
                    c.get("expected").asBoolean());
        }
        assertEquals(names.size() * names.size(), expected.size());

        List<String> mismatches = new ArrayList<>();
        int checked = 0;
        for (String rightEncoding : ENCODINGS) {
            for (String right : names) {
                byte[] constant = hex(geometries.get(right), rightEncoding);
                FilterApplier applier = applier("required", constant);
                for (String leftEncoding : ENCODINGS) {
                    List<byte[]> rows = new ArrayList<>();
                    for (String left : names) rows.add(hex(geometries.get(left), leftEncoding));
                    boolean[] mask = evaluate(applier, rows);
                    for (int i = 0; i < names.size(); i++) {
                        boolean want = expected.get(names.get(i) + "|" + right);
                        checked++;
                        if (mask[i] != want) {
                            mismatches.add(names.get(i) + "(" + leftEncoding + ") && " + right
                                    + "(" + rightEncoding + "): expected " + want + ", got " + mask[i]);
                        }
                    }
                }
            }
        }
        assertEquals(4 * expected.size(), checked);
        assertTrue(mismatches.isEmpty(),
                mismatches.size() + " mismatches:\n" + String.join("\n", mismatches));
    }

    @Test
    void nullGeometryIsNotSelected() {
        byte[] unit = HexFormat.of().parseHex(
                "0103000000010000000500000000000000000000000000000000000000000000000000f03f"
                        + "0000000000000000000000000000f03f000000000000f03f0000000000000000"
                        + "000000000000f03f00000000000000000000000000000000");
        assertArrayEquals(new boolean[] {false},
                evaluate(applier("required", unit), java.util.Collections.singletonList(null)));
    }

    @Test
    void undecodableRowValueKeepsRowsForAdvisoryAndFailsRequired() {
        byte[] point = HexFormat.of().parseHex("0101000000000000000000e03f000000000000e03f");
        byte[] far = HexFormat.of().parseHex("010100000000000000000014400000000000001440");
        // EWKB point with the SRID flag (0x20000000) and SRID 4326.
        byte[] ewkb = HexFormat.of().parseHex("0101000020e6100000000000000000e03f000000000000e03f");
        List<byte[]> bad = List.of(
                new byte[] {1, 2, (byte) 0xff},                                  // truncated
                HexFormat.of().parseHex("0108000000"),                           // unsupported type 8
                HexFormat.of().parseHex("020100000000000000000000000000000000000000"), // bad byte order
                ewkb);
        for (byte[] value : bad) {
            List<byte[]> rows = List.of(far, value, point);
            assertArrayEquals(new boolean[] {true, true, true},
                    evaluate(applier("advisory", point), rows),
                    "advisory failure must keep rows: " + HexFormat.of().formatHex(value));
            assertThrows(FilterV2Exception.class, () -> evaluate(applier("required", point), rows));
        }
        // A malformed constant is the same kind of failure.
        assertArrayEquals(new boolean[] {true, true},
                evaluate(applier("advisory", new byte[] {1}), List.of(point, far)));
        assertThrows(FilterV2Exception.class,
                () -> evaluate(applier("required", new byte[] {1}), List.of(point, far)));
    }

    @Test
    void advisoryPredicatesAreAppliedByFilterApplier() {
        byte[] point = HexFormat.of().parseHex("0101000000000000000000e03f000000000000e03f");
        byte[] far = HexFormat.of().parseHex("010100000000000000000014400000000000001440");
        assertArrayEquals(new boolean[] {true, false},
                evaluate(applier("advisory", point), List.of(point, far)));
    }

    private static FilterApplier applier(String mode, byte[] constant) {
        String expression = "{\"node\":\"call\",\"function\":{"
                + "\"namespace\":\"duckdb.spatial\",\"name\":\"intersects_extent\",\"version\":1},"
                + "\"arguments\":[{\"node\":\"column_ref\",\"column_index\":0,\"column_name\":\"geom\"},"
                + "{\"node\":\"literal\",\"value_ref\":0}]}";
        String snapshot = "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\","
                + "\"kind\":\"snapshot\",\"predicates\":[{\"id\":\"spatial\",\"revision\":0,\"mode\":\""
                + mode + "\",\"source\":\"query\",\"expression\":" + expression + "}]}";
        Field literal = new Field("value_0", new FieldType(true, new ArrowType.Binary(), null, WKB), null);
        return FilterApplier.from(payload(snapshot, literal, constant), List.of(), SCHEMA, CAPABILITIES);
    }

    private static boolean[] evaluate(FilterApplier applier, List<byte[]> rows) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, Allocators.root())) {
            root.allocateNew();
            VarBinaryVector geom = (VarBinaryVector) root.getVector(0);
            for (int i = 0; i < rows.size(); i++) {
                if (rows.get(i) == null) geom.setNull(i);
                else geom.setSafe(i, rows.get(i));
            }
            geom.setValueCount(rows.size());
            root.setRowCount(rows.size());
            return applier.current().evaluate(root);
        }
    }

    private static byte[] payload(String json, Field literal, byte[] value) {
        Field spec = new Field("filter_spec", FieldType.notNullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(List.of(spec, literal), Map.of(
                "vgi_filter_encoding", "vgi.filters.v2",
                "vgi_filter_version", "2",
                "vgi_evaluation_context", "vgi.none.v1"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            VectorScalarCodec.write(root.getVector(0), 0, json);
            VectorScalarCodec.write(root.getVector(1), 0, value);
            for (var vector : root.getFieldVectors()) vector.setValueCount(1);
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    private static byte[] hex(JsonNode geometry, String encoding) {
        return HexFormat.of().parseHex(geometry.get(encoding).asText());
    }
}
