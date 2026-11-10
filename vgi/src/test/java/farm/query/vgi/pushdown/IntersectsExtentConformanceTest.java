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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

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
        runCorpus(bytes -> bytes);
    }

    @Test
    void ewkbMatchesDuckDbCoreOnEveryCase() throws Exception {
        // Every corpus geometry re-encoded as EWKB: Z/M as high-bit flags, an SRID on the
        // outermost geometry only, and then on every (sub-)geometry.
        runCorpus(bytes -> toEwkb(bytes, false));
        runCorpus(bytes -> toEwkb(bytes, true));
    }

    @Test
    void ewkbPointWithSridAndPolygonWithZAndSridMatchIsoEquivalents() throws Exception {
        Corpus corpus = corpus();
        // POINT(0.5 0.5) as EWKB with SRID 4326, written by hand in both byte orders.
        byte[] pointLe = HexFormat.of().parseHex("0101000020e6100000000000000000e03f000000000000e03f");
        byte[] pointBe = HexFormat.of().parseHex("0020000001000010e63fe00000000000003fe0000000000000");
        // POLYGON Z ((0 0 1, 1 0 2, 1 1 3, 0 1 4, 0 0 1)), SRID 4326 = poly_unit with Z.
        double[][] ring = {{0, 0, 1}, {1, 0, 2}, {1, 1, 3}, {0, 1, 4}, {0, 0, 1}};
        for (ByteOrder order : List.of(ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN)) {
            ByteBuffer polygon = ByteBuffer.allocate(1 + 4 + 4 + 4 + 4 + ring.length * 24).order(order);
            polygon.put((byte) (order == ByteOrder.LITTLE_ENDIAN ? 1 : 0));
            polygon.putInt(0x80000000 | 0x20000000 | 3).putInt(4326).putInt(1).putInt(ring.length);
            for (double[] v : ring) polygon.putDouble(v[0]).putDouble(v[1]).putDouble(v[2]);
            assertMatchesIso(corpus, "poly_unit", polygon.array());
        }
        assertMatchesIso(corpus, "pt_half", pointLe);
        assertMatchesIso(corpus, "pt_half", pointBe);
    }

    /** {@code ewkb} must behave exactly like corpus geometry {@code iso} on either side of &&. */
    private static void assertMatchesIso(Corpus corpus, String iso, byte[] ewkb) {
        for (String encoding : ENCODINGS) {
            List<byte[]> rows = new ArrayList<>();
            for (String left : corpus.names()) rows.add(hex(corpus.geometries().get(left), encoding));
            boolean[] asConstant = evaluate(applier("required", ewkb), rows);
            boolean[] asRow = new boolean[rows.size()];
            for (int i = 0; i < rows.size(); i++) {
                asRow[i] = evaluate(applier("required", rows.get(i)), List.of(ewkb))[0];
            }
            boolean[] want = new boolean[rows.size()];
            for (int i = 0; i < rows.size(); i++) {
                want[i] = corpus.expected().get(corpus.names().get(i) + "|" + iso);
            }
            assertArrayEquals(want, asConstant, "EWKB " + iso + " as constant (" + encoding + ")");
            assertArrayEquals(want, asRow, "EWKB " + iso + " as row (" + encoding + ")");
        }
    }

    private record Corpus(Map<String, JsonNode> geometries, List<String> names,
                          Map<String, Boolean> expected) {}

    private static Corpus corpus() throws Exception {
        JsonNode doc;
        try (InputStream in = IntersectsExtentConformanceTest.class
                .getResourceAsStream("/conformance/intersects_extent_cases.json")) {
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
        return new Corpus(geometries, names, expected);
    }

    private static void runCorpus(UnaryOperator<byte[]> encode) throws Exception {
        Corpus corpus = corpus();
        Map<String, JsonNode> geometries = corpus.geometries();
        List<String> names = corpus.names();
        Map<String, Boolean> expected = corpus.expected();

        List<String> mismatches = new ArrayList<>();
        int checked = 0;
        for (String rightEncoding : ENCODINGS) {
            for (String right : names) {
                byte[] constant = encode.apply(hex(geometries.get(right), rightEncoding));
                FilterApplier applier = applier("required", constant);
                for (String leftEncoding : ENCODINGS) {
                    List<byte[]> rows = new ArrayList<>();
                    for (String left : names) rows.add(encode.apply(hex(geometries.get(left), leftEncoding)));
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
        List<byte[]> bad = List.of(
                new byte[] {1, 2, (byte) 0xff},                                  // truncated
                HexFormat.of().parseHex("0108000000"),                           // unsupported type 8
                HexFormat.of().parseHex("020100000000000000000000000000000000000000"), // bad byte order
                // EWKB Z flag on an ISO Z type (1001): two Z conventions at once.
                HexFormat.of().parseHex("01e9030080000000000000e03f000000000000e03f0000000000000000"),
                // EWKB point with the SRID flag but no room for the SRID.
                HexFormat.of().parseHex("0101000020e610"));
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

    /** Re-encode ISO WKB as EWKB in the same byte order(s): Z/M flags, SRID 4326. */
    private static byte[] toEwkb(byte[] iso, boolean sridOnEveryPart) {
        ByteBuffer in = ByteBuffer.wrap(iso);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        copyAsEwkb(in, out, sridOnEveryPart, true);
        assertEquals(0, in.remaining());
        return out.toByteArray();
    }

    private static void copyAsEwkb(ByteBuffer in, java.io.ByteArrayOutputStream out,
                                   boolean sridOnEveryPart, boolean outermost) {
        byte order = in.get();
        in.order(order == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        int type = in.getInt();
        int base = type % 1000;
        int dims = type / 1000;
        boolean srid = outermost || sridOnEveryPart;
        int flags = (dims == 1 || dims == 3 ? 0x80000000 : 0) | (dims == 2 || dims == 3 ? 0x40000000 : 0)
                | (srid ? 0x20000000 : 0);
        ByteBuffer header = ByteBuffer.allocate(9).order(in.order());
        header.put(order).putInt(base | flags);
        if (srid) header.putInt(4326);
        out.write(header.array(), 0, header.position());
        int width = 2 + (dims == 0 ? 0 : dims == 3 ? 2 : 1);
        switch (base) {
            case 1 -> copy(in, out, 8 * width);
            case 2 -> copy(in, out, 8 * width * copyCount(in, out));
            case 3 -> {
                for (int r = copyCount(in, out); r > 0; r--) copy(in, out, 8 * width * copyCount(in, out));
            }
            default -> {
                for (int n = copyCount(in, out); n > 0; n--) {
                    copyAsEwkb(in, out, sridOnEveryPart, false);
                }
            }
        }
    }

    private static int copyCount(ByteBuffer in, java.io.ByteArrayOutputStream out) {
        int n = in.getInt(in.position());
        copy(in, out, 4);
        return n;
    }

    private static void copy(ByteBuffer in, java.io.ByteArrayOutputStream out, int length) {
        out.write(in.array(), in.position(), length);
        in.position(in.position() + length);
    }

    private static byte[] hex(JsonNode geometry, String encoding) {
        return HexFormat.of().parseHex(geometry.get(encoding).asText());
    }
}
