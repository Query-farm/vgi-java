// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The example worker's spatial filter step ({@link SpatialFilterExampleFunction#applyFilters})
 * must keep exactly the rows DuckDB core's {@code &&} keeps, on every case of the conformance
 * corpus and for all four little/big-endian encoding combinations: the derived
 * {@code intersects_extent} filter must never drop a row DuckDB would keep.
 */
final class SpatialFilterConformanceTest {
    private static final String[] ENCODINGS = {"wkb_le", "wkb_be"};

    @Test
    void spatialFilterStepMatchesDuckDbCore() throws Exception {
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

        List<String> mismatches = new ArrayList<>();
        int checked = 0;
        for (String mode : List.of("required", "advisory")) {
            for (String rightEncoding : ENCODINGS) {
                for (String right : names) {
                    byte[] filter = filter(mode, hex(geometries.get(right), rightEncoding));
                    for (String leftEncoding : ENCODINGS) {
                        Set<Long> kept = run(filter, names.stream()
                                .map(n -> hex(geometries.get(n), leftEncoding)).toList());
                        for (int i = 0; i < names.size(); i++) {
                            boolean want = expected.get(names.get(i) + "|" + right);
                            boolean got = kept.contains((long) i);
                            checked++;
                            if (got != want) {
                                mismatches.add(mode + ": " + names.get(i) + "(" + leftEncoding + ") && "
                                        + right + "(" + rightEncoding + "): expected " + want
                                        + ", got " + got);
                            }
                        }
                    }
                }
            }
        }
        assertEquals(2 * 4 * expected.size(), checked);
        assertTrue(mismatches.isEmpty(),
                mismatches.size() + " mismatches:\n" + String.join("\n", mismatches));
    }

    private static Set<Long> run(byte[] filter, List<byte[]> rows) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SpatialFilterExampleFunction.OUTPUT_SCHEMA,
                Allocators.root());
        root.allocateNew();
        BigIntVector n = (BigIntVector) root.getVector("n");
        VarBinaryVector geom = (VarBinaryVector) root.getVector("geom");
        for (int i = 0; i < rows.size(); i++) {
            n.setSafe(i, i);
            root.getVector("x").setNull(i);
            root.getVector("y").setNull(i);
            geom.setSafe(i, rows.get(i));
        }
        for (var v : root.getFieldVectors()) v.setValueCount(rows.size());
        root.setRowCount(rows.size());
        try (VectorSchemaRoot out = SpatialFilterExampleFunction.applyFilters(root, filter, List.of())) {
            Set<Long> kept = new HashSet<>();
            BigIntVector survivors = (BigIntVector) out.getVector("n");
            for (int i = 0; i < out.getRowCount(); i++) kept.add(survivors.get(i));
            return kept;
        }
    }

    private static byte[] filter(String mode, byte[] constant) {
        String expression = "{\"node\":\"call\",\"function\":{"
                + "\"namespace\":\"duckdb.spatial\",\"name\":\"intersects_extent\",\"version\":1},"
                + "\"arguments\":[{\"node\":\"column_ref\",\"column_index\":3,\"column_name\":\"geom\"},"
                + "{\"node\":\"literal\",\"value_ref\":0}]}";
        String json = "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\","
                + "\"kind\":\"snapshot\",\"predicates\":[{\"id\":\"spatial\",\"revision\":0,\"mode\":\""
                + mode + "\",\"source\":\"query\",\"expression\":" + expression + "}]}";
        Field spec = new Field("filter_spec", FieldType.notNullable(new ArrowType.Utf8()), null);
        Field literal = new Field("value_0", new FieldType(true, new ArrowType.Binary(), null,
                Map.of("ARROW:extension:name", "geoarrow.wkb", "ARROW:extension:metadata", "{}")), null);
        Schema schema = new Schema(List.of(spec, literal), Map.of(
                "vgi_filter_encoding", "vgi.filters.v2",
                "vgi_filter_version", "2",
                "vgi_evaluation_context", "vgi.none.v1"));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            VectorScalarCodec.write(root.getVector(0), 0, json);
            VectorScalarCodec.write(root.getVector(1), 0, constant);
            for (var vector : root.getFieldVectors()) vector.setValueCount(1);
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    private static byte[] hex(JsonNode geometry, String encoding) {
        return HexFormat.of().parseHex(geometry.get(encoding).asText());
    }
}
