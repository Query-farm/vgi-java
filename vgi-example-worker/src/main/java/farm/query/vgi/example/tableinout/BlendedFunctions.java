// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.tableinout.RowTransformFunction;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Blended ("UNNEST-style") {@link RowTransformFunction} fixtures — positional
 * args ARE the per-row input columns, so one registration serves the literal /
 * column / LATERAL call shapes ({@code table_in_out/blended.test},
 * {@code table_in_out/lateral_batch.test}, {@code cache/exchange_*.test}).
 * Mirrors the vgi-python fixtures in {@code _test_fixtures/table_in_out.py}.
 */
public final class BlendedFunctions {

    private BlendedFunctions() {}

    private static final Schema GEOHASH_SCHEMA = new Schema(List.of(
            new Field("geohash", new FieldType(true, Schemas.UTF8, null), null)));

    /**
     * Python-parity numeric rendering: round to {@code precision} decimals
     * (ties to even, on the exact binary double) then shortest-repr — matches
     * {@code f"{round(v, p)}"} for the fixture's asserted values.
     */
    private static String pyRound(double v, int precision) {
        double rounded = new BigDecimal(v).setScale(precision, RoundingMode.HALF_EVEN).doubleValue();
        return Double.toString(rounded);
    }

    private static void setUtf8(VarCharVector v, int index, String s) {
        if (s == null) { v.setNull(index); return; }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        v.setSafe(index, b, 0, b.length);
    }

    /**
     * {@code geo_encode(latitude DOUBLE, longitude DOUBLE, precision := 4) -> geohash}
     * — the simple blended fixture: one registration serves
     * {@code geo_encode(52.0, 13.0)} (literal), {@code FROM t, geo_encode(t.x, t.y)}
     * (columns), and {@code LATERAL geo_encode(t.x, t.y)}. The positional args
     * are read from the input batch by declared name (the C++ bind builds the
     * input schema from them and casts every call shape to the declared DOUBLE
     * signature); {@code precision} is a named bind-time scalar. Emits one
     * deterministic {@code "<lat>:<lon>"} string per input row.
     */
    public static final class GeoEncodeFunction implements RowTransformFunction {

        private static final FunctionSpec SPEC = FunctionSpec.builder("geo_encode")
                .metadata(FunctionMetadata.describe("Blended per-row geo encoder (lat, lon -> geohash)")
                        .withCategories("geo", "blended"))
                .arg("latitude", Schemas.FLOAT64)
                .arg("longitude", Schemas.FLOAT64)
                .named("precision", Schemas.INT64, "4")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(GEOHASH_SCHEMA));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            int precision = (int) params.arguments().namedLong("precision", 4);
            return new GeoState(precision, /*altitude=*/false);
        }
    }

    /**
     * {@code geo_encode(latitude, longitude, altitude DOUBLE, precision := 4)}
     * — arity-overloaded blended fixture sharing the name {@code geo_encode}
     * with a third positional column. Proves same-name blended overloads
     * resolve by input-column arity: blended args are REAL value types (no
     * TABLE marker), so DuckDB permits multiple overloads.
     */
    public static final class GeoEncode3Function implements RowTransformFunction {

        private static final FunctionSpec SPEC = FunctionSpec.builder("geo_encode")
                .metadata(FunctionMetadata.describe("Blended per-row geo encoder (lat, lon, alt -> geohash)")
                        .withCategories("geo", "blended"))
                .arg("latitude", Schemas.FLOAT64)
                .arg("longitude", Schemas.FLOAT64)
                .arg("altitude", Schemas.FLOAT64)
                .named("precision", Schemas.INT64, "4")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(GEOHASH_SCHEMA));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            int precision = (int) params.arguments().namedLong("precision", 4);
            return new GeoState(precision, /*altitude=*/true);
        }
    }

    /** Shared geo_encode exchange: renders lat[:lon[:alt]] per row, null-propagating. */
    static final class GeoState extends TableInOutExchangeState {
        private final int precision;
        private final boolean altitude;

        GeoState(int precision, boolean altitude) {
            this.precision = precision;
            this.altitude = altitude;
        }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot in = input.root();
            Float8Vector lats = (Float8Vector) in.getVector("latitude");
            Float8Vector lons = (Float8Vector) in.getVector("longitude");
            Float8Vector alts = altitude ? (Float8Vector) in.getVector("altitude") : null;
            int rows = in.getRowCount();
            VectorSchemaRoot outRoot = VectorSchemaRoot.create(GEOHASH_SCHEMA, Allocators.root());
            outRoot.allocateNew();
            VarCharVector geohash = (VarCharVector) outRoot.getVector("geohash");
            for (int i = 0; i < rows; i++) {
                boolean anyNull = lats.isNull(i) || lons.isNull(i)
                        || (alts != null && alts.isNull(i));
                if (anyNull) {
                    geohash.setNull(i);
                    continue;
                }
                String code = pyRound(lats.get(i), precision) + ":" + pyRound(lons.get(i), precision);
                if (alts != null) code += ":" + pyRound(alts.get(i), precision);
                setUtf8(geohash, i, code);
            }
            outRoot.setRowCount(rows);
            out.emit(outRoot);
        }
    }

    /**
     * {@code row_sum(v1, v2, ... DOUBLE, absolute := false) -> row_sum} —
     * blended VARARGS row-wise sum. A varargs blended function has no
     * per-column declared names (the C++ bind names them {@code col0..colN-1}),
     * so the exchange reads the input columns POSITIONALLY. Nulls propagate
     * per-row (any null column value nulls that row's sum); zero columns sum
     * to {@code 0.0}.
     */
    public static final class RowSumFunction implements RowTransformFunction {

        private static final Schema OUTPUT = new Schema(List.of(
                new Field("row_sum", new FieldType(true, Schemas.FLOAT64, null), null)));

        private static final FunctionSpec SPEC = FunctionSpec.builder("row_sum")
                .metadata(FunctionMetadata.describe("Blended per-row varargs sum")
                        .withCategories("numeric", "blended"))
                .arg(new farm.query.vgi.function.ArgSpec("values", 0, Schemas.FLOAT64,
                        "Numeric input columns", false, false, "", List.of(),
                        /*varargs=*/true, false, false))
                .named("absolute", Schemas.BOOL, "false")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            boolean absolute = params.arguments().namedBool("absolute", false);
            return new TableInOutExchangeState() {
                @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                    VectorSchemaRoot in = input.root();
                    List<FieldVector> cols = in.getFieldVectors();
                    int rows = in.getRowCount();
                    VectorSchemaRoot outRoot = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                    outRoot.allocateNew();
                    Float8Vector sums = (Float8Vector) outRoot.getVector("row_sum");
                    for (int i = 0; i < rows; i++) {
                        double acc = 0.0;
                        boolean isNull = false;
                        for (FieldVector col : cols) {
                            if (col.isNull(i)) { isNull = true; break; }
                            double v = ((Float8Vector) col).get(i);
                            acc += absolute ? Math.abs(v) : v;
                        }
                        if (isNull) sums.setNull(i);
                        else sums.setSafe(i, acc);
                    }
                    outRoot.setRowCount(rows);
                    out.emit(outRoot);
                }
            };
        }
    }

    /**
     * {@code blended_drop(x DOUBLE)} — blended 1-&gt;0 map: emits a single
     * 0-row output batch for its input row, exercising the literal scan-mode
     * drain loop's "empty-but-not-EOS" branch (finish cleanly at true EOS, no
     * infinite loop re-feeding the input).
     */
    public static final class BlendedDropFunction implements RowTransformFunction {

        private static final Schema OUTPUT = new Schema(List.of(
                new Field("v", new FieldType(true, Schemas.INT64, null), null)));

        private static final FunctionSpec SPEC = FunctionSpec.builder("blended_drop")
                .metadata(FunctionMetadata.describe(
                                "Blended 1->0 map emitting a single 0-row batch (literal scan-mode)")
                        .withCategories("blended", "test"))
                .arg("x", Schemas.FLOAT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            return new TableInOutExchangeState() {
                @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                    VectorSchemaRoot outRoot = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                    outRoot.setRowCount(0);
                    out.emit(outRoot);
                }
            };
        }
    }

    /**
     * {@code blended_explode(n INT64) -> i} — blended 1-&gt;N fan-out map
     * carrying per-output-row provenance. For each input row with count
     * {@code n}, emits {@code n} rows ({@code 0..n-1}); the emitted batch's
     * {@code vgi_rpc.parent_row#b64} metadata maps every output row back to
     * the input row that produced it, so the batched correlated-LATERAL
     * operator can ship a whole input chunk in one exchange and still stamp
     * each output row's correlated columns. One fixture covers 1-&gt;0
     * ({@code n=0}), 1-&gt;1 ({@code n=1}), and 1-&gt;N ({@code n=3}).
     */
    public static final class BlendedExplodeFunction implements RowTransformFunction {

        private static final Schema OUTPUT = new Schema(List.of(
                new Field("i", new FieldType(true, Schemas.INT64, null), null)));

        private static final FunctionSpec SPEC = FunctionSpec.builder("blended_explode")
                .metadata(FunctionMetadata.describe(
                                "Blended 1->N fan-out (emit 0..n-1 per input row) with row provenance")
                        .withCategories("blended", "test"))
                .arg("n", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            return new TableInOutExchangeState() {
                @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                    VectorSchemaRoot in = input.root();
                    BigIntVector counts = (BigIntVector) in.getVector("n");
                    int rows = in.getRowCount();
                    List<Long> values = new ArrayList<>();
                    List<Integer> parents = new ArrayList<>();
                    for (int row = 0; row < rows; row++) {
                        long fan = counts.isNull(row) ? 0 : Math.max(0, counts.get(row));
                        for (long j = 0; j < fan; j++) {
                            values.add(j);
                            parents.add(row);
                        }
                    }
                    VectorSchemaRoot outRoot = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                    outRoot.allocateNew();
                    BigIntVector iVec = (BigIntVector) outRoot.getVector("i");
                    for (int j = 0; j < values.size(); j++) iVec.setSafe(j, values.get(j));
                    outRoot.setRowCount(values.size());
                    // Whole-chunk fan-out: one emit for the whole input batch,
                    // carrying the per-output-row parent index. (Identity
                    // provenance is omitted for 1->1 maps — the extension
                    // assumes it — but here the row count changes.)
                    int[] parentRows = new int[parents.size()];
                    for (int j = 0; j < parentRows.length; j++) parentRows[j] = parents.get(j);
                    out.emit(outRoot, RowTransformFunction.parentRows(
                            parentRows, values.size(), null));
                }
            };
        }
    }

    /**
     * {@code projectable_blended(x INT64) -> {a, b}} — blended 1-&gt;1 map with
     * TWO output columns ({@code a=x*10}, {@code b=x*100}) advertising
     * projection pushdown. A subset projection under correlated LATERAL makes
     * the batched operator fall back to the row-by-row path (it does not
     * support projection pushdown) — the regression fixture for the silent
     * corruption where {@code SELECT x, b} returned column {@code a}'s value.
     * The exchange emits exactly the (possibly narrowed) output schema.
     */
    public static final class ProjectableBlendedFunction implements RowTransformFunction {

        private static final Schema OUTPUT = new Schema(List.of(
                new Field("a", new FieldType(true, Schemas.INT64, null), null),
                new Field("b", new FieldType(true, Schemas.INT64, null), null)));

        private static final FunctionSpec SPEC = FunctionSpec.builder("projectable_blended")
                .metadata(FunctionMetadata.describe(
                                "Blended 1->1 map with projection_pushdown + two output columns")
                        .withCategories("blended", "test")
                        .withPushdown(true, false, false))
                .arg("x", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            Schema outSchema = params.outputSchema();
            return new TableInOutExchangeState() {
                @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                    VectorSchemaRoot in = input.root();
                    BigIntVector xs = (BigIntVector) in.getVector("x");
                    int rows = in.getRowCount();
                    VectorSchemaRoot outRoot = VectorSchemaRoot.create(outSchema, Allocators.root());
                    outRoot.allocateNew();
                    for (Field f : outSchema.getFields()) {
                        long factor = "a".equals(f.getName()) ? 10 : 100;
                        BigIntVector dst = (BigIntVector) outRoot.getVector(f.getName());
                        for (int i = 0; i < rows; i++) {
                            if (xs.isNull(i)) dst.setNull(i);
                            else dst.setSafe(i, xs.get(i) * factor);
                        }
                    }
                    outRoot.setRowCount(rows);
                    // 1->1 identity map: no provenance needed.
                    out.emit(outRoot);
                }
            };
        }
    }

    /**
     * {@code hostile_provenance(x INT64, mode := 'range') -> hv} — adversarial
     * blended fixture emitting a MALFORMED {@code vgi_rpc.parent_row} payload
     * per {@code mode} ({@code range} / {@code length} / {@code base64}),
     * simulating a buggy or hostile worker. The extension must reject each
     * rather than use the integers as unchecked array indices; asserted on
     * both transports so the subprocess and HTTP validate paths stay
     * symmetric.
     */
    public static final class HostileProvenanceFunction implements RowTransformFunction {

        private static final Schema OUTPUT = new Schema(List.of(
                new Field("hv", new FieldType(true, Schemas.INT64, null), null)));

        private static final FunctionSpec SPEC = FunctionSpec.builder("hostile_provenance")
                .metadata(FunctionMetadata.describe(
                                "Adversarial blended fixture emitting malformed vgi_rpc.parent_row")
                        .withCategories("blended", "test", "adversarial"))
                .arg("x", Schemas.INT64)
                .named("mode", Schemas.UTF8, "range")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            String mode = params.arguments().namedString("mode", "range");
            return new TableInOutExchangeState() {
                @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                    VectorSchemaRoot in = input.root();
                    BigIntVector xs = (BigIntVector) in.getVector("x");
                    int rows = in.getRowCount();
                    VectorSchemaRoot outRoot = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                    outRoot.allocateNew();
                    BigIntVector hv = (BigIntVector) outRoot.getVector("hv");
                    for (int i = 0; i < rows; i++) {
                        if (xs.isNull(i)) hv.setNull(i);
                        else hv.setSafe(i, xs.get(i));
                    }
                    outRoot.setRowCount(rows);
                    String payload;
                    if ("base64".equals(mode)) {
                        payload = "@@@ this is not base64 @@@";
                    } else if ("length".equals(mode)) {
                        // One int32 too many for the emitted row count.
                        payload = Base64.getEncoder().encodeToString(new byte[(rows + 1) * 4]);
                    } else {
                        // "range" — every parent index == rows (one past the last
                        // valid index rows-1). Set via the raw metadata so it
                        // bypasses the helper's length-only check and reaches the
                        // C++ range check unfiltered.
                        ByteBuffer raw = ByteBuffer.allocate(rows * 4).order(ByteOrder.LITTLE_ENDIAN);
                        for (int i = 0; i < rows; i++) raw.putInt(rows);
                        payload = Base64.getEncoder().encodeToString(raw.array());
                    }
                    out.emit(outRoot, Map.of(RowTransformFunction.PARENT_ROW_KEY, payload));
                }
            };
        }
    }
}
