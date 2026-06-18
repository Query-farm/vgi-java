// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * {@code typed_probe(n, ts := …, iv := …, blob := …, ub := …, f := …)} —
 * exercises typed const-argument binding (TIMESTAMPTZ, INTERVAL, BLOB,
 * UBIGINT, DOUBLE, each with a default) and typed column emit. Echoes the
 * resolved consts in normalized integer/byte form into uint64 / int64 / blob /
 * double columns, byte-identical to the vgi-go / vgi-python fixtures.
 */
public final class TypedProbeFunction extends SimpleTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("idx", Schemas.UINT64),
            Schemas.nullable("ts_us", Schemas.INT64),
            Schemas.nullable("iv_ms", Schemas.INT64),
            Schemas.nullable("payload", Schemas.BINARY),
            Schemas.nullable("ub", Schemas.UINT64),
            Schemas.nullable("f", Schemas.FLOAT64));

    // Defaults: ts=2026-01-02T03:04:05Z, iv=1500ms, blob='vgi', ub=9, f=2.5.
    private static final long DEFAULT_TS_US = 1767323045000000L;
    private static final long DEFAULT_IV_MS = 1500L;
    private static final byte[] DEFAULT_BLOB = "vgi".getBytes(StandardCharsets.UTF_8);
    private static final long DEFAULT_UB = 9L;
    private static final double DEFAULT_F = 2.5;

    @Override public String name() { return "typed_probe"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(
                "Echoes typed const args (timestamp/interval/blob/ubigint) into typed columns");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                ArgSpec.positional("n", 0, Schemas.INT64),
                ArgSpec.named("ts", Schemas.timestampMicros("UTC"), ""),
                ArgSpec.named("iv", new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO), ""),
                ArgSpec.named("blob", Schemas.BINARY, ""),
                ArgSpec.named("ub", Schemas.UINT64, ""),
                ArgSpec.named("f", Schemas.FLOAT64, ""));
    }

    @Override public long cardinality(TableBindParams p) {
        Object c = p.arguments().positionalAt(0);
        return c instanceof Number n ? n.longValue() : -1L;
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        Arguments a = params.arguments();
        ParameterExtractor p = ParameterExtractor.of(a);
        long n = p.positional(0, "n").asLong().orElse(0L);
        long tsUs = a.named().containsKey("ts") ? toMicros(a.named().get("ts")) : DEFAULT_TS_US;
        long ivMs = a.named().containsKey("iv") ? toMillis(a.named().get("iv")) : DEFAULT_IV_MS;
        Object blob = a.named().get("blob");
        byte[] payload = blob instanceof byte[] b ? b : DEFAULT_BLOB;
        long ub = a.named().containsKey("ub") ? ((Number) a.named().get("ub")).longValue() : DEFAULT_UB;
        double f = p.named("f").asDouble().orElse(DEFAULT_F);
        return new State(n, tsUs, ivMs, payload, ub, f);
    }

    private static long toMicros(Object ts) {
        if (ts instanceof Number num) return num.longValue();
        if (ts instanceof Instant i) return i.getEpochSecond() * 1_000_000L + i.getNano() / 1000L;
        if (ts instanceof ZonedDateTime z) return z.toEpochSecond() * 1_000_000L + z.getNano() / 1000L;
        if (ts instanceof LocalDateTime ldt) {
            return ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + ldt.getNano() / 1000L;
        }
        return DEFAULT_TS_US;
    }

    private static long toMillis(Object iv) {
        if (iv instanceof PeriodDuration pd) {
            long months = pd.getPeriod().toTotalMonths();
            long days = pd.getPeriod().getDays();
            long nanos = pd.getDuration().toNanos();
            return months * 30L * 24 * 3600 * 1000
                    + days * 24L * 3600 * 1000
                    + nanos / 1_000_000L;
        }
        if (iv instanceof Number n) return n.longValue();
        return DEFAULT_IV_MS;
    }

    public static final class State extends TableProducerState {
        public long n;
        public long tsUs;
        public long ivMs;
        public byte[] payload;
        public long ub;
        public double f;
        public boolean done;

        public State() {}

        State(long n, long tsUs, long ivMs, byte[] payload, long ub, double f) {
            this.n = n;
            this.tsUs = tsUs;
            this.ivMs = ivMs;
            this.payload = payload;
            this.ub = ub;
            this.f = f;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done || n <= 0) { out.finish(); return; }
            done = true;
            int rows = (int) n;
            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            UInt8Vector idx = (UInt8Vector) work.getVector("idx");
            BigIntVector tsv = (BigIntVector) work.getVector("ts_us");
            BigIntVector ivv = (BigIntVector) work.getVector("iv_ms");
            VarBinaryVector pv = (VarBinaryVector) work.getVector("payload");
            UInt8Vector ubv = (UInt8Vector) work.getVector("ub");
            Float8Vector fv = (Float8Vector) work.getVector("f");
            for (int i = 0; i < rows; i++) {
                idx.setSafe(i, i);
                tsv.setSafe(i, tsUs);
                ivv.setSafe(i, ivMs);
                pv.setSafe(i, payload);
                ubv.setSafe(i, ub);
                fv.setSafe(i, f + i);
            }
            work.setRowCount(rows);
            out.emit(work);
        }
    }
}
