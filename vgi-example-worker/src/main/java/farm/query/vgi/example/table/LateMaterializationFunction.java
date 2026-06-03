// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.pushdown.ComparisonOperator;
import farm.query.vgi.pushdown.PushdownFilter;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import farm.query.vgi.internal.BatchUtil;

/**
 * {@code late_materialization(count [, batch_size, dup_row_id, null_ord_stride])}
 * — a rowid-bearing generator that opts into DuckDB's late-materialization
 * optimizer. Mirrors vgi-python's {@code LateMaterializationFunction}.
 *
 * <p>Output schema {@code (row_id int64 [is_row_id], ord int64, payload utf8,
 * pushed utf8)}:
 * <ul>
 *   <li>{@code row_id} == the row index — unique, deterministic, and
 *       snapshot-stable, so a rowid the ordering scan emits resolves to the same
 *       logical row in the (independent) wide scan. Satisfies the
 *       late-materialization worker contract.</li>
 *   <li>{@code ord} is a <em>scrambled</em> function of the index, so a Top-N on
 *       {@code ord} yields scattered survivor rowids (the IN-list pushdown path).</li>
 *   <li>{@code payload} is the wide column the rewrite avoids materializing.</li>
 *   <li>{@code pushed} is the <em>witness</em>: it echoes the rowid filter the
 *       worker received ({@code in=<n>} join keys, {@code rng=<lo>..<hi>} bounds),
 *       proving the pushdown reached the wide scan.</li>
 * </ul>
 *
 * <p>Correctness is enforced by the SEMI join the rewrite plants over the wide
 * scan, so the fixture emits <em>every</em> row (the SEMI join keeps only the
 * survivors); it does not filter worker-side. {@code dup_row_id=true} emits a
 * deliberately non-unique {@code row_id} (index/2) to back the contract-violation
 * test; {@code null_ord_stride>0} injects NULLs into {@code ord}.
 */
public final class LateMaterializationFunction extends CountdownTableFunction {

    private static final String ROWID = "row_id";
    private static final long SCRAMBLE = 2654435761L;
    private static final long MOD = 1_000_000_007L;
    private static final String EMPTY_WITNESS = "rid:in=0;rng=none";

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            rowIdField(),
            Schemas.nullable("ord", Schemas.INT64),
            Schemas.nullable("payload", Schemas.UTF8),
            Schemas.nullable("pushed", Schemas.UTF8)));

    /** {@code row_id} marked with the {@code is_row_id} virtual-column tag (the
     *  late-materialization gate looks for it on both the table and the scan
     *  function's output schema). */
    private static Field rowIdField() {
        return new Field(ROWID,
                new FieldType(false, Schemas.INT64, null, Map.of("is_row_id", "true")),
                null);
    }

    @Override public String name() { return "late_materialization"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(
                "Rowid generator that participates in late materialization")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                .withLateMaterialization()
                .withCategories("generator", "diagnostic");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override protected long defaultBatchSize() { return 2048L; }

    @Override protected List<ArgSpec> extraArgs() {
        return List.of(
                ArgSpec.named("dup_row_id", Schemas.BOOL, "false"),
                ArgSpec.named("null_ord_stride", Schemas.INT64, "0"));
    }

    @Override public List<farm.query.vgi.catalog.ColumnStatistics> statistics(
            farm.query.vgi.table.TableBindParams params) {
        // Multi-column schema — the countdown default would return null; keep it
        // explicit so the cardinality (count) still flows but no per-column stats.
        return null;
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().orElse(2048L);
        boolean dupRowId = p.named("dup_row_id").asBool().orElse(false);
        long nullOrdStride = p.named("null_ord_stride").asLong().orElse(0L);
        List<byte[]> joinKeys = params.joinKeys() == null ? List.of() : params.joinKeys();
        String witness = computeWitness(params.pushdownFilters(), joinKeys);
        return new State(params.outputSchema(), count, batchSize, dupRowId, nullOrdStride,
                joinKeys, witness);
    }

    /** Decode the pushed filters (init or per-tick) and summarize the rowid
     *  filter the worker received. Returns {@link #EMPTY_WITNESS} when none. */
    private static String computeWitness(byte[] filterBytes, List<byte[]> joinKeys) {
        if (filterBytes == null) return EMPTY_WITNESS;
        PushdownFilters pf = PushdownFiltersDecoder.decode(
                filterBytes, joinKeys == null ? List.of() : joinKeys);
        return witnessOf(pf);
    }

    private static String witnessOf(PushdownFilters pf) {
        Acc acc = new Acc();
        for (PushdownFilter f : pf.filters()) walk(f, acc);
        String rng = (acc.lo != null || acc.hi != null)
                ? String.valueOf(acc.lo) + ".." + String.valueOf(acc.hi)
                : "none";
        return "rid:in=" + acc.in + ";rng=" + rng;
    }

    private static void walk(PushdownFilter f, Acc acc) {
        switch (f) {
            case PushdownFilter.And a -> { for (PushdownFilter c : a.children()) walk(c, acc); }
            case PushdownFilter.Or o -> { for (PushdownFilter c : o.children()) walk(c, acc); }
            case PushdownFilter.In in -> {
                if (ROWID.equals(in.columnName())) acc.in += in.values().size();
            }
            case PushdownFilter.Constant c -> {
                if (!ROWID.equals(c.columnName()) || !(c.value() instanceof Number n)) break;
                long v = n.longValue();
                switch (c.op()) {
                    case GT, GE -> acc.lo = acc.lo == null ? v : Math.min(acc.lo, v);
                    case LT, LE -> acc.hi = acc.hi == null ? v : Math.max(acc.hi, v);
                    case EQ -> { acc.lo = v; acc.hi = v; }
                    default -> { }
                }
            }
            default -> { }
        }
    }

    private static final class Acc {
        long in = 0;
        Long lo = null;
        Long hi = null;
    }

    public static final class State extends TableProducerState {
        public Schema projected;
        public long count;
        public long batchSize;
        public boolean dupRowId;
        public long nullOrdStride;
        public List<byte[]> joinKeysIpc;
        public String witness;
        public long index;

        public State() {}

        State(Schema projected, long count, long batchSize, boolean dupRowId, long nullOrdStride,
                List<byte[]> joinKeysIpc, String witness) {
            this.projected = projected;
            this.count = count;
            this.batchSize = batchSize;
            this.dupRowId = dupRowId;
            this.nullOrdStride = nullOrdStride;
            this.joinKeysIpc = joinKeysIpc;
            this.witness = witness;
            this.index = 0;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            emitNextBatch(out);
        }

        @Override
        public void produceTick(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            // The surviving-rowid filter for the probe scan arrives as a dynamic
            // per-tick filter (populated after the SEMI join's build side
            // completes). Latch any non-empty witness so later ticks keep it.
            Map<String, String> meta = input == null ? Map.of() : input.customMetadata();
            String encoded = meta == null ? null : meta.get("vgi_pushdown_filters");
            if (encoded != null && !encoded.isEmpty()) {
                try {
                    byte[] bytes = Base64.getDecoder().decode(encoded);
                    String tick = computeWitness(bytes, joinKeysIpc);
                    if (!EMPTY_WITNESS.equals(tick) || EMPTY_WITNESS.equals(witness)) {
                        witness = tick;
                    }
                } catch (Exception ignore) {
                    // Best-effort: keep the prior witness on decode error.
                }
            }
            emitNextBatch(out);
        }

        private void emitNextBatch(OutputCollector out) {
            if (index >= count) { out.finish(); return; }
            int n = (int) Math.min(batchSize, count - index);
            long start = index;
            BatchUtil.emit(projected, n, out, (root, rows, ignored) -> {
                BigIntVector rid = (BigIntVector) root.getVector(ROWID);
                BigIntVector ord = (BigIntVector) root.getVector("ord");
                VarCharVector payload = (VarCharVector) root.getVector("payload");
                VarCharVector pushed = (VarCharVector) root.getVector("pushed");
                Text witnessText = pushed == null ? null : new Text(witness);
                for (int i = 0; i < rows; i++) {
                    long idx = start + i;
                    if (rid != null) rid.setSafe(i, dupRowId ? idx / 2 : idx);
                    if (ord != null) {
                        if (nullOrdStride > 0 && idx % nullOrdStride == 0) ord.setNull(i);
                        else ord.setSafe(i, (idx * SCRAMBLE) % MOD);
                    }
                    if (payload != null) payload.setSafe(i, new Text("payload_" + idx));
                    if (pushed != null) pushed.setSafe(i, witnessText);
                }
            });
            index += n;
        }
    }
}
