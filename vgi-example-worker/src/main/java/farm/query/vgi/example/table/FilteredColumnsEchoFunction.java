// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.VectorProjector;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.CachedSchema;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * {@code filtered_columns_echo(count [, batch_size])} — reports, per query,
 * which columns the pushed-down filters reference ({@code filtered_cols}),
 * whether {@code n}/{@code tag} are filtered ({@code has_n}/{@code has_tag}),
 * and the discrete value set resolved for the string column {@code tag}
 * ({@code tag_values}, {@code "(none)"} when not an enumerable equality/IN).
 * Mirrors vgi-python's {@code FilteredColumnsEchoFunction}.
 */
public final class FilteredColumnsEchoFunction extends CountdownTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("tag", Schemas.UTF8),
            Schemas.nullable("filtered_cols", Schemas.UTF8),
            Schemas.nullable("has_n", Schemas.BOOL),
            Schemas.nullable("has_tag", Schemas.BOOL),
            Schemas.nullable("tag_values", Schemas.UTF8));

    @Override public String name() { return "filtered_columns_echo"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(
                "Echoes filtered_columns / has_filter_for_column / get_column_values_array")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                .withCategories("generator", "diagnostic");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override protected long defaultBatchSize() { return 2048L; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().ge(1).orElse(2048L);
        byte[] pfBytes = params.pushdownFilters();
        PushdownFilters pf = pfBytes == null
                ? PushdownFilters.empty()
                : PushdownFiltersDecoder.decode(pfBytes, params.joinKeys());

        String filteredCols = String.join(",", new TreeSet<>(pf.filteredColumns()));
        boolean hasN = pf.hasFilterForColumn("n");
        boolean hasTag = pf.hasFilterForColumn("tag");
        String tagValues = pf.getColumnValues("tag")
                .map(vals -> vals.stream()
                        .filter(Objects::nonNull)
                        .map(FilteredColumnsEchoFunction::valStr)
                        .sorted()
                        .collect(Collectors.joining(",")))
                .filter(s -> !s.isEmpty())
                .orElse("(none)");

        return new State(new BatchState(count, batchSize), filteredCols, hasN, hasTag, tagValues,
                pfBytes, new CachedSchema(params.outputSchema()), params.joinKeys());
    }

    private static String valStr(Object v) {
        if (v instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        return String.valueOf(v);
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public String filteredCols;
        public boolean hasN;
        public boolean hasTag;
        public String tagValues;
        public byte[] filterBytes;
        public CachedSchema projected;
        public List<byte[]> joinKeysIpc;

        public State() {}

        State(BatchState batch, String filteredCols, boolean hasN, boolean hasTag, String tagValues,
                byte[] filterBytes, CachedSchema projected, List<byte[]> joinKeysIpc) {
            this.batch = batch;
            this.filteredCols = filteredCols;
            this.hasN = hasN;
            this.hasTag = hasTag;
            this.tagValues = tagValues;
            this.filterBytes = filterBytes;
            this.projected = projected;
            this.joinKeysIpc = joinKeysIpc;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();
            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            BigIntVector nv = (BigIntVector) work.getVector("n");
            VarCharVector tagv = (VarCharVector) work.getVector("tag");
            VarCharVector fc = (VarCharVector) work.getVector("filtered_cols");
            BitVector hn = (BitVector) work.getVector("has_n");
            BitVector ht = (BitVector) work.getVector("has_tag");
            VarCharVector tv = (VarCharVector) work.getVector("tag_values");
            Text fcText = new Text(filteredCols);
            Text tvText = new Text(tagValues);
            for (int i = 0; i < n; i++) {
                long row = start + i;
                nv.setSafe(i, row);
                tagv.setSafe(i, new Text("t" + row));
                fc.setSafe(i, fcText);
                hn.setSafe(i, hasN ? 1 : 0);
                ht.setSafe(i, hasTag ? 1 : 0);
                tv.setSafe(i, tvText);
            }
            work.setRowCount(n);
            if (filterBytes != null) {
                work = FilterApplier.from(filterBytes, joinKeysIpc).apply(work);
            }
            out.emit(VectorProjector.project(work, projected.get()));
            batch.advance(n);
        }
    }
}
