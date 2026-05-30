// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@code value_prune(count [, batch_size])} — emits only the keys that
 * {@link PushdownFilters#getColumnValues} resolves for column {@code n}, and
 * echoes the resolved discrete set in the {@code resolved} column.
 *
 * <p>The {@code resolved} column carries the sorted, comma-joined discrete set
 * the accessor returned, or {@code "(scan)"} when it returned nothing (the
 * predicate is not enumerable — no filter, a bare range, or an OR with a
 * non-discrete branch). Asserting on {@code resolved} verifies the accessor's
 * AND-descent / OR-union behaviour end-to-end, independent of any residual
 * filtering. Mirrors vgi-python's {@code ValuePruneFunction}.
 */
public final class ValuePruneFunction extends CountdownTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("resolved", Schemas.UTF8));

    @Override public String name() { return "value_prune"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(
                "Prunes the key set via getColumnValues('n'); echoes the resolved discrete values")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                .withCategories("generator", "diagnostic");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override protected long defaultBatchSize() { return 2048L; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().orElse(2048L);

        Optional<List<Object>> discrete = resolveValues(params);
        String resolved;
        List<Long> emit = new ArrayList<>();
        if (discrete.isPresent()) {
            List<Long> sorted = discrete.get().stream()
                    .filter(v -> v instanceof Number)
                    .map(v -> ((Number) v).longValue())
                    .sorted()
                    .collect(Collectors.toList());
            resolved = sorted.stream().map(String::valueOf).collect(Collectors.joining(","));
            for (long v : sorted) if (v >= 0 && v < count) emit.add(v);
        } else {
            resolved = "(scan)";
            for (long v = 0; v < count; v++) emit.add(v);
        }
        // auto_apply_filters: emitted rows must satisfy the residual predicate.
        // For an enumerable filter the emit list is already the resolved set, but
        // the non-enumerable "(scan)" path emits every candidate and relies on
        // this to narrow (e.g. n>5 AND n<9). Only apply when `n` is actually
        // projected — a filter on an absent column would otherwise drop all rows
        // (e.g. SELECT DISTINCT resolved, where only `resolved` is emitted).
        boolean nProjected = params.outputSchema().getFields().stream()
                .anyMatch(f -> "n".equals(f.getName()));
        FilterApplier applier = nProjected ? params.filters() : null;
        return new State(params.outputSchema(), emit, resolved, (int) batchSize, applier);
    }

    /** Decode the init-time pushed filters and resolve the discrete set for {@code n}. */
    private static Optional<List<Object>> resolveValues(TableInitParams params) {
        byte[] pfBytes = params.pushdownFilters();
        if (pfBytes == null) return Optional.empty();
        PushdownFilters pf = PushdownFiltersDecoder.decode(
                pfBytes, params.joinKeys() == null ? List.of() : params.joinKeys());
        return pf.getColumnValues("n");
    }

    public static final class State extends TableProducerState {
        public Schema projected;
        public List<Long> values;
        public String resolved;
        public int batchSize;
        public FilterApplier applier;
        public int cursor;

        public State() {}

        State(Schema projected, List<Long> values, String resolved, int batchSize,
                FilterApplier applier) {
            this.projected = projected;
            this.values = values;
            this.resolved = resolved;
            this.batchSize = batchSize;
            this.applier = applier;
            this.cursor = 0;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (cursor >= values.size()) { out.finish(); return; }
            int n = Math.min(batchSize, values.size() - cursor);
            int start = cursor;
            VectorSchemaRoot root = VectorSchemaRoot.create(projected, Allocators.root());
            root.allocateNew();
            BigIntVector nv = (BigIntVector) root.getVector("n");
            VarCharVector rv = (VarCharVector) root.getVector("resolved");
            Text resolvedText = rv == null ? null : new Text(resolved);
            for (int i = 0; i < n; i++) {
                if (nv != null) nv.setSafe(i, values.get(start + i));
                if (rv != null) rv.setSafe(i, resolvedText);
            }
            root.setRowCount(n);
            if (applier != null) root = applier.apply(root);
            out.emit(root);
            cursor += n;
        }
    }
}
