// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.VectorProjector;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.CachedSchema;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

/**
 * {@code expression_filter_test(count [, batch_size])} — generates {@code count}
 * rows for non-spatial expression-filter testing. Backs
 * {@code table/expression_filter.test}.
 *
 * <p>Row {@code i}: {@code id = i}, {@code name = "item_" + i},
 * {@code tags = ["tag_" + (i % 5), "tag_" + ((i + 1) % 5)]}, {@code score = i * 1.1}.
 * Declares {@code list_contains}, {@code starts_with}, {@code contains} as
 * supported expression filters; {@code length(name) > 7} stays a FILTER node
 * above the scan (not declared, so not pushed).
 */
public final class ExpressionFilterTestFunction extends CountdownTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("id", Schemas.INT64),
            Schemas.nullable("name", Schemas.UTF8),
            Schemas.list("tags", Schemas.UTF8, true),
            Schemas.nullable("score", Schemas.FLOAT64));

    @Override public String name() { return "expression_filter_test"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates rows for non-spatial expression filter testing")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                .withSupportedExpressionFilters("list_contains", "starts_with", "contains")
                .withCategories("generator", "testing");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override protected long defaultBatchSize() { return 1024L; }

    @Override public java.util.List<farm.query.vgi.catalog.ColumnStatistics> statistics(
            farm.query.vgi.table.TableBindParams params) {
        return null; // multi-column list schema — no canned countdown stats
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().orElse(1024L);
        return new State(new BatchState(count, batchSize), params.pushdownFilters(),
                params.joinKeys(), new CachedSchema(params.outputSchema()));
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public byte[] filterBytes;
        public List<byte[]> joinKeysIpc;
        public CachedSchema outputSchema;

        public State() {}

        State(BatchState batch, byte[] filterBytes, List<byte[]> joinKeysIpc, CachedSchema outputSchema) {
            this.batch = batch;
            this.filterBytes = filterBytes;
            this.joinKeysIpc = joinKeysIpc;
            this.outputSchema = outputSchema;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();

            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            BigIntVector idv = (BigIntVector) work.getVector("id");
            VarCharVector namev = (VarCharVector) work.getVector("name");
            ListVector tagsv = (ListVector) work.getVector("tags");
            Float8Vector scorev = (Float8Vector) work.getVector("score");
            UnionListWriter tagW = tagsv.getWriter();
            try (ArrowBuf tmp = Allocators.root().buffer(64)) {
                for (int i = 0; i < n; i++) {
                    long row = start + i;
                    idv.setSafe(i, row);
                    namev.setSafe(i, new Text("item_" + row));
                    scorev.setSafe(i, row * 1.1);
                    tagW.setPosition(i);
                    tagW.startList();
                    writeTag(tagW, tmp, "tag_" + (row % 5));
                    writeTag(tagW, tmp, "tag_" + ((row + 1) % 5));
                    tagW.endList();
                }
            }
            tagsv.setValueCount(n);
            work.setRowCount(n);

            if (filterBytes != null) {
                FilterApplier fa = FilterApplier.from(filterBytes, joinKeysIpc);
                work = fa.apply(work);                                            // column filters (id >= 50)
                work = ExpressionFilterEvaluator.apply(work, fa.expressionPredicates()); // list_contains etc.
            }
            out.emit(VectorProjector.project(work, outputSchema.get()));
            batch.advance(n);
        }

        private static void writeTag(UnionListWriter w, ArrowBuf tmp, String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            tmp.setBytes(0, bytes);
            w.varChar().writeVarChar(0, bytes.length, tmp);
        }
    }
}
