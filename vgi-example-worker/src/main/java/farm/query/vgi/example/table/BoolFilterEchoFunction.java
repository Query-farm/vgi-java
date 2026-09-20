// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.VectorProjector;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
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


/**
 * {@code bool_filter_echo(count)} — {@code count} rows with a nullable BOOLEAN {@code flag}
 * column cycling TRUE, FALSE, NULL, plus a {@code pushed_filters} column echoing the rendering
 * of whatever DuckDB pushed down. Backs
 * {@code filter_pushdown/boolean_column_predicate.test}.
 *
 * <p>It exists because nothing else can express the shape. {@code WHERE flag} and
 * {@code WHERE NOT flag} are pushed down as a bare {@code column_ref}, not rewritten to
 * {@code flag = true}, and to see one at all you need a TABLE FUNCTION with a boolean column:
 * {@link FilterEchoFunction} has none, so the predicate cannot be written against it, and the
 * table-in-out echo path is never handed a bare boolean column as a pushed predicate, so a case
 * written there passes whether or not the worker understands one.
 *
 * <p>Echoing {@code pushed_filters} covers the half a row count cannot see: a shape that decodes
 * and evaluates correctly but renders no SQL shows up there as {@code (none)}, and for a worker
 * that builds a WHERE clause from it that is silently wrong rows — DuckDB does not re-apply a
 * predicate it pushed into a table function.
 */
public final class BoolFilterEchoFunction extends CountdownTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("flag", Schemas.BOOL),
            Schemas.nullable("pushed_filters", Schemas.UTF8));

    @Override public String name() { return "bool_filter_echo"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata
                .describe("Rows with a nullable BOOLEAN column, echoing pushed-down filters")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true);
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        return new State(params, new BatchState(count, Math.max(1L, count)));
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public String filterStr;

        public State() {}

        State(TableInitParams params, BatchState batch) {
            super(params);
            this.batch = batch;
            this.filterStr = filters.current().formatInline();
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();
            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            BigIntVector nv = (BigIntVector) work.getVector("n");
            BitVector fv = (BitVector) work.getVector("flag");
            VarCharVector pv = (VarCharVector) work.getVector("pushed_filters");
            Text filterText = new Text(filterStr);
            for (int i = 0; i < n; i++) {
                long row = start + i;
                nv.setSafe(i, row);
                // TRUE, FALSE, NULL. The NULL row is the point: it is what distinguishes
                // `WHERE flag` from `WHERE flag IS NOT FALSE`, and `WHERE NOT flag` from
                // `WHERE flag IS NOT TRUE`.
                switch ((int) (row % 3)) {
                    case 0 -> fv.setSafe(i, 1);
                    case 1 -> fv.setSafe(i, 0);
                    default -> fv.setNull(i);
                }
                pv.setSafe(i, filterText);
            }
            work.setRowCount(n);
            out.emit(VectorProjector.project(filters.apply(work), outputSchema));
            batch.advance(n);
        }
    }
}
