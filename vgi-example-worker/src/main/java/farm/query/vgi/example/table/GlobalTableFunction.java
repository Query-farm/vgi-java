// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.protocol.FunctionExample;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code global_table() -> (n int64, label utf8)} — the table member of the
 * global-registration probe family (see {@code scalar/GlobalScalarFunction} for
 * the family's rationale).
 *
 * <p>A fixed generator taking no arguments: three labelled rows, emitted once.
 * Mirrors vgi-python's {@code GlobalTableFunction}.
 */
public final class GlobalTableFunction extends SimpleTableFunction {

    private static final Schema OUTPUT = Schemas.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("label", Schemas.UTF8));

    /** Exactly three rows, always. */
    private static final long ROWS = 3L;

    private static final FunctionSpec SPEC = FunctionSpec.builder("global_table")
            .metadata(FunctionMetadata.describe("Global-registration probe (table)")
                    .withCategories("test", "global")
                    .withExamples(List.of(new FunctionExample(
                            "SELECT * FROM vgi_example_global_table()",
                            "Table probe published into system.main", null))))
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override protected Schema outputSchema() { return OUTPUT; }

    @Override public long cardinality(TableBindParams params) { return ROWS; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        return new State();
    }

    /** One-shot emit latch for the three probe rows. */
    public static final class State extends TableProducerState {

        /** Whether the single batch has been emitted. */
        public boolean emitted;

        /** Required no-arg constructor for state deserialization. */
        public State() {}

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) {
                out.finish();
                return;
            }
            emitted = true;
            BatchUtil.emit(OUTPUT, (int) ROWS, out, (root, rows, ignored) -> {
                BigIntVector n = (BigIntVector) root.getVector("n");
                VarCharVector label = (VarCharVector) root.getVector("label");
                for (int i = 0; i < rows; i++) {
                    n.setSafe(i, i);
                    label.setSafe(i, new Text("global_table:" + i));
                }
            });
        }
    }
}
