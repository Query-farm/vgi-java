// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public final class TenThousandFunction extends SimpleTableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("n", Schemas.INT64)));

    private static final FunctionSpec SPEC = FunctionSpec.builder("ten_thousand")
            .description("Generates 10000 integers from 0 to 9999")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }
    @Override public TableProducerState createProducer(TableInitParams params) {
        return new State(new BatchState(10_000, 1000));
    }
    @Override public long cardinality(TableBindParams params) { return 10_000L; }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public State() {}
        State(BatchState batch) { this.batch = batch; }
        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            BatchUtil.produceBatch(batch, OUTPUT_SCHEMA, null, out, (root, n, start) -> {
                BigIntVector v = (BigIntVector) root.getVector("n");
                for (int i = 0; i < n; i++) v.setSafe(i, start + i);
            });
        }
    }
}
