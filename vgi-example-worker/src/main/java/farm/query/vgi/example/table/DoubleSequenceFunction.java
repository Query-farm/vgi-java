// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public final class DoubleSequenceFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("n", Schemas.FLOAT64)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    @Override public String name() { return "double_sequence"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates a sequence of floating-point numbers from 0 to n-1");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true),
                ArgSpec.named("batch_size", Schemas.INT64, "1000"),
                ArgSpec.named("increment", Schemas.FLOAT64, "1.0"));
    }

    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        long batchSize = params.arguments().namedLong("batch_size", 1000L);
        double increment = params.arguments().namedDouble("increment", 1.0);
        return new State(new BatchState(count, batchSize), increment);
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public double increment;

        public State() {}

        State(BatchState batch, double increment) {
            this.batch = batch;
            this.increment = increment;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            farm.query.vgi.internal.BatchUtil.produceBatch(batch, OUTPUT_SCHEMA, null, out, (root, n, start) -> {
                Float8Vector v = (Float8Vector) root.getVector("n");
                for (int i = 0; i < n; i++) v.setSafe(i, (start + i) * increment);
            });
        }
    }
}
