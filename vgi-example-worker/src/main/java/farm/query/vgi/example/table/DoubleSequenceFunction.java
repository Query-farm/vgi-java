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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public final class DoubleSequenceFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("n", new FieldType(true, Schemas.FLOAT64, null), null)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    @Override public String name() { return "double_sequence"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates a sequence of doubles from 0.0 to (count-1)*increment");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true),
                new ArgSpec("batch_size", -1, Schemas.INT64, "", true, true, "1000", List.of(), false, false),
                new ArgSpec("increment", -1, Schemas.FLOAT64, "", true, true, "1.0", List.of(), false, false));
    }

    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        Object bsObj = params.arguments().named().get("batch_size");
        long batchSize = bsObj == null ? 1000L : ((Number) bsObj).longValue();
        Object incObj = params.arguments().named().get("increment");
        double increment = incObj == null ? 1.0 : ((Number) incObj).doubleValue();
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
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            Float8Vector v = (Float8Vector) root.getVector("n");
            for (int i = 0; i < n; i++) v.setSafe(i, (start + i) * increment);
            root.setRowCount(n);
            out.emit(root);
            batch.advance(n);
        }
    }
}
