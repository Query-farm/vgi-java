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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public final class TenThousandFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("n", Schemas.INT64)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    @Override public String name() { return "ten_thousand"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates 10000 integers from 0 to 9999");
    }
    @Override public List<ArgSpec> argumentSpecs() { return List.of(); }
    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }
    @Override public TableProducerState createProducer(TableInitParams params) {
        return new State(new BatchState(10_000, 1000));
    }
    @Override public long cardinality(TableBindParams params) { return 10_000L; }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public State() {}
        State(BatchState batch) { this.batch = batch; }
        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            farm.query.vgi.internal.BatchUtil.produceBatch(batch, OUTPUT_SCHEMA, null, out, (root, n, start) -> {
                BigIntVector v = (BigIntVector) root.getVector("n");
                for (int i = 0; i < n; i++) v.setSafe(i, start + i);
            });
        }
    }
}
