// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
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

public final class GeneratorExceptionFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("n", Schemas.INT64)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    @Override public String name() { return "generator_exception"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Raises an exception after N batches for testing").withCategories("testing");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.positional("fail_after", 0, Schemas.INT64));
    }

    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long failAfter = ((Number) params.arguments().positionalAt(0)).longValue();
        return new State(failAfter);
    }

    public static final class State extends TableProducerState {
        public long failAfter;
        public long batchCount;

        public State() {}
        State(long failAfter) { this.failAfter = failAfter; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (batchCount >= failAfter) {
                throw new RuntimeException("Intentional failure after " + failAfter + " batches");
            }
            long idx = batchCount;
            batchCount++;
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("n");
            v.setSafe(0, idx);
            root.setRowCount(1);
            out.emit(root);
        }
    }
}
