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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;
import java.util.Map;

public final class NamedParamsEchoFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("id", new FieldType(true, Schemas.INT64, null), null),
            new Field("greeting", new FieldType(true, Schemas.UTF8, null), null),
            new Field("value", new FieldType(true, Schemas.INT64, null), null),
            new Field("float_value", new FieldType(true, Schemas.FLOAT64, null), null),
            new Field("enabled", new FieldType(true, Schemas.BOOL, null), null)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    @Override public String name() { return "named_params_echo"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Echoes named parameter values in output columns");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true),
                new ArgSpec("greeting", -1, Schemas.UTF8, "", true, true, "hello", List.of(), false, false),
                new ArgSpec("multiplier", -1, Schemas.INT64, "", true, true, "1", List.of(), false, false),
                new ArgSpec("scale", -1, Schemas.FLOAT64, "", true, true, "1.0", List.of(), false, false),
                new ArgSpec("enabled", -1, Schemas.BOOL, "", true, true, "true", List.of(), false, false));
    }

    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        Map<String, Object> named = params.arguments().named();
        String greeting = (String) named.getOrDefault("greeting", "hello");
        long multiplier = named.get("multiplier") instanceof Number n ? n.longValue() : 1L;
        double scale = named.get("scale") instanceof Number n ? n.doubleValue() : 1.0;
        boolean enabled = named.get("enabled") instanceof Boolean b ? b : true;
        return new State(new BatchState(count, 1000), greeting, multiplier, scale, enabled);
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public String greeting;
        public long multiplier;
        public double scale;
        public boolean enabled;

        public State() {}

        State(BatchState batch, String greeting, long multiplier, double scale, boolean enabled) {
            this.batch = batch;
            this.greeting = greeting;
            this.multiplier = multiplier;
            this.scale = scale;
            this.enabled = enabled;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector id = (BigIntVector) root.getVector("id");
            VarCharVector g = (VarCharVector) root.getVector("greeting");
            BigIntVector v = (BigIntVector) root.getVector("value");
            Float8Vector fv = (Float8Vector) root.getVector("float_value");
            BitVector en = (BitVector) root.getVector("enabled");
            Text greetingText = new Text(greeting);
            for (int i = 0; i < n; i++) {
                long row = start + i;
                id.setSafe(i, row);
                g.setSafe(i, greetingText);
                v.setSafe(i, row * multiplier);
                fv.setSafe(i, (double) row * scale);
                en.setSafe(i, enabled ? 1 : 0);
            }
            root.setRowCount(n);
            out.emit(root);
            batch.advance(n);
        }
    }
}
