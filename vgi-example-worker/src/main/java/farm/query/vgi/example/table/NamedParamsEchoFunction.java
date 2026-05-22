// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.function.FunctionSpec;
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
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;
import java.util.Map;

public final class NamedParamsEchoFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("id", Schemas.INT64),
            Schemas.nullable("greeting", Schemas.UTF8),
            Schemas.nullable("value", Schemas.INT64),
            Schemas.nullable("float_value", Schemas.FLOAT64),
            Schemas.nullable("enabled", Schemas.BOOL)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    private static final FunctionSpec SPEC = FunctionSpec.builder("named_params_echo")
            .description("Echoes named parameter values in output columns")
            .constArg("count", Schemas.INT64)
            .named("scale", Schemas.FLOAT64, "1.0")
            .named("multiplier", Schemas.INT64, "1")
            .named("greeting", Schemas.UTF8, "hello")
            .named("enabled", Schemas.BOOL, "true")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

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
            Text greetingText = new Text(greeting);
            BatchUtil.produceBatch(batch, OUTPUT_SCHEMA, null, out, (root, n, start) -> {
                BigIntVector id = (BigIntVector) root.getVector("id");
                VarCharVector g = (VarCharVector) root.getVector("greeting");
                BigIntVector v = (BigIntVector) root.getVector("value");
                Float8Vector fv = (Float8Vector) root.getVector("float_value");
                BitVector en = (BitVector) root.getVector("enabled");
                for (int i = 0; i < n; i++) {
                    long row = start + i;
                    id.setSafe(i, row);
                    g.setSafe(i, greetingText);
                    v.setSafe(i, row * multiplier);
                    fv.setSafe(i, (double) row * scale);
                    en.setSafe(i, enabled ? 1 : 0);
                }
            });
        }
    }
}
