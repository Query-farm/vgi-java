// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.internal.SchemaUtil;
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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * {@code struct_settings(count BIGINT)} — generates {@code count} rows
 * configured by the {@code config} STRUCT(start, step, label) DuckDB setting.
 */
public final class StructSettingsFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("label", Schemas.UTF8)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    @Override public String name() { return "struct_settings"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generate a sequence configured by a struct setting");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.positional("count", 0, Schemas.INT64));
    }
    @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(OUTPUT_SCHEMA_IPC); }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        Map<String, Object> settings = params.settings();
        Object cfgObj = settings == null ? null : settings.get("config");
        long start = 0L, step = 1L;
        String label = "item";
        if (cfgObj instanceof Map<?, ?> cfg) {
            if (cfg.get("start") instanceof Number n) start = n.longValue();
            if (cfg.get("step") instanceof Number n) step = n.longValue();
            if (cfg.get("label") instanceof CharSequence cs) label = cs.toString();
        }
        return new State((int) count, start, step, label);
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public int remaining;
        public int currentIndex;
        public long start;
        public long step;
        public String label;

        public State() {}
        State(int remaining, long start, long step, String label) {
            this.remaining = remaining;
            this.start = start;
            this.step = step;
            this.label = label;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) { out.finish(); return; }
            int n = Math.min(remaining, 1000);
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector nVec = (BigIntVector) root.getVector("n");
            VarCharVector labelVec = (VarCharVector) root.getVector("label");
            for (int i = 0; i < n; i++) {
                int idx = currentIndex + i;
                nVec.setSafe(i, start + (long) idx * step);
                labelVec.setSafe(i, new Text(label + "_" + idx));
            }
            root.setRowCount(n);
            out.emit(root);
            currentIndex += n;
            remaining -= n;
        }
    }
}
