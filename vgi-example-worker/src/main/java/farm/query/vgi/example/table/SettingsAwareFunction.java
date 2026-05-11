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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code settings_aware(count BIGINT)} — emits {@code count} rows whose
 * {@code greeting}/{@code value}/optional {@code details} columns reflect the
 * {@code greeting}, {@code multiplier}, and {@code vgi_verbose_mode} settings.
 */
public final class SettingsAwareFunction implements TableFunction {

    @Override public String name() { return "settings_aware"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates data demonstrating settings are passed");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true));
    }

    private static Schema buildSchema(boolean verbose) {
        List<Field> fields = new ArrayList<>();
        fields.add(Schemas.nullable("id", Schemas.INT64));
        fields.add(Schemas.nullable("greeting", Schemas.UTF8));
        fields.add(Schemas.nullable("value", Schemas.FLOAT64));
        if (verbose) {
            fields.add(Schemas.nullable("details", Schemas.UTF8));
        }
        return new Schema(fields);
    }

    @Override public BindResponse onBind(TableBindParams params) {
        boolean verbose = isVerbose(params.settings());
        return BindResponse.forSchema(
                farm.query.vgi.internal.SchemaUtil.serializeSchema(buildSchema(verbose)));
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        Map<String, Object> settings = params.settings();
        boolean verbose = isVerbose(settings);
        String greeting = strSetting(settings, "greeting", "Hello");
        long multiplier = longSetting(settings, "multiplier", 1L);
        return new State((int) count, verbose, greeting, multiplier);
    }

    private static boolean isVerbose(Map<String, Object> settings) {
        Object v = settings == null ? null : settings.get("vgi_verbose_mode");
        if (v instanceof Boolean b) return b;
        if (v instanceof CharSequence cs) return "true".equalsIgnoreCase(cs.toString());
        return false;
    }
    private static String strSetting(Map<String, Object> s, String name, String fallback) {
        Object v = s == null ? null : s.get(name);
        return v == null ? fallback : v.toString();
    }
    private static long longSetting(Map<String, Object> s, String name, long fallback) {
        Object v = s == null ? null : s.get(name);
        if (v instanceof Number n) return n.longValue();
        if (v instanceof CharSequence cs) {
            try { return Long.parseLong(cs.toString()); } catch (NumberFormatException ignore) {}
        }
        return fallback;
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public int remaining;
        public int currentIndex;
        public boolean verbose;
        public String greeting;
        public long multiplier;

        public State() {}
        State(int remaining, boolean verbose, String greeting, long multiplier) {
            this.remaining = remaining;
            this.verbose = verbose;
            this.greeting = greeting;
            this.multiplier = multiplier;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) { out.finish(); return; }
            int n = Math.min(remaining, 1000);
            Schema schema = buildSchema(verbose);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            root.allocateNew();
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            VarCharVector greetingVec = (VarCharVector) root.getVector("greeting");
            Float8Vector valueVec = (Float8Vector) root.getVector("value");
            Text greetingTxt = new Text(greeting);
            for (int i = 0; i < n; i++) {
                int idx = currentIndex + i;
                idVec.setSafe(i, idx);
                greetingVec.setSafe(i, greetingTxt);
                valueVec.setSafe(i, idx * 2.5 * multiplier);
            }
            if (verbose) {
                VarCharVector detailsVec = (VarCharVector) root.getVector("details");
                for (int i = 0; i < n; i++) {
                    detailsVec.setSafe(i, new Text("row_" + (currentIndex + i)));
                }
            }
            root.setRowCount(n);
            out.emit(root);
            currentIndex += n;
            remaining -= n;
        }
    }
}
