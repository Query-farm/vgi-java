// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.function.FunctionSpec;
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

/**
 * Three overloads of {@code make_pairs}, dispatched by arg types at 2-arg arity:
 * <ul>
 *   <li>{@code (start BIGINT, stop BIGINT)} — emits {@code (i, i*2)} for
 *       {@code i in start..stop-1}.</li>
 *   <li>{@code (prefix VARCHAR, suffix VARCHAR)} — emits 5 string pairs
 *       {@code (prefix+i, suffix+i)} for {@code i in 0..4}.</li>
 *   <li>{@code (start BIGINT, suffix VARCHAR)} — emits 5 mixed pairs
 *       {@code (start+i, suffix+i)} for {@code i in 0..4}.</li>
 * </ul>
 */
public final class MakePairsFunctions {

    private MakePairsFunctions() {}

    private static final Schema INT_SCHEMA = new Schema(List.of(
            Schemas.nullable("a", Schemas.INT64),
            Schemas.nullable("b", Schemas.INT64)));
    private static final byte[] INT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(INT_SCHEMA);

    private static final Schema STR_SCHEMA = new Schema(List.of(
            Schemas.nullable("a", Schemas.UTF8),
            Schemas.nullable("b", Schemas.UTF8)));
    private static final byte[] STR_SCHEMA_IPC =
            SchemaUtil.serializeSchema(STR_SCHEMA);

    private static final Schema MIXED_SCHEMA = new Schema(List.of(
            Schemas.nullable("a", Schemas.INT64),
            Schemas.nullable("b", Schemas.UTF8)));
    private static final byte[] MIXED_SCHEMA_IPC =
            SchemaUtil.serializeSchema(MIXED_SCHEMA);

    public static final class IntVariant implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("make_pairs")
                .description("Generate integer pairs (i, i*2)")
                .constArg("start", Schemas.INT64)
                .constArg("stop", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(INT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            long start = ex.positional(0, "start").asLong().required();
            long stop = ex.positional(1, "stop").asLong().required();
            return new IntState(start, stop);
        }
    }

    public static final class StrVariant implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("make_pairs")
                .description("Generate string pairs with prefix and suffix")
                .constArg("prefix", Schemas.UTF8)
                .constArg("suffix", Schemas.UTF8)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(STR_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            String prefix = ex.positional(0, "prefix").asString().required();
            String suffix = ex.positional(1, "suffix").asString().required();
            return new StrState(prefix, suffix);
        }
    }

    public static final class MixedVariant implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("make_pairs")
                .description("Generate mixed int/string pairs")
                .constArg("start", Schemas.INT64)
                .constArg("label", Schemas.UTF8)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(MIXED_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            long start = ex.positional(0, "start").asLong().required();
            String suffix = ex.positional(1, "label").asString().required();
            return new MixedState(start, suffix);
        }
    }

    public static final class IntState extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public long pos;
        public long stop;
        public boolean done;

        public IntState() {}

        IntState(long start, long stop) { this.pos = start; this.stop = stop; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done || pos >= stop) { out.finish(); done = true; return; }
            int n = (int) Math.min(1024L, stop - pos);
            long startPos = pos;
            BatchUtil.emit(INT_SCHEMA, n, out, (root, rows, start) -> {
                BigIntVector a = (BigIntVector) root.getVector("a");
                BigIntVector b = (BigIntVector) root.getVector("b");
                for (int i = 0; i < rows; i++) {
                    long val = startPos + i;
                    a.setSafe(i, val);
                    b.setSafe(i, val * 2);
                }
            });
            pos += n;
        }
    }

    public static final class StrState extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public String prefix;
        public String suffix;
        public boolean done;

        public StrState() {}

        StrState(String prefix, String suffix) { this.prefix = prefix; this.suffix = suffix; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            BatchUtil.emit(STR_SCHEMA, 5, out, (root, rows, start) -> {
                VarCharVector a = (VarCharVector) root.getVector("a");
                VarCharVector b = (VarCharVector) root.getVector("b");
                for (int i = 0; i < rows; i++) {
                    a.setSafe(i, new Text(prefix + i));
                    b.setSafe(i, new Text(suffix + i));
                }
            });
            done = true;
        }
    }

    public static final class MixedState extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public long start;
        public String suffix;
        public boolean done;

        public MixedState() {}

        MixedState(long start, String suffix) { this.start = start; this.suffix = suffix; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            BatchUtil.emit(MIXED_SCHEMA, 5, out, (root, rows, startIdx) -> {
                BigIntVector a = (BigIntVector) root.getVector("a");
                VarCharVector b = (VarCharVector) root.getVector("b");
                for (int i = 0; i < rows; i++) {
                    a.setSafe(i, start + i);
                    b.setSafe(i, new Text(suffix + i));
                }
            });
            done = true;
        }
    }
}
