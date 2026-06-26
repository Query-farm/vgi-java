// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.TaggedUnion;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code union_varargs(values...)} — union-typed variadic table function.
 *
 * <p>Each argument is a SQL {@code UNION(i BIGINT, s VARCHAR)}. DuckDB
 * serializes a SQL UNION as a <em>sparse</em> Arrow union; the worker declares
 * the vararg's Arrow type as a sparse union with members {@code [i BIGINT,
 * s VARCHAR]} so DuckDB renders the parameter type as exactly
 * {@code UNION(i BIGINT, s VARCHAR)}. Each vararg is decoded SDK-side into a
 * {@link TaggedUnion}, which preserves the active member discriminator that a
 * plain {@code getObject} would drop.
 *
 * <p>Emits one row per vararg in positional order:
 * <pre>
 *   idx BIGINT  — 0-based positional index
 *   tag VARCHAR — active union member name ('i' or 's')
 *   value VARCHAR — active member value stringified
 * </pre>
 *
 * <p>Mirrors the Python fixture {@code UnionVarargsFunction} in
 * {@code vgi/_test_fixtures/table/pairs.py}.
 */
public final class UnionVarargsFunction implements TableFunction {

    /** Sparse union shared by every {@code union_varargs} argument. */
    private static final ArrowType UNION_TYPE =
            new ArrowType.Union(UnionMode.Sparse, new int[]{0, 1});

    /** Member fields of the union, in type-id order. */
    private static final List<Field> UNION_MEMBERS = List.of(
            Schemas.nullable("i", Schemas.INT64),
            Schemas.nullable("s", Schemas.UTF8));

    /** Fixed output schema: idx BIGINT, tag VARCHAR, value VARCHAR. */
    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("idx", Schemas.INT64),
            Schemas.nullable("tag", Schemas.UTF8),
            Schemas.nullable("value", Schemas.UTF8)));

    private static final byte[] OUTPUT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    private static final FunctionSpec SPEC = FunctionSpec.builder("union_varargs")
            .description("Echo the active member tag and value of each union vararg")
            .arg(ArgSpec.nested("values", 0, UNION_TYPE, UNION_MEMBERS, /*varargs=*/true))
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableBindParams p) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams p) {
        List<Object> positionals = p.arguments().positional();
        List<String> tags = new ArrayList<>(positionals.size());
        List<String> values = new ArrayList<>(positionals.size());
        for (Object o : positionals) {
            if (o instanceof TaggedUnion tu) {
                tags.add(tu.tag());
                values.add(tu.value() == null ? null : String.valueOf(tu.value()));
            } else {
                // Defensive: a non-union arg (shouldn't happen for this fixture).
                tags.add(null);
                values.add(o == null ? null : String.valueOf(o));
            }
        }
        return new State(tags, values);
    }

    /** One-shot producer: emits a single batch of (idx, tag, value) rows. */
    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public List<String> tags;
        public List<String> values;
        public boolean done;

        public State() {}
        State(List<String> tags, List<String> values) {
            this.tags = tags;
            this.values = values;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            int n = tags.size();
            BatchUtil.emit(OUTPUT_SCHEMA, n, out, (root, rows, start) -> {
                BigIntVector idx = (BigIntVector) root.getVector("idx");
                VarCharVector tag = (VarCharVector) root.getVector("tag");
                VarCharVector value = (VarCharVector) root.getVector("value");
                for (int i = 0; i < rows; i++) {
                    idx.setSafe(i, i);
                    if (tags.get(i) == null) tag.setNull(i);
                    else tag.setSafe(i, new Text(tags.get(i)));
                    if (values.get(i) == null) value.setNull(i);
                    else value.setSafe(i, new Text(values.get(i)));
                }
            });
        }
    }
}
