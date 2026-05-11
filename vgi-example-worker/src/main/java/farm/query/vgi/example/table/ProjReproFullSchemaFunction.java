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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code proj_repro_full_schema(count BIGINT [const])} — projection-pushdown
 * reproducer that always emits the full 12-column WIDE_SCHEMA. value_schema_id
 * and key_schema_id are always NULL. Used to verify the C++ extension's
 * column-id mapping selects the right column from a wider emit.
 */
public class ProjReproFullSchemaFunction implements TableFunction {

    /** {@code proj_repro_chunked(n)} — same emission shape as
     *  {@link ProjReproFullSchemaFunction}; kept as a distinct registered
     *  function for the test's count-of-tables assertion. */
    public static final class Chunked extends ProjReproFullSchemaFunction {
        @Override public String name() { return "proj_repro_chunked"; }
    }

    /** {@code proj_repro_multi_worker(n)} — same shape, different name. */
    public static final class MultiWorker extends ProjReproFullSchemaFunction {
        @Override public String name() { return "proj_repro_multi_worker"; }
    }

    /** {@code proj_repro_strict(n)} — projection-aware variant. */
    public static final class Strict extends ProjReproFullSchemaFunction {
        @Override public String name() { return "proj_repro_strict"; }
    }


    static final Schema OUTPUT_SCHEMA;
    static final byte[] OUTPUT_SCHEMA_IPC;

    static {
        List<Field> fields = new ArrayList<>();
        fields.add(Schemas.nonNull("topic", Schemas.UTF8));
        fields.add(new Field("partition", new FieldType(false, new ArrowType.Int(32, true), null), null));
        fields.add(Schemas.nonNull("offset", Schemas.INT64));
        fields.add(Schemas.nullable("timestamp", Schemas.INT64));
        fields.add(Schemas.nullable("timestamp_type", Schemas.UTF8));
        fields.add(new Field("key", new FieldType(true, new ArrowType.Binary(), null), null));
        fields.add(Schemas.nullable("key_string", Schemas.UTF8));
        fields.add(new Field("key_schema_id", new FieldType(true, new ArrowType.Int(32, true), null), null));
        fields.add(new Field("value", new FieldType(true, new ArrowType.Binary(), null), null));
        fields.add(Schemas.nullable("value_string", Schemas.UTF8));
        fields.add(new Field("value_schema_id", new FieldType(true, new ArrowType.Int(32, true), null), null));
        fields.add(new Field("headers", new FieldType(false, new ArrowType.List(), null),
                List.of(new Field("item", new FieldType(true, new ArrowType.Struct(), null),
                        List.of(
                                Schemas.nullable("k", Schemas.UTF8),
                                new Field("v", new FieldType(true, new ArrowType.Binary(), null), null))))));
        OUTPUT_SCHEMA = new Schema(fields);
        OUTPUT_SCHEMA_IPC = farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);
    }

    @Override public String name() { return "proj_repro_full_schema"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Projection-pushdown reproducer (emits full FIXED_SCHEMA)");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.positional("n", 0, Schemas.INT64));
    }
    @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(OUTPUT_SCHEMA_IPC); }
    @Override public long cardinality(TableBindParams p) {
        Object c = p.arguments().positionalAt(0);
        return c instanceof Number n ? n.longValue() : -1L;
    }
    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        return new State((int) count);
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public int total;
        public boolean done;
        public State() {}
        State(int total) { this.total = total; }
        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            VarCharVector topic = (VarCharVector) root.getVector("topic");
            IntVector partition = (IntVector) root.getVector("partition");
            BigIntVector offset = (BigIntVector) root.getVector("offset");
            VarBinaryVector key = (VarBinaryVector) root.getVector("key");
            VarCharVector keyString = (VarCharVector) root.getVector("key_string");
            VarBinaryVector value = (VarBinaryVector) root.getVector("value");
            VarCharVector valueString = (VarCharVector) root.getVector("value_string");
            // headers: empty list per row (per the python fixture)
            org.apache.arrow.vector.complex.ListVector headers =
                    (org.apache.arrow.vector.complex.ListVector) root.getVector("headers");
            org.apache.arrow.vector.complex.impl.UnionListWriter hw = headers.getWriter();
            for (int i = 0; i < total; i++) {
                topic.setSafe(i, new Text("demo_topic"));
                partition.setSafe(i, i % 4);
                offset.setSafe(i, i);
                root.getVector("timestamp").setNull(i);
                root.getVector("timestamp_type").setNull(i);
                key.setSafe(i, ("k" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                keyString.setSafe(i, new Text("k" + i));
                root.getVector("key_schema_id").setNull(i);
                value.setSafe(i, ("v" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                valueString.setSafe(i, new Text("v" + i));
                root.getVector("value_schema_id").setNull(i);
                hw.setPosition(i);
                hw.startList();
                hw.endList();
            }
            root.setRowCount(total);
            out.emit(root);
            out.finish();
        }
    }
}
