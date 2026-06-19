// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.narrowbind;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
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

/**
 * Narrow-bind reproducer scan functions for the {@code narrow_bind} catalog.
 * Both back catalog tables advertised with two columns {@code {id, val}}:
 *
 * <ul>
 *   <li>{@code narrow_bind_narrow_scan} binds to {@code {id}} only — fewer
 *       columns than the table advertises. The fixed C++ client must refuse
 *       this at bind with a clear {@code BinderException} rather than walking
 *       off the end of the worker's 1-column batch and segfaulting.</li>
 *   <li>{@code narrow_bind_wide_scan} binds to the full {@code {id, val}} —
 *       the positive control that must keep working.</li>
 * </ul>
 *
 * Mirrors vgi-python's {@code narrow_bind} fixture catalog. The function names
 * carry the {@code narrow_bind_} prefix so the {@code narrow_bind} extra
 * catalog owns them (hidden from the example catalog's listings).
 */
public final class NarrowBindFunctions {

    private NarrowBindFunctions() {}

    /** What the catalog advertises for both tables: two columns. */
    public static final Schema TABLE_SCHEMA = Schemas.of(
            Schemas.nullable("id", Schemas.INT64),
            Schemas.nullable("val", Schemas.INT64));

    /** What the narrow scan function actually binds to: one column. */
    static final Schema NARROW_SCHEMA = Schemas.of(Schemas.nullable("id", Schemas.INT64));

    private static final byte[] TABLE_IPC = SchemaUtil.serializeSchema(TABLE_SCHEMA);
    private static final byte[] NARROW_IPC = SchemaUtil.serializeSchema(NARROW_SCHEMA);

    /** Binds to a NARROWER schema than the catalog advertises (the bug). */
    public static final class NarrowScan implements TableFunction {

        private static final FunctionSpec SPEC = FunctionSpec.builder("narrow_bind_narrow_scan")
                .metadata(FunctionMetadata.describe(
                        "bind reports a narrower schema than the table advertises"))
                .constArg("count", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableBindParams p) {
            return BindResponse.forSchema(NARROW_IPC);
        }

        @Override public TableProducerState createProducer(TableInitParams p) {
            return new NarrowState();
        }
    }

    /** Binds to the full advertised schema (positive control — must work). */
    public static final class WideScan implements TableFunction {

        private static final FunctionSpec SPEC = FunctionSpec.builder("narrow_bind_wide_scan")
                .metadata(FunctionMetadata.describe(
                        "bind matches the table's advertised schema"))
                .constArg("count", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableBindParams p) {
            return BindResponse.forSchema(TABLE_IPC);
        }

        @Override public TableProducerState createProducer(TableInitParams p) {
            return new WideState();
        }
    }

    /** Emits one 1-column batch then finishes. */
    public static final class NarrowState extends TableProducerState {
        public boolean done;

        public NarrowState() {}

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            VectorSchemaRoot root = VectorSchemaRoot.create(NARROW_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector id = (BigIntVector) root.getVector("id");
            for (int i = 0; i < 3; i++) id.setSafe(i, i);
            root.setRowCount(3);
            out.emit(root);
        }
    }

    /** Emits one 2-column batch then finishes. */
    public static final class WideState extends TableProducerState {
        public boolean done;

        public WideState() {}

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            VectorSchemaRoot root = VectorSchemaRoot.create(TABLE_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector id = (BigIntVector) root.getVector("id");
            BigIntVector val = (BigIntVector) root.getVector("val");
            for (int i = 0; i < 3; i++) {
                id.setSafe(i, i);
                val.setSafe(i, (i + 1) * 10L);
            }
            root.setRowCount(3);
            out.emit(root);
        }
    }
}
