// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.VectorProjector;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.CachedSchema;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * {@code rff_rowid_scan()} — backs {@code example.data.rff_rowid}, a table with a
 * virtual row-id column alongside a {@code bbox} STRUCT whose four corners are
 * declared {@code required_field_filter_paths}. Backs
 * {@code test/sql/integration/table/required_field_filter_paths_rowid.test}.
 *
 * <p>Emits 10 rows: {@code row_id} 0..9, {@code bbox.xmin = row index} (the other
 * corners constant), {@code other = row*10}. {@code filter_pushdown} +
 * {@code auto_apply_filters} route the WHERE predicates (including the
 * sentinel-keyed {@code rowid} filter, which the C++ optimizer resolves to the
 * {@code row_id} field name) into the scan; the function applies them so a
 * {@code WHERE rowid = N} selects exactly one row. {@code projection_pushdown}
 * narrows the emitted batch to the requested columns.
 */
public final class RffRowidScanFunction extends SimpleTableFunction {

    private static final ArrowType FLOAT32 = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

    private static Field f32(String name) {
        return new Field(name, FieldType.nullable(FLOAT32), null);
    }

    static Field bboxField() {
        return new Field("bbox", FieldType.nullable(new ArrowType.Struct()),
                List.of(f32("xmin"), f32("ymin"), f32("xmax"), f32("ymax")));
    }

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("row_id", new FieldType(false, Schemas.INT64, null), null),
            bboxField(),
            Schemas.nullable("other", Schemas.INT64)));

    private static final int ROWS = 10;

    @Override public String name() { return "rff_rowid_scan"; }

    @Override public List<farm.query.vgi.function.ArgSpec> argumentSpecs() { return List.of(); }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(
                "rff_rowid — row_id virtual column + bbox.* required filters")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                .withCategories("generator", "diagnostic", "testing");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        return new State(params.pushdownFilters(),
                new CachedSchema(params.outputSchema()), params.joinKeys());
    }

    public static final class State extends TableProducerState {
        public byte[] filterBytes;
        public CachedSchema projected;
        public List<byte[]> joinKeysIpc;
        public boolean done;

        public State() {}

        State(byte[] filterBytes, CachedSchema projected, List<byte[]> joinKeysIpc) {
            this.filterBytes = filterBytes;
            this.projected = projected;
            this.joinKeysIpc = joinKeysIpc;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            BigIntVector rowId = (BigIntVector) work.getVector("row_id");
            BigIntVector other = (BigIntVector) work.getVector("other");
            StructVector bbox = (StructVector) work.getVector("bbox");
            Float4Vector xmin = bbox.getChild("xmin", Float4Vector.class);
            Float4Vector ymin = bbox.getChild("ymin", Float4Vector.class);
            Float4Vector xmax = bbox.getChild("xmax", Float4Vector.class);
            Float4Vector ymax = bbox.getChild("ymax", Float4Vector.class);
            for (int i = 0; i < ROWS; i++) {
                rowId.setSafe(i, i);
                other.setSafe(i, (long) i * 10);
                xmin.setSafe(i, i);
                ymin.setSafe(i, 2.0f);
                xmax.setSafe(i, 3.0f);
                ymax.setSafe(i, 4.0f);
                bbox.setIndexDefined(i);
            }
            work.setRowCount(ROWS);
            if (filterBytes != null) {
                work = FilterApplier.from(filterBytes, joinKeysIpc).apply(work);
            }
            out.emit(VectorProjector.project(work, projected.get()));
        }
    }
}
