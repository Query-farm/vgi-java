// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.VectorProjector;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.CachedSchema;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * {@code spatial_filter_example(count [, batch_size])} — generates {@code count}
 * points on a deterministic grid in {@code [0,1) x [0,1)} with a GEOMETRY column,
 * so spatial bounding-box filter counts are predictable. Backs
 * {@code table/expression_filter.test}.
 *
 * <p>Point {@code i} sits at {@code x = (i % cols) / cols}, {@code y = (i // cols) / cols}
 * where {@code cols = ceil(sqrt(count))}. Declares {@code &&} and
 * {@code st_intersects_extent} as supported expression filters; the engine pushes
 * those predicates into the function (removing the FILTER node), and the worker
 * applies them via {@link ExpressionFilterEvaluator}.
 */
public final class SpatialFilterExampleFunction extends CountdownTableFunction {

    private static final Field GEOM = new Field("geom",
            new FieldType(true, new ArrowType.Binary(), null,
                    Map.of("ARROW:extension:name", "geoarrow.wkb", "ARROW:extension:metadata", "{}")),
            null);

    private static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("x", Schemas.FLOAT64),
            Schemas.nullable("y", Schemas.FLOAT64),
            GEOM);

    @Override public String name() { return "spatial_filter_example"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates points on a grid with geometry for spatial filter testing")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                .withSupportedExpressionFilters("&&", "st_intersects_extent")
                .withCategories("generator", "spatial", "testing");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override protected long defaultBatchSize() { return 1024L; }

    @Override public java.util.List<farm.query.vgi.catalog.ColumnStatistics> statistics(
            farm.query.vgi.table.TableBindParams params) {
        return null; // multi-column geometry schema — no canned countdown stats
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().orElse(1024L);
        return new State(new BatchState(count, batchSize), count, params.pushdownFilters(),
                params.joinKeys(), new CachedSchema(params.outputSchema()));
    }

    /** Encode a 2D point as little-endian WKB (byte_order=1, type=1=Point, x, y). */
    static byte[] wkbPoint(double x, double y) {
        ByteBuffer b = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        b.put((byte) 1);
        b.putInt(1);
        b.putDouble(x);
        b.putDouble(y);
        return b.array();
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public long totalCount;
        public byte[] filterBytes;
        public List<byte[]> joinKeysIpc;
        public CachedSchema outputSchema;

        public State() {}

        State(BatchState batch, long totalCount, byte[] filterBytes, List<byte[]> joinKeysIpc,
                CachedSchema outputSchema) {
            this.batch = batch;
            this.totalCount = totalCount;
            this.filterBytes = filterBytes;
            this.joinKeysIpc = joinKeysIpc;
            this.outputSchema = outputSchema;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();
            int cols = Math.max(1, (int) Math.ceil(Math.sqrt((double) totalCount)));

            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            BigIntVector nv = (BigIntVector) work.getVector("n");
            Float8Vector xv = (Float8Vector) work.getVector("x");
            Float8Vector yv = (Float8Vector) work.getVector("y");
            VarBinaryVector gv = (VarBinaryVector) work.getVector("geom");
            for (int i = 0; i < n; i++) {
                long row = start + i;
                double x = (row % cols) / (double) cols;
                double y = (row / cols) / (double) cols;
                nv.setSafe(i, row);
                xv.setSafe(i, x);
                yv.setSafe(i, y);
                gv.setSafe(i, wkbPoint(x, y));
            }
            work.setRowCount(n);

            if (filterBytes != null) {
                FilterApplier fa = FilterApplier.from(filterBytes, joinKeysIpc);
                work = fa.apply(work);                                            // column filters (n < 50)
                work = ExpressionFilterEvaluator.apply(work, fa.expressionPredicates()); // spatial && etc.
            }
            out.emit(VectorProjector.project(work, outputSchema.get()));
            batch.advance(n);
        }
    }
}
