// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * {@code sum_all_columns(data TABLE)} — column-wise sums across all input
 * batches, emitting one summary row at finalize. Numeric input columns form
 * the output (integers widen to int64; floats and decimals to float64);
 * non-numeric columns are dropped. Raises at bind if no numeric column
 * remains. Mirrors vgi-python {@code SumAllColumnsFunction} (a buffering
 * Sink+Source function).
 */
public class SumAllColumnsBufferingFunction extends AbstractBufferAndDrain {

    private static final byte[] NS_RAW = "raw".getBytes(StandardCharsets.UTF_8);

    private static final FunctionSpec SPEC = FunctionSpec.builder("sum_all_columns")
            .metadata(FunctionMetadata.describe("Computes column-wise sums across all batches")
                    .withCategories("aggregation", "numeric"))
            .table("data")
            .named("logging", Schemas.BOOL, "false")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        if (in == null) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        List<Field> outFields = new ArrayList<>();
        for (Field f : in.getFields()) {
            ArrowType t = f.getType();
            if (t instanceof ArrowType.Int) {
                outFields.add(new Field(f.getName(), new FieldType(true, Schemas.INT64, null), null));
            } else if (t instanceof ArrowType.FloatingPoint || t instanceof ArrowType.Decimal) {
                outFields.add(new Field(f.getName(), new FieldType(true, Schemas.FLOAT64, null), null));
            }
        }
        if (outFields.isEmpty()) {
            StringJoiner sj = new StringJoiner(", ");
            for (Field f : in.getFields()) sj.add(f.getName() + ": " + f.getType());
            throw new IllegalArgumentException(
                    "sum_all_columns requires at least one numeric (integer, floating-point, "
                    + "or decimal) input column, got [" + sj + "]");
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(outFields)));
    }

    @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        if (params.ctx() != null) {
            params.ctx().clientLog(farm.query.vgirpc.log.Level.INFO,
                    "Processing batch with " + batch.getRowCount() + " rows");
        }
        params.storage().stateAppend(NS_RAW, KEY, BatchUtil.writeSingleBatch(batch));
        return params.executionId();
    }

    @Override public List<byte[]> combine(List<byte[]> stateIds,
            farm.query.vgi.buffering.TableBufferingCombineParams params) {
        if (params.ctx() != null) {
            params.ctx().clientLog(farm.query.vgirpc.log.Level.INFO,
                    "Combining " + stateIds.size() + " state_ids");
        }
        return super.combine(stateIds, params);
    }

    @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        return new SumProducer(params);
    }

    /** Sums all buffered batches per the (numeric) output schema, emits one row. */
    private static final class SumProducer extends BufferingFinalizeProducer {
        private boolean emitted = false;

        private SumProducer() {}

        SumProducer(TableBufferingFinalizeParams params) { super(params); }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            VectorSchemaRoot result = VectorSchemaRoot.create(outputSchema, Allocators.root());
            result.allocateNew();
            // Seed every numeric column to 0 so empty storage (e.g. a function
            // that buffers nothing) finalizes to a clean zero-sum row rather
            // than NULLs — mirrors vgi-python passing the schema explicitly.
            for (Field f : outputSchema.getFields()) {
                FieldVector dst = result.getVector(f.getName());
                if (dst instanceof BigIntVector bi) bi.setSafe(0, 0L);
                else if (dst instanceof Float8Vector fl) fl.setSafe(0, 0.0);
            }
            for (FunctionStorage.LogEntry e : storage().stateLogScan(NS_RAW, KEY, -1, Integer.MAX_VALUE)) {
                try (VectorSchemaRoot src = BatchUtil.readSingleBatch(e.value(), Allocators.root())) {
                    int rows = src.getRowCount();
                    for (Field f : outputSchema.getFields()) {
                        FieldVector col = src.getVector(f.getName());
                        if (col == null) continue;
                        FieldVector dst = result.getVector(f.getName());
                        if (dst instanceof BigIntVector bi) {
                            long s = bi.isNull(0) ? 0 : bi.get(0);
                            for (int i = 0; i < rows; i++) if (!col.isNull(i)) s += ScalarHelpers.toLong(col, i);
                            bi.setSafe(0, s);
                        } else if (dst instanceof Float8Vector fl) {
                            double s = fl.isNull(0) ? 0 : fl.get(0);
                            for (int i = 0; i < rows; i++) if (!col.isNull(i)) s += ScalarHelpers.toDouble(col, i);
                            fl.setSafe(0, s);
                        }
                    }
                }
            }
            result.setRowCount(1);
            out.emit(result);
            emitted = true;
        }
    }
}
