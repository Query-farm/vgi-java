// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * {@code repeat_inputs(repeat_count BIGINT [const], data TABLE) -> *} —
 * duplicates each input batch {@code repeat_count} times.
 */
public final class RepeatInputsFunction implements TableInOutFunction {

    @Override public String name() { return "repeat_inputs"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Duplicates each input batch N times");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("repeat_count", 0, Schemas.INT64, /*isConst=*/true),
                ArgSpec.table("data", 1));
    }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        if (in == null || in.getFields().isEmpty()) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(in));
    }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        return new RepeatState(Math.max(1L, count));
    }

    public static final class RepeatState extends TableInOutExchangeState {
        public long count;

        public RepeatState() {}
        RepeatState(long count) { this.count = count; }

        @Override
        public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            if (count == 1) {
                out.emit(input.root());
                return;
            }
            VectorSchemaRoot src = input.root();
            Schema outputSchema = src.getSchema();
            int srcRows = src.getRowCount();
            int totalRows = (int) (srcRows * count);

            VectorSchemaRoot dst = VectorSchemaRoot.create(outputSchema, Allocators.root());
            dst.allocateNew();
            for (int colIdx = 0; colIdx < dst.getFieldVectors().size(); colIdx++) {
                FieldVector srcVec = src.getVector(colIdx);
                FieldVector dstVec = dst.getVector(colIdx);
                for (long r = 0; r < count; r++) {
                    long base = r * srcRows;
                    for (int i = 0; i < srcRows; i++) {
                        dstVec.copyFromSafe(i, (int) (base + i), srcVec);
                    }
                }
            }
            dst.setRowCount(totalRows);
            out.emit(dst);
        }
    }
}
