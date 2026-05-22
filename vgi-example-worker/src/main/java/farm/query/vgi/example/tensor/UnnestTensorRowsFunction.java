// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.tensor;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code unnest_tensor_rows(data TABLE) -> TABLE(value, axes)} — table-in-out
 * variant of {@link UnnestTensorFunction}. Emits one row per cell of the
 * Cartesian product. Composes with DuckDB's LATERAL joins.
 */
public final class UnnestTensorRowsFunction implements TableInOutFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("unnest_tensor_rows")
            .description("Invert nest_tensor, streaming one row per cell (LATERAL-friendly)")
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        if (in == null || in.getFields().isEmpty()) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        if (in.getFields().size() != 1) {
            throw new IllegalArgumentException(
                    "unnest_tensor_rows: input table must have exactly one column (the nest_tensor struct)");
        }
        Schema out = TensorCodec.unnestRowsOutput(in.getFields().get(0));
        return BindResponse.forSchema(SchemaUtil.serializeSchema(out));
    }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        return new State(params.outputSchema());
    }

    public static final class State extends TableInOutExchangeState {
        private final Schema outputSchema;
        private final ArrowType valueType;
        private final List<String> axisNames = new ArrayList<>();
        private final List<ArrowType> axisTypes = new ArrayList<>();

        public State(Schema outputSchema) {
            this.outputSchema = outputSchema;
            Field valueField = null, axesField = null;
            for (Field f : outputSchema.getFields()) {
                if ("value".equals(f.getName())) valueField = f;
                else if ("axes".equals(f.getName())) axesField = f;
            }
            this.valueType = valueField == null ? new ArrowType.Null() : valueField.getType();
            if (axesField != null) {
                for (Field f : axesField.getChildren()) {
                    axisNames.add(f.getName());
                    axisTypes.add(f.getType());
                }
            }
        }

        @Override
        public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot root = input.root();
            FieldVector inputVec = root.getFieldVectors().get(0);
            if (!(inputVec instanceof StructVector sv)) { return; }
            int rows = root.getRowCount();

            VectorSchemaRoot outRoot = VectorSchemaRoot.create(outputSchema, Allocators.root());
            outRoot.allocateNew();
            FieldVector valueVec = outRoot.getVector("value");
            StructVector axesVec = (StructVector) outRoot.getVector("axes");
            NullableStructWriter axesWriter = axesVec.getWriter();

            int outRow = 0;
            int dims = axisNames.size();
            int[] sizes = new int[dims];
            int[] idx = new int[dims];
            for (int i = 0; i < rows; i++) {
                if (sv.isNull(i)) continue;
                TensorCodec.TensorRow tr = TensorCodec.readTensorRow(sv, i);
                if (tr == null) continue;
                List<List<Object>> coords = new ArrayList<>(dims);
                boolean empty = false;
                for (int a = 0; a < dims; a++) {
                    List<Object> c = tr.axes.getOrDefault(axisNames.get(a), List.of());
                    coords.add(c);
                    sizes[a] = c.size();
                    if (sizes[a] == 0) { empty = true; break; }
                }
                if (empty) continue;
                java.util.Arrays.fill(idx, 0);
                while (true) {
                    Object cell = TensorCodec.walkTensor(tr.tensor, idx);
                    if (cell == null) valueVec.setNull(outRow);
                    else farm.query.vgi.internal.VectorScalarCodec.write(valueVec, outRow, cell);

                    axesWriter.setPosition(outRow);
                    axesWriter.start();
                    for (int a = 0; a < dims; a++) {
                        UnnestTensorFunction.writeStructScalar(axesWriter, axisNames.get(a),
                                axisTypes.get(a), coords.get(a).get(idx[a]));
                    }
                    axesWriter.end();
                    axesVec.setIndexDefined(outRow);

                    outRow++;
                    int d = dims - 1;
                    while (d >= 0) {
                        idx[d]++;
                        if (idx[d] < sizes[d]) break;
                        idx[d] = 0;
                        d--;
                    }
                    if (d < 0) break;
                }
            }
            outRoot.setRowCount(outRow);
            out.emit(outRoot);
        }
    }
}
