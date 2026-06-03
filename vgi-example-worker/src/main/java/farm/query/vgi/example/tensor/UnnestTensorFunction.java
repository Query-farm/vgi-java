// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.tensor;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code unnest_tensor(t) -> list<struct<value, axes>>} — invert
 * {@link NestTensorFunction}. Emits one struct per cell of the Cartesian
 * product, including null-valued cells.
 */
public final class UnnestTensorFunction implements ScalarFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("unnest_tensor")
            .description("Invert nest_tensor: list of {value, axes} structs per cell")
            .arg(new ArgSpec("tensor", 0, new ArrowType.Null(), "", false, false, "",
                    List.of(), false, true))
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(ScalarBindParams p) {
        Schema input = p.inputSchema();
        if (input == null || input.getFields().isEmpty()) {
            // Catalog enumeration with no concrete arg — return the canonical
            // ANY-typed placeholder. The vgi_type=any metadata on the field
            // is what DuckDB's catalog-enumeration path reads to render the
            // return type as ANY in duckdb_functions(); a bare ArrowType.Null
            // without that metadata gets shown as NULL instead.
            return BindResponse.forSchema(Schemas.singleResultAnyIpc());
        }
        Field inputField = input.getFields().get(0);
        Schema out = TensorCodec.unnestScalarOutput(inputField);
        return BindResponse.forSchema(SchemaUtil.serializeSchema(out));
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input,
                                       BufferAllocator alloc) {
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        ListVector resultVec = (ListVector) out.getVector("result");
        FieldVector inputVec = input.getFieldVectors().get(0);
        if (!(inputVec instanceof StructVector sv)) {
            for (int i = 0; i < rows; i++) resultVec.setNull(i);
            out.setRowCount(rows);
            return out;
        }
        Field outRow = resultVec.getField().getChildren().get(0);
        Field axesOutField = null, valueField = null;
        for (Field f : outRow.getChildren()) {
            if ("value".equals(f.getName())) valueField = f;
            else if ("axes".equals(f.getName())) axesOutField = f;
        }
        if (valueField == null || axesOutField == null) {
            for (int i = 0; i < rows; i++) resultVec.setNull(i);
            out.setRowCount(rows);
            return out;
        }
        ArrowType valueType = valueField.getType();
        List<String> axisNames = new ArrayList<>();
        List<ArrowType> axisTypes = new ArrayList<>();
        for (Field f : axesOutField.getChildren()) {
            axisNames.add(f.getName());
            axisTypes.add(f.getType());
        }

        org.apache.arrow.vector.complex.impl.UnionListWriter lw = resultVec.getWriter();
        for (int i = 0; i < rows; i++) {
            lw.setPosition(i);
            if (sv.isNull(i)) { resultVec.setNull(i); continue; }
            TensorCodec.TensorRow tr = TensorCodec.readTensorRow(sv, i);
            if (tr == null) { resultVec.setNull(i); continue; }
            writeCells(lw, tr, axisNames, axisTypes, valueType);
        }
        out.setRowCount(rows);
        return out;
    }

    static void writeCells(BaseWriter.ListWriter lw, TensorCodec.TensorRow tr,
                              List<String> axisNames, List<ArrowType> axisTypes, ArrowType valueType) {
        lw.startList();
        int dims = axisNames.size();
        int[] sizes = new int[dims];
        List<List<Object>> coords = new ArrayList<>(dims);
        for (int a = 0; a < dims; a++) {
            List<Object> c = tr.axes.getOrDefault(axisNames.get(a), List.of());
            coords.add(c);
            sizes[a] = c.size();
            if (sizes[a] == 0) { lw.endList(); return; }
        }
        int[] idx = new int[dims];
        while (true) {
            BaseWriter.StructWriter sw = lw.struct();
            sw.start();
            Object cell = TensorCodec.walkTensor(tr.tensor, idx);
            writeStructScalar(sw, "value", valueType, cell);
            BaseWriter.StructWriter axesW = sw.struct("axes");
            axesW.start();
            for (int a = 0; a < dims; a++) {
                writeStructScalar(axesW, axisNames.get(a), axisTypes.get(a), coords.get(a).get(idx[a]));
            }
            axesW.end();
            sw.end();
            // Advance idx (rightmost-fastest).
            int d = dims - 1;
            while (d >= 0) {
                idx[d]++;
                if (idx[d] < sizes[d]) break;
                idx[d] = 0;
                d--;
            }
            if (d < 0) break;
        }
        lw.endList();
    }

    static void writeStructScalar(BaseWriter.StructWriter sw, String name, ArrowType type, Object value) {
        if (value == null) {
            // For nullable struct fields, we still need to advance the writer.
            // Use type-specific writeNull on the field writer.
            switch (type) {
                case ArrowType.Int it -> {
                    if (it.getBitWidth() == 64) sw.bigInt(name).writeNull();
                    else if (it.getBitWidth() == 32) sw.integer(name).writeNull();
                    else if (it.getBitWidth() == 16) sw.smallInt(name).writeNull();
                    else sw.tinyInt(name).writeNull();
                }
                case ArrowType.FloatingPoint fp -> {
                    if (fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)
                        sw.float8(name).writeNull();
                    else sw.float4(name).writeNull();
                }
                case ArrowType.Bool b -> sw.bit(name).writeNull();
                case ArrowType.Utf8 u -> sw.varChar(name).writeNull();
                case ArrowType.Date d -> sw.dateDay(name).writeNull();
                default -> sw.bigInt(name).writeNull();
            }
            return;
        }
        switch (type) {
            case ArrowType.Int it -> {
                long n = ((Number) value).longValue();
                if (it.getBitWidth() == 64) sw.bigInt(name).writeBigInt(n);
                else if (it.getBitWidth() == 32) sw.integer(name).writeInt((int) n);
                else if (it.getBitWidth() == 16) sw.smallInt(name).writeSmallInt((short) n);
                else sw.tinyInt(name).writeTinyInt((byte) n);
            }
            case ArrowType.FloatingPoint fp -> {
                double d = ((Number) value).doubleValue();
                if (fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)
                    sw.float8(name).writeFloat8(d);
                else sw.float4(name).writeFloat4((float) d);
            }
            case ArrowType.Bool b -> sw.bit(name).writeBit(((Boolean) value) ? 1 : 0);
            case ArrowType.Utf8 u -> {
                byte[] bytes = ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                try (org.apache.arrow.memory.ArrowBuf tmp =
                        farm.query.vgirpc.wire.Allocators.root().buffer(bytes.length)) {
                    tmp.setBytes(0, bytes);
                    sw.varChar(name).writeVarChar(0, bytes.length, tmp);
                }
            }
            case ArrowType.Date d -> sw.dateDay(name).writeDateDay(((Number) value).intValue());
            default -> {
                if (value instanceof Number n) sw.bigInt(name).writeBigInt(n.longValue());
                else if (value instanceof String s) {
                    byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    try (org.apache.arrow.memory.ArrowBuf tmp =
                            farm.query.vgirpc.wire.Allocators.root().buffer(bytes.length)) {
                        tmp.setBytes(0, bytes);
                        sw.varChar(name).writeVarChar(0, bytes.length, tmp);
                    }
                } else throw new IllegalArgumentException("UnnestTensor: unsupported struct field type " + type);
            }
        }
    }
}
