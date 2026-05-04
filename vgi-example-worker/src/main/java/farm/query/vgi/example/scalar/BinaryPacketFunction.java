// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * {@code binary_packet(header BLOB [const], payload BLOB, config STRUCT(label,version) [const]) -> BLOB}.
 *
 * <p>Concatenates {@code header || payload || utf8(config.label) || lowByte(config.version)}.
 */
public final class BinaryPacketFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.BINARY);

    @Override public String name() { return "binary_packet"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Build binary packets with header, payload, and config");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        Field labelField = new Field("label",
                new FieldType(true, Schemas.UTF8, null), null);
        Field versionField = new Field("version",
                new FieldType(true, Schemas.INT64, null), null);
        return List.of(
                new ArgSpec("header", 0, Schemas.BINARY, /*isConst=*/true),
                new ArgSpec("payload", 1, Schemas.BINARY),
                ArgSpec.nested("config", 2,
                        ArrowType.Struct.INSTANCE,
                        List.of(labelField, versionField),
                        false));
    }

    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        // header (const) comes via args; payload + config arrive as input batch columns.
        byte[] header = (byte[]) params.arguments().positionalAt(0);
        if (header == null) header = new byte[0];

        VarBinaryVector payload = (VarBinaryVector) input.getFieldVectors().get(0);
        StructVector config = (StructVector) input.getFieldVectors().get(1);
        VarCharVector labelV = (VarCharVector) config.getChild("label");
        BigIntVector versionV = (BigIntVector) config.getChild("version");

        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        VarBinaryVector v = (VarBinaryVector) out.getVector("result");
        int rows = input.getRowCount();
        for (int i = 0; i < rows; i++) {
            byte[] payloadBytes = payload.isNull(i) ? new byte[0] : payload.get(i);
            byte[] labelBytes = labelV.isNull(i) ? new byte[0] : labelV.get(i);
            long version = versionV.isNull(i) ? 0L : versionV.get(i);
            int suffixLen = labelBytes.length + 1;
            byte[] result = new byte[header.length + payloadBytes.length + suffixLen];
            int off = 0;
            System.arraycopy(header, 0, result, off, header.length); off += header.length;
            System.arraycopy(payloadBytes, 0, result, off, payloadBytes.length); off += payloadBytes.length;
            System.arraycopy(labelBytes, 0, result, off, labelBytes.length); off += labelBytes.length;
            result[off] = (byte) (version & 0xFF);
            v.setSafe(i, result);
        }
        v.setValueCount(rows);
        out.setRowCount(rows);
        return out;
    }
}
