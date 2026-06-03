// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;

/**
 * {@code binary_packet(header BLOB [const], payload BLOB, config STRUCT(label,version) [const]) -> BLOB}.
 *
 * <p>Concatenates {@code header || payload || utf8(config.label) || lowByte(config.version)}.
 * Override {@link #argumentSpecs()} because {@code config}'s STRUCT type carries explicit
 * child fields that don't auto-derive from {@code StructVector.class}.
 */
public final class BinaryPacketFunction extends ScalarFn {

    @Override public String name() { return "binary_packet"; }
    @Override public String description() { return "Build binary packets with header, payload, and config"; }

    @Override
    public List<ArgSpec> argumentSpecs() {
        return List.of(
                ArgSpec.positional("header", 0, Schemas.BINARY),
                new ArgSpec("payload", 1, Schemas.BINARY),
                ArgSpec.nested("config", 2, ArrowType.Struct.INSTANCE, List.of(
                        new Field("label", new FieldType(true, Schemas.UTF8, null), null),
                        new Field("version", new FieldType(true, Schemas.INT64, null), null)),
                        false));
    }

    public void compute(
            @Const byte[] header,
            @Vector VarBinaryVector payload,
            @Vector StructVector config,
            VarBinaryVector result) {
        byte[] hdr = header == null ? new byte[0] : header;
        VarCharVector labelV = (VarCharVector) config.getChild("label");
        BigIntVector versionV = (BigIntVector) config.getChild("version");
        int rows = payload.getValueCount();
        for (int i = 0; i < rows; i++) {
            byte[] payloadBytes = payload.isNull(i) ? new byte[0] : payload.get(i);
            byte[] labelBytes = labelV.isNull(i) ? new byte[0] : labelV.get(i);
            long version = versionV.isNull(i) ? 0L : versionV.get(i);
            int suffixLen = labelBytes.length + 1;
            byte[] out = new byte[hdr.length + payloadBytes.length + suffixLen];
            int off = 0;
            System.arraycopy(hdr, 0, out, off, hdr.length); off += hdr.length;
            System.arraycopy(payloadBytes, 0, out, off, payloadBytes.length); off += payloadBytes.length;
            System.arraycopy(labelBytes, 0, out, off, labelBytes.length); off += labelBytes.length;
            out[off] = (byte) (version & 0xFF);
            result.setSafe(i, out);
        }
    }
}
