// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import farm.query.vgi.SettingSpec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Map;

/**
 * Serialises a {@link SettingSpec} to the wire format DuckDB expects: a 1-row
 * IPC stream with schema
 * {@code {name: utf8, description: utf8, type: binary, default_value: binary?}}.
 *
 * <p>The {@code type} blob is itself an IPC-encoded schema describing a single
 * field named {@code value} with the setting's Arrow type. The
 * {@code default_value}, if present, is an IPC-encoded 1-row record batch with
 * that same schema.
 */
public final class SettingSpecSerializer {

    private SettingSpecSerializer() {}

    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType BINARY = new ArrowType.Binary();

    public static byte[] serialize(SettingSpec spec) {
        BufferAllocator alloc = Allocators.root();
        Schema schema = new Schema(List.of(
                new Field("name", new FieldType(false, UTF8, null), null),
                new Field("description", new FieldType(false, UTF8, null), null),
                new Field("type", new FieldType(false, BINARY, null), null),
                new Field("default_value", new FieldType(true, BINARY, null), null)));

        Schema typeSchema = new Schema(List.of(
                new Field("value", new FieldType(true, spec.type(), null),
                        spec.children() == null ? List.of() : spec.children())));
        byte[] typeBytes = SchemaUtil.serializeSchema(typeSchema);
        byte[] defaultBytes = spec.defaultValue() == null
                ? null
                : encodeDefaultValue(typeSchema, spec.type(), spec.defaultValue(), alloc);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
            root.allocateNew();
            ((VarCharVector) root.getVector("name")).setSafe(0, new Text(spec.name()));
            ((VarCharVector) root.getVector("description")).setSafe(0, new Text(spec.description()));
            ((VarBinaryVector) root.getVector("type")).setSafe(0, typeBytes);
            VarBinaryVector defaultVec = (VarBinaryVector) root.getVector("default_value");
            if (defaultBytes == null) defaultVec.setNull(0);
            else defaultVec.setSafe(0, defaultBytes);
            root.setRowCount(1);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("SettingSpec serialize failed", e);
        }
    }

    private static byte[] encodeDefaultValue(Schema typeSchema, ArrowType t, Object value, BufferAllocator alloc) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(typeSchema, alloc)) {
            root.allocateNew();
            FieldVector v = root.getVector("value");
            writeScalar(v, t, value);
            root.setRowCount(1);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("SettingSpec default serialize failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void writeScalar(FieldVector v, ArrowType t, Object value) {
        if (v instanceof VarCharVector vc) {
            vc.setSafe(0, new Text((String) value));
        } else if (v instanceof BigIntVector bi) {
            bi.setSafe(0, ((Number) value).longValue());
        } else if (v instanceof Float8Vector f) {
            f.setSafe(0, ((Number) value).doubleValue());
        } else if (v instanceof BitVector b) {
            b.setSafe(0, ((Boolean) value) ? 1 : 0);
        } else if (v instanceof StructVector s) {
            // Default values for struct settings: write as nested map. Phase 3 only
            // needs primitive defaults; struct defaults arrive when the struct-typed
            // 'config' setting actually grows a default.
            NullableStructWriter w = s.getWriter();
            w.start();
            if (value instanceof Map<?, ?> m) {
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    String name = (String) e.getKey();
                    Object child = e.getValue();
                    if (child instanceof String str) w.varChar(name).writeVarChar(str);
                    else if (child instanceof Number n) w.bigInt(name).writeBigInt(n.longValue());
                    else if (child instanceof Double d) w.float8(name).writeFloat8(d);
                    else if (child instanceof Boolean bv) w.bit(name).writeBit(bv ? 1 : 0);
                }
            }
            w.end();
        } else {
            throw new IllegalArgumentException("setting default unsupported for vector " + v.getClass().getSimpleName());
        }
    }
}
