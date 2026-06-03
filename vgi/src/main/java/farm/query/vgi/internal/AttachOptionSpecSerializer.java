// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

import java.util.List;

/**
 * Serialises an {@link AttachOptionSpec} to the wire format:
 * one-row IPC stream with schema
 * {@code {name: utf8, description: utf8, type: binary, default_value: binary?}}.
 *
 * <p>{@code type} is an IPC-encoded schema with a single field "value" of the
 * spec's type (children included). {@code default_value} is an IPC-encoded
 * one-row record batch with that schema, populated by copying the spec's
 * pre-materialised default vector via {@link TransferPair}.
 */
public final class AttachOptionSpecSerializer {

    private AttachOptionSpecSerializer() {}

    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType BINARY = new ArrowType.Binary();

    /**
     * Serialise one option spec to its one-row IPC wire bytes.
     *
     * @param spec the spec to serialise
     * @return the IPC stream bytes
     */
    public static byte[] serialize(AttachOptionSpec spec) {
        BufferAllocator alloc = Allocators.root();
        Schema specSchema = new Schema(List.of(
                new Field("name", new FieldType(false, UTF8, null), null),
                new Field("description", new FieldType(false, UTF8, null), null),
                new Field("type", new FieldType(false, BINARY, null), null),
                new Field("default_value", new FieldType(true, BINARY, null), null)));

        Schema typeSchema = new Schema(List.of(spec.valueField()));
        byte[] typeBytes = SchemaUtil.serializeSchema(typeSchema);
        byte[] defaultBytes = spec.defaultVector() == null
                ? null
                : encodeDefaultBatch(spec.defaultVector(), typeSchema, alloc);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(specSchema, alloc)) {
            root.allocateNew();
            ((VarCharVector) root.getVector("name")).setSafe(0, new Text(spec.name()));
            ((VarCharVector) root.getVector("description")).setSafe(0, new Text(spec.description()));
            ((VarBinaryVector) root.getVector("type")).setSafe(0, typeBytes);
            VarBinaryVector defaultVec = (VarBinaryVector) root.getVector("default_value");
            if (defaultBytes == null) defaultVec.setNull(0);
            else defaultVec.setSafe(0, defaultBytes);
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    /** Copy the default vector into a fresh one-row VSR (schema {value: type})
     *  and IPC-encode it. */
    private static byte[] encodeDefaultBatch(FieldVector defaultVec, Schema typeSchema,
                                              BufferAllocator alloc) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(typeSchema, alloc)) {
            root.allocateNew();
            FieldVector target = root.getVector("value");
            TransferPair tp = defaultVec.makeTransferPair(target);
            tp.copyValueSafe(0, 0);
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }
}
