// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
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
 * {@code {name: utf8, description: utf8, type: binary, default_value: binary?,
 * required: bool?, secret: bool?}}.
 *
 * <p>{@code required} and then {@code secret} are nullable and appended after
 * the original columns, so a peer that predates them reads the batch by name and
 * simply doesn't see them; absent and explicit-null both mean {@code false}.
 * Both are written explicitly rather than left null so readers see
 * {@code false}. {@link #decode(byte[])} reads every column by name.
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
    private static final ArrowType BOOL = new ArrowType.Bool();

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
                new Field("default_value", new FieldType(true, BINARY, null), null),
                new Field("required", new FieldType(true, BOOL, null), null),
                new Field("secret", new FieldType(true, BOOL, null), null)));

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
            ((BitVector) root.getVector("required")).setSafe(0, spec.required() ? 1 : 0);
            ((BitVector) root.getVector("secret")).setSafe(0, spec.secret() ? 1 : 0);
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    /**
     * A spec as read off the wire. The default stays in its encoded form (a
     * one-row IPC batch with a single {@code value} column), since decoding it
     * needs an allocator the reader would then own.
     *
     * @param name         option name
     * @param description  human-readable description
     * @param valueField   the option's value field (named {@code "value"})
     * @param defaultValue IPC-encoded one-row default batch, or {@code null} for none
     * @param required     the option must be supplied at ATTACH time
     * @param secret       the option carries a credential
     */
    public record Decoded(String name, String description, Field valueField,
                          byte[] defaultValue, boolean required, boolean secret) {}

    /**
     * Decode one option spec's wire bytes. Columns are looked up by name;
     * {@code required} and {@code secret} read as {@code false} when the column
     * is absent (an older peer) or null.
     *
     * @param bytes the one-row IPC stream
     * @return the decoded spec
     */
    public static Decoded decode(byte[] bytes) {
        return BatchUtil.withReadBatch(bytes, Allocators.root(), root -> {
            if (root == null || root.getRowCount() != 1) {
                throw new IllegalArgumentException("attach option spec must be a one-row batch");
            }
            String name = ((VarCharVector) root.getVector("name")).getObject(0).toString();
            String description = ((VarCharVector) root.getVector("description")).getObject(0).toString();
            byte[] typeBytes = ((VarBinaryVector) root.getVector("type")).getObject(0);
            Field valueField = SchemaUtil.deserializeSchema(typeBytes).getFields().get(0);
            VarBinaryVector defaultVec = (VarBinaryVector) root.getVector("default_value");
            byte[] defaultValue = defaultVec == null || defaultVec.isNull(0) ? null : defaultVec.getObject(0);
            return new Decoded(name, description, valueField, defaultValue,
                    readFlag(root, "required"), readFlag(root, "secret"));
        });
    }

    /** A nullable boolean column by name; absent or null reads as {@code false}. */
    private static boolean readFlag(VectorSchemaRoot root, String column) {
        FieldVector v = root.getVector(column);
        if (!(v instanceof BitVector bits) || bits.isNull(0)) return false;
        return bits.get(0) != 0;
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
