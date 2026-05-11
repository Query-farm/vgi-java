// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;

/**
 * Serialises an {@link AttachOptionSpec} to the wire format:
 * one-row IPC stream with schema
 * {@code {name: utf8, description: utf8, type: binary, default_value: binary?}}.
 *
 * <p>{@code type} is an IPC-encoded schema with a single field "value" of the
 * spec's type (children included). {@code default_value} is an IPC-encoded
 * one-row record batch with that same schema, populated via
 * {@link AttachOptionValueCodec}.
 */
public final class AttachOptionSpecSerializer {

    private AttachOptionSpecSerializer() {}

    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType BINARY = new ArrowType.Binary();

    public static byte[] serialize(AttachOptionSpec spec) {
        BufferAllocator alloc = Allocators.root();
        Schema specSchema = new Schema(List.of(
                new Field("name", new FieldType(false, UTF8, null), null),
                new Field("description", new FieldType(false, UTF8, null), null),
                new Field("type", new FieldType(false, BINARY, null), null),
                new Field("default_value", new FieldType(true, BINARY, null), null)));

        Field valueField = new Field("value",
                new FieldType(true, spec.type(), null),
                spec.children() == null ? List.of() : spec.children());
        Schema typeSchema = new Schema(List.of(valueField));
        byte[] typeBytes = SchemaUtil.serializeSchema(typeSchema);
        byte[] defaultBytes = spec.defaultValue() == null
                ? null
                : encodeDefault(typeSchema, spec.defaultValue(), alloc);

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

    /** Build a single-row IPC batch matching {@code typeSchema} with the
     *  default value written into the "value" column at row 0. */
    public static byte[] encodeDefault(Schema typeSchema, Object value, BufferAllocator alloc) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(typeSchema, alloc)) {
            root.allocateNew();
            FieldVector v = root.getVector("value");
            AttachOptionValueCodec.writeValue(v, 0, value);
            root.setRowCount(1);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("AttachOptionSpec default encode failed", e);
        }
    }
}
